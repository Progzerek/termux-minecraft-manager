use std::{
    collections::HashMap,
    env, fmt, fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Local, NaiveTime, TimeDelta, TimeZone};
use tokio::{
    sync::{Mutex, mpsc::UnboundedSender},
    time::sleep,
};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

use crate::minecraft::{ServerEvent, ServerManager, ServerState};

const DEFAULT_BACKUP_TIME: &str = "05:00";
const BACKUP_RETENTION: usize = 14;
const WARNING_LEAD_MINUTES: i64 = 5;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(2);
const COPY_BUFFER_SIZE: usize = 64 * 1024;

/// Compression level passed to the system `zip` binary.
const DEFAULT_ZIP_LEVEL: u8 = 1;

/// Files that are already compressed; the archiver stores them instead of deflating.
const STORED_SUFFIXES: [&str; 4] = [".mca", ".dat", ".ldb", ".sst"];

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub server_dir: PathBuf,
    pub backup_dir: PathBuf,
    pub time: NaiveTime,
}

#[derive(Debug)]
pub struct BackupOutcome {
    pub archive: PathBuf,
    pub bytes: u64,
    pub restarted: bool,
    pub removed: usize,
}

/// A progress update emitted while a backup is running.
#[derive(Debug, Clone)]
pub enum BackupProgress {
    Phase(&'static str),
    Archive {
        files_done: usize,
        files_total: usize,
        bytes_done: u64,
        bytes_total: u64,
        current: String,
    },
}

impl BackupProgress {
    pub fn message(&self) -> String {
        match self {
            BackupProgress::Phase(phase) => format!("**Backup:** {phase}…"),
            BackupProgress::Archive {
                files_done,
                files_total,
                bytes_done,
                bytes_total,
                current,
            } => {
                let percent = if *bytes_total == 0 {
                    100
                } else {
                    (bytes_done * 100 / bytes_total).min(100)
                };

                let mut message = format!(
                    "**Backing up world… {percent}%**\n{} / {} · {files_done} / {files_total} files",
                    human_bytes(*bytes_done),
                    human_bytes(*bytes_total),
                );

                if !current.is_empty() {
                    message.push_str(&format!("\n`{}`", short_name(current)));
                }

                message
            }
        }
    }
}

/// Receives progress updates from the blocking archive task.
pub type ProgressSink = Arc<dyn Fn(BackupProgress) + Send + Sync>;

fn emit_progress(sink: Option<&ProgressSink>, progress: BackupProgress) {
    if let Some(sink) = sink {
        sink(progress);
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    format!("{value:.1} {}", UNITS[unit])
}

fn short_name(path: &str) -> String {
    const MAX_CHARS: usize = 40;

    let path = path.replace('\\', "/");

    if path.chars().count() <= MAX_CHARS {
        return path;
    }

    let tail: String = path
        .chars()
        .rev()
        .take(MAX_CHARS - 1)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    format!("…{tail}")
}

#[derive(Debug)]
pub enum BackupError {
    Busy(&'static str),
    PlayersOnline(Vec<String>),
    MissingWorld(PathBuf),
    InvalidDestination(String),
    Server(String),
    Io(std::io::Error),
    Zip(String),
    Task(String),
}

impl fmt::Display for BackupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackupError::Busy(state) => write!(f, "Server is busy ({state})"),
            BackupError::PlayersOnline(players) => {
                write!(f, "players are online: {}", players.join(", "))
            }
            BackupError::MissingWorld(path) => {
                write!(f, "World folder is missing: {}", path.display())
            }
            BackupError::InvalidDestination(reason) => {
                write!(f, "Invalid backup destination: {reason}")
            }
            BackupError::Server(reason) => write!(f, "Server error: {reason}"),
            BackupError::Io(error) => write!(f, "I/O error: {error}"),
            BackupError::Zip(reason) => write!(f, "Zip error: {reason}"),
            BackupError::Task(reason) => write!(f, "Background task failed: {reason}"),
        }
    }
}

impl std::error::Error for BackupError {}

impl From<std::io::Error> for BackupError {
    fn from(error: std::io::Error) -> Self {
        BackupError::Io(error)
    }
}

struct ArchiveFile {
    path: PathBuf,
    bytes: u64,
    removed: usize,
}

pub fn load_config(server_dir: &Path) -> Result<Option<BackupConfig>, BackupError> {
    let Ok(directory) = env::var("BACKUP_DIR") else {
        return Ok(None);
    };

    let directory = directory.trim();

    if directory.is_empty() {
        return Ok(None);
    }

    let backup_dir = PathBuf::from(directory);
    let world_dir = server_dir.join("world");
    let world_abs = std::path::absolute(&world_dir).unwrap_or_else(|_| world_dir.clone());
    let backup_abs = std::path::absolute(&backup_dir).unwrap_or_else(|_| backup_dir.clone());

    if backup_abs.starts_with(&world_abs) {
        return Err(BackupError::InvalidDestination(
            "BACKUP_DIR cannot be inside the world folder".to_string(),
        ));
    }

    let time = match env::var("BACKUP_TIME") {
        Ok(value) => match NaiveTime::parse_from_str(value.trim(), "%H:%M") {
            Ok(time) => time,
            Err(_) => {
                eprintln!(
                    "Invalid BACKUP_TIME '{}'; using {DEFAULT_BACKUP_TIME}",
                    value.trim()
                );
                default_backup_time()
            }
        },
        Err(_) => default_backup_time(),
    };

    Ok(Some(BackupConfig {
        server_dir: server_dir.to_path_buf(),
        backup_dir,
        time,
    }))
}

fn default_backup_time() -> NaiveTime {
    NaiveTime::parse_from_str(DEFAULT_BACKUP_TIME, "%H:%M")
        .expect("default backup time must be valid")
}

pub async fn start_loop(server: Arc<Mutex<ServerManager>>, event_tx: UnboundedSender<ServerEvent>) {
    let server_dir = match env::var("RUN_SH_PATH") {
        Ok(path) => PathBuf::from(path.trim()),
        Err(_) => {
            eprintln!("RUN_SH_PATH is not set; backups disabled");
            return;
        }
    };

    let config = match load_config(&server_dir) {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("BACKUP_DIR not set; daily backups disabled");
            return;
        }
        Err(error) => {
            eprintln!("Daily backups disabled: {error}");
            return;
        }
    };

    println!(
        "Daily backups enabled at {} in {}",
        config.time.format("%H:%M"),
        config.backup_dir.display()
    );

    loop {
        let run_at = next_run(config.time);

        let warn_five = run_at - TimeDelta::minutes(WARNING_LEAD_MINUTES);
        let warn_one = run_at - TimeDelta::minutes(1);
        let now = Local::now();

        if warn_five > now {
            sleep(duration_until(warn_five)).await;
            announce(&server, "Server will stop for a backup in 5 minutes").await;
        }

        if warn_one > Local::now() {
            sleep(duration_until(warn_one)).await;
        }

        announce(&server, "Server will stop for a backup in 1 minute").await;
        sleep(duration_until(run_at)).await;

        let progress_sink: Option<ProgressSink> = {
            let event_tx = event_tx.clone();
            let sink: ProgressSink = Arc::new(move |progress: BackupProgress| {
                let _ = event_tx.send(ServerEvent::BackupProgress {
                    message: progress.message(),
                });
            });
            Some(sink)
        };

        match perform_backup(&server, &config, false, progress_sink).await {
            Ok(outcome) => {
                let message = outcome_message(&outcome);
                println!("{message}");
                let _ = event_tx.send(ServerEvent::BackupFinished { message });
            }
            Err(error) => {
                let message = format!("Backup failed: {error}");
                eprintln!("{message}");
                let _ = event_tx.send(ServerEvent::BackupFinished { message });
            }
        }
    }
}

pub async fn perform_backup(
    server: &Arc<Mutex<ServerManager>>,
    config: &BackupConfig,
    require_empty: bool,
    progress: Option<ProgressSink>,
) -> Result<BackupOutcome, BackupError> {
    let world_dir = config.server_dir.join("world");

    if !world_dir.is_dir() {
        return Err(BackupError::MissingWorld(world_dir));
    }

    let was_online = {
        let mut manager = server.lock().await;

        let was_online = match manager.state() {
            ServerState::Online => {
                if require_empty {
                    let players = manager.get_players().await.map_err(|error| {
                        BackupError::Server(format!("couldn't verify online players: {error}"))
                    })?;

                    if !players.is_empty() {
                        return Err(BackupError::PlayersOnline(players));
                    }
                }

                emit_progress(progress.as_ref(), BackupProgress::Phase("stopping server"));

                manager
                    .stop()
                    .await
                    .map_err(|error| BackupError::Server(error.to_string()))?;

                true
            }
            ServerState::Offline => false,
            other => return Err(BackupError::Busy(other.as_text())),
        };

        manager
            .begin_backup()
            .map_err(|error| BackupError::Server(error.to_string()))?;

        was_online
    };

    emit_progress(progress.as_ref(), BackupProgress::Phase("scanning world"));

    let archive_result =
        create_archive(world_dir, config.backup_dir.clone(), progress.clone()).await;

    let restart_result = {
        let mut manager = server.lock().await;
        manager.finish_backup();

        if was_online {
            emit_progress(progress.as_ref(), BackupProgress::Phase("starting server"));

            manager
                .start(&config.server_dir.to_string_lossy(), Arc::clone(server))
                .await
                .err()
                .map(|error| error.to_string())
        } else {
            None
        }
    };

    let archive = archive_result?;

    if let Some(reason) = restart_result {
        return Err(BackupError::Server(format!(
            "backup completed but the server failed to restart: {reason}"
        )));
    }

    Ok(BackupOutcome {
        archive: archive.path,
        bytes: archive.bytes,
        restarted: was_online,
        removed: archive.removed,
    })
}

impl ArchiveFile {
    fn with_removed(mut self, removed: usize) -> ArchiveFile {
        self.removed = removed;
        self
    }
}

async fn create_archive(
    world_dir: PathBuf,
    backup_dir: PathBuf,
    progress: Option<ProgressSink>,
) -> Result<ArchiveFile, BackupError> {
    let archive = tokio::task::spawn_blocking(move || {
        let on_progress = move |event: BackupProgress| {
            if let Some(sink) = &progress {
                sink(event);
            }
        };
        let archive = write_archive(&world_dir, &backup_dir, &on_progress)?;
        let removed = match prune_old_backups(&backup_dir, BACKUP_RETENTION) {
            Ok(removed) => removed,
            Err(error) => {
                eprintln!("Failed to prune old backups: {error}");
                0
            }
        };

        Ok::<_, BackupError>(archive.with_removed(removed))
    })
    .await
    .map_err(|error| BackupError::Task(error.to_string()))??;

    Ok(archive)
}

fn write_archive(
    world_dir: &Path,
    backup_dir: &Path,
    on_progress: &dyn Fn(BackupProgress),
) -> Result<ArchiveFile, BackupError> {
    fs::create_dir_all(backup_dir)?;

    let (files_total, bytes_total, sizes) = measure_directory(world_dir)?;
    let mut reporter = ProgressReporter {
        on_progress,
        files_total,
        bytes_total,
        files_done: 0,
        bytes_done: 0,
        last_emit: Instant::now(),
    };

    reporter.emit(true, "");

    let stamp = Local::now().format("%Y%m%d-%H%M%S");
    let final_path = backup_dir.join(format!("world-{stamp}.zip"));
    let temp_path = backup_dir.join(format!("world-{stamp}.zip.tmp"));

    let result = archive_to(world_dir, &temp_path, &sizes, &mut reporter).and_then(|()| {
        on_progress(BackupProgress::Phase("flushing to disk"));
        finalize_archive(&temp_path, &final_path)
    });

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
        let _ = fs::remove_file(appended_zip_path(&temp_path));
    }

    result
}

/// Builds the archive with the system `zip` binary, falling back to the built-in writer if
/// Info-ZIP is not installed. The external binary is far faster: it is compiled for the device,
/// runs at level 1, and stores already-compressed files instead of deflating them.
fn archive_to(
    world_dir: &Path,
    archive_path: &Path,
    sizes: &HashMap<String, u64>,
    reporter: &mut ProgressReporter<'_>,
) -> Result<(), BackupError> {
    match run_external_zip(world_dir, archive_path, sizes, reporter) {
        Ok(()) => {}
        Err(ZipRun::Unavailable(reason)) => {
            eprintln!(
                "System `zip` unavailable ({reason}); using the slower built-in archiver. \
                 Install Info-ZIP with: pkg install zip"
            );
            let _ = fs::remove_file(archive_path);
            write_archive_native(world_dir, archive_path, reporter)?;
        }
        Err(ZipRun::Failed(error)) => return Err(error),
    }

    reporter.emit(true, "");

    Ok(())
}

enum ZipRun {
    Unavailable(String),
    Failed(BackupError),
}

fn run_external_zip(
    world_dir: &Path,
    archive_path: &Path,
    sizes: &HashMap<String, u64>,
    reporter: &mut ProgressReporter<'_>,
) -> Result<(), ZipRun> {
    let binary = env::var("BACKUP_ZIP_BIN").unwrap_or_else(|_| "zip".to_string());
    let level = zip_level();

    let root_name = world_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| {
            ZipRun::Failed(BackupError::InvalidDestination(format!(
                "cannot archive {}",
                world_dir.display()
            )))
        })?;

    let working_dir = match world_dir.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };

    let mut child = Command::new(&binary)
        .current_dir(working_dir)
        .arg("-r")
        .arg(format!("-{level}"))
        .arg("-n")
        .arg(STORED_SUFFIXES.join(":"))
        .arg(archive_path)
        .arg(&root_name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| ZipRun::Unavailable(format!("{binary}: {error}")))?;

    // Drain stderr in the background so a chatty `zip` can never block on a full pipe.
    let stderr = child.stderr.take().map(|mut stream| {
        thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = stream.read_to_end(&mut buffer);
            buffer
        })
    });

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ZipRun::Failed(BackupError::Zip("could not read zip output".to_string())))?;

    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else {
            break;
        };

        if let Some(name) = archive_entry_name(&line) {
            let bytes = sizes.get(name).copied().unwrap_or_default();
            reporter.add_bytes(bytes, name);
            reporter.file_done(name);
        }
    }

    let status = child
        .wait()
        .map_err(|error| ZipRun::Failed(BackupError::Io(error)))?;

    let stderr = stderr
        .and_then(|handle| handle.join().ok())
        .map(|buffer| String::from_utf8_lossy(&buffer).into_owned())
        .unwrap_or_default();

    if !status.success() {
        return Err(ZipRun::Failed(BackupError::Zip(format!(
            "zip exited with {status}: {}",
            stderr.trim().lines().last().unwrap_or("no output")
        ))));
    }

    Ok(())
}

/// Extracts the entry name from an Info-ZIP progress line such as
/// `  adding: world/region/r.0.0.mca (deflated 3%)`.
fn archive_entry_name(line: &str) -> Option<&str> {
    let line = line.trim_start();
    let rest = line
        .strip_prefix("adding: ")
        .or_else(|| line.strip_prefix("updating: "))?;
    let name = &rest[..rest.rfind(" (")?];

    (!name.ends_with('/')).then_some(name)
}

fn zip_level() -> u8 {
    env::var("BACKUP_ZIP_LEVEL")
        .ok()
        .and_then(|value| value.trim().parse::<u8>().ok())
        .filter(|level| *level <= 9)
        .unwrap_or(DEFAULT_ZIP_LEVEL)
}

fn finalize_archive(temp_path: &Path, final_path: &Path) -> Result<ArchiveFile, BackupError> {
    // Info-ZIP appends ".zip" when the archive name has no extension; tolerate that.
    let produced = if temp_path.exists() {
        temp_path.to_path_buf()
    } else {
        let appended = appended_zip_path(temp_path);

        if appended.exists() {
            appended
        } else {
            return Err(BackupError::Zip(
                "zip did not create an archive".to_string(),
            ));
        }
    };

    fs::OpenOptions::new()
        .write(true)
        .open(&produced)?
        .sync_all()?;
    fs::rename(&produced, final_path)?;

    Ok(ArchiveFile {
        path: final_path.to_path_buf(),
        bytes: fs::metadata(final_path)?.len(),
        removed: 0,
    })
}

fn appended_zip_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".zip");
    PathBuf::from(name)
}

fn write_archive_native(
    world_dir: &Path,
    archive_path: &Path,
    reporter: &mut ProgressReporter<'_>,
) -> Result<(), BackupError> {
    let root_name = world_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "world".to_string());

    let file = fs::File::create(archive_path)?;
    let mut zip = ZipWriter::new(file);
    add_directory(&mut zip, world_dir, world_dir, &root_name, reporter)?;

    let file = zip
        .finish()
        .map_err(|error| BackupError::Zip(error.to_string()))?;
    file.sync_all()?;

    Ok(())
}

struct ProgressReporter<'a> {
    on_progress: &'a dyn Fn(BackupProgress),
    files_total: usize,
    bytes_total: u64,
    files_done: usize,
    bytes_done: u64,
    last_emit: Instant,
}

impl ProgressReporter<'_> {
    fn emit(&mut self, force: bool, current: &str) {
        let now = Instant::now();

        if !force && now.duration_since(self.last_emit) < PROGRESS_INTERVAL {
            return;
        }

        self.last_emit = now;

        (self.on_progress)(BackupProgress::Archive {
            files_done: self.files_done,
            files_total: self.files_total,
            bytes_done: self.bytes_done,
            bytes_total: self.bytes_total,
            current: current.to_string(),
        });
    }

    fn add_bytes(&mut self, bytes: u64, current: &str) {
        self.bytes_done += bytes;
        self.emit(false, current);
    }

    fn file_done(&mut self, current: &str) {
        self.files_done += 1;
        self.emit(self.files_done >= self.files_total, current);
    }
}

fn add_directory(
    zip: &mut ZipWriter<fs::File>,
    root: &Path,
    directory: &Path,
    root_name: &str,
    reporter: &mut ProgressReporter<'_>,
) -> Result<(), BackupError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|error| BackupError::Zip(error.to_string()))?;
        let name = format!(
            "{root_name}/{}",
            relative.to_string_lossy().replace('\\', "/")
        );

        if metadata.is_dir() {
            zip.add_directory(
                format!("{name}/"),
                SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Stored)
                    .unix_permissions(0o755),
            )
            .map_err(|error| BackupError::Zip(error.to_string()))?;
            add_directory(zip, root, &path, root_name, reporter)?;
        } else if metadata.is_file() {
            zip.start_file(name.clone(), file_options(&name))
                .map_err(|error| BackupError::Zip(error.to_string()))?;

            let mut source = fs::File::open(&path)?;
            let mut buffer = [0u8; COPY_BUFFER_SIZE];

            loop {
                let read = source.read(&mut buffer)?;

                if read == 0 {
                    break;
                }

                zip.write_all(&buffer[..read])?;
                reporter.add_bytes(read as u64, &name);
            }

            reporter.file_done(&name);
        }
    }

    Ok(())
}

fn file_options(name: &str) -> SimpleFileOptions {
    let options = SimpleFileOptions::default().unix_permissions(0o644);

    if STORED_SUFFIXES.iter().any(|suffix| name.ends_with(suffix)) {
        options.compression_method(CompressionMethod::Stored)
    } else {
        options
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(i64::from(DEFAULT_ZIP_LEVEL)))
    }
}

fn measure_directory(root: &Path) -> Result<(usize, u64, HashMap<String, u64>), BackupError> {
    let root_name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "world".to_string());

    let mut files = 0;
    let mut bytes = 0;
    let mut sizes = HashMap::new();
    let mut pending = vec![root.to_path_buf()];

    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;

            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| BackupError::Zip(error.to_string()))?;
                let name = format!(
                    "{root_name}/{}",
                    relative.to_string_lossy().replace('\\', "/")
                );

                files += 1;
                bytes += metadata.len();
                sizes.insert(name, metadata.len());
            }
        }
    }

    Ok((files, bytes, sizes))
}

fn prune_old_backups(backup_dir: &Path, keep: usize) -> Result<usize, BackupError> {
    let mut backups = Vec::new();

    for entry in fs::read_dir(backup_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name.starts_with("world-") && name.ends_with(".zip.tmp") && entry.file_type()?.is_file()
        {
            fs::remove_file(entry.path())?;
            continue;
        }

        if name.starts_with("world-") && name.ends_with(".zip") && entry.file_type()?.is_file() {
            backups.push(entry.path());
        }
    }

    backups.sort();

    let remove_count = backups.len().saturating_sub(keep);
    let mut removed = 0;

    for path in backups.into_iter().take(remove_count) {
        fs::remove_file(path)?;
        removed += 1;
    }

    Ok(removed)
}

async fn announce(server: &Arc<Mutex<ServerManager>>, message: &str) {
    let mut manager = server.lock().await;

    if manager.state() != ServerState::Online {
        return;
    }

    if let Err(error) = manager
        .send_console_command(&format!("say {message}"))
        .await
    {
        eprintln!("Failed to announce backup warning: {error}");
    }
}

fn next_run(time: NaiveTime) -> DateTime<Local> {
    let mut date = Local::now().date_naive();

    loop {
        if let Some(target) = Local.from_local_datetime(&date.and_time(time)).earliest()
            && target > Local::now()
        {
            return target;
        }

        date = date
            .succ_opt()
            .expect("date overflow while scheduling backup");
    }
}

fn duration_until(target: DateTime<Local>) -> Duration {
    (target - Local::now()).to_std().unwrap_or(Duration::ZERO)
}

fn outcome_message(outcome: &BackupOutcome) -> String {
    let restart = if outcome.restarted {
        "server restarted"
    } else {
        "server left offline"
    };

    let mut message = format!(
        "Backup completed: {} ({:.1} MB), {}",
        outcome
            .archive
            .file_name()
            .map(|name| name.to_string_lossy())
            .unwrap_or_default(),
        outcome.bytes as f64 / 1024.0 / 1024.0,
        restart
    );

    if outcome.removed > 0 {
        message.push_str(&format!(", removed {} old backup(s)", outcome.removed));
    }

    message
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let directory = env::temp_dir().join(format!(
            "termux-mc-manager-{name}-{}-{}",
            std::process::id(),
            Local::now().timestamp_nanos_opt().unwrap_or_default()
        ));

        fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn archive_contains_world_files() {
        let root = test_dir("archive");
        let world = root.join("world");
        let backups = root.join("backups");

        fs::create_dir_all(world.join("region")).unwrap();
        fs::write(world.join("level.dat"), b"level").unwrap();
        fs::write(world.join("region/r.0.0.mca"), b"region").unwrap();

        let archive = write_archive(&world, &backups, &|_| {}).unwrap();
        let file = fs::File::open(&archive.path).unwrap();
        let mut zip = zip::ZipArchive::new(file).unwrap();
        let mut names = Vec::new();

        for index in 0..zip.len() {
            let entry = zip.by_index(index).unwrap();
            names.push(entry.name().unwrap().to_string());
        }

        assert!(names.contains(&"world/level.dat".to_string()));
        assert!(names.contains(&"world/region/r.0.0.mca".to_string()));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn pruning_keeps_newest_archives() {
        let backups = test_dir("prune");

        for day in 1..=16 {
            fs::write(
                backups.join(format!("world-202601{day:02}-000000.zip")),
                b"backup",
            )
            .unwrap();
        }

        fs::write(backups.join("unrelated.zip"), b"keep").unwrap();

        let removed = prune_old_backups(&backups, 14).unwrap();

        assert_eq!(removed, 2);
        assert!(backups.join("world-20260116-000000.zip").exists());
        assert!(!backups.join("world-20260101-000000.zip").exists());
        assert!(backups.join("unrelated.zip").exists());

        fs::remove_dir_all(backups).ok();
    }

    #[test]
    fn archive_reports_progress() {
        use std::cell::RefCell;

        let root = test_dir("progress");
        let world = root.join("world");
        let backups = root.join("backups");

        fs::create_dir_all(world.join("region")).unwrap();
        fs::write(world.join("level.dat"), vec![0u8; 1024]).unwrap();
        fs::write(world.join("region/r.0.0.mca"), vec![0u8; 4096]).unwrap();

        let events = RefCell::new(Vec::new());
        let archive = write_archive(&world, &backups, &|progress| {
            events.borrow_mut().push(progress)
        })
        .unwrap();

        let events = events.into_inner();

        assert!(events.len() >= 2);

        let (files_done, files_total, bytes_done, bytes_total) = events
            .iter()
            .rev()
            .find_map(|progress| match progress {
                BackupProgress::Archive {
                    files_done,
                    files_total,
                    bytes_done,
                    bytes_total,
                    ..
                } => Some((*files_done, *files_total, *bytes_done, *bytes_total)),
                BackupProgress::Phase(_) => None,
            })
            .unwrap();

        assert_eq!(files_done, 2);
        assert_eq!(files_total, 2);
        assert_eq!(bytes_done, 5120);
        assert_eq!(bytes_total, 5120);
        assert!(archive.bytes > 0);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn progress_message_shows_percent_and_sizes() {
        let progress = BackupProgress::Archive {
            files_done: 5,
            files_total: 10,
            bytes_done: 512,
            bytes_total: 1024,
            current: "region/r.0.0.mca".to_string(),
        };

        let message = progress.message();

        assert!(message.contains("50%"));
        assert!(message.contains("512 B / 1.0 KB"));
        assert!(message.contains("5 / 10 files"));
        assert!(message.contains("region/r.0.0.mca"));

        let empty = BackupProgress::Archive {
            files_done: 0,
            files_total: 0,
            bytes_done: 0,
            bytes_total: 0,
            current: String::new(),
        };

        assert!(empty.message().contains("100%"));
        assert!(
            BackupProgress::Phase("stopping server")
                .message()
                .contains("stopping server")
        );
    }

    #[test]
    fn parses_info_zip_progress_lines() {
        assert_eq!(
            archive_entry_name("  adding: world/region/r.0.0.mca (deflated 3%)"),
            Some("world/region/r.0.0.mca")
        );
        assert_eq!(
            archive_entry_name("  updating: world/level.dat (stored 0%)"),
            Some("world/level.dat")
        );
        assert_eq!(
            archive_entry_name("  adding: world/region/ (stored 0%)"),
            None
        );
        assert_eq!(
            archive_entry_name("zip warning: name not matched: nope"),
            None
        );
    }

    #[test]
    fn finalize_archive_renames_and_reports_size() {
        let root = test_dir("finalize");
        let temp = root.join("world-20260101-000000.zip.tmp");
        let final_path = root.join("world-20260101-000000.zip");

        fs::write(&temp, b"archive-bytes").unwrap();

        let archive = finalize_archive(&temp, &final_path).unwrap();

        assert!(!temp.exists());
        assert_eq!(archive.path, final_path);
        assert_eq!(archive.bytes, 13);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn finalize_archive_accepts_zip_appended_name() {
        let root = test_dir("finalize-appended");
        let temp = root.join("world-20260101-000001.zip.tmp");
        let final_path = root.join("world-20260101-000001.zip");
        let appended = appended_zip_path(&temp);

        fs::write(&appended, b"archive").unwrap();

        let archive = finalize_archive(&temp, &final_path).unwrap();

        assert!(!appended.exists());
        assert_eq!(archive.bytes, 7);

        fs::remove_dir_all(root).ok();
    }
}
