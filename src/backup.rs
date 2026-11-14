use std::{
    env, fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
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

#[derive(Debug)]
pub enum BackupError {
    Busy(&'static str),
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

        match perform_backup(&server, &config).await {
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
) -> Result<BackupOutcome, BackupError> {
    let world_dir = config.server_dir.join("world");

    if !world_dir.is_dir() {
        return Err(BackupError::MissingWorld(world_dir));
    }

    let was_online = {
        let mut manager = server.lock().await;

        let state = manager.state();
        let was_online = match state {
            ServerState::Online => {
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

    let archive_result = create_archive(world_dir, config.backup_dir.clone()).await;
    let restart_result = {
        let mut manager = server.lock().await;
        manager.finish_backup();

        if was_online {
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
) -> Result<ArchiveFile, BackupError> {
    let archive = tokio::task::spawn_blocking(move || {
        let archive = write_archive(&world_dir, &backup_dir)?;
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

fn write_archive(world_dir: &Path, backup_dir: &Path) -> Result<ArchiveFile, BackupError> {
    fs::create_dir_all(backup_dir)?;

    let stamp = Local::now().format("%Y%m%d-%H%M%S");
    let final_path = backup_dir.join(format!("world-{stamp}.zip"));
    let temp_path = backup_dir.join(format!("world-{stamp}.zip.tmp"));

    let result = (|| {
        let file = fs::File::create(&temp_path)?;
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .unix_permissions(0o644);

        add_directory(&mut zip, world_dir, world_dir, options)?;

        let file = zip
            .finish()
            .map_err(|error| BackupError::Zip(error.to_string()))?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp_path, &final_path)?;
        let bytes = fs::metadata(&final_path)?.len();

        Ok::<_, BackupError>(ArchiveFile {
            path: final_path.clone(),
            bytes,
            removed: 0,
        })
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    result
}

fn add_directory(
    zip: &mut ZipWriter<fs::File>,
    root: &Path,
    directory: &Path,
    options: SimpleFileOptions,
) -> Result<(), BackupError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|error| BackupError::Zip(error.to_string()))?;
        let name = format!("world/{}", relative.to_string_lossy().replace('\\', "/"));

        if metadata.is_dir() {
            zip.add_directory(format!("{name}/"), options.unix_permissions(0o755))
                .map_err(|error| BackupError::Zip(error.to_string()))?;
            add_directory(zip, root, &path, options)?;
        } else if metadata.is_file() {
            zip.start_file(name, options)
                .map_err(|error| BackupError::Zip(error.to_string()))?;
            let mut source = fs::File::open(&path)?;
            std::io::copy(&mut source, zip)?;
        }
    }

    Ok(())
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

        let archive = write_archive(&world, &backups).unwrap();
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
}
