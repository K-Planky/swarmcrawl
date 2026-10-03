//! One private, atomically published connection configuration, outside the repository.
use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use swarmcrawl::{config::RedisConfig, jobs::validate_namespace};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Saved {
    pub version: u8,
    pub redis_url: String,
    pub namespace: String,
    pub owned: Option<Owned>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Owned {
    // Random identity, also embedded in the name and Docker ownership label.
    pub token: String,
    pub bind: std::net::IpAddr,
    pub port: u16,
}

impl fmt::Debug for Saved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Saved { [redacted] }")
    }
}

impl Saved {
    pub fn validate(&self) -> Result<()> {
        self.validate_metadata()?;
        RedisConfig::new(&self.redis_url, 5)?;
        validate_namespace(&self.namespace)?;
        if let Some(owned) = &self.owned {
            let info = redis::IntoConnectionInfo::into_connection_info(self.redis_url.clone())
                .map_err(|_| "invalid owned connection")?;
            let redis::ConnectionAddr::Tcp(host, port) = info.addr() else {
                return Err("owned connection must use TCP".into());
            };
            if host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .ok()
                != Some(owned.bind)
                || *port != owned.port
                || info.redis_settings().db() > 15
                || info
                    .redis_settings()
                    .username()
                    .is_some_and(|name| !name.is_empty())
            {
                return Err("owned endpoint differs from saved deployment metadata".into());
            }
        }
        Ok(())
    }

    fn validate_metadata(&self) -> Result<()> {
        if self.version != 1 {
            return Err("unsupported saved configuration version".into());
        }
        if let Some(owned) = &self.owned
            && (owned.token.len() != 32
                || !owned.token.bytes().all(|b| b.is_ascii_hexdigit())
                || owned.bind.is_unspecified()
                || owned.bind.is_multicast()
                || owned.port == 0)
        {
            return Err("invalid saved deployment metadata".into());
        }
        Ok(())
    }
}

pub struct Storage {
    pub dir: PathBuf,
}

impl Storage {
    pub fn locate() -> Result<Self> {
        let dir = match std::env::var_os("SWARMCRAWL_CONFIG_DIR") {
            Some(path) => PathBuf::from(path),
            None => dirs::config_dir()
                .ok_or("cannot locate user configuration directory; set SWARMCRAWL_CONFIG_DIR")?
                .join("swarmcrawl"),
        };
        if !dir.is_absolute() {
            return Err("SWARMCRAWL_CONFIG_DIR must be an absolute path".into());
        }
        Ok(Self { dir })
    }

    pub fn load(&self) -> Result<Option<Saved>> {
        match fs::symlink_metadata(&self.dir) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("cannot inspect configuration directory".into()),
            Ok(_) => (),
        }
        private_metadata(&self.dir, true)?;
        let path = self.dir.join("config.json");
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err("cannot inspect saved configuration".into()),
            Ok(_) => (),
        }
        private_metadata(&path, false)?;
        let data = fs::read(&path).map_err(|_| "cannot read saved configuration")?;
        let saved: Saved = serde_json::from_slice(&data)
            .map_err(|_| "invalid saved configuration JSON (contents withheld)")?;
        // Connection fields are validated AFTER flags/environment select values.
        saved.validate_metadata()?;
        Ok(Some(saved))
    }

    /// The held file lock serializes setup/lifecycle across local CLI processes.
    /// Readers need no lock: config.json is published as one complete file.
    pub fn lock(&self) -> Result<File> {
        create_private_dir(&self.dir)?;
        let path = self.dir.join("deployment.lock");
        let file = private_open(&path)?;
        file.try_lock()
            .map_err(|_| "another cluster command is running; try again after it exits")?;
        Ok(file)
    }

    /// Only these application-owned files are removable, and only under the held
    /// deployment lock. Do not unlink the lock inode: another process could then
    /// acquire a different lock and race a new init against this removal.
    pub fn check_removable(&self) -> Result<()> {
        for name in ["redis.conf", "config.json"] {
            let path = self.dir.join(name);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(_) => return Err("cannot inspect connection files for removal".into()),
                Ok(_) => private_metadata(&path, false)?,
            }
        }
        Ok(())
    }

    pub fn remove_connection(&self) -> Result<()> {
        self.check_removable()?;
        // Metadata is last: on partial failure it still identifies the deployment
        // for a retry. No backup/rename and no recursive deletion of user files.
        for name in ["redis.conf", "config.json"] {
            match fs::remove_file(self.dir.join(name)) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(_) => return Err("local connection cleanup incomplete; check private directory permissions and retry cluster remove".into()),
            }
        }
        Ok(())
    }

    pub fn save_new(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        let data =
            serde_json::to_vec_pretty(saved).map_err(|_| "cannot encode saved configuration")?;
        self.write_new("config.json", &data)
    }

    pub fn write_new(&self, name: &str, data: &[u8]) -> Result<()> {
        let mut temporary = tempfile::NamedTempFile::new_in(&self.dir)
            .map_err(|_| "cannot create private configuration file")?;
        temporary
            .write_all(data)
            .map_err(|_| "cannot write private configuration file")?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|_| "cannot sync private configuration file")?;
        temporary
            .persist_noclobber(self.dir.join(name))
            .map_err(|_| "cannot publish configuration file; existing files are never replaced")?;
        Ok(())
    }
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|_| "cannot create private configuration directory")?;
    private_metadata(path, true)
}

#[cfg(unix)]
fn private_open(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            private_metadata(path, false)?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|_| "cannot open configuration lock".into())
        }
        Err(_) => Err("cannot create private configuration lock".into()),
    }
}

#[cfg(unix)]
pub fn private_metadata(path: &Path, directory: bool) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::symlink_metadata(path)
        .map_err(|_| "cannot inspect private configuration permissions")?;
    if meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
        || (directory && !meta.is_dir())
        || (!directory && (!meta.is_file() || meta.nlink() != 1))
    {
        return Err("configuration must be owned by this user, not symlinked/shared: use directory mode 700 and file mode 600".into());
    }
    Ok(())
}

// Fail closed rather than pretending Unix modes protect Windows credentials.
#[cfg(not(unix))]
fn create_private_dir(_: &Path) -> Result<()> {
    Err("saved cluster setup currently requires Unix permissions; use Linux/WSL or existing flag/environment configuration".into())
}
#[cfg(not(unix))]
fn private_open(_: &Path) -> Result<File> {
    create_private_dir(Path::new(""))?;
    unreachable!()
}
#[cfg(not(unix))]
pub fn private_metadata(_: &Path, _: bool) -> Result<()> {
    create_private_dir(Path::new(""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_parse_errors_never_reveal_values() {
        let saved = Saved {
            version: 1,
            redis_url: "redis://:fixture-secret@localhost/0".into(),
            namespace: "swarmcrawl:v1".into(),
            owned: None,
        };
        assert!(!format!("{saved:?}").contains("fixture-secret"));
    }

    #[cfg(unix)]
    #[test]
    fn private_atomic_storage_refuses_overwrite_and_shared_files() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage {
            dir: temp.path().join("private"),
        };
        assert!(storage.load().unwrap().is_none());
        let _lock = storage.lock().unwrap();
        assert!(storage.lock().is_err());
        let saved = Saved {
            version: 1,
            redis_url: "redis://localhost/0".into(),
            namespace: "test".into(),
            owned: None,
        };
        storage.save_new(&saved).unwrap();
        assert!(storage.save_new(&saved).is_err());
        assert_eq!(storage.load().unwrap().unwrap().namespace, "test");
        fs::set_permissions(
            storage.dir.join("config.json"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(storage.load().is_err());
    }
}
