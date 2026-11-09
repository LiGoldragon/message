//! A socket placed at its name only once it listens.
//!
//! A peer watching for one of Message's sockets waits on one event: the
//! socket's name moved in (inotify's `IN_MOVED_TO`). The socket is bound
//! under a temporary name beside its name, `.<name>.<pid>`, its mode set to
//! `0600`, made to listen, then renamed to its name; rename(2) is atomic,
//! so the name never holds a socket that does not listen. A watcher that
//! sees the name moved in connects at once. The rule is the one every Nexus
//! at a place keeps (Flow's `placed_socket`).

use std::{
    ffi::{CString, OsString},
    fs,
    io::{self, ErrorKind},
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, PermissionsExt},
        io::{FromRawFd, OwnedFd},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
};

/// A socket's name and the temporary name it is made under, in the same
/// directory, so the rename never leaves it and a leftover of a crash is
/// never taken for the socket.
pub struct PlacedSocket {
    name: PathBuf,
    temporary: PathBuf,
}

/// What stands at a socket's name before the socket is placed there.
pub enum AtSocketName {
    /// Nothing: the socket is renamed in without replacing anything, so a
    /// socket another process placed there since is never replaced.
    Absent,
    /// A socket nobody accepts on, left by a process that ended: the rename
    /// replaces it, with no moment at which the name is absent.
    Stale,
}

/// Places a socket at its name, listening.
pub trait PlacesSocket: Sized {
    /// The socket to be placed at `name`.
    fn at(name: &Path) -> io::Result<Self>;
    /// What stands at the name. A socket that accepts a connection belongs
    /// to a live process and is refused, as is a file that is no socket.
    fn found(&self) -> io::Result<AtSocketName>;
    /// A socket bound at the temporary name, its mode `0600`, listening. A
    /// leftover at this process's own temporary name is an ended process's
    /// that had the same id, and is removed first.
    fn listening(&self) -> io::Result<UnixListener>;
    /// Renames the temporary name to the name, over a stale socket or into
    /// an absent name.
    fn place(&self, found: AtSocketName) -> io::Result<()>;
    /// The socket listening at its name; on any refusal nothing of it is
    /// left at either name.
    fn claim(&self) -> io::Result<UnixListener>;
}

impl PlacesSocket for PlacedSocket {
    fn at(name: &Path) -> io::Result<Self> {
        let unplaced = || {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "socket path {} names no file in a directory",
                    name.display()
                ),
            )
        };
        let directory = name.parent().ok_or_else(unplaced)?;
        let file_name = name.file_name().ok_or_else(unplaced)?;
        let mut temporary = OsString::from(".");
        temporary.push(file_name);
        temporary.push(format!(".{}", std::process::id()));
        Ok(Self {
            name: name.to_owned(),
            temporary: directory.join(temporary),
        })
    }

    fn found(&self) -> io::Result<AtSocketName> {
        let name = &self.name;
        match fs::symlink_metadata(name) {
            Ok(metadata) if metadata.file_type().is_socket() => match UnixStream::connect(name) {
                Ok(_peer) => Err(io::Error::new(
                    ErrorKind::AddrInUse,
                    format!("socket {} is already accepting connections", name.display()),
                )),
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    Ok(AtSocketName::Stale)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(AtSocketName::Absent),
                Err(error) => Err(error),
            },
            Ok(_) => Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("socket path {} exists and is not a socket", name.display()),
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(AtSocketName::Absent),
            Err(error) => Err(error),
        }
    }

    fn listening(&self) -> io::Result<UnixListener> {
        let temporary = &self.temporary;
        match fs::remove_file(temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let path = temporary.as_os_str().as_bytes();
        // SAFETY: an all-zero sockaddr_un is valid; its path is filled
        // below within its bounds, leaving the terminating zero.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        if path.len() >= address.sun_path.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("socket path {} is too long", temporary.display()),
            ));
        }
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (slot, byte) in address.sun_path.iter_mut().zip(path) {
            *slot = *byte as libc::c_char;
        }
        // SAFETY: socket takes no pointer; a non-negative result is a
        // descriptor this process owns, handed to OwnedFd at once.
        let descriptor =
            unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor was just opened and is owned by nothing else.
        let socket = unsafe { OwnedFd::from_raw_fd(descriptor) };
        // SAFETY: the address is a valid sockaddr_un of the length given.
        let bound = unsafe {
            libc::bind(
                descriptor,
                (&raw const address).cast::<libc::sockaddr>(),
                std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
            )
        };
        if bound != 0 {
            return Err(io::Error::last_os_error());
        }
        let protected = fs::set_permissions(temporary, fs::Permissions::from_mode(0o600));
        // SAFETY: listen takes the descriptor this process owns.
        let listened = protected.and_then(|()| {
            if unsafe { libc::listen(descriptor, libc::SOMAXCONN) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
        if let Err(error) = listened {
            let _ = fs::remove_file(temporary);
            return Err(error);
        }
        Ok(UnixListener::from(socket))
    }

    fn place(&self, found: AtSocketName) -> io::Result<()> {
        let path = |path: &Path| {
            CString::new(path.as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("socket path {} holds a zero byte", path.display()),
                )
            })
        };
        let from = path(&self.temporary)?;
        let to = path(&self.name)?;
        let flags = match found {
            AtSocketName::Absent => libc::RENAME_NOREPLACE,
            AtSocketName::Stale => 0,
        };
        // SAFETY: both paths are zero-terminated and outlive the call;
        // AT_FDCWD resolves them as given, and both are in one directory.
        let renamed = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                flags,
            )
        };
        if renamed != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn claim(&self) -> io::Result<UnixListener> {
        let found = self.found()?;
        let listener = self.listening()?;
        if let Err(error) = self.place(found) {
            drop(listener);
            let _ = fs::remove_file(&self.temporary);
            return Err(error);
        }
        Ok(listener)
    }
}

#[cfg(test)]
mod tests {
    use super::{PlacedSocket, PlacesSocket};
    use std::os::unix::{fs::FileTypeExt, net::UnixStream};

    #[test]
    fn a_placed_socket_listens_at_its_name_and_leaves_no_temporary_name() {
        let directory = tempfile::tempdir().unwrap();
        let name = directory.path().join("message.sock");
        let placed = PlacedSocket::at(&name).unwrap();
        let _listener = placed.claim().unwrap();
        assert!(
            std::fs::symlink_metadata(&name)
                .unwrap()
                .file_type()
                .is_socket()
        );
        UnixStream::connect(&name).expect("the placed socket listens");
        let names: Vec<_> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, ["message.sock"]);
        // A second Nexus at the same name is refused while the first listens.
        assert!(PlacedSocket::at(&name).unwrap().claim().is_err());
    }

    #[test]
    fn a_stale_socket_is_replaced_in_place() {
        let directory = tempfile::tempdir().unwrap();
        let name = directory.path().join("message.sock");
        drop(std::os::unix::net::UnixListener::bind(&name).unwrap());
        let _listener = PlacedSocket::at(&name).unwrap().claim().unwrap();
        UnixStream::connect(&name).expect("the new socket listens");
    }
}
