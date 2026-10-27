use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corrupt(String),
    OutOfOrder { last: i64, got: i64 },
    BadValue(f64),
    BadConfig(&'static str),
    Locked,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corrupt(m) => write!(f, "corrupt data: {m}"),
            Error::OutOfOrder { last, got } => {
                write!(f, "out-of-order point: ts {got} <= last ts {last}")
            }
            Error::BadValue(v) => write!(f, "non-finite value rejected: {v}"),
            Error::BadConfig(m) => write!(f, "bad config: {m}"),
            Error::Locked => write!(f, "data directory is locked by another process"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
