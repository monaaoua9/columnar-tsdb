use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corrupt(String),
    // late point older than the retention window; it would be expired right away
    TooOld { cutoff: i64, got: i64 },
    BadValue(f64),
    BadConfig(&'static str),
    // parse / eval problem in a query string; the message says where
    Query(String),
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
            Error::TooOld { cutoff, got } => {
                write!(f, "point ts {got} is older than retention cutoff {cutoff}")
            }
            Error::BadValue(v) => write!(f, "non-finite value rejected: {v}"),
            Error::BadConfig(m) => write!(f, "bad config: {m}"),
            Error::Query(m) => write!(f, "query: {m}"),
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
