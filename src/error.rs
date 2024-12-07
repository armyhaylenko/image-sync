use chrono::NaiveDate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Date not found: {0}")]
    BadDate(NaiveDate),
    #[error("Could not parse date: {0}")]
    InvalidDateFormat(String),
    #[error("Could not parse hex-encoded hashes from the request")]
    InvalidHashFormat,
    #[error("Something went wrong...")]
    InternalError,
}
