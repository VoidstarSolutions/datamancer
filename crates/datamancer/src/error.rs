use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Inner state machine terminated unexpectedly")]
    UnexpectedTermination,
    #[error(transparent)]
    Alpaca(#[from] oxidized_alpaca::Error),
}
