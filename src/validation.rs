use std::fmt;

/// A framework configuration field and the rule it violates. Contains no secrets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigError {
    pub field: &'static str,
    pub reason: &'static str,
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}
impl std::error::Error for ConfigError {}

impl crate::Config {
    /// Check backoff bounds. Zero hook deadlines are immediate deadlines; `None`
    /// disables an optional deadline. Existing unchecked constructors stay unchanged.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.retry_delay.is_zero() {
            return Err(ConfigError {
                field: "retry_delay",
                reason: "must be positive",
            });
        }
        if self.max_retry_delay < self.retry_delay {
            return Err(ConfigError {
                field: "max_retry_delay",
                reason: "must be at least retry_delay",
            });
        }
        Ok(())
    }
}
impl crate::RetryPolicy {
    /// All representable policies are valid. A zero deadline returns Timeout
    /// without starting an operation; max_attempts is nonzero by construction.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}
