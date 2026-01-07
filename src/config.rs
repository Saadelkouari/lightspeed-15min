use anyhow::{Context, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub url: Option<String>,
    pub logging_enabled: bool,
    pub auto_create_schema: bool,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub polymarket_api_key: Option<String>,
    pub polymarket_api_secret: Option<String>,
    pub polymarket_api_passphrase: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            database: DatabaseConfig::from_env()?,
            polymarket_api_key: env::var("POLYMARKET_API_KEY").ok(),
            polymarket_api_secret: env::var("POLYMARKET_API_SECRET").ok(),
            polymarket_api_passphrase: env::var("POLYMARKET_API_PASSPHRASE").ok(),
        })
    }
}

impl DatabaseConfig {
    fn parse_bool(var: &str, default: bool) -> bool {
        env::var(var)
            .ok()
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(default)
    }

    pub fn from_env() -> Result<Self> {
        let logging_enabled = Self::parse_bool("DB_LOGGING_ENABLED", false);
        let auto_create_schema = Self::parse_bool("DB_AUTO_CREATE_SCHEMA", false);
        let url = env::var("DATABASE_URL").ok();

        if logging_enabled {
            url.clone()
                .context("DATABASE_URL is required when DB logging is enabled")?;
        }

        Ok(Self {
            url,
            logging_enabled,
            auto_create_schema,
        })
    }
}
