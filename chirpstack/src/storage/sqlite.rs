use std::sync::RwLock;

use anyhow::{Context, Result};
use tracing::info;

use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::sqlite::SqliteConnection;

use crate::config::Postgresql;

pub type SqlitePool = Pool<ConnectionManager<SqliteConnection>>;
pub type SqlitePoolConnection = PooledConnection<ConnectionManager<SqliteConnection>>;

lazy_static! {
    static ref SQLITE_POOL: RwLock<Option<SqlitePool>> = RwLock::new(None);
}

pub fn setup(conf: &Postgresql) -> Result<()> {
    info!("Setting up Sqlite connection pool");
    let pool = SqlitePool::builder()
        .max_size(conf.max_open_connections)
        .min_idle(match conf.min_idle_connections {
            0 => None,
            _ => Some(conf.min_idle_connections),
        })
        .build(ConnectionManager::<SqliteConnection>::new(&conf.dsn))
        .context("Setup Sqlite connection pool error")?;
    set_db_pool(pool);
    Ok(())
}

fn get_db_pool() -> Result<SqlitePool> {
    let pool_r = SQLITE_POOL.read().unwrap();
    let pool = pool_r
        .as_ref()
        .ok_or_else(|| anyhow!("Sqlite connection pool is not initialized (yet)"))?
        .clone();
    Ok(pool)
}

fn set_db_pool(p: SqlitePool) {
    let mut pool_w = SQLITE_POOL.write().unwrap();
    *pool_w = Some(p);
}

pub fn get_db_conn() -> Result<SqlitePoolConnection> {
    let pool = get_db_pool()?;
    Ok(pool.get()?)
}
