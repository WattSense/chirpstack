use std::sync::RwLock;

use anyhow::{Context, Result};
use tracing::info;

use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};

use crate::config::Postgresql;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PgPoolConnection = PooledConnection<ConnectionManager<PgConnection>>;

lazy_static! {
    static ref PG_POOL: RwLock<Option<PgPool>> = RwLock::new(None);
}

pub fn setup(conf: &Postgresql) -> Result<()> {
    info!("Setting up PostgreSQL connection pool");
    let pg_pool = PgPool::builder()
        .max_size(conf.max_open_connections)
        .min_idle(match conf.min_idle_connections {
            0 => None,
            _ => Some(conf.min_idle_connections),
        })
        .build(ConnectionManager::new(&conf.dsn))
        .context("Setup PostgreSQL connection pool error")?;
    set_db_pool(pg_pool);
    Ok(())
}

fn get_db_pool() -> Result<PgPool> {
    let pool_r = PG_POOL.read().unwrap();
    let pool = pool_r
        .as_ref()
        .ok_or_else(|| anyhow!("PostgreSQL connection pool is not initialized (yet)"))?
        .clone();
    Ok(pool)
}

fn set_db_pool(p: PgPool) {
    let mut pool_w = PG_POOL.write().unwrap();
    *pool_w = Some(p);
}

pub fn get_db_conn() -> Result<PgPoolConnection> {
    let pool = get_db_pool()?;
    Ok(pool.get()?)
}
