use diesel::{
    backend::Backend,
    {deserialize, serialize},
};
#[cfg(feature = "postgres")]
use diesel::{pg::Pg, sql_types::Jsonb};
#[cfg(feature = "sqlite")]
use diesel::{sql_types::Text, sqlite::Sqlite};

use serde::{Deserialize, Serialize};

// TimestampTz is represented differently in Diesel
// but it can essentially convert from/to chrono::*DateTime*
#[cfg(feature = "postgres")]
pub type DbTimestamptz = diesel::sql_types::Timestamptz;
#[cfg(feature = "sqlite")]
pub type DbTimestamptz = diesel::sql_types::TimestamptzSqlite;

// Sqlite has no native json type so use text
#[cfg(feature = "postgres")]
pub type DbJsonT = Jsonb;
#[cfg(feature = "sqlite")]
pub type DbJsonT = Text;

// Sqlite has no native json type so use text
#[cfg(feature = "postgres")]
pub type DbUuid = diesel::sql_types::Uuid;
#[cfg(feature = "sqlite")]
pub type DbUuid = Text;

#[derive(Deserialize, Serialize, Copy, Clone, Debug, Eq, PartialEq, AsExpression, FromSqlRow)]
#[serde(transparent)]
#[cfg_attr(feature = "postgres", diesel(sql_type = Jsonb))]
#[cfg_attr(feature = "sqlite", diesel(sql_type = Text))]
pub struct Uuid(uuid::Uuid);

impl std::convert::AsRef<uuid::Uuid> for Uuid {
    fn as_ref(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl std::convert::From<uuid::Uuid> for Uuid {
    fn from(u: uuid::Uuid) -> Self {
        Self(u)
    }
}

impl std::ops::Deref for Uuid {
    type Target = uuid::Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Uuid {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(feature = "postgres")]
impl deserialize::FromSql<diesel::sql_types::Uuid, Pg> for Uuid {
    fn from_sql(value: <Pg as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let u = <uuid::Uuid>::from_sql(value)?;
        Ok(Uuid(u))
    }
}

#[cfg(feature = "postgres")]
impl serialize::ToSql<diesel::sql_types::Uuid, Pg> for Uuid {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Pg>) -> serialize::Result {
        <uuid::Uuid as serialize::ToSql<diesel::sql_types::Uuid, Pg>>::to_sql(
            &self.0,
            &mut out.reborrow(),
        )
    }
}

#[cfg(feature = "sqlite")]
impl deserialize::FromSql<Text, Sqlite> for Uuid {
    fn from_sql(value: <Sqlite as Backend>::RawValue<'_>) -> deserialize::Result<Self> {
        let s = <*const str>::from_sql(value)?;
        let u = uuid::Uuid::try_parse(s)?;
        Ok(Uuid(u))
    }
}

#[cfg(feature = "sqlite")]
impl serialize::ToSql<Text, Sqlite> for Uuid {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        let str_uuid = self
            .0
            .hyphenated()
            .encode_lower(&uuid::Uuid::encode_buffer());
        <str>::to_sql(str_uuid, out)
    }
}
