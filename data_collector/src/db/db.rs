use super::models::BlockRecord;
use super::schema::blocks::dsl::blocks;
use crate::db::models::LogRecord;
use crate::db::schema::logs::dsl::logs;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use dotenv::dotenv;

pub fn establish_connection(database_url: &str) -> PgConnection {
    dotenv().ok();
    PgConnection::establish(database_url).expect(&format!("Error connecting to {}", database_url))
}

pub fn load_data(conn: &PgConnection) -> Vec<BlockRecord> {
    blocks
        .load::<BlockRecord>(conn)
        .expect("Error loading data")
}

pub fn insert_data(conn: &PgConnection, new_data: BlockRecord) {
    diesel::insert_into(blocks)
        .values(&new_data)
        .execute(conn)
        .expect("Error inserting data");
}

pub fn insert_multiple_data(conn: &PgConnection, new_data: Vec<BlockRecord>) {
    diesel::insert_into(blocks)
        .values(&new_data)
        .execute(conn)
        .expect("Error inserting data");
}

pub fn insert_multiple_logs(conn: &PgConnection, new_logs: Vec<LogRecord>) {
    diesel::insert_into(logs)
        .values(&new_logs)
        .execute(conn)
        .expect("Error inserting logs");
}
