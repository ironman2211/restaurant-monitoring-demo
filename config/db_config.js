import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'resturant_mngmnt',
    password: 'Brahma#8984',
    port: 5432,
});

const createTables = async () => {
    const queries = [
        `CREATE TABLE IF NOT EXISTS store_status (
            store_id BIGINT,
            timestamp_utc TIMESTAMP,
            status VARCHAR(10)
        )`,
        `CREATE TABLE IF NOT EXISTS business_hours (
            store_id BIGINT,
            day_of_week INT,
            start_time_local TIME,
            end_time_local TIME
        )`,
        `CREATE TABLE IF NOT EXISTS store_timezones (
            store_id BIGINT,
            timezone_str VARCHAR(50)
        )`,
        `CREATE TABLE IF NOT EXISTS report_status (
            report_id UUID PRIMARY KEY,
            status VARCHAR(10),
            result TEXT
        )`
    ];

    for (const query of queries) {
        await pool.query(query);
    }
};

export { pool, createTables };
