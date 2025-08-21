-- Test data for PostgreSQL monitor
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);
INSERT INTO test_table (value) VALUES ('text1'), ('text2'), ('text3');
