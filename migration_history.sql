CREATE SCHEMA IF NOT EXISTS changeset_migrate;

CREATE TABLE IF NOT EXISTS changeset_migrate.migration_history (
    name VARCHAR(1024) ENCODE zstd,
    type VARCHAR(32) ENCODE zstd,
    hash VARCHAR(32) ENCODE zstd
)
DISTKEY(name)
SORTKEY(name);