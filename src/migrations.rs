use duckdb::{params, DuckdbConnectionManager, OptionalExt};

static META_MIGRATION: &str = r"
CREATE TABLE IF NOT EXISTS meta_migration_schema (
  next_migration_idx INTEGER
);
";

static MIGRATIONS: &[&str] = &[
    r"
CREATE SEQUENCE seq_scans_id START 1;
    ",
    r"
CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY DEFAULT nextval('seq_scans_id'),
    status TEXT NOT NULL,
    path TEXT NOT NULL,
    scanned_at TIMESTAMP NOT NULL
);",
];

pub async fn migrate(r2d2_pool: &r2d2::Pool<DuckdbConnectionManager>) {
    println!("Running migrations...");
    let conn = r2d2_pool.get().unwrap();

    conn.execute(META_MIGRATION, params![]).unwrap();

    let next_migration_idx_query = conn
        .query_row(
            "SELECT next_migration_idx FROM meta_migration_schema",
            params![],
            |row| row.get(0),
        )
        .optional();

    let next_migration_idx = match next_migration_idx_query {
        Ok(Some(idx)) => idx,
        _ => {
            conn.execute(
                "INSERT INTO meta_migration_schema (next_migration_idx) VALUES (0)",
                params![],
            )
            .unwrap();
            0
        }
    };

    println!("Next migration index: {}", next_migration_idx);

    for (idx, migration) in MIGRATIONS[next_migration_idx..].iter().enumerate() {
        // Backup database in case
        if let Some(path) = conn.path() {
            let backup_path = format!("{}.pre-{}-backup", path.display(), idx + next_migration_idx);
            std::fs::copy(path, backup_path).unwrap();
        }

        println!("Applying migration {}...", idx + next_migration_idx);
        conn.execute(migration, params![]).unwrap();
        conn.execute(
            "UPDATE meta_migration_schema SET next_migration_idx = ?",
            params![idx + next_migration_idx + 1],
        )
        .unwrap();
    }

    println!("Migrations complete!");
}