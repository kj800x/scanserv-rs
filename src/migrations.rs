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
    CREATE SEQUENCE seq_scan_dividers_id START 1;
    ",
    r"
    CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_scans_id'),
        status TEXT NOT NULL,
        scanner TEXT NOT NULL,
        scan_parameters TEXT NOT NULL,
        path TEXT NOT NULL,
        scanned_at TIMESTAMP NOT NULL
    );
    ",
    r"
    CREATE TABLE IF NOT EXISTS scan_dividers (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_scan_dividers_id'),
        ts TIMESTAMP NOT NULL
    );",
    r"
    CREATE SEQUENCE seq_scan_groups_id START 1;
    ",
    r"
    CREATE TABLE IF NOT EXISTS scan_groups (
        id INTEGER PRIMARY KEY DEFAULT nextval('seq_scan_groups_id'),
        title TEXT NOT NULL
    );",
    r"
    ALTER TABLE scans ADD COLUMN scan_group_id INTEGER;
    ",
    // New migrations for enhanced group model
    r"
    ALTER TABLE scan_groups ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    ",
    r"
    ALTER TABLE scan_groups ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    ",
    r"
    ALTER TABLE scan_groups ADD COLUMN status TEXT DEFAULT 'scanning';
    ",
    r"
    ALTER TABLE scan_groups ADD COLUMN comment TEXT DEFAULT '';
    ",
    r"
    ALTER TABLE scan_groups ADD COLUMN tags TEXT DEFAULT '[]';
    ",
    // New migrations for image editing features
    r"
    ALTER TABLE scans ADD COLUMN rotation INTEGER DEFAULT 0;
    ",
    r"
    ALTER TABLE scans ADD COLUMN crop_coordinates TEXT DEFAULT NULL;
    ",
    r"
    ALTER TABLE scans ADD COLUMN original_path TEXT;
    ",
    r"
    ALTER TABLE scans ADD COLUMN edited_path TEXT;
    ",
    // Update existing scan records to set original_path = path
    r"
    UPDATE scans SET original_path = path WHERE original_path IS NULL;
    ",
    // Migrate existing dividers to create proper groups
    r"
    INSERT INTO scan_groups (title, status, created_at)
    SELECT 'Untitled Group ' || d.id, 'scanning', d.ts
    FROM scan_dividers d
    WHERE NOT EXISTS (
        SELECT 1
        FROM scan_groups g
        WHERE g.created_at = d.ts
    );
    ",
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
