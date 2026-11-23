import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { getPool, connectWithRetry, closePool } from '../src/db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MIGRATIONS_DIR = path.resolve(__dirname, '../migrations');

/**
 * Run pending database migrations idempotently.
 * @param {import('pg').Pool} [pool]
 * @returns {Promise<string[]>} List of applied migration names
 */
export async function runMigrations(pool = getPool()) {
  const client = await connectWithRetry(pool);
  const appliedMigrations = [];

  try {
    // 1. Ensure migrations tracking table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id serial PRIMARY KEY,
        name text NOT NULL UNIQUE,
        applied_at timestamptz NOT NULL DEFAULT now()
      );
    `);

    // 2. Fetch already applied migrations
    const { rows } = await client.query('SELECT name FROM schema_migrations ORDER BY id ASC');
    const appliedSet = new Set(rows.map((r) => r.name));

    // 3. Read migration files
    const files = fs
      .readdirSync(MIGRATIONS_DIR)
      .filter((file) => file.endsWith('.sql'))
      .sort();

    for (const file of files) {
      if (appliedSet.has(file)) {
        continue;
      }

      const filePath = path.join(MIGRATIONS_DIR, file);
      const sql = fs.readFileSync(filePath, 'utf8');

      process.stdout.write(`[Migrate] Applying ${file}...\n`);
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
        appliedMigrations.push(file);
        process.stdout.write(`[Migrate] Applied ${file} successfully.\n`);
      } catch (err) {
        await client.query('ROLLBACK');
        process.stderr.write(`[Migrate] Error applying ${file}: ${err.message}\n`);
        throw err;
      }
    }

    return appliedMigrations;
  } finally {
    client.release();
  }
}

/**
 * List existing public tables
 */
export async function listTables(pool = getPool()) {
  const { rows } = await pool.query(`
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name;
  `);
  return rows.map((r) => r.table_name);
}

// CLI execution
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  (async () => {
    try {
      const applied = await runMigrations();
      if (applied.length === 0) {
        process.stdout.write('[Migrate] Database schema is up to date. No new migrations.\n');
      }
      const tables = await listTables();
      process.stdout.write(`[Migrate] Database tables: ${tables.join(', ')}\n`);
      await closePool();
      process.exit(0);
    } catch (err) {
      process.stderr.write(`[Migrate Failed] ${err.message}\n`);
      await closePool();
      process.exit(1);
    }
  })();
}
