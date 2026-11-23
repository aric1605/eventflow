import pg from 'pg';
import { getConfig, maskUrl } from './config.js';

const { Pool } = pg;

let pool = null;

/**
 * Initialize and get the PostgreSQL pool singleton.
 * Configured with connection timeout and small max connections for serverless/pooled Neon.
 * @returns {pg.Pool}
 */
export function getPool() {
  if (pool) return pool;

  const config = getConfig();

  // Strip channel_binding if inadvertently present in connection string
  const cleanConnectionString = config.DATABASE_URL.replace(/&channel_binding=require/g, '').replace(/\?channel_binding=require&?/g, '?');

  pool = new Pool({
    connectionString: cleanConnectionString,
    max: 5,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  pool.on('error', (err) => {
    process.stderr.write(`[Postgres Pool Error] ${err.message}\n`);
  });

  return pool;
}

/**
 * Connect to Postgres with automatic retries for Neon cold starts.
 * Retries up to 3 times with exponential delay.
 * @param {pg.Pool} [targetPool]
 * @param {number} [maxRetries=3]
 * @returns {Promise<pg.PoolClient>}
 */
export async function connectWithRetry(targetPool = getPool(), maxRetries = 3) {
  let attempt = 0;
  while (attempt < maxRetries) {
    attempt++;
    try {
      const client = await targetPool.connect();
      return client;
    } catch (err) {
      process.stderr.write(
        `[Postgres Connection] Attempt ${attempt}/${maxRetries} failed: ${err.message}. ${
          attempt < maxRetries ? 'Retrying in 2s...' : 'Exhausted retries.'
        }\n`
      );
      if (attempt >= maxRetries) {
        throw err;
      }
      await new Promise((res) => setTimeout(res, 2000));
    }
  }
}

/**
 * Close pool gracefully
 */
export async function closePool() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
