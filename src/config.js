import { z } from 'zod';

/**
 * Robust parser for Redis connection configuration.
 * Handles:
 * - Standard URLs: rediss://default:token@host:6379 or redis://...
 * - Upstash CLI command strings: redis-cli --tls -u rediss://...
 * - Bare authentication strings: default:token@host:6379
 * - Bare hosts: host:6379
 * - Enclosing quotes and whitespace
 *
 * @param {string} raw
 * @returns {{ host: string, port: number, username?: string, password?: string, tls?: object }}
 */
export function parseRedisConfig(raw) {
  if (!raw || typeof raw !== 'string') {
    throw new Error('REDIS_URL is required and must be a string');
  }

  let str = raw.trim();

  // Strip enclosing quotes
  if (
    (str.startsWith('"') && str.endsWith('"')) ||
    (str.startsWith("'") && str.endsWith("'"))
  ) {
    str = str.slice(1, -1).trim();
  }

  // Extract from CLI command if present: e.g. "redis-cli --tls -u rediss://..."
  const cliMatch = str.match(/(rediss?:\/\/[^\s"']+)/);
  if (cliMatch) {
    str = cliMatch[1];
  }

  const useTls = str.startsWith('rediss://') || str.includes('upstash.io');
  if (!str.startsWith('redis://') && !str.startsWith('rediss://')) {
    str = (useTls ? 'rediss://' : 'redis://') + str;
  }

  try {
    const parsed = new URL(str);
    return {
      host: parsed.hostname,
      port: parsed.port ? parseInt(parsed.port, 10) : 6379,
      username: parsed.username ? decodeURIComponent(parsed.username) : undefined,
      password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
      tls: (parsed.protocol === 'rediss:' || useTls) ? { rejectUnauthorized: false } : undefined,
    };
  } catch {
    const match = str.replace(/^rediss?:\/\//, '').match(/^(?:([^:]+):([^@]+)@)?([^:]+)(?::(\d+))?$/);
    if (match) {
      const [, user, pass, host, port] = match;
      return {
        host,
        port: port ? parseInt(port, 10) : 6379,
        username: user ? decodeURIComponent(user) : undefined,
        password: pass ? decodeURIComponent(pass) : undefined,
        tls: useTls ? { rejectUnauthorized: false } : undefined,
      };
    }
    throw new Error('Invalid Redis connection string format');
  }
}

/**
 * Sanitize Redis connection strings for URL representation
 * @param {string} raw
 * @returns {string}
 */
export function sanitizeRedisUrl(raw) {
  if (!raw || typeof raw !== 'string') return raw;
  try {
    const cfg = parseRedisConfig(raw);
    const auth = cfg.username && cfg.password
      ? `${encodeURIComponent(cfg.username)}:${encodeURIComponent(cfg.password)}@`
      : (cfg.password ? `:${encodeURIComponent(cfg.password)}@` : '');
    const proto = cfg.tls ? 'rediss://' : 'redis://';
    return `${proto}${auth}${cfg.host}:${cfg.port}`;
  } catch {
    return raw.trim();
  }
}

/**
 * Sanitize PostgreSQL connection strings (strips channel_binding for Neon pooled connections)
 * @param {string} raw
 * @returns {string}
 */
export function sanitizeDatabaseUrl(raw) {
  if (!raw || typeof raw !== 'string') return raw;
  let trimmed = raw.trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    trimmed = trimmed.slice(1, -1).trim();
  }
  return trimmed
    .replace(/&channel_binding=require/g, '')
    .replace(/\?channel_binding=require&?/g, '?');
}

/**
 * Mask secret credentials in a connection string or URL for safe logging.
 * Replaces credentials with '***:***'.
 * @param {string} rawUrl
 * @returns {string}
 */
export function maskUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== 'string') return '';
  try {
    // Try Redis config first
    if (rawUrl.includes('redis') || rawUrl.includes('upstash') || !rawUrl.includes('postgres')) {
      const cfg = parseRedisConfig(rawUrl);
      const proto = cfg.tls ? 'rediss://' : 'redis://';
      const user = cfg.username || 'default';
      return `${proto}${user}:***@${cfg.host}:${cfg.port}`;
    }
  } catch {
    // fall through to generic URL masking
  }

  try {
    const parsed = new URL(sanitizeDatabaseUrl(rawUrl));
    if (parsed.password || parsed.username) {
      parsed.username = '***';
      parsed.password = '***';
    }
    return parsed.toString();
  } catch {
    const parts = rawUrl.split('://');
    if (parts.length === 2) {
      return `${parts[0]}://***`;
    }
    return '***';
  }
}

/**
 * Environment configuration schema
 */
const envSchema = z.object({
  DATABASE_URL: z
    .string()
    .min(1, 'DATABASE_URL is required')
    .transform(sanitizeDatabaseUrl),
  REDIS_URL: z
    .string()
    .min(1, 'REDIS_URL is required')
    .transform(sanitizeRedisUrl),
  API_KEY: z.string().min(1, 'API_KEY is required'),
  PORT: z.coerce.number().int().positive().default(10000),
  ROLE: z.enum(['all', 'api', 'worker']).default('all'),
  DEMO_MODE: z
    .string()
    .optional()
    .default('true')
    .transform((val) => val === 'true' || val === '1'),
  LOG_LEVEL: z.string().default('info'),
  WORKER_CONCURRENCY: z.coerce.number().int().min(1).max(50).default(5),
  MAX_ATTEMPTS: z.coerce.number().int().min(1).max(10).default(4),
  RETRY_BASE_MS: z.coerce.number().int().min(100).default(2000),
  RETRY_MAX_MS: z.coerce.number().int().min(1000).default(60000),
  VISIBILITY_TIMEOUT_MS: z.coerce.number().int().min(1000).default(30000),
  HANDLER_TIMEOUT_MS: z.coerce.number().int().min(500).default(8000),
  IDLE_POLL_MAX_MS: z.coerce.number().int().min(1000).default(60000),
  SWEEP_INTERVAL_MS: z.coerce.number().int().min(1000).default(30000),
  STREAM_MAXLEN: z.coerce.number().int().min(100).default(10000),
  DLQ_MAXLEN: z.coerce.number().int().min(100).default(1000),
});

/**
 * Load and validate configuration from process.env.
 * Prints clear error naming the missing variables (never their values).
 * @param {Record<string, string | undefined>} [sourceEnv=process.env]
 * @returns {z.infer<typeof envSchema>}
 */
export function loadConfig(sourceEnv = process.env) {
  const result = envSchema.safeParse(sourceEnv);

  if (!result.success) {
    const missingOrInvalid = result.error.issues.map((issue) => {
      const field = issue.path.join('.');
      return `${field} (${issue.message})`;
    });
    const message = `FATAL: Invalid environment configuration. Missing or invalid variable(s): ${missingOrInvalid.join(', ')}`;
    // Never print secret values
    process.stderr.write(message + '\n');
    throw new Error(message);
  }

  return result.data;
}

// Default singleton config (lazy loaded when imported directly)
let cachedConfig = null;
export function getConfig() {
  if (!cachedConfig) {
    cachedConfig = loadConfig(process.env);
  }
  return cachedConfig;
}
