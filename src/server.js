/**
 * EventFlow Server Stub (Phase 3)
 * Full Fastify HTTP API and routes will be implemented in Phase 4.
 */
import { getConfig } from './config.js';

const config = getConfig();
process.stdout.write(`[EventFlow Stub] Loaded configuration for role: ${config.ROLE}\n`);
