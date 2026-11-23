/**
 * Custom error hierarchy for EventFlow execution
 */

export class TimeoutError extends Error {
  constructor(message = 'Handler execution timed out') {
    super(message);
    this.name = 'TimeoutError';
  }
}

export class PermanentError extends Error {
  constructor(message = 'Permanent unrecoverable handler error') {
    super(message);
    this.name = 'PermanentError';
  }
}

export class CrashSimulated extends Error {
  constructor(message = 'Simulated worker crash during execution') {
    super(message);
    this.name = 'CrashSimulated';
  }
}
