// Keeps credentials out of log lines. Use on anything that is logged and may carry
// an instance token, the global API key, webhook auth headers or provider credentials.
const SECRET_KEY = /^(apikey|api_key|apikeyglobal|token|auth_token|access_token|authorization|jwt_key|secret|password|accesskey|secretaccesskey|hash)$/i;

export function redactSecrets<T>(value: T, depth = 0): T {
  if (value === null || typeof value !== 'object' || depth > 8) return value;
  if (Array.isArray(value)) return value.map((v) => redactSecrets(v, depth + 1)) as unknown as T;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    out[k] = SECRET_KEY.test(k) && v ? '***' : redactSecrets(v, depth + 1);
  }
  return out as T;
}

// For JSON.stringify(x, redactReplacer)
export function redactReplacer(key: string, value: unknown): unknown {
  return SECRET_KEY.test(key) && value ? '***' : value;
}
