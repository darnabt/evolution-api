import { prismaRepository } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { CacheConf, configService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { INSTANCE_DIR } from '@config/path.config';
import { AuthenticationState, BufferJSON, initAuthCreds, WAProto as proto } from 'baileys';
import fs from 'fs/promises';
import path from 'path';

const fixFileName = (file: string): string | undefined => {
  if (!file) {
    return undefined;
  }
  const replacedSlash = file.replace(/\//g, '__');
  const replacedColon = replacedSlash.replace(/:/g, '-');
  return replacedColon;
};

export async function keyExists(sessionId: string): Promise<any> {
  try {
    const key = await prismaRepository.session.findUnique({ where: { sessionId: sessionId } });
    return !!key;
  } catch {
    return false;
  }
}

// darnabt 2026-11-18 (scan study F1): one upsert instead of exists + create/update. Before, a failed exists
// read answered "no row", the create then hit the unique key and the error was swallowed: the creds update
// (prekey counters, app-state key id) was lost without a line. Now it answers true/false and logs the reason.
export async function saveKey(sessionId: string, keyJson: any): Promise<boolean> {
  try {
    const creds = JSON.stringify(keyJson);
    await prismaRepository.session.upsert({
      where: { sessionId: sessionId },
      create: { sessionId: sessionId, creds },
      update: { creds },
    });
    return true;
  } catch (error) {
    logger.warn(`creds save failed: sessionId=${sessionId} reason=${error?.message ?? error}`);
    return false;
  }
}

export async function getAuthKey(sessionId: string): Promise<any> {
  try {
    const register = await keyExists(sessionId);
    if (!register) return null;
    const auth = await prismaRepository.session.findUnique({ where: { sessionId: sessionId } });
    return JSON.parse(auth?.creds);
  } catch {
    return null;
  }
}

async function deleteAuthKey(sessionId: string): Promise<any> {
  try {
    const register = await keyExists(sessionId);
    if (!register) return;
    await prismaRepository.session.delete({ where: { sessionId: sessionId } });
  } catch {
    return;
  }
}

async function fileExists(file: string): Promise<any> {
  try {
    const stat = await fs.stat(file);
    if (stat.isFile()) return true;
  } catch {
    return;
  }
}

const logger = new Logger('useMultiFileAuthStatePrisma');

// darnabt 2026-11-18 (scan study F1): a key file is written to a temp name and renamed over the real one, so a
// SIGTERM / crash mid-write (every deploy) can never leave a half-written file that reads back as "missing"
// (Bad MAC, "app-state key missing"). The temp name never matches a key name, so a leftover is never read.
async function writeFileAtomic(file: string, dataString: string) {
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  try {
    await fs.writeFile(tmp, dataString);
    await fs.rename(tmp, file);
  } catch (error) {
    await fs.unlink(tmp).catch(() => undefined);
    throw error;
  }
}

// darnabt 2026-11-18 (scan study F1): creds saves go through ONE writer per instance (shared by every auth
// state built for it, so an old socket's save and a new socket's save never race) that always writes the
// NEWEST creds. A save that fails (database down) is retried (2 s, 4 s … max 30 s, ~12 min in all) instead of
// being dropped, and a newer save replaces the pending one, so an old retry can never land after a newer save.
// The same creds are mirrored to <instance>/creds.json (atomic) as a copy that survives a lost row; it is NOT
// read back by the bridge. A logout (removeCreds) drops whatever is still pending.
const credsPending = new Map<string, string>();
const credsWriter = new Map<string, Promise<void>>();

function saveCredsLatest(sessionId: string, dataString: string, mirrorFile: string): Promise<void> {
  credsPending.set(sessionId, dataString);
  writeFileAtomic(mirrorFile, dataString).catch((error) =>
    logger.warn(`creds file mirror failed: sessionId=${sessionId} reason=${error?.message ?? error}`),
  );
  const running = credsWriter.get(sessionId);
  if (running) return running;
  const writer = (async () => {
    let failures = 0;
    while (credsPending.has(sessionId)) {
      const current = credsPending.get(sessionId);
      if (await saveKey(sessionId, current)) {
        if (credsPending.get(sessionId) === current) credsPending.delete(sessionId);
        if (failures) logger.warn(`creds saved after ${failures} failed tries: sessionId=${sessionId}`);
        failures = 0;
      } else {
        failures++;
        if (failures === 5) logger.error(`creds save still failing after 5 tries, still retrying: sessionId=${sessionId}`);
        if (failures >= 30) {
          // e.g. the instance row was deleted; the next creds.update carries the whole newest creds again
          logger.error(`creds save gave up after ${failures} tries: sessionId=${sessionId}`);
          if (credsPending.get(sessionId) === current) credsPending.delete(sessionId);
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, Math.min(failures * 2000, 30_000)));
      }
    }
  })().finally(() => {
    credsWriter.delete(sessionId);
    // a save that came in after the loop's last check (or after it gave up) is not left unsaved
    const late = credsPending.get(sessionId);
    if (late !== undefined) saveCredsLatest(sessionId, late, mirrorFile);
  });
  credsWriter.set(sessionId, writer);
  return writer;
}

export default async function useMultiFileAuthStatePrisma(
  sessionId: string,
  cache: CacheService,
): Promise<{
  state: AuthenticationState;
  saveCreds: () => Promise<void>;
  removeCreds: () => Promise<void>;
}> {
  const localFolder = path.join(INSTANCE_DIR, sessionId);
  const localFile = (key: string) => path.join(localFolder, fixFileName(key) + '.json');
  await fs.mkdir(localFolder, { recursive: true });

  async function writeData(data: any, key: string): Promise<any> {
    const dataString = JSON.stringify(data, BufferJSON.replacer);
    const cacheConfig = configService.get<CacheConf>('CACHE');

    if (key != 'creds') {
      if (cacheConfig.REDIS.ENABLED) {
        return await cache.hSet(sessionId, key, data);
      } else {
        await writeFileAtomic(localFile(key), dataString);
        return;
      }
    }
    // darnabt: never write empty creds over a saved login.
    if (!data) return;
    await saveCredsLatest(sessionId, dataString, localFile('creds'));
    return;
  }

  async function readData(key: string): Promise<any> {
    try {
      let rawData;
      const cacheConfig = configService.get<CacheConf>('CACHE');

      if (key != 'creds') {
        if (cacheConfig.REDIS.ENABLED) {
          return await cache.hGet(sessionId, key);
        } else {
          if (!(await fileExists(localFile(key)))) return null;
          rawData = await fs.readFile(localFile(key), { encoding: 'utf-8' });
          try {
            return JSON.parse(rawData, BufferJSON.reviver);
          } catch (error) {
            // darnabt 2026-11-18 (scan study F1): a file that exists but cannot be parsed is said out loud
            // (name and size only, never the content) instead of silently reading as "missing"
            logger.warn(
              `auth key file unreadable: sessionId=${sessionId} key=${key.replace(/\d{5,}/g, '…')} bytes=${rawData?.length ?? 0} reason=${
                error?.message ?? error
              }`,
            );
            return null;
          }
        }
      } else {
        rawData = await getAuthKey(sessionId);
      }

      const parsedData = JSON.parse(rawData, BufferJSON.reviver);
      return parsedData;
    } catch {
      return null;
    }
  }

  async function removeData(key: string): Promise<any> {
    try {
      const cacheConfig = configService.get<CacheConf>('CACHE');

      if (key != 'creds') {
        if (cacheConfig.REDIS.ENABLED) {
          return await cache.hDelete(sessionId, key);
        } else {
          await fs.unlink(localFile(key));
        }
      } else {
        await deleteAuthKey(sessionId);
      }
    } catch {
      return;
    }
  }

  async function removeCreds(): Promise<any> {
    const cacheConfig = configService.get<CacheConf>('CACHE');

    // Redis
    try {
      if (cacheConfig.REDIS.ENABLED) {
        await cache.delete(sessionId);
        logger.info({ action: 'redis.delete', sessionId });

        return;
      }
    } catch (err) {
      logger.warn({ action: 'redis.delete', sessionId, err });
    }

    logger.info({ action: 'auth.key.delete', sessionId });

    // darnabt 2026-11-18: a creds save still retrying must not write the logged-out creds back after this delete
    credsPending.delete(sessionId);
    await deleteAuthKey(sessionId);
  }

  // darnabt: a database hiccup must never cost the WhatsApp login. Only a read that WORKED and found no row
  // means "new instance". A failed read (error, timeout, empty or unreadable row) throws, so the connection
  // attempt fails and the reconnect path tries again later; it never falls through to initAuthCreds().
  async function readSavedCreds(): Promise<{ found: boolean; creds?: any }> {
    let lastError: any;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const row = await prismaRepository.session.findUnique({ where: { sessionId: sessionId } });
        if (!row) return { found: false };
        if (!row.creds) throw new Error('saved creds row is empty');
        const rawData = JSON.parse(row.creds);
        const parsedData = JSON.parse(rawData, BufferJSON.reviver);
        if (!parsedData) throw new Error('saved creds could not be read');
        return { found: true, creds: parsedData };
      } catch (error) {
        lastError = error;
        if (attempt < 3) await new Promise((resolve) => setTimeout(resolve, attempt * 2000));
      }
    }
    const reason = lastError?.message ?? lastError;
    logger.error(`auth read failed, keeping saved login, will retry: sessionId=${sessionId} reason=${reason}`);
    throw new Error('auth read failed, keeping saved login, will retry');
  }

  const saved = await readSavedCreds();
  let creds = saved.creds;
  if (!saved.found) {
    creds = initAuthCreds();
    await writeData(creds, 'creds');
  }

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                // darnabt: fromObject, not create — the key is saved through the proto toJSON, so keyData is a
                // base64 STRING on disk; create() kept the string and every app-state key read back from
                // disk (after the 5-min key cache) was garbage ("bad decrypt" / "Invalid patch mac").
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }

              data[id] = value;
            }),
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;

              tasks.push(value ? writeData(value, key) : removeData(key));
            }
          }
          await Promise.all(tasks);
        },
      },
    },
    saveCreds: () => {
      return writeData(creds, 'creds');
    },

    removeCreds,
  };
}
