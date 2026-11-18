// darnabt: liveness routes + self-check (study SCAN-ROOT-CAUSE-v1 §5 (c) F5 and (d) F6).
// Nothing in this file logs a number out, deletes a session, removes a key file or pairs a device.
// It only (1) asks the socket for a real round trip, (2) asks WhatsApp which devices it lists for our
// own account, (3) ends the whole process so Railway starts it again (creds row + key files survive),
// and (4) after a database outage ends, ends only the sockets that no longer answer, so the existing
// close handler reconnects them once — escalating to (3) when that does not heal them.
import { prismaRepository, waMonitor } from '@api/server.module';
import { Integration } from '@api/types/wa.types';
import { Logger } from '@config/logger.config';
import { INSTANCE_DIR } from '@config/path.config';
import { jidDecode, jidNormalizedUser, S_WHATSAPP_NET, USyncQuery, USyncUser, WASocket } from 'baileys';
import { promises as fsp } from 'fs';
import { join } from 'path';

/** A real `w:p` ping must come back within this time, or the socket counts as dead. */
const PING_TIMEOUT_MS = 5_000;
/** The device-list (usync) question gets a little longer: it is a bigger answer. */
const DEVICE_LIST_TIMEOUT_MS = 10_000;
/** A second process restart within this window is refused with 429. */
const PROCESS_RESTART_MIN_GAP_MS = 10 * 60 * 1000;
/** The answer goes out first; the process ends this long after. */
const PROCESS_EXIT_DELAY_MS = 500;
/**
 * The stamp of the last process restart. INSTANCE_DIR is `/evolution/instances`, the Railway volume
 * (study §2.1), so the stamp survives the restart it records. A dot-file: nothing lists this folder
 * as instances (instances are loaded from the database).
 */
const PROCESS_RESTART_STAMP = join(INSTANCE_DIR, '.liveness-process-restart.json');

/**
 * While a number shows a QR / pairing code (state «connecting», no login yet), the exit waits — checking
 * every 5 s — so a restart never kills a scan in progress. It exits anyway after this long.
 */
const PROCESS_EXIT_PAIRING_HOLD_MAX_MS = 120_000;
const PROCESS_EXIT_PAIRING_POLL_MS = 5_000;

/**
 * Self-check: how often the database is probed (one tiny read; no other work while it answers).
 * 10 s so a ~10-20 s outage (11-15 event E) cannot fall between two probes.
 */
const SELF_CHECK_DB_PROBE_MS = 10_000;
const SELF_CHECK_DB_PROBE_TIMEOUT_MS = 5_000;
/** After the database answers again, give the sockets this long to settle before pinging them. */
const SELF_CHECK_SETTLE_MS = 15_000;
/**
 * After ending the dead sockets, re-ping them after each of these waits. A socket that claims open but
 * does not answer (ok:false) → process restart at once. One still reconnecting («connecting», ok:null) is
 * given the next wait; only one still not open after all of them (~3 min) counts as stuck → restart.
 */
const SELF_CHECK_RECHECK_MS = [60_000, 120_000];

export type SocketPingAnswer = {
  instance: string;
  /**
   * true  = the socket answered a real ping in time.
   * false = the number claims «open» but its socket is dead (ws_not_open, timeout, error: …).
   * null  = cannot say: the number is not open yet (reconnecting, QR on screen) or not loaded here
   *         (not_open, not_loaded, no_socket, not_baileys). Never read null as dead.
   * Nawah reads `ok`.
   */
  ok: boolean | null;
  /** Same value as `ok` (true / false / null). */
  alive: boolean | null;
  /** Round-trip time of the ping in ms (also set when it timed out / failed: time spent waiting). */
  ms: number | null;
  /** The bridge's own connection state for the number (`open`, `connecting`, `close`). */
  state: string | null;
  /** Whether the websocket under the number reports itself open. */
  wsOpen: boolean;
  /**
   * Why it is not alive.
   * ok:false → ws_not_open | timeout | error: …
   * ok:null  → not_open | not_loaded | no_socket | not_baileys
   */
  reason?: string;
};

export type DeviceListedAnswer = {
  instance: string;
  /** true / false only from a fresh answer of WhatsApp; 'unknown' whenever it could not be asked. */
  listed: true | false | 'unknown';
  /** Our own device id (e.g. 80 for …:80@s.whatsapp.net), when known. */
  ownDevice: number | null;
  /** Every device id WhatsApp lists for the account right now (0 = the phone). Raw, unfiltered. */
  devices: number[];
  /** The listed device ids that are Meta-hosted. */
  hostedDevices: number[];
  /** The account we asked about (our own id, device part dropped), when known. */
  askedId: string | null;
  /** The id of the usync entry the device list was read from (proves it is our own account). */
  answeredId: string | null;
  /** The bridge's own connection state for the number at the time of asking. */
  state: string | null;
  ms: number | null;
  /** Why the answer is 'unknown'. */
  reason?: string;
};

export type ProcessRestartAnswer = {
  restarting: true;
  /** ISO time this restart was stamped. */
  at: string;
  /** ISO time of the previous restart, when one was stamped. */
  previousAt: string | null;
  /** The process exits this many ms after the answer at the earliest (later while a QR is out). */
  exitInMs: number;
  /** Numbers showing a QR right now; the exit waits for them (at most 120 s). Usually []. */
  waitsForPairing: string[];
};

class TooManyRequestsException {
  constructor(...objectError: any[]) {
    throw {
      status: 429,
      error: 'Too Many Requests',
      message: objectError.length > 0 ? objectError : undefined,
    };
  }
}

class LivenessInternalException {
  constructor(...objectError: any[]) {
    throw {
      status: 500,
      error: 'Internal Server Error',
      message: objectError.length > 0 ? objectError : undefined,
    };
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timeout after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function isTimeout(error: unknown): boolean {
  const text = String((error as any)?.message ?? error ?? '').toLowerCase();
  return text.includes('timed out') || text.includes('timeout');
}

export class LivenessService {
  private readonly logger = new Logger('LivenessService');

  /** Set once a restart is scheduled in this process, so two calls in the same second cannot both pass. */
  private restartScheduled = false;

  private selfCheckTimer: NodeJS.Timeout | null = null;
  private dbProbeRunning = false;
  private dbDown = false;
  private healRunning = false;

  // ---------------------------------------------------------------------------------------------
  // Socket ping
  // ---------------------------------------------------------------------------------------------

  /** Finds the Baileys socket of a loaded instance, or says why there is none. */
  private socketOf(instanceName: string): { sock: WASocket | null; state: string | null; reason?: string } {
    const instance = waMonitor?.waInstances?.[instanceName];
    if (!instance) return { sock: null, state: null, reason: 'not_loaded' };
    const state: string | null = instance.connectionStatus?.state ?? null;
    if (instance.integration && instance.integration !== Integration.WHATSAPP_BAILEYS) {
      return { sock: null, state, reason: 'not_baileys' };
    }
    const sock: WASocket | undefined = instance.client;
    if (!sock) return { sock: null, state, reason: 'no_socket' };
    return { sock, state };
  }

  /**
   * A real round trip on the number's socket: the same `iq xmlns="w:p" <ping/>` Baileys sends as its
   * keep-alive, answered by WhatsApp's server. «open» in the table with a closed websocket, or a
   * websocket that does not answer within 5 s, is `ok:false`. A number that is not open yet (reconnecting,
   * QR on screen) or not loaded is `ok:null` — the bridge cannot say, so it never claims dead.
   */
  public async socketPing(instanceName: string): Promise<SocketPingAnswer> {
    const { sock, state, reason } = this.socketOf(instanceName);
    const base = { instance: instanceName, state };

    if (!sock) return { ...base, ok: null, alive: null, ms: null, wsOpen: false, reason };

    const wsOpen = !!(sock as any).ws?.isOpen;
    if (state !== 'open') return { ...base, ok: null, alive: null, ms: null, wsOpen, reason: 'not_open' };
    if (!wsOpen) return { ...base, ok: false, alive: false, ms: null, wsOpen, reason: 'ws_not_open' };

    const started = Date.now();
    try {
      await withTimeout(
        sock.query(
          {
            tag: 'iq',
            attrs: { to: S_WHATSAPP_NET, type: 'get', xmlns: 'w:p' },
            content: [{ tag: 'ping', attrs: {} }],
          },
          PING_TIMEOUT_MS,
        ),
        PING_TIMEOUT_MS + 500,
        'ping',
      );
      return { ...base, ok: true, alive: true, ms: Date.now() - started, wsOpen };
    } catch (error) {
      const ms = Date.now() - started;
      const why = isTimeout(error) ? 'timeout' : `error: ${String((error as any)?.message ?? error)}`;
      this.logger.warn(`socketPing ${instanceName}: no pong after ${ms} ms (${why})`);
      return { ...base, ok: false, alive: false, ms, wsOpen: !!(sock as any).ws?.isOpen, reason: why };
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Device listed (F6)
  // ---------------------------------------------------------------------------------------------

  /**
   * Asks WhatsApp afresh (usync, no cache, nothing stored) which devices it lists for our own account,
   * and whether our own device id is among them. `false` only from a real answer that lacks our id;
   * every failure to ask is `'unknown'` — never a guessed false.
   */
  public async deviceListed(instanceName: string): Promise<DeviceListedAnswer> {
    const { sock, state, reason } = this.socketOf(instanceName);
    let askedId: string | null = null;

    const unknown = (
      reason: string,
      ownDevice: number | null = null,
      ms: number | null = null,
    ): DeviceListedAnswer => ({
      instance: instanceName,
      listed: 'unknown',
      ownDevice,
      devices: [],
      hostedDevices: [],
      askedId,
      answeredId: null,
      state,
      ms,
      reason,
    });

    if (!sock) return unknown(reason);

    const meId: string | undefined = sock.user?.id ?? sock.authState?.creds?.me?.id;
    const me = meId ? jidDecode(meId) : undefined;
    const ownDevice = typeof me?.device === 'number' ? me.device : null;
    if (!meId || !me?.user) return unknown('no_own_id');
    askedId = jidNormalizedUser(meId);
    if (ownDevice === null) return unknown('no_own_device_id');

    if (!(sock as any).ws?.isOpen) return unknown('ws_not_open', ownDevice);
    if (state !== 'open') return unknown('not_open', ownDevice);

    const started = Date.now();
    try {
      const query = new USyncQuery()
        .withContext('message')
        .withDeviceProtocol()
        .withUser(new USyncUser().withId(askedId));

      const result = await withTimeout(sock.executeUSyncQuery(query), DEVICE_LIST_TIMEOUT_MS, 'deviceListed');
      const ms = Date.now() - started;
      if (!result || !Array.isArray(result.list)) return unknown('no_answer', ownDevice, ms);

      const entry =
        result.list.find((item) => jidDecode(item?.id)?.user === me.user) ??
        (result.list.length === 1 ? result.list[0] : undefined);
      const deviceList = (entry as any)?.devices?.deviceList;
      if (!Array.isArray(deviceList)) return unknown('no_device_list', ownDevice, ms);
      // WhatsApp always lists the phone (0) for a live account; an empty list is not a real answer.
      if (deviceList.length === 0) return unknown('empty_device_list', ownDevice, ms);

      const devices: number[] = deviceList.map((d: any) => Number(d?.id)).filter((n: number) => Number.isFinite(n));
      const hostedDevices: number[] = deviceList
        .filter((d: any) => d?.isHosted)
        .map((d: any) => Number(d?.id))
        .filter((n: number) => Number.isFinite(n));
      const listed = devices.includes(ownDevice);

      if (!listed) {
        const listedText = devices.join(', ');
        this.logger.warn(`deviceListed ${instanceName}: WhatsApp does NOT list device :${ownDevice} (${listedText})`);
      }

      const answeredId: string | null = typeof (entry as any)?.id === 'string' ? (entry as any).id : null;
      return { instance: instanceName, listed, ownDevice, devices, hostedDevices, askedId, answeredId, state, ms };
    } catch (error) {
      const ms = Date.now() - started;
      const why = isTimeout(error) ? 'timeout' : `error: ${String((error as any)?.message ?? error)}`;
      this.logger.warn(`deviceListed ${instanceName}: could not ask (${why})`);
      return unknown(why, ownDevice, ms);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Process restart (F5)
  // ---------------------------------------------------------------------------------------------

  private async readRestartStamp(): Promise<number | null> {
    try {
      const raw = await fsp.readFile(PROCESS_RESTART_STAMP, 'utf8');
      const at = Number(JSON.parse(raw)?.at);
      return Number.isFinite(at) ? at : null;
    } catch (error) {
      if ((error as any)?.code !== 'ENOENT') {
        this.logger.warn(`processRestart: stamp unreadable, treated as none (${String(error)})`);
      }
      return null;
    }
  }

  /** Written to a temp file and renamed, so a half-written stamp never exists. */
  private async writeRestartStamp(at: number, by: string): Promise<void> {
    await fsp.mkdir(INSTANCE_DIR, { recursive: true });
    const tmp = `${PROCESS_RESTART_STAMP}.${process.pid}.tmp`;
    await fsp.writeFile(tmp, JSON.stringify({ at, iso: new Date(at).toISOString(), by, pid: process.pid }));
    await fsp.rename(tmp, PROCESS_RESTART_STAMP);
  }

  /**
   * The bridge ends its own process; Railway starts it again. No logout, no session or key file is
   * touched: the next boot loads every number from the creds row + the volume, as after any deploy.
   * Refused (429) when one was stamped in the last 10 minutes — also across the restart itself.
   */
  public async processRestart(by = 'api'): Promise<ProcessRestartAnswer> {
    if (this.restartScheduled) {
      throw new TooManyRequestsException('A process restart is already under way.');
    }

    const now = Date.now();
    const previous = await this.readRestartStamp();
    if (previous !== null && now - previous < PROCESS_RESTART_MIN_GAP_MS) {
      const waitS = Math.ceil((PROCESS_RESTART_MIN_GAP_MS - (now - previous)) / 1000);
      throw new TooManyRequestsException(
        `The last process restart was at ${new Date(previous).toISOString()}; at most one per 10 minutes.`,
        `Try again in ${waitS} s.`,
      );
    }

    // Re-check after the await: a second call may have scheduled one meanwhile.
    if (this.restartScheduled) {
      throw new TooManyRequestsException('A process restart is already under way.');
    }
    this.restartScheduled = true;

    try {
      await this.writeRestartStamp(now, by);
    } catch (error) {
      // Without a stamp nothing stops a restart loop, so refuse instead.
      this.restartScheduled = false;
      throw new LivenessInternalException('Could not record the restart stamp; restart refused.', String(error));
    }

    const waitsForPairing = this.pairingInstanceNames();
    this.logger.warn(
      `processRestart (${by}): exit in ${PROCESS_EXIT_DELAY_MS} ms` +
        (waitsForPairing.length ? ` (later: waiting for a QR on ${waitsForPairing.join(', ')}, at most 120 s)` : '') +
        '; Railway starts it again. No number is logged out.',
    );
    this.scheduleExit(Date.now() + PROCESS_EXIT_PAIRING_HOLD_MAX_MS);

    return {
      restarting: true,
      at: new Date(now).toISOString(),
      previousAt: previous !== null ? new Date(previous).toISOString() : null,
      exitInMs: PROCESS_EXIT_DELAY_MS,
      waitsForPairing,
    };
  }

  /** Numbers showing a QR / pairing code right now: «connecting» with no login in their creds yet. */
  private pairingInstanceNames(): string[] {
    const all = waMonitor?.waInstances ?? {};
    return Object.keys(all).filter((name) => {
      const instance = all[name];
      return (
        instance?.client && instance.connectionStatus?.state === 'connecting' && !instance.client.authState?.creds?.me
      );
    });
  }

  /** Exits after PROCESS_EXIT_DELAY_MS, or later while a QR is out (re-checked every 5 s, until the deadline). */
  private scheduleExit(deadline: number): void {
    const tick = () => {
      const pairing = this.pairingInstanceNames();
      if (pairing.length > 0 && Date.now() < deadline) {
        setTimeout(tick, PROCESS_EXIT_PAIRING_POLL_MS);
        return;
      }
      if (pairing.length > 0) {
        this.logger.warn(`processRestart: QR still out on ${pairing.join(', ')} after 120 s; exiting anyway`);
      }
      process.exit(1);
    };
    setTimeout(tick, PROCESS_EXIT_DELAY_MS);
  }

  // ---------------------------------------------------------------------------------------------
  // Self-check after a database recovery
  // ---------------------------------------------------------------------------------------------

  /**
   * Probes the database every 10 s (one tiny `Instance` read). Nothing else happens while it answers. When an
   * outage ends: wait 15 s, ping every number the table calls «open»; end() only the dead sockets (ok:false;
   * the existing close handler reconnects each once); re-ping after 60 s and again after 120 s more. A socket
   * still dead (ok:false), or still not open after both waits → process restart (the proven cure, study
   * §2.4), under the same 10-minute stamp. Turn off with LIVENESS_SELF_CHECK=false.
   */
  public startSelfCheck(): void {
    if (this.selfCheckTimer) return;
    if (String(process.env.LIVENESS_SELF_CHECK ?? '').toLowerCase() === 'false') {
      this.logger.info('self-check off (LIVENESS_SELF_CHECK=false)');
      return;
    }
    this.selfCheckTimer = setInterval(() => {
      this.probeDatabase().catch((error) => this.logger.error(`self-check: ${String(error)}`));
    }, SELF_CHECK_DB_PROBE_MS);
    this.selfCheckTimer.unref?.();
  }

  private async probeDatabase(): Promise<void> {
    if (this.dbProbeRunning) return;
    this.dbProbeRunning = true;
    try {
      let up = true;
      try {
        // A plain model read (not $queryRaw): it goes through the same path as every other query here.
        await withTimeout(
          prismaRepository.instance.findFirst({ select: { id: true } }),
          SELF_CHECK_DB_PROBE_TIMEOUT_MS,
          'db probe',
        );
      } catch {
        up = false;
      }

      if (!up && !this.dbDown) {
        this.dbDown = true;
        this.logger.warn('self-check: database not answering; will check every socket once it is back');
        return;
      }

      if (up && this.dbDown) {
        this.dbDown = false;
        this.logger.warn('self-check: database answers again; checking sockets');
        setTimeout(() => {
          this.healAfterDatabaseRecovery().catch((error) => this.logger.error(`self-check heal: ${String(error)}`));
        }, SELF_CHECK_SETTLE_MS).unref?.();
      }
    } finally {
      this.dbProbeRunning = false;
    }
  }

  private openBaileysInstanceNames(): string[] {
    const all = waMonitor?.waInstances ?? {};
    return Object.keys(all).filter((name) => {
      const instance = all[name];
      return (
        instance?.client &&
        (!instance.integration || instance.integration === Integration.WHATSAPP_BAILEYS) &&
        instance.connectionStatus?.state === 'open'
      );
    });
  }

  private async healAfterDatabaseRecovery(): Promise<void> {
    if (this.healRunning) return;
    this.healRunning = true;
    try {
      const names = this.openBaileysInstanceNames();
      const first = await Promise.all(names.map((name) => this.socketPing(name)));
      // Only a socket that claims open but does not answer is dead; ok:null (cannot say) is left alone.
      const dead = first.filter((answer) => answer.ok === false).map((answer) => answer.instance);

      if (dead.length === 0) {
        this.logger.info(`self-check: all ${names.length} open sockets answer after the database recovery`);
        return;
      }

      for (const name of dead) {
        const client: WASocket | undefined = waMonitor?.waInstances?.[name]?.client;
        this.logger.warn(`self-check: ${name} does not answer after the database recovery; ending its socket once`);
        try {
          client?.end(new Error('liveness self-check: dead socket after database recovery'));
        } catch (error) {
          this.logger.warn(`self-check: end() on ${name} threw: ${String(error)}`);
        }
      }

      // Re-ping after each wait. ok:false → restart now. Still reconnecting (not_open) → next wait; still not
      // open after the last wait → stuck → restart. Gone from this process (not_loaded, …) → dropped.
      let pending = dead;
      let stillDead: string[] = [];
      for (const waitMs of SELF_CHECK_RECHECK_MS) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
        const answers = await Promise.all(pending.map((name) => this.socketPing(name)));
        stillDead = answers.filter((answer) => answer.ok === false).map((answer) => answer.instance);
        pending = answers
          .filter((answer) => answer.ok === null && answer.reason === 'not_open')
          .map((answer) => answer.instance);
        if (stillDead.length > 0 || pending.length === 0) break;
        this.logger.info(`self-check: still reconnecting, looking again later: ${pending.join(', ')}`);
      }
      if (stillDead.length === 0) stillDead = pending;

      if (stillDead.length === 0) {
        this.logger.info(`self-check: healed without a restart: ${dead.join(', ')}`);
        return;
      }

      this.logger.warn(`self-check: still dead or stuck after reconnect: ${stillDead.join(', ')}; restarting`);
      try {
        await this.processRestart('self-check');
      } catch (error) {
        // 429 (a restart in the last 10 min) or no stamp: leave it to Nawah's watchdog.
        this.logger.warn(`self-check: process restart refused: ${JSON.stringify(error)}`);
      }
    } finally {
      this.healRunning = false;
    }
  }
}

export const livenessService = new LivenessService();
