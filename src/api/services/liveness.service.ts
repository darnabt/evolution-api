// darnabt: liveness routes + self-check (study SCAN-ROOT-CAUSE-v1 §5 (c) F5 and (d) F6).
// Nothing in this file logs a number out, deletes a session, removes a key file or pairs a device.
// It only (1) asks the socket for a real round trip, (2) asks WhatsApp which devices it lists for our
// own account, (3) ends the whole process so Railway starts it again (creds row + key files survive),
// and (4) after a database outage ends, ends only the sockets that no longer answer, so the existing
// close handler reconnects them once — escalating to (3) when that does not heal them.
import { prismaErrorListeners } from '@api/repository/repository.service';
import { prismaRepository, waMonitor } from '@api/server.module';
import { Integration } from '@api/types/wa.types';
import { Logger } from '@config/logger.config';
import { INSTANCE_DIR } from '@config/path.config';
import { jidDecode, jidNormalizedUser, S_WHATSAPP_NET, USyncQuery, USyncUser, WASocket } from 'baileys';
import { promises as fsp } from 'fs';
import { join } from 'path';

/**
 * LIVENESS_PROCESS_RESTART (default OFF): only the exact value «true» turns the process restart on.
 * Off → the POST /instance/processRestart route is not registered at all (an unknown route: the
 * framework's own 404, exactly as before this branch), and processRestart() refuses before any stamp
 * or exit, so the self-check heal can never end the process either. socketPing / deviceListed
 * (read-only) are not affected. Read at call time; the route list is fixed at boot.
 */
export function processRestartEnabled(): boolean {
  return String(process.env.LIVENESS_PROCESS_RESTART ?? '').toLowerCase() === 'true';
}

/** A real `w:p` ping must come back within this time, or the socket counts as dead. */
const PING_TIMEOUT_MS = 5_000;
/** «open» with a closed websocket is looked at again after this long before it counts as dead. */
const MID_RECONNECT_RECHECK_MS = 2_000;
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
 * A restart never lands during a pairing. A number is «pairing» while it shows a QR / pairing code
 * («connecting», no login in its creds) and «just paired» from the moment a login appears on a number
 * that had none until it has been open for 60 s (the new creds row + first-login keys are written in
 * that time), or 10 min at most. processRestart refuses (409) while any number is pairing or just
 * paired. If one starts in the 500 ms before the exit, the exit waits (checked every 2 s); still busy
 * after 120 s → the restart is cancelled, never forced.
 */
const PAIRING_WATCH_MS = 2_000;
const JUST_PAIRED_OPEN_SETTLE_MS = 60_000;
const JUST_PAIRED_MAX_MS = 10 * 60 * 1000;
const PROCESS_EXIT_PAIRING_HOLD_MAX_MS = 120_000;
const PROCESS_EXIT_PAIRING_POLL_MS = 2_000;

/**
 * Self-check: the database is probed with ONE tiny read every 30 s (the limit). A short outage between
 * two probes is learned from the errors other queries already hit (Prisma error events, no extra read);
 * the probe then only confirms the recovery.
 */
const SELF_CHECK_DB_PROBE_MS = 30_000;
const SELF_CHECK_DB_PROBE_TIMEOUT_MS = 10_000;
/** Prisma error texts that mean the database itself is not answering (P1001, P1017, P2024 and friends). */
const DB_DOWN_ERROR_PATTERNS = [
  /can't reach database server/i,
  /server has closed the connection/i,
  /timed out fetching a new connection/i,
  /connection pool/i,
  /\bP1001\b|\bP1017\b|\bP2024\b/,
  /ECONNREFUSED|ECONNRESET|ETIMEDOUT/,
  /terminating connection|connection terminated|connection reset/i,
];
/** WhatsApp answers after which the bridge does not reconnect; a process restart cannot help them. */
const NO_RECONNECT_CODES = [401, 402, 403, 406];
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
  /** The process exits this many ms after the answer (later only if a pairing starts in that time). */
  exitInMs: number;
  /** Always [] now: a pairing at call time is refused with 409. Kept so the answer shape stays the same. */
  waitsForPairing: string[];
};

class LivenessConflictException {
  constructor(...objectError: any[]) {
    throw {
      status: 409,
      error: 'Conflict',
      message: objectError.length > 0 ? objectError : undefined,
    };
  }
}

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

  private pairingWatchTimer: NodeJS.Timeout | null = null;
  /** Numbers seen with a socket but no login in their creds (a QR / pairing code may be out). */
  private readonly sawNoLogin = new Set<string>();
  /** Numbers whose login appeared after sawNoLogin → when. Cleared after 60 s open (or 10 min). */
  private readonly justPairedAt = new Map<string, number>();
  /** When each number was first seen «open» in a row. */
  private readonly openSince = new Map<string, number>();

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
    let { sock, state, reason } = this.socketOf(instanceName);
    let base = { instance: instanceName, state };

    if (!sock) return { ...base, ok: null, alive: null, ms: null, wsOpen: false, reason };

    let wsOpen = !!(sock as any).ws?.isOpen;
    if (state !== 'open') return { ...base, ok: null, alive: null, ms: null, wsOpen, reason: 'not_open' };
    if (!wsOpen) {
      // The websocket closed but the «close» event may still be waiting in the serial event queue (state
      // stays «open» until it is handled). Look again after 2 s: a routine reconnect is not a dead socket.
      await new Promise((resolve) => setTimeout(resolve, MID_RECONNECT_RECHECK_MS));
      ({ sock, state, reason } = this.socketOf(instanceName));
      base = { instance: instanceName, state };
      if (!sock) return { ...base, ok: null, alive: null, ms: null, wsOpen: false, reason };
      wsOpen = !!(sock as any).ws?.isOpen;
      if (state !== 'open') return { ...base, ok: null, alive: null, ms: null, wsOpen, reason: 'not_open' };
      if (!wsOpen) return { ...base, ok: false, alive: false, ms: null, wsOpen, reason: 'ws_not_open' };
    }

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
      // The socket started reconnecting during the ping: cannot say, not dead.
      const now = this.socketOf(instanceName);
      if (now.state !== 'open') {
        return {
          instance: instanceName,
          state: now.state,
          ok: null,
          alive: null,
          ms,
          wsOpen: !!(now.sock as any)?.ws?.isOpen,
          reason: now.sock ? 'not_open' : now.reason,
        };
      }
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
    if (!processRestartEnabled()) {
      throw new LivenessConflictException('Process restart is off (LIVENESS_PROCESS_RESTART is not true).');
    }
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
    // Never during a pairing: a QR / pairing code is out, or a new login is still being written.
    const busy = this.pairingBusyNames();
    if (busy.length > 0) {
      throw new LivenessConflictException(
        `A number is pairing or just paired (${busy.join(', ')}); a restart now could lose its new login.`,
        'Try again once it has been open for 60 s.',
      );
    }
    this.restartScheduled = true;

    try {
      await this.writeRestartStamp(now, by);
    } catch (error) {
      // Without a stamp nothing stops a restart loop, so refuse instead.
      this.restartScheduled = false;
      throw new LivenessInternalException('Could not record the restart stamp; restart refused.', String(error));
    }

    this.logger.warn(
      `processRestart (${by}): exit in ${PROCESS_EXIT_DELAY_MS} ms; Railway starts it again. No number is logged out.`,
    );
    this.scheduleExit(Date.now() + PROCESS_EXIT_PAIRING_HOLD_MAX_MS);

    return {
      restarting: true,
      at: new Date(now).toISOString(),
      previousAt: previous !== null ? new Date(previous).toISOString() : null,
      exitInMs: PROCESS_EXIT_DELAY_MS,
      waitsForPairing: [],
    };
  }

  private hasLogin(instance: any): boolean {
    return !!instance?.client?.authState?.creds?.me;
  }

  /** In memory only (no database): notes which numbers have no login, gain one, and stay open. */
  private watchPairing(): void {
    const all = waMonitor?.waInstances ?? {};
    const now = Date.now();
    for (const name of Object.keys(all)) {
      const instance = all[name];
      if (instance?.connectionStatus?.state === 'open') {
        if (!this.openSince.has(name)) this.openSince.set(name, now);
      } else {
        this.openSince.delete(name);
      }
      if (instance?.client && !this.hasLogin(instance)) {
        this.sawNoLogin.add(name);
        this.justPairedAt.delete(name);
      } else if (this.hasLogin(instance) && this.sawNoLogin.has(name)) {
        this.sawNoLogin.delete(name);
        this.justPairedAt.set(name, now);
        this.logger.info(`pairing: ${name} has a new login; no process restart until it has been open for 60 s`);
      }
      const pairedAt = this.justPairedAt.get(name);
      if (pairedAt !== undefined) {
        const openAt = this.openSince.get(name);
        const openFor = openAt !== undefined ? now - openAt : 0;
        if (openFor >= JUST_PAIRED_OPEN_SETTLE_MS || now - pairedAt >= JUST_PAIRED_MAX_MS) {
          this.justPairedAt.delete(name);
        }
      }
    }
    for (const map of [this.openSince, this.justPairedAt]) {
      for (const name of Array.from(map.keys())) if (!all[name]) map.delete(name);
    }
    for (const name of Array.from(this.sawNoLogin)) if (!all[name]) this.sawNoLogin.delete(name);
  }

  /** Runs the in-memory pairing watch every 2 s (always on; it does no database work). */
  private startPairingWatch(): void {
    if (this.pairingWatchTimer) return;
    this.pairingWatchTimer = setInterval(() => {
      try {
        this.watchPairing();
      } catch (error) {
        this.logger.warn(`pairing watch: ${String(error)}`);
      }
    }, PAIRING_WATCH_MS);
    this.pairingWatchTimer.unref?.();
  }

  /** Whether this number is pairing (QR / code out) or just paired (new login not yet open for 60 s). */
  private isPairingBusy(name: string): boolean {
    const instance = waMonitor?.waInstances?.[name];
    if (!instance) return false;
    const login = this.hasLogin(instance);
    if (instance.client && instance.connectionStatus?.state === 'connecting' && !login) return true;
    if (this.justPairedAt.has(name)) return true;
    // The login appeared after the last watch tick (≤ 2 s ago).
    return login && this.sawNoLogin.has(name);
  }

  private pairingBusyNames(): string[] {
    this.watchPairing();
    return Object.keys(waMonitor?.waInstances ?? {}).filter((name) => this.isPairingBusy(name));
  }

  /**
   * Exits after PROCESS_EXIT_DELAY_MS. If a pairing started in that time, waits (re-checked every 2 s); still
   * busy at the deadline → the restart is cancelled. It never exits during a pairing.
   */
  private scheduleExit(deadline: number): void {
    const tick = () => {
      const busy = this.pairingBusyNames();
      if (busy.length > 0 && Date.now() < deadline) {
        setTimeout(tick, PROCESS_EXIT_PAIRING_POLL_MS);
        return;
      }
      if (busy.length > 0) {
        this.restartScheduled = false;
        this.logger.warn(
          `processRestart: still pairing on ${busy.join(', ')} after 120 s; restart CANCELLED (never during a pairing)`,
        );
        return;
      }
      process.exit(1);
    };
    setTimeout(tick, PROCESS_EXIT_DELAY_MS);
  }

  // ---------------------------------------------------------------------------------------------
  // Self-check after a database recovery
  // ---------------------------------------------------------------------------------------------

  /**
   * Probes the database every 30 s (ONE tiny `Instance` read). A shorter outage is learned from the Prisma
   * errors other queries already hit. Nothing else happens while it answers. When an outage ends: wait 15 s,
   * ping every number the table calls «open»; end() only the dead sockets (ok:false; the existing close
   * handler reconnects each once); re-ping after 60 s and again after 120 s more. A socket still dead
   * (ok:false), or still reconnecting with its login after both waits → process restart (the proven cure,
   * study §2.4), under the same 10-minute stamp and never during a pairing. Turn off with
   * LIVENESS_SELF_CHECK=false (the pairing watch stays on).
   */
  public startSelfCheck(): void {
    this.startPairingWatch();
    if (this.selfCheckTimer) return;
    if (String(process.env.LIVENESS_SELF_CHECK ?? '').toLowerCase() === 'false') {
      this.logger.info('self-check off (LIVENESS_SELF_CHECK=false)');
      return;
    }
    prismaErrorListeners.push((message) => this.onDatabaseError(message));
    this.selfCheckTimer = setInterval(() => {
      this.probeDatabase().catch((error) => this.logger.error(`self-check: ${String(error)}`));
    }, SELF_CHECK_DB_PROBE_MS);
    this.selfCheckTimer.unref?.();
  }

  /** A query elsewhere hit a «database not answering» error: mark it down; the next probe confirms recovery. */
  private onDatabaseError(message: string): void {
    if (this.dbDown) return;
    if (!DB_DOWN_ERROR_PATTERNS.some((pattern) => pattern.test(message))) return;
    this.dbDown = true;
    this.logger.warn(
      `self-check: a query hit a database outage (${message.slice(0, 160).replace(/\s+/g, ' ')}); ` +
        'will check every socket once the database answers again',
    );
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

  /**
   * Not open, but still a number a restart can help: it has its login, WhatsApp did not refuse it
   * (401/402/403/406 → no reconnect), and it is not pairing.
   */
  private isStuckReconnecting(name: string): boolean {
    const instance = waMonitor?.waInstances?.[name];
    if (!instance?.client) return false;
    const state = instance.connectionStatus?.state;
    if (state !== 'connecting' && state !== 'close') return false;
    if (!this.hasLogin(instance)) return false;
    if (NO_RECONNECT_CODES.includes(Number(instance.connectionStatus?.statusReason))) return false;
    return !this.isPairingBusy(name);
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

      // Re-ping after each wait. ok:false → restart now. Still reconnecting with its login → next wait; still
      // not open after the last wait → stuck → restart. Dropped (a restart cannot help them): gone from this
      // process (not_loaded, …), WhatsApp said 401/402/403/406 (logged out / banned), or a QR / new login.
      let pending = dead;
      let stillDead: string[] = [];
      for (const waitMs of SELF_CHECK_RECHECK_MS) {
        await new Promise((resolve) => setTimeout(resolve, waitMs));
        const answers = await Promise.all(pending.map((name) => this.socketPing(name)));
        stillDead = answers.filter((answer) => answer.ok === false).map((answer) => answer.instance);
        pending = answers
          .filter((answer) => answer.ok === null && answer.reason === 'not_open')
          .map((answer) => answer.instance)
          .filter((name) => this.isStuckReconnecting(name));
        if (stillDead.length > 0 || pending.length === 0) break;
        this.logger.info(`self-check: still reconnecting, looking again later: ${pending.join(', ')}`);
      }
      if (stillDead.length === 0) stillDead = pending;

      if (stillDead.length === 0) {
        this.logger.info(`self-check: healed without a restart: ${dead.join(', ')}`);
        return;
      }

      if (!processRestartEnabled()) {
        this.logger.warn(`self-check: still dead or stuck: ${stillDead.join(', ')}; restart off, not restarting`);
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
