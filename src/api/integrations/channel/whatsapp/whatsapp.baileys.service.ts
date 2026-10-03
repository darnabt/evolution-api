import { getCollectionsDto } from '@api/dto/business.dto';
import { OfferCallDto } from '@api/dto/call.dto';
import {
  ArchiveChatDto,
  BlockUserDto,
  DeleteMessage,
  getBase64FromMediaMessageDto,
  LastMessage,
  MarkChatUnreadDto,
  NumberBusiness,
  OnWhatsAppDto,
  PrivacySettingDto,
  ReadMessageDto,
  SendPresenceDto,
  UpdateMessageDto,
  WhatsAppNumberDto,
} from '@api/dto/chat.dto';
import {
  AcceptGroupInvite,
  CreateGroupDto,
  GetParticipant,
  GroupDescriptionDto,
  GroupInvite,
  GroupJid,
  GroupPictureDto,
  GroupSendInvite,
  GroupSubjectDto,
  GroupToggleEphemeralDto,
  GroupUpdateParticipantDto,
  GroupUpdateSettingDto,
} from '@api/dto/group.dto';
import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { HandleLabelDto, LabelDto } from '@api/dto/label.dto';
import {
  Button,
  ContactMessage,
  KeyType,
  MediaMessage,
  Options,
  SendAudioDto,
  SendButtonsDto,
  SendContactDto,
  SendListDto,
  SendLocationDto,
  SendMediaDto,
  SendPollDto,
  SendPtvDto,
  SendReactionDto,
  SendStatusDto,
  SendStickerDto,
  SendTextDto,
  StatusMessage,
  TypeButton,
} from '@api/dto/sendMessage.dto';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import * as s3Service from '@api/integrations/storage/s3/libs/minio.server';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository, Query } from '@api/repository/repository.service';
import { chatbotController, waMonitor } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { ChannelStartupService } from '@api/services/channel.service';
import { Events, MessageSubtype, TypeMediaMessage, wa } from '@api/types/wa.types';
import { CacheEngine } from '@cache/cacheengine';
import {
  AudioConverter,
  CacheConf,
  Chatwoot,
  ConfigService,
  configService,
  ConfigSessionPhone,
  Database,
  Log,
  Openai,
  ProviderSession,
  QrCode,
  S3,
} from '@config/env.config';
import { BadRequestException, InternalServerErrorException, NotFoundException } from '@exceptions';
import ffmpegPath from '@ffmpeg-installer/ffmpeg';
import { Boom } from '@hapi/boom';
import { createId as cuid } from '@paralleldrive/cuid2';
import { Instance, Message } from '@prisma/client';
import { createJid } from '@utils/createJid';
import { fetchLatestWaWebVersion } from '@utils/fetchLatestWaWebVersion';
import { makeProxyAgent, makeProxyAgentUndici } from '@utils/makeProxyAgent';
import { getOnWhatsappCache, saveOnWhatsappCache } from '@utils/onWhatsappCache';
import { status } from '@utils/renderStatus';
import { sendTelemetry } from '@utils/sendTelemetry';
import useMultiFileAuthStatePrisma from '@utils/use-multi-file-auth-state-prisma';
import { AuthStateProvider } from '@utils/use-multi-file-auth-state-provider-files';
import { useMultiFileAuthStateRedisDb } from '@utils/use-multi-file-auth-state-redis-db';
import axios from 'axios';
import makeWASocket, {
  aesDecrypt,
  AnyMessageContent,
  BufferedEventData,
  BufferJSON,
  CacheStore,
  CatalogCollection,
  Chat,
  chatModificationToAppPatch,
  ConnectionState,
  Contact,
  decryptPollVote,
  delay,
  DisconnectReason,
  downloadContentFromMessage,
  downloadExternalPatch,
  downloadMediaMessage,
  encodeSyncdPatch,
  extractSyncdPatches,
  generateWAMessageFromContent,
  getAggregateVotesInPollMessage,
  getBinaryNodeChild,
  getBinaryNodeChildren,
  GetCatalogOptions,
  getContentType,
  getDevice,
  GroupMetadata,
  hkdf,
  hmacSign,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  isPnUser,
  jidNormalizedUser,
  LT_HASH_ANTI_TAMPERING,
  makeCacheableSignalKeyStore,
  MessageUpsertType,
  MessageUserReceiptUpdate,
  MiscMessageGenerationOptions,
  ParticipantAction,
  prepareWAMessageMedia,
  Product,
  proto,
  S_WHATSAPP_NET,
  UserFacingSocketConfig,
  WABrowserDescription,
  WAMediaUpload,
  WAMessage,
  WAMessageKey,
  WAPresence,
  WASocket,
} from 'baileys';
import { Label } from 'baileys/lib/Types/Label';
import { LabelAssociation } from 'baileys/lib/Types/LabelAssociation';
import { spawn } from 'child_process';
import { isArray, isBase64, isURL } from 'class-validator';
import { createHash } from 'crypto';
import EventEmitter2 from 'eventemitter2';
import ffmpeg from 'fluent-ffmpeg';
import FormData from 'form-data';
import Long from 'long';
import mimeTypes from 'mime-types';
import NodeCache from 'node-cache';
import cron from 'node-cron';
import { release } from 'os';
import { join } from 'path';
import P from 'pino';
import qrcode, { QRCodeToDataURLOptions } from 'qrcode';
import qrcodeTerminal from 'qrcode-terminal';
import sharp from 'sharp';
import { PassThrough, Readable } from 'stream';
import { v4 } from 'uuid';

import { BaileysMessageProcessor } from './baileysMessage.processor';
import { useVoiceCallsBaileys } from './voiceCalls/useVoiceCallsBaileys';

export interface ExtendedIMessageKey extends proto.IMessageKey {
  remoteJidAlt?: string;
  participantAlt?: string;
  server_id?: string;
  isViewOnce?: boolean;
}

const groupMetadataCache = new CacheService(new CacheEngine(configService, 'groups').getEngine());

// Adicione a função getVideoDuration no início do arquivo
async function getVideoDuration(input: Buffer | string | Readable): Promise<number> {
  const MediaInfoFactory = (await import('mediainfo.js')).default;
  const mediainfo = await MediaInfoFactory({ format: 'JSON' });

  let fileSize: number;
  let readChunk: (size: number, offset: number) => Promise<Buffer>;

  if (Buffer.isBuffer(input)) {
    fileSize = input.length;
    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return input.slice(offset, offset + size);
    };
  } else if (typeof input === 'string') {
    const fs = await import('fs');
    const stat = await fs.promises.stat(input);
    fileSize = stat.size;
    const fd = await fs.promises.open(input, 'r');

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      const buffer = Buffer.alloc(size);
      await fd.read(buffer, 0, size, offset);
      return buffer;
    };

    try {
      const result = await mediainfo.analyzeData(() => fileSize, readChunk);
      const jsonResult = JSON.parse(result);

      const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
      const duration = generalTrack.Duration;

      return Math.round(parseFloat(duration));
    } finally {
      await fd.close();
    }
  } else if (input instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of input) {
      chunks.push(chunk);
    }
    const data = Buffer.concat(chunks);
    fileSize = data.length;

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return data.slice(offset, offset + size);
    };
  } else {
    throw new Error('Tipo de entrada não suportado');
  }

  const result = await mediainfo.analyzeData(() => fileSize, readChunk);
  const jsonResult = JSON.parse(result);

  const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
  const duration = generalTrack.Duration;

  return Math.round(parseFloat(duration));
}

export class BaileysStartupService extends ChannelStartupService {
  private messageProcessor = new BaileysMessageProcessor();

  constructor(
    public readonly configService: ConfigService,
    public readonly eventEmitter: EventEmitter2,
    public readonly prismaRepository: PrismaRepository,
    public readonly cache: CacheService,
    public readonly chatwootCache: CacheService,
    public readonly baileysCache: CacheService,
    private readonly providerFiles: ProviderFiles,
  ) {
    super(configService, eventEmitter, prismaRepository, chatwootCache);
    this.instance.qrcode = { count: 0 };
    this.messageProcessor.mount({
      onMessageReceive: this.messageHandle['messages.upsert'].bind(this), // Bind the method to the current context
    });

    this.authStateProvider = new AuthStateProvider(this.providerFiles);
  }

  private authStateProvider: AuthStateProvider;
  private readonly msgRetryCounterCache: CacheStore = new NodeCache();
  private readonly userDevicesCache: CacheStore = new NodeCache({ stdTTL: 300000, useClones: false });
  private endSession = false;
  private logBaileys = this.configService.get<Log>('LOG').BAILEYS;
  private eventProcessingQueue: Promise<void> = Promise.resolve();

  // Cache TTL constants (in seconds)
  private readonly MESSAGE_CACHE_TTL_SECONDS = 5 * 60; // 5 minutes - avoid duplicate message processing
  private readonly UPDATE_CACHE_TTL_SECONDS = 30 * 60; // 30 minutes - avoid duplicate status updates

  public stateConnection: wa.StateConnection = { state: 'close' };

  public phoneNumber: string;

  public get connectionStatus() {
    return this.stateConnection;
  }

  public async logoutInstance() {
    this.messageProcessor.onDestroy();
    await this.client?.logout('Log out instance: ' + this.instanceName);

    this.client?.ws?.close();

    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');
    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      const authState = await this.authStateProvider.authStateProvider(this.instance.id);

      await authState.removeCreds();
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      const authState = await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);

      await authState.removeCreds();
    }

    if (db.SAVE_DATA.INSTANCE) {
      const authState = await useMultiFileAuthStatePrisma(this.instance.id, this.cache);

      await authState.removeCreds();
    }

    const sessionExists = await this.prismaRepository.session.findFirst({ where: { sessionId: this.instanceId } });
    if (sessionExists) {
      await this.prismaRepository.session.delete({ where: { sessionId: this.instanceId } });
    }
  }

  public async getProfileName() {
    let profileName = this.client.user?.name ?? this.client.user?.verifiedName;
    if (!profileName) {
      const data = await this.prismaRepository.session.findUnique({ where: { sessionId: this.instanceId } });

      if (data) {
        const creds = JSON.parse(JSON.stringify(data.creds), BufferJSON.reviver);
        profileName = creds.me?.name || creds.me?.verifiedName;
      }
    }

    return profileName;
  }

  public async getProfileStatus() {
    const status = await this.client.fetchStatus(this.instance.wuid);

    return status[0]?.status;
  }

  public get profilePictureUrl() {
    return this.instance.profilePictureUrl;
  }

  public get qrCode(): wa.QrCode {
    return {
      pairingCode: this.instance.qrcode?.pairingCode,
      code: this.instance.qrcode?.code,
      base64: this.instance.qrcode?.base64,
      count: this.instance.qrcode?.count,
    };
  }

  private async connectionUpdate({ qr, connection, lastDisconnect }: Partial<ConnectionState>) {
    if (qr) {
      if (this.instance.qrcode.count === this.configService.get<QrCode>('QRCODE').LIMIT) {
        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          message: 'QR code limit reached, please login again',
          statusCode: DisconnectReason.badSession,
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { message: 'QR code limit reached, please login again', statusCode: DisconnectReason.badSession },
          );
        }

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          state: 'refused',
          statusReason: DisconnectReason.connectionClosed,
          wuid: this.instance.wuid,
          profileName: await this.getProfileName(),
          profilePictureUrl: this.instance.profilePictureUrl,
        });

        this.endSession = true;

        return this.eventEmitter.emit('no.connection', this.instance.name);
      }

      this.instance.qrcode.count++;

      const color = this.configService.get<QrCode>('QRCODE').COLOR;

      const optsQrcode: QRCodeToDataURLOptions = {
        margin: 3,
        scale: 4,
        errorCorrectionLevel: 'H',
        color: { light: '#ffffff', dark: color },
      };

      if (this.phoneNumber) {
        await delay(1000);
        this.instance.qrcode.pairingCode = await this.client.requestPairingCode(this.phoneNumber);
      } else {
        this.instance.qrcode.pairingCode = null;
      }

      qrcode.toDataURL(qr, optsQrcode, (error, base64) => {
        if (error) {
          this.logger.error('Qrcode generate failed:' + error.toString());
          return;
        }

        this.instance.qrcode.base64 = base64;
        this.instance.qrcode.code = qr;

        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.QRCODE_UPDATED,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            {
              qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
            },
          );
        }
      });

      qrcodeTerminal.generate(qr, { small: true }, (qrcode) =>
        this.logger.log(
          `\n{ instance: ${this.instance.name} pairingCode: ${this.instance.qrcode.pairingCode}, qrcodeCount: ${this.instance.qrcode.count} }\n` +
            qrcode,
        ),
      );

      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data: { connectionStatus: 'connecting' },
      });
    }

    if (connection) {
      this.stateConnection = {
        state: connection,
        statusReason: (lastDisconnect?.error as Boom)?.output?.statusCode ?? 200,
      };
    }

    if (connection === 'close') {
      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const codesToNotReconnect = [DisconnectReason.loggedOut, DisconnectReason.forbidden, 402, 406];
      const shouldReconnect = !codesToNotReconnect.includes(statusCode);
      if (shouldReconnect) {
        await this.connectToWhatsapp(this.phoneNumber);
      } else {
        this.sendDataWebhook(Events.STATUS_INSTANCE, {
          instance: this.instance.name,
          status: 'closed',
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        await this.prismaRepository.instance.update({
          where: { id: this.instanceId },
          data: {
            connectionStatus: 'close',
            disconnectionAt: new Date(),
            disconnectionReasonCode: statusCode,
            disconnectionObject: JSON.stringify(lastDisconnect),
          },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
            Events.STATUS_INSTANCE,
            { instanceName: this.instance.name, instanceId: this.instanceId },
            { instance: this.instance.name, status: 'closed' },
          );
        }

        this.eventEmitter.emit('logout.instance', this.instance.name, 'inner');
        this.client?.ws?.close();
        this.client.end(new Error('Close connection'));

        this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
      }
    }

    if (connection === 'open') {
      this.instance.wuid = this.client.user.id.replace(/:\d+/, '');
      try {
        const profilePic = await this.profilePicture(this.instance.wuid);
        this.instance.profilePictureUrl = profilePic.profilePictureUrl;
      } catch {
        this.instance.profilePictureUrl = null;
      }
      const formattedWuid = this.instance.wuid.split('@')[0].padEnd(30, ' ');
      const formattedName = this.instance.name;
      this.logger.info(
        `
        ┌──────────────────────────────┐
        │    CONNECTED TO WHATSAPP     │
        └──────────────────────────────┘`.replace(/^ +/gm, '  '),
      );
      this.logger.info(
        `
        wuid: ${formattedWuid}
        name: ${formattedName}
      `,
      );

      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data: {
          ownerJid: this.instance.wuid,
          profileName: (await this.getProfileName()) as string,
          profilePicUrl: this.instance.profilePictureUrl,
          connectionStatus: 'open',
        },
      });

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
        this.chatwootService.eventWhatsapp(
          Events.CONNECTION_UPDATE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          { instance: this.instance.name, status: 'open' },
        );
        this.syncChatwootLostMessages();
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, {
        instance: this.instance.name,
        wuid: this.instance.wuid,
        profileName: await this.getProfileName(),
        profilePictureUrl: this.instance.profilePictureUrl,
        ...this.stateConnection,
      });

      // Resync all app state patches so labeled chats populate Chat.labels in DB
      setTimeout(async () => {
        try {
          await (this.client as any).resyncAppState(
            ['critical_block', 'critical_unblock_low', 'regular_high', 'regular_low', 'regular'],
            false,
          );
          this.logger.info('Label app state resync completed');
        } catch (error) {
          this.logger.warn('Failed to resync label app state: ' + error);
        }
      }, 8000);
    }

    if (connection === 'connecting') {
      this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
    }
  }

  private async getMessage(key: proto.IMessageKey, full = false) {
    try {
      // Use raw SQL to avoid JSON path issues
      const webMessageInfo = (await this.prismaRepository.$queryRaw`
        SELECT * FROM "Message"
        WHERE "instanceId" = ${this.instanceId}
        AND "key"->>'id' = ${key.id}
      `) as proto.IWebMessageInfo[];

      if (full) {
        return webMessageInfo[0];
      }
      if (webMessageInfo[0].message?.pollCreationMessage) {
        const messageSecretBase64 = webMessageInfo[0].message?.messageContextInfo?.messageSecret;

        if (typeof messageSecretBase64 === 'string') {
          const messageSecret = Buffer.from(messageSecretBase64, 'base64');

          const msg = {
            messageContextInfo: { messageSecret },
            pollCreationMessage: webMessageInfo[0].message?.pollCreationMessage,
          };

          return msg;
        }
      }

      return webMessageInfo[0].message;
    } catch {
      return { conversation: '' };
    }
  }

  private async defineAuthState() {
    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');

    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      return await this.authStateProvider.authStateProvider(this.instance.id);
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      this.logger.info('Redis enabled');
      return await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);
    }

    if (db.SAVE_DATA.INSTANCE) {
      return await useMultiFileAuthStatePrisma(this.instance.id, this.cache);
    }
  }

  private async createClient(number?: string): Promise<WASocket> {
    this.instance.authState = await this.defineAuthState();

    const session = this.configService.get<ConfigSessionPhone>('CONFIG_SESSION_PHONE');

    let browserOptions = {};

    if (number || this.phoneNumber) {
      this.phoneNumber = number;

      this.logger.info(`Phone number: ${number}`);
    } else {
      const browser: WABrowserDescription = [session.CLIENT, session.NAME, release()];
      browserOptions = { browser };

      this.logger.info(`Browser: ${browser}`);
    }

    const baileysVersion = await fetchLatestWaWebVersion({});
    const version = baileysVersion.version;
    const log = `Baileys version: ${version.join('.')}`;

    this.logger.info(log);

    this.logger.info(`Group Ignore: ${this.localSettings.groupsIgnore}`);

    let options;

    if (this.localProxy?.enabled) {
      this.logger.info('Proxy enabled: ' + this.localProxy?.host);

      if (this.localProxy?.host?.includes('proxyscrape')) {
        try {
          const response = await axios.get(this.localProxy?.host);
          const text = response.data;
          const proxyUrls = text.split('\r\n');
          const rand = Math.floor(Math.random() * Math.floor(proxyUrls.length));
          const proxyUrl = 'http://' + proxyUrls[rand];
          options = { agent: makeProxyAgent(proxyUrl), fetchAgent: makeProxyAgentUndici(proxyUrl) };
        } catch {
          this.localProxy.enabled = false;
        }
      } else {
        options = {
          agent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
          fetchAgent: makeProxyAgentUndici({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
        };
      }
    }

    const socketConfig: UserFacingSocketConfig = {
      ...options,
      version,
      logger: P({ level: this.logBaileys }),
      printQRInTerminal: false,
      auth: {
        creds: this.instance.authState.state.creds,
        keys: makeCacheableSignalKeyStore(
          this.watchAppStateDrops(this.instance.authState.state.keys),
          P({ level: 'error' }) as any,
        ),
      },
      msgRetryCounterCache: this.msgRetryCounterCache,
      generateHighQualityLinkPreview: true,
      getMessage: async (key) => (await this.getMessage(key)) as Promise<proto.IMessage>,
      ...browserOptions,
      markOnlineOnConnect: this.localSettings.alwaysOnline,
      retryRequestDelayMs: 350,
      maxMsgRetryCount: 4,
      fireInitQueries: true,
      connectTimeoutMs: 30_000,
      keepAliveIntervalMs: 30_000,
      qrTimeout: 45_000,
      emitOwnEvents: false,
      shouldIgnoreJid: (jid) => {
        if (this.localSettings.syncFullHistory && isJidGroup(jid)) {
          return false;
        }

        const isGroupJid = this.localSettings.groupsIgnore && isJidGroup(jid);
        const isBroadcast = !this.localSettings.readStatus && isJidBroadcast(jid);
        const isNewsletter = isJidNewsletter(jid);

        return isGroupJid || isBroadcast || isNewsletter;
      },
      syncFullHistory: this.localSettings.syncFullHistory,
      shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => {
        return this.historySyncNotification(msg);
      },
      cachedGroupMetadata: this.getGroupMetadataCache,
      userDevicesCache: this.userDevicesCache,
      transactionOpts: { maxCommitRetries: 10, delayBetweenTriesMs: 3000 },
      patchMessageBeforeSending(message) {
        if (
          message.deviceSentMessage?.message?.listMessage?.listType === proto.Message.ListMessage.ListType.PRODUCT_LIST
        ) {
          message = JSON.parse(JSON.stringify(message));

          message.deviceSentMessage.message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        if (message.listMessage?.listType == proto.Message.ListMessage.ListType.PRODUCT_LIST) {
          message = JSON.parse(JSON.stringify(message));

          message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        return message;
      },
    };

    this.endSession = false;

    this.client = makeWASocket(socketConfig);

    if (this.localSettings.wavoipToken && this.localSettings.wavoipToken.length > 0) {
      useVoiceCallsBaileys(this.localSettings.wavoipToken, this.client, this.connectionStatus.state as any, true);
    }

    this.eventHandler();

    this.client.ws.on('CB:call', (packet) => {
      console.log('CB:call', packet);
      const payload = { event: 'CB:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.client.ws.on('CB:ack,class:call', (packet) => {
      console.log('CB:ack,class:call', packet);
      const payload = { event: 'CB:ack,class:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.phoneNumber = number;

    return this.client;
  }

  public async connectToWhatsapp(number?: string): Promise<WASocket> {
    try {
      this.loadChatwoot();
      this.loadSettings();
      this.loadWebhook();
      this.loadProxy();

      // Remontar o messageProcessor para garantir que está funcionando após reconexão
      this.messageProcessor.mount({
        onMessageReceive: this.messageHandle['messages.upsert'].bind(this),
      });

      return await this.createClient(number);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  public async reloadConnection(): Promise<WASocket> {
    try {
      return await this.createClient(this.phoneNumber);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  private readonly chatHandle = {
    'chats.upsert': async (chats: Chat[]) => {
      const existingChatIds = await this.prismaRepository.chat.findMany({
        where: { instanceId: this.instanceId },
        select: { remoteJid: true },
      });

      const existingChatIdSet = new Set(existingChatIds.map((chat) => chat.remoteJid));

      const chatsToInsert = chats
        .filter((chat) => !existingChatIdSet?.has(chat.id))
        .map((chat) => ({
          remoteJid: chat.id,
          instanceId: this.instanceId,
          name: chat.name,
          unreadMessages: chat.unreadCount !== undefined ? chat.unreadCount : 0,
        }));

      this.sendDataWebhook(Events.CHATS_UPSERT, chatsToInsert);

      if (chatsToInsert.length > 0) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS)
          await this.prismaRepository.chat.createMany({ data: chatsToInsert, skipDuplicates: true });
      }
    },

    'chats.update': async (
      chats: Partial<
        proto.IConversation & { lastMessageRecvTimestamp?: number } & {
          conditional: (bufferedData: BufferedEventData) => boolean;
        }
      >[],
    ) => {
      // darnabt: forward the unread state so "Mark as unread"/"Mark as read" on the phone or
      // WhatsApp Web reaches the webhook. Baileys: unreadCount -1 = marked unread (app-state
      // markChatAsReadAction), 0 = marked read, n > 0 = new-message tally. Stock dropped it.
      // darnabt: a chat the phone stores under a @lid — also name its phone JID (Baileys' LID
      // store), so the receiver can find the chat even when it never saw that LID before.
      const altOf: Record<string, string> = {};
      for (const chat of chats) {
        const id = chat.id;
        if (typeof id !== 'string' || !id.endsWith('@lid') || id in altOf) continue;
        try {
          const pn = await this.client.signalRepository.lidMapping.getPNForLID(jidNormalizedUser(id));
          if (pn) altOf[id] = jidNormalizedUser(pn);
        } catch {
          // no mapping known — the receiver falls back to its own LID book
        }
      }
      const chatsRaw = chats.map((chat) => {
        const raw: {
          remoteJid: string;
          remoteJidAlt?: string;
          instanceId: string;
          unreadCount?: number;
          markedAsUnread?: boolean;
          pinned?: number | null;
          archived?: boolean;
        } = {
          remoteJid: chat.id,
          instanceId: this.instanceId,
        };
        if (altOf[chat.id]) raw.remoteJidAlt = altOf[chat.id];
        if (typeof chat.unreadCount === 'number') raw.unreadCount = chat.unreadCount;
        if (typeof chat.markedAsUnread === 'boolean') raw.markedAsUnread = chat.markedAsUnread;
        // darnabt: pin / archive done on the phone or WhatsApp Web (app-state pinAction /
        // archiveChatAction). Baileys: pinned = pin time in seconds, null = unpinned;
        // archived = true/false. Nawah orders and hides its chat list by them.
        if ('pinned' in chat) {
          const p = chat.pinned as unknown as number | { toNumber?: () => number } | null | undefined;
          const n = p == null ? 0 : typeof p === 'number' ? p : (p.toNumber?.() ?? Number(p));
          raw.pinned = n > 0 ? n : null;
        }
        if (typeof chat.archived === 'boolean') raw.archived = chat.archived;
        return raw;
      });

      this.sendDataWebhook(Events.CHATS_UPDATE, chatsRaw);

      for (const chat of chats) {
        await this.prismaRepository.chat.updateMany({
          where: { instanceId: this.instanceId, remoteJid: chat.id, name: chat.name },
          data: { remoteJid: chat.id },
        });
      }
    },

    'chats.delete': async (chats: string[]) => {
      chats.forEach(
        async (chat) =>
          await this.prismaRepository.chat.deleteMany({ where: { instanceId: this.instanceId, remoteJid: chat } }),
      );

      this.sendDataWebhook(Events.CHATS_DELETE, [...chats]);
    },
  };

  private readonly contactHandle = {
    'contacts.upsert': async (contacts: Contact[]) => {
      try {
        const contactsRaw: any = contacts.map((contact) => ({
          remoteJid: contact.id,
          pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
          profilePicUrl: null,
          instanceId: this.instanceId,
        }));

        if (contactsRaw.length > 0) {
          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactsRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.createMany({ data: contactsRaw, skipDuplicates: true });

          const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }
        }

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importContacts &&
          contactsRaw.length
        ) {
          this.chatwootService.addHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            contactsRaw,
          );
          chatwootImport.importHistoryContacts(
            { instanceName: this.instance.name, instanceId: this.instance.id },
            this.localChatwoot,
          );
        }

        const updatedContacts = await Promise.all(
          contacts.map(async (contact) => ({
            remoteJid: contact.id,
            pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
            profilePicUrl: (await this.profilePicture(contact.id)).profilePictureUrl,
            instanceId: this.instanceId,
          })),
        );

        if (updatedContacts.length > 0) {
          const usersContacts = updatedContacts.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }

          this.sendDataWebhook(Events.CONTACTS_UPDATE, updatedContacts);
          await Promise.all(
            updatedContacts.map(async (contact) => {
              if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
                await this.prismaRepository.contact.updateMany({
                  where: { remoteJid: contact.remoteJid, instanceId: this.instanceId },
                  data: { profilePicUrl: contact.profilePicUrl },
                });
              }

              if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
                const instance = { instanceName: this.instance.name, instanceId: this.instance.id };

                const findParticipant = await this.chatwootService.findContact(
                  instance,
                  contact.remoteJid.split('@')[0],
                );

                if (!findParticipant) {
                  return;
                }

                this.chatwootService.updateContact(instance, findParticipant.id, {
                  name: contact.pushName,
                  avatar_url: contact.profilePicUrl,
                });
              }
            }),
          );
        }
      } catch (error) {
        console.error(error);
        this.logger.error(`Error: ${error.message}`);
      }
    },

    'contacts.update': async (contacts: Partial<Contact>[]) => {
      const contactsRaw: { remoteJid: string; pushName?: string; profilePicUrl?: string; instanceId: string }[] = [];
      for await (const contact of contacts) {
        this.logger.debug(`Updating contact: ${JSON.stringify(contact, null, 2)}`);
        contactsRaw.push({
          remoteJid: contact.id,
          pushName: contact?.name ?? contact?.verifiedName,
          profilePicUrl: (await this.profilePicture(contact.id)).profilePictureUrl,
          instanceId: this.instanceId,
        });
      }

      this.sendDataWebhook(Events.CONTACTS_UPDATE, contactsRaw);

      if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS) {
        const updateTransactions = contactsRaw.map((contact) =>
          this.prismaRepository.contact.upsert({
            where: { remoteJid_instanceId: { remoteJid: contact.remoteJid, instanceId: contact.instanceId } },
            create: contact,
            update: contact,
          }),
        );
        await this.prismaRepository.$transaction(updateTransactions);
      }

      //const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
    },
  };

  private readonly messageHandle = {
    'messaging-history.set': async ({
      messages,
      chats,
      contacts,
      isLatest,
      progress,
      syncType,
      phoneNumberToLidMappings,
    }: {
      chats: Chat[];
      contacts: Contact[];
      messages: WAMessage[];
      isLatest?: boolean;
      progress?: number;
      syncType?: proto.HistorySync.HistorySyncType;
      // darnabt: kept by Docker/scripts/patch-baileys-history.js (rc.9 drops it)
      phoneNumberToLidMappings?: proto.IPhoneNumberToLIDMapping[];
    }) => {
      try {
        if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
          console.log('received on-demand history sync, messages=', messages);
        }
        console.log(
          `recv ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (is latest: ${isLatest}, progress: ${progress}%), type: ${syncType}`,
        );

        const instance: InstanceDto = { instanceName: this.instance.name };

        let timestampLimitToImport = null;

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
          const daysLimitToImport = this.localChatwoot?.enabled ? this.localChatwoot.daysLimitImportMessages : 1000;

          const date = new Date();
          timestampLimitToImport = new Date(date.setDate(date.getDate() - daysLimitToImport)).getTime() / 1000;

          const maxBatchTimestamp = Math.max(...messages.map((message) => message.messageTimestamp as number));

          const processBatch = maxBatchTimestamp >= timestampLimitToImport;

          if (!processBatch) {
            return;
          }
        }

        const contactsMap = new Map();

        for (const contact of contacts) {
          if (contact.id && (contact.notify || contact.name)) {
            contactsMap.set(contact.id, { name: contact.name ?? contact.notify, jid: contact.id });
          }
        }

        // darnabt 2026-10-01 — history keys carry the chat id exactly as the phone stores it,
        // often only a @lid with no phone number. Live upserts carry remoteJidAlt/participantAlt;
        // give history the same so a receiver can tell who the chat is. Source: this chunk's own
        // conversations (Baileys maps chat.lidJid/pnJid onto the contacts), then Baileys' LID store.
        const lidToPn = new Map<string, string>();
        for (const c of contacts as any[]) {
          const jids = [c?.id, c?.lid, c?.phoneNumber].filter((j) => typeof j === 'string');
          const lid = jids.find((j) => j.endsWith('@lid'));
          const pn = jids.find((j) => j.endsWith('@s.whatsapp.net'));
          if (lid && pn) lidToPn.set(jidNormalizedUser(lid), jidNormalizedUser(pn));
        }
        // WhatsApp's own LID<->phone pairs for this history (often in a chunk with no messages):
        // remember them in Baileys' LID store so later chunks and live messages resolve too.
        const historyPairs: { lid: string; pn: string }[] = [];
        for (const mp of phoneNumberToLidMappings ?? []) {
          if (mp?.lidJid && mp?.pnJid) {
            const lid = jidNormalizedUser(mp.lidJid.includes('@') ? mp.lidJid : `${mp.lidJid}@lid`);
            const pn = jidNormalizedUser(mp.pnJid.includes('@') ? mp.pnJid : `${mp.pnJid}@s.whatsapp.net`);
            lidToPn.set(lid, pn);
            historyPairs.push({ lid, pn });
          }
        }
        if (historyPairs.length) {
          try {
            await this.client.signalRepository.lidMapping.storeLIDPNMappings(historyPairs);
          } catch (e) {
            this.logger.warn(`history LID mappings not stored: ${(e as Error)?.message ?? e}`);
          }
        }
        console.log(`history lid->pn: ${historyPairs.length} pairs from WhatsApp, ${lidToPn.size} known`);
        const pnForLid = async (jid?: string | null): Promise<string | undefined> => {
          if (!jid || !jid.endsWith('@lid')) return undefined;
          const lid = jidNormalizedUser(jid);
          if (lidToPn.has(lid)) return lidToPn.get(lid) || undefined;
          let pn: string | undefined;
          try {
            const found = await this.client.signalRepository.lidMapping.getPNForLID(lid);
            if (found) pn = jidNormalizedUser(found);
          } catch {
            pn = undefined;
          }
          lidToPn.set(lid, pn ?? '');
          return pn;
        };

        const chatsRaw: { remoteJid: string; instanceId: string; name?: string }[] = [];
        const chatsRepository = new Set(
          (await this.prismaRepository.chat.findMany({ where: { instanceId: this.instanceId } })).map(
            (chat) => chat.remoteJid,
          ),
        );

        for (const chat of chats) {
          if (chatsRepository?.has(chat.id)) {
            continue;
          }

          chatsRaw.push({ remoteJid: chat.id, instanceId: this.instanceId, name: chat.name });
        }

        this.sendDataWebhook(Events.CHATS_SET, chatsRaw);

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.chat.createMany({ data: chatsRaw, skipDuplicates: true });
        }

        const messagesRaw: any[] = [];

        const messagesRepository: Set<string> = new Set(
          chatwootImport.getRepositoryMessagesCache(instance) ??
            (
              await this.prismaRepository.message.findMany({
                select: { key: true },
                where: { instanceId: this.instanceId },
              })
            ).map((message) => {
              const key = message.key as { id: string };

              return key.id;
            }),
        );

        if (chatwootImport.getRepositoryMessagesCache(instance) === null) {
          chatwootImport.setRepositoryMessagesCache(instance, messagesRepository);
        }

        for (const m of messages) {
          if (!m.message || !m.key || !m.messageTimestamp) {
            continue;
          }

          if (Long.isLong(m?.messageTimestamp)) {
            m.messageTimestamp = m.messageTimestamp?.toNumber();
          }

          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
            if (m.messageTimestamp <= timestampLimitToImport) {
              continue;
            }
          }

          if (messagesRepository?.has(m.key.id)) {
            continue;
          }

          const key = m.key as typeof m.key & { remoteJidAlt?: string; participantAlt?: string };
          if (!key.remoteJidAlt) {
            const pn = await pnForLid(key.remoteJid);
            if (pn) key.remoteJidAlt = pn;
          }
          if (key.participant && !key.participantAlt) {
            const pn = await pnForLid(key.participant);
            if (pn) key.participantAlt = pn;
          }

          if (!m.pushName && !m.key.fromMe) {
            const participantJid = m.participant || m.key.participant || m.key.remoteJid;
            if (participantJid && contactsMap.has(participantJid)) {
              m.pushName = contactsMap.get(participantJid).name;
            } else if (participantJid) {
              m.pushName = participantJid.split('@')[0];
            }
          }

          messagesRaw.push(this.prepareMessage(m));
        }

        this.sendDataWebhook(Events.MESSAGES_SET, [...messagesRaw], true, undefined, {
          isLatest,
          progress,
        });

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.message.createMany({ data: messagesRaw, skipDuplicates: true });
        }

        if (
          this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
          this.localChatwoot?.enabled &&
          this.localChatwoot.importMessages &&
          messagesRaw.length > 0
        ) {
          this.chatwootService.addHistoryMessages(
            instance,
            messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)),
          );
        }

        await this.contactHandle['contacts.upsert'](
          contacts.filter((c) => !!c.notify || !!c.name).map((c) => ({ id: c.id, name: c.name ?? c.notify })),
        );

        contacts = undefined;
        messages = undefined;
        chats = undefined;
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.upsert': async (
      { messages, type, requestId }: { messages: WAMessage[]; type: MessageUpsertType; requestId?: string },
      settings: any,
    ) => {
      try {
        for (const received of messages) {
          if (
            received?.messageStubParameters?.some?.((param) =>
              [
                'No matching sessions found for message',
                'Bad MAC',
                'failed to decrypt message',
                'SessionError',
                'Invalid PreKey ID',
                'No session record',
                'No session found to decrypt message',
                'Message absent from node',
              ].some((err) => param?.includes?.(err)),
            )
          ) {
            this.logger.warn(`Message ignored with messageStubParameters: ${JSON.stringify(received, null, 2)}`);
            continue;
          }
          if (received.message?.conversation || received.message?.extendedTextMessage?.text) {
            const text = received.message?.conversation || received.message?.extendedTextMessage?.text;

            if (text == 'requestPlaceholder' && !requestId) {
              const messageId = await this.client.requestPlaceholderResend(received.key);

              console.log('requested placeholder resync, id=', messageId);
            } else if (requestId) {
              console.log('Message received from phone, id=', requestId, received);
            }

            if (text == 'onDemandHistSync') {
              const messageId = await this.client.fetchMessageHistory(50, received.key, received.messageTimestamp!);
              console.log('requested on-demand sync, id=', messageId);
            }
          }

          const editedMessage =
            received?.message?.protocolMessage || received?.message?.editedMessage?.message?.protocolMessage;

          if (editedMessage) {
            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
              this.chatwootService.eventWhatsapp(
                'messages.edit',
                { instanceName: this.instance.name, instanceId: this.instance.id },
                editedMessage,
              );

            await this.sendDataWebhook(Events.MESSAGES_EDITED, editedMessage);

            if (received.key?.id && editedMessage.key?.id) {
              await this.baileysCache.set(`protocol_${received.key.id}`, editedMessage.key.id, 60 * 60 * 24);
            }

            const oldMessage = await this.getMessage(editedMessage.key, true);
            if ((oldMessage as any)?.id) {
              const editedMessageTimestamp = Long.isLong(received?.messageTimestamp)
                ? Math.floor(received?.messageTimestamp.toNumber())
                : Math.floor(received?.messageTimestamp as number);

              await this.prismaRepository.message.update({
                where: { id: (oldMessage as any).id },
                data: {
                  message: editedMessage.editedMessage as any,
                  messageTimestamp: editedMessageTimestamp,
                  status: 'EDITED',
                },
              });
              await this.prismaRepository.messageUpdate.create({
                data: {
                  fromMe: editedMessage.key.fromMe,
                  keyId: editedMessage.key.id,
                  remoteJid: editedMessage.key.remoteJid,
                  status: 'EDITED',
                  instanceId: this.instanceId,
                  messageId: (oldMessage as any).id,
                },
              });
            }
          }

          if ((type !== 'notify' && type !== 'append') || editedMessage || !received?.message) {
            continue;
          }

          if (Long.isLong(received.messageTimestamp)) {
            received.messageTimestamp = received.messageTimestamp?.toNumber();
          }

          if (settings?.groupsIgnore && received.key.remoteJid.includes('@g.us')) {
            continue;
          }

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: received.key.remoteJid },
            select: { id: true, name: true },
          });

          if (
            existingChat &&
            received.pushName &&
            existingChat.name !== received.pushName &&
            received.pushName.trim().length > 0 &&
            !received.key.fromMe &&
            !received.key.remoteJid.includes('@g.us')
          ) {
            this.sendDataWebhook(Events.CHATS_UPSERT, [{ ...existingChat, name: received.pushName }]);
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
              try {
                await this.prismaRepository.chat.update({
                  where: { id: existingChat.id },
                  data: { name: received.pushName },
                });
              } catch {
                console.log(`Chat insert record ignored: ${received.key.remoteJid} - ${this.instanceId}`);
              }
            }
          }

          const messageRaw = this.prepareMessage(received);

          if (messageRaw.messageType === 'pollUpdateMessage') {
            const pollCreationKey = messageRaw.message.pollUpdateMessage.pollCreationMessageKey;
            const pollMessage = (await this.getMessage(pollCreationKey, true)) as proto.IWebMessageInfo;
            const pollMessageSecret = (await this.getMessage(pollCreationKey)) as any;

            if (pollMessage) {
              const pollOptions =
                (pollMessage.message as any).pollCreationMessage?.options ||
                (pollMessage.message as any).pollCreationMessageV3?.options ||
                [];
              const pollVote = messageRaw.message.pollUpdateMessage.vote;

              const voterJid = received.key.fromMe
                ? this.instance.wuid
                : received.key.participant || received.key.remoteJid;

              let pollEncKey = pollMessageSecret?.messageContextInfo?.messageSecret;

              let successfulVoterJid = voterJid;

              if (typeof pollEncKey === 'string') {
                pollEncKey = Buffer.from(pollEncKey, 'base64');
              } else if (pollEncKey?.type === 'Buffer' && Array.isArray(pollEncKey.data)) {
                pollEncKey = Buffer.from(pollEncKey.data);
              }

              if (Buffer.isBuffer(pollEncKey) && pollEncKey.length === 44) {
                pollEncKey = Buffer.from(pollEncKey.toString('utf8'), 'base64');
              }

              if (pollVote.encPayload && pollEncKey) {
                const creatorCandidates = [
                  this.instance.wuid,
                  this.client.user?.lid,
                  pollMessage.key.participant,
                  (pollMessage.key as any).participantAlt,
                  pollMessage.key.remoteJid,
                ];

                const key = received.key as any;
                const voterCandidates = [
                  this.instance.wuid,
                  this.client.user?.lid,
                  key.participant,
                  key.participantAlt,
                  key.remoteJidAlt,
                  key.remoteJid,
                ];

                const uniqueCreators = [
                  ...new Set(creatorCandidates.filter(Boolean).map((id) => jidNormalizedUser(id))),
                ];
                const uniqueVoters = [...new Set(voterCandidates.filter(Boolean).map((id) => jidNormalizedUser(id)))];

                let decryptedVote;

                for (const creator of uniqueCreators) {
                  for (const voter of uniqueVoters) {
                    try {
                      decryptedVote = decryptPollVote(pollVote, {
                        pollCreatorJid: creator,
                        pollMsgId: pollMessage.key.id,
                        pollEncKey,
                        voterJid: voter,
                      } as any);
                      if (decryptedVote) {
                        successfulVoterJid = voter;
                        break;
                      }
                    } catch {
                      // Continue trying
                    }
                  }
                  if (decryptedVote) break;
                }

                if (decryptedVote) {
                  Object.assign(pollVote, decryptedVote);
                }
              }

              const selectedOptions = pollVote?.selectedOptions || [];

              const selectedOptionNames = pollOptions
                .filter((option) => {
                  const hash = createHash('sha256').update(option.optionName).digest();
                  return selectedOptions.some((selected) => Buffer.compare(selected, hash) === 0);
                })
                .map((option) => option.optionName);

              messageRaw.message.pollUpdateMessage.vote.selectedOptions = selectedOptionNames;

              const pollUpdates = pollOptions.map((option) => ({
                name: option.optionName,
                voters: selectedOptionNames.includes(option.optionName) ? [successfulVoterJid] : [],
              }));

              messageRaw.pollUpdates = pollUpdates;
            }
          }

          const isMedia =
            received?.message?.imageMessage ||
            received?.message?.videoMessage ||
            received?.message?.stickerMessage ||
            received?.message?.documentMessage ||
            received?.message?.documentWithCaptionMessage ||
            received?.message?.ptvMessage ||
            received?.message?.audioMessage;

          const isVideo = received?.message?.videoMessage;

          if (this.localSettings.readMessages && received.key.id !== 'status@broadcast') {
            // darnabt 2026-10-03: 'read-self' only (own devices). Contacts never get a blue tick from this
            // linked device, whatever the number's read-receipts privacy says.
            await this.client.sendReceipts([received.key], 'read-self');
          }

          if (this.localSettings.readStatus && received.key.id === 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }

          if (
            this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
            this.localChatwoot?.enabled &&
            !received.key.id.includes('@broadcast')
          ) {
            const chatwootSentMessage = await this.chatwootService.eventWhatsapp(
              Events.MESSAGES_UPSERT,
              { instanceName: this.instance.name, instanceId: this.instanceId },
              messageRaw,
            );

            if (chatwootSentMessage?.id) {
              messageRaw.chatwootMessageId = chatwootSentMessage.id;
              messageRaw.chatwootInboxId = chatwootSentMessage.inbox_id;
              messageRaw.chatwootConversationId = chatwootSentMessage.conversation_id;
            }
          }

          if (this.configService.get<Openai>('OPENAI').ENABLED && received?.message?.audioMessage) {
            const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
              where: { instanceId: this.instanceId },
              include: { OpenaiCreds: true },
            });

            if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
              messageRaw.message.speechToText = `[audio] ${await this.openaiService.speechToText(received, this)}`;
            }
          }

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { pollUpdates, ...messageData } = messageRaw;
            const msg = await this.prismaRepository.message.create({ data: messageData });

            const { remoteJid } = received.key;
            const timestamp = msg.messageTimestamp;
            const fromMe = received.key.fromMe.toString();
            const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;

            const cachedTimestamp = await this.baileysCache.get(messageKey);

            if (!cachedTimestamp) {
              if (!received.key.fromMe) {
                if (msg.status === status[3]) {
                  this.logger.log(`Update not read messages ${remoteJid}`);
                  await this.updateChatUnreadMessages(remoteJid);
                } else if (msg.status === status[4]) {
                  this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                  await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                }
              } else {
                // is send message by me
                this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
              }

              await this.baileysCache.set(messageKey, true, this.MESSAGE_CACHE_TTL_SECONDS);
            } else {
              this.logger.info(`Update readed messages duplicated ignored [avoid deadlock]: ${messageKey}`);
            }

            if (isMedia) {
              if (this.configService.get<S3>('S3').ENABLE) {
                try {
                  if (isVideo && !this.configService.get<S3>('S3').SAVE_VIDEO) {
                    this.logger.warn('Video upload is disabled. Skipping video upload.');
                    // Skip video upload by returning early from this block
                    return;
                  }

                  const message: any = received;

                  // Verificação adicional para garantir que há conteúdo de mídia real
                  const hasRealMedia = this.hasValidMediaContent(message);

                  if (!hasRealMedia) {
                    this.logger.warn('Message detected as media but contains no valid media content');
                  } else {
                    const media = await this.getBase64FromMediaMessage({ message }, true);

                    if (!media) {
                      this.logger.verbose('No valid media to upload (messageContextInfo only), skipping MinIO');
                      return;
                    }

                    const { buffer, mediaType, fileName, size } = media;
                    const mimetype = mimeTypes.lookup(fileName).toString();
                    const fullName = join(
                      `${this.instance.id}`,
                      received.key.remoteJid,
                      mediaType,
                      `${Date.now()}_${fileName}`,
                    );
                    await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

                    await this.prismaRepository.media.create({
                      data: {
                        messageId: msg.id,
                        instanceId: this.instanceId,
                        type: mediaType,
                        fileName: fullName,
                        mimetype,
                      },
                    });

                    const mediaUrl = await s3Service.getObjectUrl(fullName);

                    messageRaw.message.mediaUrl = mediaUrl;

                    await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
                  }
                } catch (error) {
                  this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
                }
              }
            }
          }

          if (this.localWebhook.enabled) {
            if (isMedia && this.localWebhook.webhookBase64) {
              try {
                const buffer = await downloadMediaMessage(
                  { key: received.key, message: received?.message },
                  'buffer',
                  {},
                  { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                );

                if (buffer) {
                  messageRaw.message.base64 = buffer.toString('base64');
                } else {
                  // retry to download media
                  const buffer = await downloadMediaMessage(
                    { key: received.key, message: received?.message },
                    'buffer',
                    {},
                    { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                  );

                  if (buffer) {
                    messageRaw.message.base64 = buffer.toString('base64');
                  }
                }
              } catch (error) {
                this.logger.error(['Error converting media to base64', error?.message]);
              }
            }
          }

          this.logger.verbose(messageRaw);

          sendTelemetry(`received.message.${messageRaw.messageType ?? 'unknown'}`);
          if (messageRaw.key.remoteJid?.includes('@lid') && messageRaw.key.remoteJidAlt) {
            messageRaw.key.remoteJid = messageRaw.key.remoteJidAlt;
          }
          console.log(messageRaw);

          this.sendDataWebhook(Events.MESSAGES_UPSERT, messageRaw);

          await chatbotController.emit({
            instance: { instanceName: this.instance.name, instanceId: this.instanceId },
            remoteJid: messageRaw.key.remoteJid,
            msg: messageRaw,
            pushName: messageRaw.pushName,
          });

          const contact = await this.prismaRepository.contact.findFirst({
            where: { remoteJid: received.key.remoteJid, instanceId: this.instanceId },
          });

          const contactRaw: {
            remoteJid: string;
            pushName: string;
            profilePicUrl?: string;
            instanceId: string;
          } = {
            remoteJid: received.key.remoteJid,
            pushName: received.key.fromMe ? '' : received.key.fromMe == null ? '' : received.pushName,
            profilePicUrl: (await this.profilePicture(received.key.remoteJid)).profilePictureUrl,
            instanceId: this.instanceId,
          };

          if (contactRaw.remoteJid === 'status@broadcast') {
            continue;
          }

          if (contactRaw.remoteJid.includes('@s.whatsapp') || contactRaw.remoteJid.includes('@lid')) {
            await saveOnWhatsappCache([
              {
                remoteJid:
                  messageRaw.key.addressingMode === 'lid' ? messageRaw.key.remoteJidAlt : messageRaw.key.remoteJid,
                remoteJidAlt: messageRaw.key.remoteJidAlt,
                lid: messageRaw.key.addressingMode === 'lid' ? 'lid' : null,
              },
            ]);
          }

          if (contact) {
            this.sendDataWebhook(Events.CONTACTS_UPDATE, contactRaw);

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              await this.chatwootService.eventWhatsapp(
                Events.CONTACTS_UPDATE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                contactRaw,
              );
            }

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
              await this.prismaRepository.contact.upsert({
                where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } },
                create: contactRaw,
                update: contactRaw,
              });

            continue;
          }

          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.upsert({
              where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } },
              update: contactRaw,
              create: contactRaw,
            });
        }
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.update': async (args: { update: Partial<WAMessage>; key: WAMessageKey }[], settings: any) => {
      this.logger.verbose(`Update messages ${JSON.stringify(args, undefined, 2)}`);

      const readChatToUpdate: Record<string, true> = {}; // {remoteJid: true}

      for await (const { key, update } of args) {
        if (settings?.groupsIgnore && key.remoteJid?.includes('@g.us')) {
          continue;
        }

        const updateKey = `${this.instance.id}_${key.id}_${update.status}`;

        const cached = await this.baileysCache.get(updateKey);

        const secondsSinceEpoch = Math.floor(Date.now() / 1000);
        console.log('CACHE:', { cached, updateKey, messageTimestamp: update.messageTimestamp, secondsSinceEpoch });

        if (
          (update.messageTimestamp && update.messageTimestamp === cached) ||
          (!update.messageTimestamp && secondsSinceEpoch === cached)
        ) {
          this.logger.info(`Update Message duplicated ignored [avoid deadlock]: ${updateKey}`);
          continue;
        }

        if (update.messageTimestamp) {
          await this.baileysCache.set(updateKey, update.messageTimestamp, 30 * 60);
        } else {
          await this.baileysCache.set(updateKey, secondsSinceEpoch, 30 * 60);
        }

        if (status[update.status] === 'READ' && key.fromMe) {
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
            this.chatwootService.eventWhatsapp(
              'messages.read',
              { instanceName: this.instance.name, instanceId: this.instanceId },
              { key: key },
            );
          }
        }

        if (key.remoteJid !== 'status@broadcast' && key.id !== undefined) {
          let pollUpdates: any;

          if (update.pollUpdates) {
            const pollCreation = await this.getMessage(key);

            if (pollCreation) {
              pollUpdates = getAggregateVotesInPollMessage({
                message: pollCreation as proto.IMessage,
                pollUpdates: update.pollUpdates,
              });
            }
          }

          const message: any = {
            keyId: key.id,
            remoteJid: key?.remoteJid,
            fromMe: key.fromMe,
            participant: key?.participant,
            status: status[update.status] ?? 'SERVER_ACK',
            pollUpdates,
            instanceId: this.instanceId,
          };

          if (update.message) {
            message.message = update.message;
          }

          let findMessage: any;
          const configDatabaseData = this.configService.get<Database>('DATABASE').SAVE_DATA;
          if (configDatabaseData.HISTORIC || configDatabaseData.NEW_MESSAGE) {
            // Use raw SQL to avoid JSON path issues
            const protocolMapKey = `protocol_${key.id}`;
            const originalMessageId = (await this.baileysCache.get(protocolMapKey)) as string;

            if (originalMessageId) {
              message.keyId = originalMessageId;
            }

            const searchId = originalMessageId || key.id;

            const messages = (await this.prismaRepository.$queryRaw`
              SELECT * FROM "Message"
              WHERE "instanceId" = ${this.instanceId}
              AND "key"->>'id' = ${searchId}
              LIMIT 1
            `) as any[];
            findMessage = messages[0] || null;

            if (!findMessage?.id) {
              this.logger.warn(`Original message not found for update. Skipping. Key: ${JSON.stringify(key)}`);
              continue;
            }
            message.messageId = findMessage.id;
          }

          if (update.message === null && update.status === undefined) {
            this.sendDataWebhook(Events.MESSAGES_DELETE, { ...key, status: 'DELETED' });

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE)
              await this.prismaRepository.messageUpdate.create({ data: message });

            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              this.chatwootService.eventWhatsapp(
                Events.MESSAGES_DELETE,
                { instanceName: this.instance.name, instanceId: this.instanceId },
                { key: key },
              );
            }

            continue;
          }

          if (findMessage && update.status !== undefined && status[update.status] !== findMessage.status) {
            if (!key.fromMe && key.remoteJid) {
              readChatToUpdate[key.remoteJid] = true;

              const { remoteJid } = key;
              const timestamp = findMessage.messageTimestamp;
              const fromMe = key.fromMe.toString();
              const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;

              const cachedTimestamp = await this.baileysCache.get(messageKey);

              if (!cachedTimestamp) {
                if (status[update.status] === status[4]) {
                  this.logger.log(`Update as read in message.update ${remoteJid} - ${timestamp}`);
                  await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                  await this.baileysCache.set(messageKey, true, this.MESSAGE_CACHE_TTL_SECONDS);
                }

                await this.prismaRepository.message.update({
                  where: { id: findMessage.id },
                  data: { status: status[update.status] },
                });
              } else {
                this.logger.info(
                  `Update readed messages duplicated ignored in message.update [avoid deadlock]: ${messageKey}`,
                );
              }
            }
          }

          this.sendDataWebhook(Events.MESSAGES_UPDATE, message);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { message: _msg, ...messageData } = message;
            await this.prismaRepository.messageUpdate.create({ data: messageData });
          }

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: message.remoteJid },
          });

          if (existingChat) {
            const chatToInsert = { remoteJid: message.remoteJid, instanceId: this.instanceId, unreadMessages: 0 };

            this.sendDataWebhook(Events.CHATS_UPSERT, [chatToInsert]);
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
              try {
                await this.prismaRepository.chat.update({ where: { id: existingChat.id }, data: chatToInsert });
              } catch {
                console.log(`Chat insert record ignored: ${chatToInsert.remoteJid} - ${chatToInsert.instanceId}`);
              }
            }
          }
        }
      }

      await Promise.all(Object.keys(readChatToUpdate).map((remoteJid) => this.updateChatUnreadMessages(remoteJid)));
    },
  };

  private readonly groupHandler = {
    'groups.upsert': (groupMetadata: GroupMetadata[]) => {
      this.sendDataWebhook(Events.GROUPS_UPSERT, groupMetadata);
    },

    'groups.update': (groupMetadataUpdate: Partial<GroupMetadata>[]) => {
      this.sendDataWebhook(Events.GROUPS_UPDATE, groupMetadataUpdate);

      groupMetadataUpdate.forEach((group) => {
        if (isJidGroup(group.id)) {
          this.updateGroupMetadataCache(group.id);
        }
      });
    },

    'group-participants.update': async (participantsUpdate: {
      id: string;
      participants: string[];
      action: ParticipantAction;
    }) => {
      // ENHANCEMENT: Adds participantsData field while maintaining backward compatibility
      // MAINTAINS: participants: string[] (original JID strings)
      // ADDS: participantsData: { jid: string, phoneNumber: string, name?: string, imgUrl?: string }[]
      // This enables LID to phoneNumber conversion without breaking existing webhook consumers

      // Helper to normalize participantId as phone number
      const normalizePhoneNumber = (id: string | null | undefined): string => {
        // Remove @lid, @s.whatsapp.net suffixes and extract just the number part
        return String(id || '').split('@')[0];
      };

      try {
        // Usa o mesmo método que o endpoint /group/participants
        const groupParticipants = await this.findParticipants({ groupJid: participantsUpdate.id });

        // Validação para garantir que temos dados válidos
        if (!groupParticipants?.participants || !Array.isArray(groupParticipants.participants)) {
          throw new Error('Invalid participant data received from findParticipants');
        }

        // Filtra apenas os participantes que estão no evento
        const resolvedParticipants = participantsUpdate.participants.map((participantId) => {
          const participantData = groupParticipants.participants.find((p) => p.id === participantId);

          let phoneNumber: string;
          if (participantData?.phoneNumber) {
            phoneNumber = participantData.phoneNumber;
          } else {
            phoneNumber = normalizePhoneNumber(participantId);
          }

          return {
            jid: participantId,
            phoneNumber,
            name: participantData?.name,
            imgUrl: participantData?.imgUrl,
          };
        });

        // Mantém formato original + adiciona dados resolvidos
        const enhancedParticipantsUpdate = {
          ...participantsUpdate,
          participants: participantsUpdate.participants, // Mantém array original de strings
          // Adiciona dados resolvidos em campo separado
          participantsData: resolvedParticipants,
        };

        this.sendDataWebhook(Events.GROUP_PARTICIPANTS_UPDATE, enhancedParticipantsUpdate);
      } catch (error) {
        this.logger.error(
          `Failed to resolve participant data for GROUP_PARTICIPANTS_UPDATE webhook: ${error.message} | Group: ${participantsUpdate.id} | Participants: ${participantsUpdate.participants.length}`,
        );
        // Fallback - envia sem conversão
        this.sendDataWebhook(Events.GROUP_PARTICIPANTS_UPDATE, participantsUpdate);
      }

      this.updateGroupMetadataCache(participantsUpdate.id);
    },
  };

  private readonly labelHandle = {
    [Events.LABELS_EDIT]: async (label: Label) => {
      this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });

      const labelsRepository = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

      const savedLabel = labelsRepository.find((l) => l.labelId === label.id);
      if (label.deleted && savedLabel) {
        await this.prismaRepository.label.delete({
          where: { labelId_instanceId: { instanceId: this.instanceId, labelId: label.id } },
        });
        this.sendDataWebhook(Events.LABELS_EDIT, { ...label, instance: this.instance.name });
        return;
      }

      const labelName = label.name.replace(/[^\x20-\x7E]/g, '');
      if (!savedLabel || savedLabel.color !== `${label.color}` || savedLabel.name !== labelName) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.LABELS) {
          const labelData = {
            color: `${label.color}`,
            name: labelName,
            labelId: label.id,
            predefinedId: label.predefinedId,
            instanceId: this.instanceId,
          };
          await this.prismaRepository.label.upsert({
            where: { labelId_instanceId: { instanceId: labelData.instanceId, labelId: labelData.labelId } },
            update: labelData,
            create: labelData,
          });
        }
      }
    },

    [Events.LABELS_ASSOCIATION]: async (
      data: { association: LabelAssociation; type: 'remove' | 'add' },
      database: Database,
    ) => {
      this.logger.info(
        `labels association - ${data?.association?.chatId} (${data.type}-${data?.association?.type}): ${data?.association?.labelId}`,
      );
      if (database.SAVE_DATA.CHATS) {
        const instanceId = this.instanceId;
        const chatId = data.association.chatId;
        const labelId = data.association.labelId;

        if (data.type === 'add') {
          await this.addLabel(labelId, instanceId, chatId);
        } else if (data.type === 'remove') {
          await this.removeLabel(labelId, instanceId, chatId);
        }
      }

      this.sendDataWebhook(Events.LABELS_ASSOCIATION, {
        instance: this.instance.name,
        type: data.type,
        chatId: data.association.chatId,
        labelId: data.association.labelId,
      });
    },
  };

  private eventHandler() {
    this.client.ev.process(async (events) => {
      this.eventProcessingQueue = this.eventProcessingQueue.then(async () => {
        try {
          if (!this.endSession) {
            const database = this.configService.get<Database>('DATABASE');
            const settings = await this.findSettings();

            if (events.call) {
              const call = events.call[0];

              if (settings?.rejectCall && call.status == 'offer') {
                this.client.rejectCall(call.id, call.from);
              }

              if (settings?.msgCall?.trim().length > 0 && call.status == 'offer') {
                if (call.from.endsWith('@lid')) {
                  call.from = await this.client.signalRepository.lidMapping.getPNForLID(call.from as string);
                }
                const msg = await this.client.sendMessage(call.from, { text: settings.msgCall });

                this.client.ev.emit('messages.upsert', { messages: [msg], type: 'notify' });
              }

              this.sendDataWebhook(Events.CALL, call);
            }

            if (events['connection.update']) {
              this.connectionUpdate(events['connection.update']);
            }

            if (events['creds.update']) {
              this.instance.authState.saveCreds();
            }

            if (events['messaging-history.set']) {
              const payload = events['messaging-history.set'];
              await this.messageHandle['messaging-history.set'](payload);
            }

            if (events['messages.upsert']) {
              const payload = events['messages.upsert'];

              // this.messageProcessor.processMessage(payload, settings);
              await this.messageHandle['messages.upsert'](payload, settings);
            }

            if (events['messages.update']) {
              const payload = events['messages.update'];
              await this.messageHandle['messages.update'](payload, settings);
            }

            if (events['message-receipt.update']) {
              const payload = events['message-receipt.update'] as MessageUserReceiptUpdate[];
              const remotesJidMap: Record<string, number> = {};

              for (const event of payload) {
                if (typeof event.key.remoteJid === 'string' && typeof event.receipt.readTimestamp === 'number') {
                  remotesJidMap[event.key.remoteJid] = event.receipt.readTimestamp;
                }
              }

              await Promise.all(
                Object.keys(remotesJidMap).map(async (remoteJid) =>
                  this.updateMessagesReadedByTimestamp(remoteJid, remotesJidMap[remoteJid]),
                ),
              );
            }

            if (events['presence.update']) {
              const payload = events['presence.update'];

              if (settings?.groupsIgnore && payload.id.includes('@g.us')) {
                return;
              }

              this.sendDataWebhook(Events.PRESENCE_UPDATE, payload);
            }

            if (!settings?.groupsIgnore) {
              if (events['groups.upsert']) {
                const payload = events['groups.upsert'];
                this.groupHandler['groups.upsert'](payload);
              }

              if (events['groups.update']) {
                const payload = events['groups.update'];
                this.groupHandler['groups.update'](payload);
              }

              if (events['group-participants.update']) {
                const payload = events['group-participants.update'] as any;
                this.groupHandler['group-participants.update'](payload);
              }
            }

            if (events['chats.upsert']) {
              const payload = events['chats.upsert'];
              this.chatHandle['chats.upsert'](payload);
            }

            if (events['chats.update']) {
              const payload = events['chats.update'];
              this.chatHandle['chats.update'](payload);
            }

            if (events['chats.delete']) {
              const payload = events['chats.delete'];
              this.chatHandle['chats.delete'](payload);
            }

            if (events['contacts.upsert']) {
              const payload = events['contacts.upsert'];
              this.contactHandle['contacts.upsert'](payload);
            }

            if (events['contacts.update']) {
              const payload = events['contacts.update'];
              this.contactHandle['contacts.update'](payload);
            }

            if (events[Events.LABELS_ASSOCIATION]) {
              const payload = events[Events.LABELS_ASSOCIATION];
              this.labelHandle[Events.LABELS_ASSOCIATION](payload, database);
              return;
            }

            if (events[Events.LABELS_EDIT]) {
              const payload = events[Events.LABELS_EDIT];
              this.labelHandle[Events.LABELS_EDIT](payload);
              return;
            }
          }
        } catch (error) {
          this.logger.error(error);
        }
      });
    });
  }

  private historySyncNotification(msg: proto.Message.IHistorySyncNotification) {
    const instance: InstanceDto = { instanceName: this.instance.name };

    if (
      this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
      this.localChatwoot?.enabled &&
      this.localChatwoot.importMessages &&
      this.isSyncNotificationFromUsedSyncType(msg)
    ) {
      if (msg.chunkOrder === 1) {
        this.chatwootService.startImportHistoryMessages(instance);
      }

      if (msg.progress === 100) {
        setTimeout(() => {
          this.chatwootService.importHistoryMessages(instance);
        }, 10000);
      }
    }

    return true;
  }

  private isSyncNotificationFromUsedSyncType(msg: proto.Message.IHistorySyncNotification) {
    return (
      (this.localSettings.syncFullHistory && msg?.syncType === 2) ||
      (!this.localSettings.syncFullHistory && msg?.syncType === 3)
    );
  }

  public async profilePicture(number: string) {
    const jid = createJid(number);

    try {
      const profilePictureUrl = await this.client.profilePictureUrl(jid, 'image');

      return { wuid: jid, profilePictureUrl };
    } catch {
      return { wuid: jid, profilePictureUrl: null };
    }
  }

  public async getStatus(number: string) {
    const jid = createJid(number);

    try {
      return { wuid: jid, status: (await this.client.fetchStatus(jid))[0]?.status };
    } catch {
      return { wuid: jid, status: null };
    }
  }

  public async fetchProfile(instanceName: string, number?: string) {
    const jid = number ? createJid(number) : this.client?.user?.id;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      if (number) {
        const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
        const picture = await this.profilePicture(info?.jid);
        const status = await this.getStatus(info?.jid);
        const business = await this.fetchBusinessProfile(info?.jid);

        return {
          wuid: info?.jid || jid,
          name: info?.name,
          numberExists: info?.exists,
          picture: picture?.profilePictureUrl,
          status: status?.status,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      } else {
        const instanceNames = instanceName ? [instanceName] : null;
        const info: Instance = await waMonitor.instanceInfo(instanceNames);
        const business = await this.fetchBusinessProfile(jid);

        return {
          wuid: jid,
          name: info?.profileName,
          numberExists: true,
          picture: info?.profilePicUrl,
          status: info?.connectionStatus,
          isBusiness: business.isBusiness,
          email: business?.email,
          description: business?.description,
          website: business?.website?.shift(),
        };
      }
    } catch {
      return { wuid: jid, name: null, picture: null, status: null, os: null, isBusiness: false };
    }
  }

  public async offerCall({ number, isVideo, callDuration }: OfferCallDto) {
    const jid = createJid(number);

    try {
      // const call = await this.client.offerCall(jid, isVideo);
      // setTimeout(() => this.client.terminateCall(call.id, call.to), callDuration * 1000);

      // return call;
      return { id: '123', jid, isVideo, callDuration };
    } catch (error) {
      return error;
    }
  }

  private async sendMessage(
    sender: string,
    message: any,
    mentions: any,
    linkPreview: any,
    quoted: any,
    messageId?: string,
    ephemeralExpiration?: number,
    contextInfo?: any,
    // participants?: GroupParticipant[],
  ) {
    sender = sender.toLowerCase();

    const option: any = { quoted };

    if (isJidGroup(sender)) {
      option.useCachedGroupMetadata = true;
      // if (participants)
      //   option.cachedGroupMetadata = async () => {
      //     return { participants: participants as GroupParticipant[] };
      //   };
    }

    if (ephemeralExpiration) option.ephemeralExpiration = ephemeralExpiration;

    // NOTE: NÃO DEVEMOS GERAR O messageId AQUI, SOMENTE SE VIER INFORMADO POR PARAMETRO. A GERAÇÃO ANTERIOR IMPEDE O WZAP DE IDENTIFICAR A SOURCE.
    if (messageId) option.messageId = messageId;

    if (message['viewOnceMessage']) {
      const m = generateWAMessageFromContent(sender, message, {
        timestamp: new Date(),
        userJid: this.instance.wuid,
        messageId,
        quoted,
      });
      const id = await this.client.relayMessage(sender, message, { messageId });
      m.key = { id: id, remoteJid: sender, participant: isPnUser(sender) ? sender : undefined, fromMe: true };
      for (const [key, value] of Object.entries(m)) {
        if (!value || (isArray(value) && value.length) === 0) {
          delete m[key];
        }
      }
      return m;
    }

    if (
      !message['audio'] &&
      !message['poll'] &&
      !message['sticker'] &&
      !message['conversation'] &&
      sender !== 'status@broadcast'
    ) {
      if (message['reactionMessage']) {
        return await this.client.sendMessage(
          sender,
          {
            react: { text: message['reactionMessage']['text'], key: message['reactionMessage']['key'] },
          } as unknown as AnyMessageContent,
          option as unknown as MiscMessageGenerationOptions,
        );
      }
    }

    if (contextInfo) {
      message['contextInfo'] = contextInfo;
    }

    if (message['conversation']) {
      return await this.client.sendMessage(
        sender,
        {
          text: message['conversation'],
          mentions,
          linkPreview: linkPreview,
          contextInfo: message['contextInfo'],
        } as unknown as AnyMessageContent,
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (!message['audio'] && !message['poll'] && !message['sticker'] && sender != 'status@broadcast') {
      return await this.client.sendMessage(
        sender,
        {
          forward: { key: { remoteJid: this.instance.wuid, fromMe: true }, message },
          mentions,
          contextInfo: message['contextInfo'],
        },
        option as unknown as MiscMessageGenerationOptions,
      );
    }

    if (sender === 'status@broadcast') {
      let jidList;
      if (message['status'].option.allContacts) {
        const contacts = await this.prismaRepository.contact.findMany({
          where: { instanceId: this.instanceId, remoteJid: { not: { endsWith: '@g.us' } } },
        });

        jidList = contacts.map((contact) => contact.remoteJid);
      } else {
        jidList = message['status'].option.statusJidList;
      }

      const batchSize = 10;

      const batches = Array.from({ length: Math.ceil(jidList.length / batchSize) }, (_, i) =>
        jidList.slice(i * batchSize, i * batchSize + batchSize),
      );

      let msgId: string | null = null;

      let firstMessage: WAMessage;

      const firstBatch = batches.shift();

      if (firstBatch) {
        firstMessage = await this.client.sendMessage(
          sender,
          message['status'].content as unknown as AnyMessageContent,
          {
            backgroundColor: message['status'].option.backgroundColor,
            font: message['status'].option.font,
            statusJidList: firstBatch,
          } as unknown as MiscMessageGenerationOptions,
        );

        msgId = firstMessage.key.id;
      }

      if (batches.length === 0) return firstMessage;

      await Promise.allSettled(
        batches.map(async (batch) => {
          const messageSent = await this.client.sendMessage(
            sender,
            message['status'].content as unknown as AnyMessageContent,
            {
              backgroundColor: message['status'].option.backgroundColor,
              font: message['status'].option.font,
              statusJidList: batch,
              messageId: msgId,
            } as unknown as MiscMessageGenerationOptions,
          );

          return messageSent;
        }),
      );

      return firstMessage;
    }

    return await this.client.sendMessage(
      sender,
      message as unknown as AnyMessageContent,
      option as unknown as MiscMessageGenerationOptions,
    );
  }

  private async sendMessageWithTyping<T = proto.IMessage>(
    number: string,
    message: T,
    options?: Options,
    isIntegration = false,
  ) {
    const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

    if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
      throw new BadRequestException(isWA);
    }

    const sender = isWA.jid.toLowerCase();

    this.logger.verbose(`Sending message to ${sender}`);

    try {
      if (options?.delay) {
        this.logger.verbose(`Typing for ${options.delay}ms to ${sender}`);
        if (options.delay > 20000) {
          let remainingDelay = options.delay;
          while (remainingDelay > 20000) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(20000);

            await this.client.sendPresenceUpdate('paused', sender);

            remainingDelay -= 20000;
          }
          if (remainingDelay > 0) {
            await this.client.presenceSubscribe(sender);

            await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

            await delay(remainingDelay);

            await this.client.sendPresenceUpdate('paused', sender);
          }
        } else {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((options.presence as WAPresence) ?? 'composing', sender);

          await delay(options.delay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      }

      const linkPreview = options?.linkPreview != false ? undefined : false;

      let quoted: WAMessage;

      if (options?.quoted) {
        const m = options?.quoted;

        const msg = m?.message ? m : ((await this.getMessage(m.key, true)) as WAMessage);

        if (msg) {
          quoted = msg;
        }
      }

      let messageSent: WAMessage;

      let mentions: string[];
      let contextInfo: any;

      if (isJidGroup(sender)) {
        let group;
        try {
          const cache = this.configService.get<CacheConf>('CACHE');
          if (!cache.REDIS.ENABLED && !cache.LOCAL.ENABLED) group = await this.findGroup({ groupJid: sender }, 'inner');
          else group = await this.getGroupMetadataCache(sender);
          // group = await this.findGroup({ groupJid: sender }, 'inner');
        } catch {
          throw new NotFoundException('Group not found');
        }

        if (!group) {
          throw new NotFoundException('Group not found');
        }

        if (options?.mentionsEveryOne) {
          mentions = group.participants.map((participant) => participant.id);
        } else if (options?.mentioned?.length) {
          mentions = options.mentioned.map((mention) => {
            const jid = createJid(mention);
            if (isJidGroup(jid)) {
              return null;
            }
            return jid;
          });
        }

        messageSent = await this.sendMessage(
          sender,
          message,
          mentions,
          linkPreview,
          quoted,
          null,
          group?.ephemeralDuration,
          // group?.participants,
        );
      } else {
        contextInfo = {
          mentionedJid: [],
          groupMentions: [],
          //expiration: 7776000,
          ephemeralSettingTimestamp: {
            low: Math.floor(Date.now() / 1000) - 172800,
            high: 0,
            unsigned: false,
          },
          disappearingMode: { initiator: 0 },
        };
        messageSent = await this.sendMessage(
          sender,
          message,
          mentions,
          linkPreview,
          quoted,
          null,
          undefined,
          contextInfo,
        );
      }

      if (Long.isLong(messageSent?.messageTimestamp)) {
        messageSent.messageTimestamp = messageSent.messageTimestamp?.toNumber();
      }

      const messageRaw = this.prepareMessage(messageSent);

      const isMedia =
        messageSent?.message?.imageMessage ||
        messageSent?.message?.videoMessage ||
        messageSent?.message?.stickerMessage ||
        messageSent?.message?.ptvMessage ||
        messageSent?.message?.documentMessage ||
        messageSent?.message?.documentWithCaptionMessage ||
        messageSent?.message?.ptvMessage ||
        messageSent?.message?.audioMessage;

      const isVideo = messageSent?.message?.videoMessage;

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && !isIntegration) {
        this.chatwootService.eventWhatsapp(
          Events.SEND_MESSAGE,
          { instanceName: this.instance.name, instanceId: this.instanceId },
          messageRaw,
        );
      }

      if (this.configService.get<Openai>('OPENAI').ENABLED && messageRaw?.message?.audioMessage) {
        const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({
          where: { instanceId: this.instanceId },
          include: { OpenaiCreds: true },
        });

        if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
          messageRaw.message.speechToText = `[audio] ${await this.openaiService.speechToText(messageRaw, this)}`;
        }
      }

      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        const msg = await this.prismaRepository.message.create({ data: messageRaw });

        if (isMedia && this.configService.get<S3>('S3').ENABLE) {
          try {
            if (isVideo && !this.configService.get<S3>('S3').SAVE_VIDEO) {
              throw new Error('Video upload is disabled.');
            }

            const message: any = messageRaw;

            // Verificação adicional para garantir que há conteúdo de mídia real
            const hasRealMedia = this.hasValidMediaContent(message);

            if (!hasRealMedia) {
              this.logger.warn('Message detected as media but contains no valid media content');
            } else {
              const media = await this.getBase64FromMediaMessage({ message }, true);

              if (!media) {
                this.logger.verbose('No valid media to upload (messageContextInfo only), skipping MinIO');
                return;
              }

              const { buffer, mediaType, fileName, size } = media;

              const mimetype = mimeTypes.lookup(fileName).toString();

              const fullName = join(
                `${this.instance.id}`,
                messageRaw.key.remoteJid,
                `${messageRaw.key.id}`,
                mediaType,
                fileName,
              );

              await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });

              await this.prismaRepository.media.create({
                data: { messageId: msg.id, instanceId: this.instanceId, type: mediaType, fileName: fullName, mimetype },
              });

              const mediaUrl = await s3Service.getObjectUrl(fullName);

              messageRaw.message.mediaUrl = mediaUrl;

              await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
            }
          } catch (error) {
            this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
          }
        }
      }

      if (this.localWebhook.enabled) {
        if (isMedia && this.localWebhook.webhookBase64) {
          try {
            const buffer = await downloadMediaMessage(
              { key: messageRaw.key, message: messageRaw?.message },
              'buffer',
              {},
              { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
            );

            if (buffer) {
              messageRaw.message.base64 = buffer.toString('base64');
            } else {
              // retry to download media
              const buffer = await downloadMediaMessage(
                { key: messageRaw.key, message: messageRaw?.message },
                'buffer',
                {},
                { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
              );

              if (buffer) {
                messageRaw.message.base64 = buffer.toString('base64');
              }
            }
          } catch (error) {
            this.logger.error(['Error converting media to base64', error?.message]);
          }
        }
      }

      this.logger.verbose(messageSent);

      this.sendDataWebhook(Events.SEND_MESSAGE, messageRaw);

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled && isIntegration) {
        await chatbotController.emit({
          instance: { instanceName: this.instance.name, instanceId: this.instanceId },
          remoteJid: messageRaw.key.remoteJid,
          msg: messageRaw,
          pushName: messageRaw.pushName,
          isIntegration,
        });
      }

      return messageRaw;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Instance Controller
  public async sendPresence(data: SendPresenceDto) {
    try {
      const { number } = data;

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      if (data?.delay && data?.delay > 20000) {
        let remainingDelay = data?.delay;
        while (remainingDelay > 20000) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(20000);

          await this.client.sendPresenceUpdate('paused', sender);

          remainingDelay -= 20000;
        }
        if (remainingDelay > 0) {
          await this.client.presenceSubscribe(sender);

          await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

          await delay(remainingDelay);

          await this.client.sendPresenceUpdate('paused', sender);
        }
      } else {
        await this.client.presenceSubscribe(sender);

        await this.client.sendPresenceUpdate((data?.presence as WAPresence) ?? 'composing', sender);

        await delay(data?.delay);

        await this.client.sendPresenceUpdate('paused', sender);
      }

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Presence Controller
  public async setPresence(data: SetPresenceDto) {
    try {
      await this.client.sendPresenceUpdate(data.presence);

      return { presence: data.presence };
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  // Send Message Controller
  public async textMessage(data: SendTextDto, isIntegration = false) {
    const text = data.text;

    if (!text || text.trim().length === 0) {
      throw new BadRequestException('Text is required');
    }

    return await this.sendMessageWithTyping(
      data.number,
      { conversation: data.text },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );
  }

  public async pollMessage(data: SendPollDto) {
    return await this.sendMessageWithTyping(
      data.number,
      { poll: { name: data.name, selectableCount: data.selectableCount, values: data.values } },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        linkPreview: data?.linkPreview,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  private async formatStatusMessage(status: StatusMessage) {
    if (!status.type) {
      throw new BadRequestException('Type is required');
    }

    if (!status.content) {
      throw new BadRequestException('Content is required');
    }

    if (status.allContacts) {
      const contacts = await this.prismaRepository.contact.findMany({ where: { instanceId: this.instanceId } });

      if (!contacts.length) {
        throw new BadRequestException('Contacts not found');
      }

      status.statusJidList = contacts.filter((contact) => contact.pushName).map((contact) => contact.remoteJid);
    }

    if (!status.statusJidList?.length && !status.allContacts) {
      throw new BadRequestException('StatusJidList is required');
    }

    if (status.type === 'text') {
      if (!status.backgroundColor) {
        throw new BadRequestException('Background color is required');
      }

      if (!status.font) {
        throw new BadRequestException('Font is required');
      }

      return {
        content: { text: status.content },
        option: { backgroundColor: status.backgroundColor, font: status.font, statusJidList: status.statusJidList },
      };
    }
    if (status.type === 'image') {
      return {
        content: { image: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'video') {
      return {
        content: { video: { url: status.content }, caption: status.caption },
        option: { statusJidList: status.statusJidList },
      };
    }

    if (status.type === 'audio') {
      const convert = await this.processAudioMp4(status.content);
      if (Buffer.isBuffer(convert)) {
        const result = {
          content: { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus' },
          option: { statusJidList: status.statusJidList },
        };

        return result;
      } else {
        throw new InternalServerErrorException(convert);
      }
    }

    throw new BadRequestException('Type not found');
  }

  public async statusMessage(data: SendStatusDto, file?: any) {
    const mediaData: SendStatusDto = { ...data };

    if (file) mediaData.content = file.buffer.toString('base64');

    const status = await this.formatStatusMessage(mediaData);

    const statusSent = await this.sendMessageWithTyping('status@broadcast', { status });

    return statusSent;
  }

  private async prepareMediaMessage(mediaMessage: MediaMessage) {
    try {
      const type = mediaMessage.mediatype === 'ptv' ? 'video' : mediaMessage.mediatype;

      let mediaInput: any;
      if (mediaMessage.mediatype === 'image') {
        let imageBuffer: Buffer;
        if (isURL(mediaMessage.media)) {
          let config: any = { responseType: 'arraybuffer' };

          if (this.localProxy?.enabled) {
            config = {
              ...config,
              httpsAgent: makeProxyAgent({
                host: this.localProxy.host,
                port: this.localProxy.port,
                protocol: this.localProxy.protocol,
                username: this.localProxy.username,
                password: this.localProxy.password,
              }),
            };
          }

          const response = await axios.get(mediaMessage.media, config);
          imageBuffer = Buffer.from(response.data, 'binary');
        } else {
          imageBuffer = Buffer.from(mediaMessage.media, 'base64');
        }

        mediaInput = await sharp(imageBuffer).jpeg().toBuffer();
        mediaMessage.fileName ??= 'image.jpg';
        mediaMessage.mimetype = 'image/jpeg';
      } else {
        mediaInput = isURL(mediaMessage.media)
          ? { url: mediaMessage.media }
          : Buffer.from(mediaMessage.media, 'base64');
      }

      const prepareMedia = await prepareWAMessageMedia(
        {
          [type]: mediaInput,
        } as any,
        { upload: this.client.waUploadToServer },
      );

      const mediaType = mediaMessage.mediatype + 'Message';

      if (mediaMessage.mediatype === 'document' && !mediaMessage.fileName) {
        const regex = new RegExp(/.*\/(.+?)\./);
        const arrayMatch = regex.exec(mediaMessage.media);
        mediaMessage.fileName = arrayMatch[1];
      }

      if (mediaMessage.mediatype === 'image' && !mediaMessage.fileName) {
        mediaMessage.fileName = 'image.jpg';
      }

      if (mediaMessage.mediatype === 'video' && !mediaMessage.fileName) {
        mediaMessage.fileName = 'video.mp4';
      }

      let mimetype: string | false;

      if (mediaMessage.mimetype) {
        mimetype = mediaMessage.mimetype;
      } else {
        mimetype = mimeTypes.lookup(mediaMessage.fileName);

        if (!mimetype && isURL(mediaMessage.media)) {
          let config: any = { responseType: 'arraybuffer' };

          if (this.localProxy?.enabled) {
            config = {
              ...config,
              httpsAgent: makeProxyAgent({
                host: this.localProxy.host,
                port: this.localProxy.port,
                protocol: this.localProxy.protocol,
                username: this.localProxy.username,
                password: this.localProxy.password,
              }),
            };
          }

          const response = await axios.get(mediaMessage.media, config);

          mimetype = response.headers['content-type'];
        }
      }

      if (mediaMessage.mediatype === 'ptv') {
        prepareMedia[mediaType] = prepareMedia[type + 'Message'];
        mimetype = 'video/mp4';

        if (!prepareMedia[mediaType]) {
          throw new Error('Failed to prepare video message');
        }

        try {
          let mediaInput;
          if (isURL(mediaMessage.media)) {
            mediaInput = mediaMessage.media;
          } else {
            const mediaBuffer = Buffer.from(mediaMessage.media, 'base64');
            if (!mediaBuffer || mediaBuffer.length === 0) {
              throw new Error('Invalid media buffer');
            }
            mediaInput = mediaBuffer;
          }

          const duration = await getVideoDuration(mediaInput);
          if (!duration || duration <= 0) {
            throw new Error('Invalid media duration');
          }

          this.logger.verbose(`Video duration: ${duration} seconds`);
          prepareMedia[mediaType].seconds = duration;
        } catch (error) {
          this.logger.error('Error getting video duration:');
          this.logger.error(error);
          throw new Error(`Failed to get video duration: ${error.message}`);
        }
      }

      if (mediaMessage?.fileName) {
        mimetype = mimeTypes.lookup(mediaMessage.fileName).toString();
        if (mimetype === 'application/mp4') {
          mimetype = 'video/mp4';
        }
      }

      prepareMedia[mediaType].caption = mediaMessage?.caption;
      prepareMedia[mediaType].mimetype = mimetype;
      prepareMedia[mediaType].fileName = mediaMessage.fileName;

      if (mediaMessage.mediatype === 'video') {
        prepareMedia[mediaType].gifPlayback = false;
      }

      return generateWAMessageFromContent(
        '',
        { [mediaType]: { ...prepareMedia[mediaType] } },
        { userJid: this.instance.wuid },
      );
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString() || error);
    }
  }

  private async convertToWebP(image: string): Promise<Buffer> {
    try {
      let imageBuffer: Buffer;

      if (isBase64(image)) {
        const base64Data = image.replace(/^data:image\/(jpeg|png|gif);base64,/, '');
        imageBuffer = Buffer.from(base64Data, 'base64');
      } else {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        const response = await axios.get(url, config);
        imageBuffer = Buffer.from(response.data, 'binary');
      }

      const isAnimated = this.isAnimated(image, imageBuffer);

      if (isAnimated) {
        return await sharp(imageBuffer, { animated: true }).webp({ quality: 80 }).toBuffer();
      } else {
        return await sharp(imageBuffer).webp().toBuffer();
      }
    } catch (error) {
      console.error('Erro ao converter a imagem para WebP:', error);
      throw error;
    }
  }

  private isAnimatedWebp(buffer: Buffer): boolean {
    if (buffer.length < 12) return false;

    return buffer.indexOf(Buffer.from('ANIM')) !== -1;
  }

  private isAnimated(image: string, buffer: Buffer): boolean {
    const lowerCaseImage = image.toLowerCase();

    if (lowerCaseImage.includes('.gif')) return true;

    if (lowerCaseImage.includes('.webp')) return this.isAnimatedWebp(buffer);

    return false;
  }

  public async mediaSticker(data: SendStickerDto, file?: any) {
    const mediaData: SendStickerDto = { ...data };

    if (file) mediaData.sticker = file.buffer.toString('base64');

    const convert = data?.notConvertSticker
      ? Buffer.from(data.sticker, 'base64')
      : await this.convertToWebP(data.sticker);
    const gifPlayback = data.sticker.includes('.gif');
    const result = await this.sendMessageWithTyping(
      data.number,
      { sticker: convert, gifPlayback },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );

    return result;
  }

  public async mediaMessage(data: SendMediaDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = { ...data };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async ptvMessage(data: SendPtvDto, file?: any, isIntegration = false) {
    const mediaData: SendMediaDto = {
      number: data.number,
      media: data.video,
      mediatype: 'ptv',
      delay: data?.delay,
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
    };

    if (file) mediaData.media = file.buffer.toString('base64');

    const generate = await this.prepareMediaMessage(mediaData);

    const mediaSent = await this.sendMessageWithTyping(
      data.number,
      { ...generate.message },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
      isIntegration,
    );

    return mediaSent;
  }

  public async processAudioMp4(audio: string) {
    let inputStream: PassThrough;

    if (isURL(audio)) {
      const response = await axios.get(audio, { responseType: 'stream' });
      inputStream = response.data;
    } else {
      const audioBuffer = Buffer.from(audio, 'base64');
      inputStream = new PassThrough();
      inputStream.end(audioBuffer);
    }

    return new Promise<Buffer>((resolve, reject) => {
      const ffmpegProcess = spawn(ffmpegPath.path, [
        '-i',
        'pipe:0',
        '-vn',
        '-ab',
        '128k',
        '-ar',
        '44100',
        '-f',
        'mp4',
        '-movflags',
        'frag_keyframe+empty_moov',
        'pipe:1',
      ]);

      const outputChunks: Buffer[] = [];
      let stderrData = '';

      ffmpegProcess.stdout.on('data', (chunk) => {
        outputChunks.push(chunk);
      });

      ffmpegProcess.stderr.on('data', (data) => {
        stderrData += data.toString();
        this.logger.verbose(`ffmpeg stderr: ${data}`);
      });

      ffmpegProcess.on('error', (error) => {
        console.error('Error in ffmpeg process', error);
        reject(error);
      });

      ffmpegProcess.on('close', (code) => {
        if (code === 0) {
          this.logger.verbose('Audio converted to mp4');
          const outputBuffer = Buffer.concat(outputChunks);
          resolve(outputBuffer);
        } else {
          this.logger.error(`ffmpeg exited with code ${code}`);
          this.logger.error(`ffmpeg stderr: ${stderrData}`);
          reject(new Error(`ffmpeg exited with code ${code}: ${stderrData}`));
        }
      });

      inputStream.pipe(ffmpegProcess.stdin);

      inputStream.on('error', (err) => {
        console.error('Error in inputStream', err);
        ffmpegProcess.stdin.end();
        reject(err);
      });
    });
  }

  public async processAudio(audio: string): Promise<Buffer> {
    const audioConverterConfig = this.configService.get<AudioConverter>('AUDIO_CONVERTER');
    if (audioConverterConfig.API_URL) {
      this.logger.verbose('Using audio converter API');
      const formData = new FormData();

      if (isURL(audio)) {
        formData.append('url', audio);
      } else {
        formData.append('base64', audio);
      }

      const { data } = await axios.post(audioConverterConfig.API_URL, formData, {
        headers: { ...formData.getHeaders(), apikey: audioConverterConfig.API_KEY },
      });

      if (!data.audio) {
        throw new InternalServerErrorException('Failed to convert audio');
      }

      this.logger.verbose('Audio converted');
      return Buffer.from(data.audio, 'base64');
    } else {
      let inputAudioStream: PassThrough;

      if (isURL(audio)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(audio);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        const config: any = { responseType: 'stream' };

        const response = await axios.get(url, config);
        inputAudioStream = response.data.pipe(new PassThrough());
      } else {
        const audioBuffer = Buffer.from(audio, 'base64');
        inputAudioStream = new PassThrough();
        inputAudioStream.end(audioBuffer);
      }

      const isLpcm = isURL(audio) && /\.lpcm($|\?)/i.test(audio);

      return new Promise((resolve, reject) => {
        const outputAudioStream = new PassThrough();
        const chunks: Buffer[] = [];

        outputAudioStream.on('data', (chunk) => chunks.push(chunk));
        outputAudioStream.on('end', () => {
          const outputBuffer = Buffer.concat(chunks);
          resolve(outputBuffer);
        });

        outputAudioStream.on('error', (error) => {
          console.log('error', error);
          reject(error);
        });

        ffmpeg.setFfmpegPath(ffmpegPath.path);

        let command = ffmpeg(inputAudioStream);

        if (isLpcm) {
          this.logger.verbose('Detected LPCM input – applying raw PCM settings');
          command = command.inputFormat('s16le').inputOptions(['-ar', '24000', '-ac', '1']);
        }

        command
          .outputFormat('ogg')
          .noVideo()
          .audioCodec('libopus')
          .addOutputOptions('-avoid_negative_ts make_zero')
          .audioBitrate('128k')
          .audioFrequency(48000)
          .audioChannels(1)
          .outputOptions([
            '-write_xing',
            '0',
            '-compression_level',
            '10',
            '-application',
            'voip',
            '-fflags',
            '+bitexact',
            '-flags',
            '+bitexact',
            '-id3v2_version',
            '0',
            '-map_metadata',
            '-1',
            '-map_chapters',
            '-1',
            '-write_bext',
            '0',
          ])
          .pipe(outputAudioStream, { end: true })
          .on('error', function (error) {
            console.log('error', error);
            reject(error);
          });
      });
    }
  }

  public async audioWhatsapp(data: SendAudioDto, file?: any, isIntegration = false) {
    const mediaData: SendAudioDto = { ...data };

    if (file?.buffer) {
      mediaData.audio = file.buffer.toString('base64');
    } else if (!isURL(data.audio) && !isBase64(data.audio)) {
      console.error('Invalid file or audio source');
      throw new BadRequestException('File buffer, URL, or base64 audio is required');
    }

    if (!data?.encoding && data?.encoding !== false) {
      data.encoding = true;
    }

    if (data?.encoding) {
      const convert = await this.processAudio(mediaData.audio);

      if (Buffer.isBuffer(convert)) {
        const result = this.sendMessageWithTyping<AnyMessageContent>(
          data.number,
          { audio: convert, ptt: true, mimetype: 'audio/ogg; codecs=opus' },
          { presence: 'recording', delay: data?.delay },
          isIntegration,
        );

        return result;
      } else {
        throw new InternalServerErrorException('Failed to convert audio');
      }
    }

    return await this.sendMessageWithTyping<AnyMessageContent>(
      data.number,
      {
        audio: isURL(data.audio) ? { url: data.audio } : Buffer.from(data.audio, 'base64'),
        ptt: true,
        mimetype: 'audio/ogg; codecs=opus',
      },
      { presence: 'recording', delay: data?.delay },
      isIntegration,
    );
  }

  private generateRandomId(length = 11) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
  }

  private toJSONString(button: Button): string {
    const toString = (obj: any) => JSON.stringify(obj);

    const json = {
      call: () => toString({ display_text: button.displayText, phone_number: button.phoneNumber }),
      reply: () => toString({ display_text: button.displayText, id: button.id }),
      copy: () => toString({ display_text: button.displayText, copy_code: button.copyCode }),
      url: () => toString({ display_text: button.displayText, url: button.url, merchant_url: button.url }),
      pix: () =>
        toString({
          currency: button.currency,
          total_amount: { value: 0, offset: 100 },
          reference_id: this.generateRandomId(),
          type: 'physical-goods',
          order: {
            status: 'pending',
            subtotal: { value: 0, offset: 100 },
            order_type: 'ORDER',
            items: [
              { name: '', amount: { value: 0, offset: 100 }, quantity: 0, sale_amount: { value: 0, offset: 100 } },
            ],
          },
          payment_settings: [
            {
              type: 'pix_static_code',
              pix_static_code: {
                merchant_name: button.name,
                key: button.key,
                key_type: this.mapKeyType.get(button.keyType),
              },
            },
          ],
          share_payment_status: false,
        }),
    };

    return json[button.type]?.() || '';
  }

  private readonly mapType = new Map<TypeButton, string>([
    ['reply', 'quick_reply'],
    ['copy', 'cta_copy'],
    ['url', 'cta_url'],
    ['call', 'cta_call'],
    ['pix', 'payment_info'],
  ]);

  private readonly mapKeyType = new Map<KeyType, string>([
    ['phone', 'PHONE'],
    ['email', 'EMAIL'],
    ['cpf', 'CPF'],
    ['cnpj', 'CNPJ'],
    ['random', 'EVP'],
  ]);

  public async buttonMessage(data: SendButtonsDto) {
    if (data.buttons.length === 0) {
      throw new BadRequestException('At least one button is required');
    }

    const hasReplyButtons = data.buttons.some((btn) => btn.type === 'reply');

    const hasPixButton = data.buttons.some((btn) => btn.type === 'pix');

    const hasOtherButtons = data.buttons.some((btn) => btn.type !== 'reply' && btn.type !== 'pix');

    if (hasReplyButtons) {
      if (data.buttons.length > 3) {
        throw new BadRequestException('Maximum of 3 reply buttons allowed');
      }
      if (hasOtherButtons) {
        throw new BadRequestException('Reply buttons cannot be mixed with other button types');
      }
    }

    if (hasPixButton) {
      if (data.buttons.length > 1) {
        throw new BadRequestException('Only one PIX button is allowed');
      }
      if (hasOtherButtons) {
        throw new BadRequestException('PIX button cannot be mixed with other button types');
      }

      const message: proto.IMessage = {
        viewOnceMessage: {
          message: {
            interactiveMessage: {
              nativeFlowMessage: {
                buttons: [{ name: this.mapType.get('pix'), buttonParamsJson: this.toJSONString(data.buttons[0]) }],
                messageParamsJson: JSON.stringify({ from: 'api', templateId: v4() }),
              },
            },
          },
        },
      };

      return await this.sendMessageWithTyping(data.number, message, {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      });
    }

    const generate = await (async () => {
      if (data?.thumbnailUrl) {
        return await this.prepareMediaMessage({ mediatype: 'image', media: data.thumbnailUrl });
      }
    })();

    const buttons = data.buttons.map((value) => {
      return { name: this.mapType.get(value.type), buttonParamsJson: this.toJSONString(value) };
    });

    const message: proto.IMessage = {
      viewOnceMessage: {
        message: {
          interactiveMessage: {
            body: {
              text: (() => {
                let t = '*' + data.title + '*';
                if (data?.description) {
                  t += '\n\n';
                  t += data.description;
                  t += '\n';
                }
                return t;
              })(),
            },
            footer: { text: data?.footer },
            header: (() => {
              if (generate?.message?.imageMessage) {
                return {
                  hasMediaAttachment: !!generate.message.imageMessage,
                  imageMessage: generate.message.imageMessage,
                };
              }
            })(),
            nativeFlowMessage: {
              buttons: buttons,
              messageParamsJson: JSON.stringify({ from: 'api', templateId: v4() }),
            },
          },
        },
      },
    };

    return await this.sendMessageWithTyping(data.number, message, {
      delay: data?.delay,
      presence: 'composing',
      quoted: data?.quoted,
      mentionsEveryOne: data?.mentionsEveryOne,
      mentioned: data?.mentioned,
    });
  }

  public async locationMessage(data: SendLocationDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        locationMessage: {
          degreesLatitude: data.latitude,
          degreesLongitude: data.longitude,
          name: data?.name,
          address: data?.address,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async listMessage(data: SendListDto) {
    return await this.sendMessageWithTyping(
      data.number,
      {
        listMessage: {
          title: data.title,
          description: data.description,
          buttonText: data?.buttonText,
          footerText: data?.footerText,
          sections: data.sections,
          listType: 2,
        },
      },
      {
        delay: data?.delay,
        presence: 'composing',
        quoted: data?.quoted,
        mentionsEveryOne: data?.mentionsEveryOne,
        mentioned: data?.mentioned,
      },
    );
  }

  public async contactMessage(data: SendContactDto) {
    const message: proto.IMessage = {};

    const vcard = (contact: ContactMessage) => {
      let result = 'BEGIN:VCARD\n' + 'VERSION:3.0\n' + `N:${contact.fullName}\n` + `FN:${contact.fullName}\n`;

      if (contact.organization) {
        result += `ORG:${contact.organization};\n`;
      }

      if (contact.email) {
        result += `EMAIL:${contact.email}\n`;
      }

      if (contact.url) {
        result += `URL:${contact.url}\n`;
      }

      if (!contact.wuid) {
        contact.wuid = createJid(contact.phoneNumber);
      }

      result += `item1.TEL;waid=${contact.wuid}:${contact.phoneNumber}\n` + 'item1.X-ABLabel:Celular\n' + 'END:VCARD';

      return result;
    };

    if (data.contact.length === 1) {
      message.contactMessage = { displayName: data.contact[0].fullName, vcard: vcard(data.contact[0]) };
    } else {
      message.contactsArrayMessage = {
        displayName: `${data.contact.length} contacts`,
        contacts: data.contact.map((contact) => {
          return { displayName: contact.fullName, vcard: vcard(contact) };
        }),
      };
    }

    return await this.sendMessageWithTyping(data.number, { ...message }, {});
  }

  public async reactionMessage(data: SendReactionDto) {
    return await this.sendMessageWithTyping(data.key.remoteJid, {
      reactionMessage: { key: data.key, text: data.reaction },
    });
  }

  // Chat Controller
  public async whatsappNumber(data: WhatsAppNumberDto) {
    const jids: {
      groups: { number: string; jid: string }[];
      broadcast: { number: string; jid: string }[];
      users: { number: string; jid: string; name?: string }[];
    } = { groups: [], broadcast: [], users: [] };

    data.numbers.forEach((number) => {
      const jid = createJid(number);

      if (isJidGroup(jid)) {
        jids.groups.push({ number, jid });
      } else if (jid === 'status@broadcast') {
        jids.broadcast.push({ number, jid });
      } else {
        jids.users.push({ number, jid });
      }
    });

    const onWhatsapp: OnWhatsAppDto[] = [];

    // BROADCAST
    onWhatsapp.push(...jids.broadcast.map(({ jid, number }) => new OnWhatsAppDto(jid, false, number)));

    // GROUPS
    const groups = await Promise.all(
      jids.groups.map(async ({ jid, number }) => {
        const group = await this.findGroup({ groupJid: jid }, 'inner');

        if (!group) {
          return new OnWhatsAppDto(jid, false, number);
        }

        return new OnWhatsAppDto(group.id, true, number, group?.subject);
      }),
    );
    onWhatsapp.push(...groups);

    // USERS
    const contacts: any[] = await this.prismaRepository.contact.findMany({
      where: { instanceId: this.instanceId, remoteJid: { in: jids.users.map(({ jid }) => jid) } },
    });

    // Unified cache verification for all numbers (normal and LID)
    const numbersToVerify = jids.users.map(({ jid }) => jid.replace('+', ''));

    // Get all numbers from cache
    const cachedNumbers = await getOnWhatsappCache(numbersToVerify);

    // Separate numbers that are and are not in cache
    const cachedJids = new Set(cachedNumbers.flatMap((cached) => cached.jidOptions));
    const numbersNotInCache = numbersToVerify.filter((jid) => !cachedJids.has(jid));

    // Only call Baileys for normal numbers (@s.whatsapp.net) that are not in cache
    let verify: { jid: string; exists: boolean }[] = [];
    const normalNumbersNotInCache = numbersNotInCache.filter((jid) => !jid.includes('@lid'));

    if (normalNumbersNotInCache.length > 0) {
      this.logger.verbose(`Checking ${normalNumbersNotInCache.length} numbers via Baileys (not found in cache)`);
      verify = await this.client.onWhatsApp(...normalNumbersNotInCache);
    }

    const verifiedUsers = await Promise.all(
      jids.users.map(async (user) => {
        // Try to get from cache first (works for all: normal and LID)
        const cached = cachedNumbers.find((cached) => cached.jidOptions.includes(user.jid.replace('+', '')));

        if (cached) {
          this.logger.verbose(`Number ${user.number} found in cache`);
          return new OnWhatsAppDto(
            cached.remoteJid,
            true,
            user.number,
            contacts.find((c) => c.remoteJid === cached.remoteJid)?.pushName,
            cached.lid || (cached.remoteJid.includes('@lid') ? 'lid' : undefined),
          );
        }

        // If it's a LID number and not in cache, consider it valid
        if (user.jid.includes('@lid')) {
          return new OnWhatsAppDto(
            user.jid,
            true,
            user.number,
            contacts.find((c) => c.remoteJid === user.jid)?.pushName,
            'lid',
          );
        }

        // If not in cache and is a normal number, use Baileys verification
        let numberVerified: (typeof verify)[0] | null = null;

        // Brazilian numbers
        if (user.number.startsWith('55')) {
          const numberWithDigit =
            user.number.slice(4, 5) === '9' && user.number.length === 13
              ? user.number
              : `${user.number.slice(0, 4)}9${user.number.slice(4)}`;
          const numberWithoutDigit =
            user.number.length === 12 ? user.number : user.number.slice(0, 4) + user.number.slice(5);

          numberVerified = verify.find(
            (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
          );
        }

        // Mexican/Argentina numbers
        // Ref: https://faq.whatsapp.com/1294841057948784
        if (!numberVerified && (user.number.startsWith('52') || user.number.startsWith('54'))) {
          let prefix = '';
          if (user.number.startsWith('52')) {
            prefix = '1';
          }
          if (user.number.startsWith('54')) {
            prefix = '9';
          }

          const numberWithDigit =
            user.number.slice(2, 3) === prefix && user.number.length === 13
              ? user.number
              : `${user.number.slice(0, 2)}${prefix}${user.number.slice(2)}`;
          const numberWithoutDigit =
            user.number.length === 12 ? user.number : user.number.slice(0, 2) + user.number.slice(3);

          numberVerified = verify.find(
            (v) => v.jid === `${numberWithDigit}@s.whatsapp.net` || v.jid === `${numberWithoutDigit}@s.whatsapp.net`,
          );
        }

        if (!numberVerified) {
          numberVerified = verify.find((v) => v.jid === user.jid);
        }

        const numberJid = numberVerified?.jid || user.jid;

        return new OnWhatsAppDto(
          numberJid,
          !!numberVerified?.exists,
          user.number,
          contacts.find((c) => c.remoteJid === numberJid)?.pushName,
          undefined,
        );
      }),
    );

    // Combine results
    onWhatsapp.push(...verifiedUsers);

    // TODO: Salvar no cache apenas números que NÃO estavam no cache
    const numbersToCache = onWhatsapp.filter((user) => {
      if (!user.exists) return false;
      // Verifica se estava no cache usando jidOptions
      const cached = cachedNumbers?.find((cached) => cached.jidOptions.includes(user.jid.replace('+', '')));
      return !cached;
    });

    if (numbersToCache.length > 0) {
      this.logger.verbose(`Salvando ${numbersToCache.length} números no cache`);
      await saveOnWhatsappCache(
        numbersToCache.map((user) => ({
          remoteJid: user.jid,
          lid: user.lid === 'lid' ? 'lid' : undefined,
        })),
      );
    }

    return onWhatsapp;
  }

  public async markMessageAsRead(data: ReadMessageDto) {
    try {
      const keys: proto.IMessageKey[] = [];
      data.readMessages.forEach((read) => {
        if (isJidGroup(read.remoteJid) || isPnUser(read.remoteJid)) {
          keys.push({ remoteJid: read.remoteJid, fromMe: read.fromMe, id: read.id });
        }
      });
      // darnabt 2026-10-03: always 'read-self' (syncs the read to our own phone/Web, never a blue tick to the
      // contact). Baileys' readMessages picks 'read' when read receipts are on — the owner never wants that.
      await this.client.sendReceipts(keys, 'read-self');
      return { message: 'Read messages', read: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Read messages fail', error.toString());
    }
  }

  public async getLastMessage(number: string) {
    const where: any = { key: { remoteJid: number }, instanceId: this.instance.id };

    const messages = await this.prismaRepository.message.findMany({
      where,
      orderBy: { messageTimestamp: 'desc' },
      take: 1,
    });

    if (messages.length === 0) {
      throw new NotFoundException('Messages not found');
    }

    let lastMessage = messages.pop();

    for (const message of messages) {
      if (message.messageTimestamp >= lastMessage.messageTimestamp) {
        lastMessage = message;
      }
    }

    return lastMessage as unknown as LastMessage;
  }

  // --- darnabt: checked app-state patches -----------------------------------------------
  // Baileys' chatModify/appPatch sends the patch and never reads WhatsApp's per-collection
  // answer, so a rejected patch (stale version, broken local state) came back as "success".
  // These helpers send the same patch but (1) make sure the local collection state matches
  // the server first, (2) read the server's reply, (3) retry once from a fresh server
  // snapshot, and (4) throw when WhatsApp did not accept it.
  private appStateRebuilt = new Set<string>();

  // darnabt 2026-10-01: Baileys drops a collection's state (sets it to null) whenever an incoming
  // app-state sync fails ("bad decrypt", "Invalid patch mac", missing key) and then simply gives
  // up — phone/Web mark-read/unread stop reaching the webhook with nothing in the logs at error
  // level. Watch for that drop and heal: wrong/missing keys -> re-request them from our own phone
  // (resyncs too); keys fine -> rebuild the state from the server so the next change applies.
  private appStateOwnWrite = false;
  private appStateHealTimer: NodeJS.Timeout | null = null;
  private appStateHealPending = new Set<string>();
  private appStateLastKeyRequest = 0;

  private watchAppStateDrops(keys: any) {
    return {
      ...keys,
      get: (type: any, ids: any) => keys.get(type, ids),
      set: async (data: any) => {
        const versions = data?.['app-state-sync-version'];
        if (versions && !this.appStateOwnWrite) {
          for (const name of Object.keys(versions)) {
            if (versions[name] === null) this.scheduleAppStateHeal(name);
          }
        }
        return keys.set(data);
      },
    };
  }

  private scheduleAppStateHeal(name: string) {
    this.appStateHealPending.add(name);
    if (this.appStateHealTimer) return;
    this.appStateHealTimer = setTimeout(() => {
      this.appStateHealTimer = null;
      const names = [...this.appStateHealPending];
      this.appStateHealPending.clear();
      this.healAppState(names).catch((e) => this.logger.warn(`app-state heal failed: ${e}`));
    }, 20_000);
  }

  private async healAppState(names: string[]) {
    const sock: any = this.client;
    if (!sock?.authState || this.stateConnection?.state !== 'open') return;
    const keys: any = sock.authState.keys;
    const dropped: string[] = [];
    for (const name of names) {
      const state = (await keys.get('app-state-sync-version', [name]))[name];
      if (!state) dropped.push(name);
    }
    if (!dropped.length) return; // Baileys recovered on its own (retry from scratch worked)
    let keysBad = false;
    for (const name of dropped) {
      try {
        const scan = await this.scanAppStateKeys(name);
        if (scan.total.bad + scan.total.missing > 0) keysBad = true;
        this.logger.warn(`app-state heal: ${name} was dropped by Baileys; key check ${JSON.stringify(scan.total)}`);
      } catch (error) {
        this.logger.warn(`app-state heal: scan ${name} failed: ${error}`);
      }
    }
    if (keysBad && Date.now() - this.appStateLastKeyRequest > 10 * 60_000) {
      this.appStateLastKeyRequest = Date.now();
      const r: any = await this.requestAppStateKeys({ waitSeconds: 30 });
      this.logger.warn(`app-state heal: key re-request ok=${r?.ok} resync=${JSON.stringify(r?.resync ?? {})}`);
      return;
    }
    const getKey = async (id: string) => (await keys.get('app-state-sync-key', [id]))[id];
    for (const name of dropped) {
      try {
        this.appStateOwnWrite = true;
        await this.rebuildAppStateFromServer(name, getKey);
        this.appStateRebuilt.add(name);
      } catch (error) {
        this.logger.warn(`app-state heal: rebuild ${name} failed: ${error}`);
      } finally {
        this.appStateOwnWrite = false;
      }
    }
  }

  private appStateReplyError(result: any, name: string): string | null {
    const sync = getBinaryNodeChild(result, 'sync');
    const collections = sync ? getBinaryNodeChildren(sync, 'collection') : [];
    const collection = collections.find((c: any) => c?.attrs?.name === name) ?? collections[0];
    if (!collection) return null;
    const error = getBinaryNodeChild(collection, 'error');
    if (collection.attrs?.type === 'error' || error) {
      return `code=${error?.attrs?.code ?? '?'} text=${error?.attrs?.text ?? '?'} collection=${JSON.stringify(
        collection.attrs,
      )}`;
    }
    return null;
  }

  // Rebuild one collection's LTHash state from WhatsApp's own snapshot + patches, without
  // firing events and WITHOUT decrypting values: the LTHash only needs each record's index
  // MAC and value MAC, so records encrypted with a key we hold wrongly ("bad decrypt" in
  // Baileys' resync) no longer block it. REMOVEs whose SET we never saw are skipped.
  // Then the result is checked against WhatsApp's own snapshot/patch MAC, and our own
  // app-state key is checked against records it encrypted — if either fails we refuse to
  // upload (an upload would be unreadable for the phone and WhatsApp Web).
  private async rebuildAppStateFromServer(name: string, getKey: (id: string) => Promise<any>) {
    const sock: any = this.client;
    const keys: any = sock.authState.keys;
    const myKeyId: string = sock.authState.creds.myAppStateKeyId;
    const keyStats: Record<string, { ok: number; bad: number; missing: number }> = {};
    const derived: Record<string, any> = {};
    const keysFor = async (b64: string) => {
      if (!(b64 in derived)) {
        const key = await getKey(b64);
        derived[b64] = key?.keyData
          ? await hkdf(Buffer.from(key.keyData), 160, { info: 'WhatsApp Mutation Keys' })
          : null;
      }
      return derived[b64];
    };
    const sampleRecord = async (record: any) => {
      const b64 = Buffer.from(record?.keyId?.id || []).toString('base64');
      const st = (keyStats[b64] = keyStats[b64] || { ok: 0, bad: 0, missing: 0 });
      if (st.ok + st.bad + st.missing >= 25) return;
      const expanded = await keysFor(b64);
      if (!expanded) {
        st.missing++;
        return;
      }
      try {
        const blob = Buffer.from(record.value.blob);
        aesDecrypt(blob.slice(0, -32), expanded.slice(32, 64));
        st.ok++;
      } catch {
        st.bad++;
      }
    };
    const macOf = async (hash: Buffer, version: number, keyIdBytes: any) => {
      const expanded = await keysFor(Buffer.from(keyIdBytes || []).toString('base64'));
      if (!expanded) return null;
      const v = Buffer.alloc(8);
      v.writeUInt32BE(version, 4);
      return hmacSign(Buffer.concat([hash, v, Buffer.from(name, 'utf-8')]), expanded.slice(96, 128), 'sha256');
    };

    let version = 0;
    let hash: Buffer = Buffer.alloc(128);
    let indexValueMap: Record<string, { valueMac: Buffer }> = {};
    let skippedRemoves = 0;
    let lastCheck: { mac: any; keyId: any; version: number } | null = null;
    // darnabt 2026-10-01: check the MAC at EVERY step, not just the last. Another device whose own
    // state broke (WA Web "MacMismatchFatal") uploads patches with a wrong snapshot MAC; the records
    // are still fine, so a hash rebuilt from all records that matched an earlier honest MAC is right.
    const macMatched: number[] = [];
    const macMismatched: number[] = [];
    const checkStep = async (mac: any, keyIdBytes: any, v: number) => {
      if (!mac) return;
      const computed = await macOf(hash, v, keyIdBytes);
      if (!computed) return;
      (Buffer.compare(computed, Buffer.from(mac)) === 0 ? macMatched : macMismatched).push(v);
    };

    for (let round = 0; round < 50; round++) {
      const result = await sock.query({
        tag: 'iq',
        attrs: { to: S_WHATSAPP_NET, xmlns: 'w:sync:app:state', type: 'set' },
        content: [
          {
            tag: 'sync',
            attrs: {},
            content: [
              { tag: 'collection', attrs: { name, version: version.toString(), return_snapshot: (!version).toString() } },
            ],
          },
        ],
      });
      const replyError = this.appStateReplyError(result, name);
      if (replyError) throw new Error(`WhatsApp refused to send ${name} state: ${replyError}`);
      const decoded: any = await extractSyncdPatches(result, {} as any);
      const { patches = [], hasMorePatches = false, snapshot = undefined } = decoded[name] || {};
      if (snapshot) {
        const adds: ArrayBuffer[] = [];
        indexValueMap = {};
        for (const record of snapshot.records || []) {
          const valueMac = Buffer.from(record.value.blob).slice(-32);
          indexValueMap[Buffer.from(record.index.blob).toString('base64')] = { valueMac };
          adds.push(new Uint8Array(valueMac).buffer);
          await sampleRecord(record);
        }
        hash = Buffer.from(await LT_HASH_ANTI_TAMPERING.subtractThenAdd(new Uint8Array(128).buffer, adds, []));
        version = Number(snapshot.version?.version || 0);
        lastCheck = { mac: snapshot.mac, keyId: snapshot.keyId?.id, version };
        await checkStep(snapshot.mac, snapshot.keyId?.id, version);
      }
      for (const patch of patches) {
        if (patch.externalMutations) {
          const ext = await downloadExternalPatch(patch.externalMutations, {} as any);
          patch.mutations = [...(patch.mutations || []), ...(ext.mutations || [])];
        }
        const adds: ArrayBuffer[] = [];
        const subs: ArrayBuffer[] = [];
        for (const m of patch.mutations || []) {
          const record = m.record;
          const indexB64 = Buffer.from(record.index.blob).toString('base64');
          const valueMac = Buffer.from(record.value.blob).slice(-32);
          const prev = indexValueMap[indexB64];
          if (m.operation === proto.SyncdMutation.SyncdOperation.REMOVE) {
            if (!prev) {
              skippedRemoves++;
              continue;
            }
            delete indexValueMap[indexB64];
          } else {
            adds.push(new Uint8Array(valueMac).buffer);
            indexValueMap[indexB64] = { valueMac };
            await sampleRecord(record);
          }
          if (prev) subs.push(new Uint8Array(prev.valueMac).buffer);
        }
        hash = Buffer.from(await LT_HASH_ANTI_TAMPERING.subtractThenAdd(new Uint8Array(hash).buffer, adds, subs));
        version = Number(patch.version?.version || version + 1);
        lastCheck = { mac: patch.snapshotMac, keyId: patch.keyId?.id, version };
        await checkStep(patch.snapshotMac, patch.keyId?.id, version);
      }
      if (!hasMorePatches) break;
    }

    const state = { version, hash, indexValueMap };
    let macResult = 'no snapshot/patch to check';
    if (lastCheck?.mac) {
      const computed = await macOf(hash, lastCheck.version, lastCheck.keyId);
      macResult = !computed
        ? 'check key missing'
        : Buffer.compare(computed, Buffer.from(lastCheck.mac)) === 0
          ? 'MATCH'
          : 'MISMATCH';
    }
    this.logger.warn(
      `app-state ${name}: rebuilt from server at v${version}; MAC check ${macResult} (matched at v${
        macMatched.slice(-5).join(',v') || '-'
      }; mismatched at v${macMismatched.slice(-5).join(',v') || '-'}); skipped ${skippedRemoves} orphan removes; myAppStateKeyId=${myKeyId}; key decrypt sample ${JSON.stringify(
        keyStats,
      )}`,
    );
    const own = keyStats[myKeyId];
    if (own && own.bad > 0 && own.ok === 0) {
      throw new Error(
        `this linked device's own app-state key (${myKeyId}) cannot decrypt ${name} records it should own — WhatsApp would not accept our change`,
      );
    }
    if (macResult === 'MISMATCH' && !(macMatched.length && skippedRemoves === 0)) {
      throw new Error(`rebuilt ${name} state does not match WhatsApp's MAC at v${version} — not uploading`);
    }
    if (macResult === 'MISMATCH') {
      this.logger.warn(
        `app-state ${name}: last MAC (v${version}) came from a device with a broken state; our rebuild matched v${
          macMatched[macMatched.length - 1]
        } and is built from every record — uploading on the true hash`,
      );
    }
    await keys.set({ 'app-state-sync-version': { [name]: state } });
    return state;
  }

  private async sendAppPatchChecked(patchCreate: any): Promise<number> {
    const name: string = patchCreate.type;
    const sock: any = this.client;
    const keys: any = sock.authState.keys;
    const myAppStateKeyId: string = sock.authState.creds.myAppStateKeyId;
    if (!myAppStateKeyId) {
      throw new Error('App state key not present on this linked device');
    }
    const getKey = async (id: string) => (await keys.get('app-state-sync-key', [id]))[id];

    let lastError = '';
    let keysRequested = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        // normal Baileys catch-up first, so new remote changes still fire chats.update
        await sock.resyncAppState(['regular_low', 'regular_high', 'regular'], false);
      } catch (error) {
        this.logger.warn(`app-state resync before ${name} patch failed: ${error}`);
      }
      // darnabt 2026-10-01: ALWAYS rebuild from WhatsApp's server copy and MAC-check it before
      // uploading. The cached local state went stale twice today (07:22Z the phone re-uploaded
      // every collection from v1; Baileys stacked the new patches on its old v206 hash) and our
      // next patch carried a wrong snapshot MAC -> WA Web "failed validate snapshot mac".
      let state: any;
      try {
        state = await this.rebuildAppStateFromServer(name, getKey);
        this.appStateRebuilt.add(name);
      } catch (error) {
        lastError = String(error);
        // wrong / missing app-state key -> ask our own phone for the keys once, then retry
        if (!keysRequested && /decrypt|failed to find key|MISMATCH|check key missing/i.test(lastError)) {
          keysRequested = true;
          this.logger.warn(`app-state ${name}: ${lastError} -> re-requesting keys from the phone`);
          await this.requestAppStateKeys({ waitSeconds: 25 }).catch((e) =>
            this.logger.warn(`app-state key re-request failed: ${e}`),
          );
          continue;
        }
        throw error;
      }
      // the key request above can rotate our own key id -> read it fresh
      const ownKeyId: string = sock.authState.creds.myAppStateKeyId || myAppStateKeyId;
      const { patch, state: next } = await encodeSyncdPatch(patchCreate, ownKeyId, state, getKey);
      const result = await sock.query({
        tag: 'iq',
        attrs: { to: S_WHATSAPP_NET, type: 'set', xmlns: 'w:sync:app:state' },
        content: [
          {
            tag: 'sync',
            attrs: {},
            content: [
              {
                tag: 'collection',
                attrs: { name, version: (next.version - 1).toString(), return_snapshot: 'false' },
                content: [{ tag: 'patch', attrs: {}, content: proto.SyncdPatch.encode(patch).finish() }],
              },
            ],
          },
        ],
      });
      const replyError = this.appStateReplyError(result, name);
      if (!replyError) {
        await keys.set({ 'app-state-sync-version': { [name]: next } });
        this.logger.warn(`app-state ${name}: patch ACCEPTED by WhatsApp at v${next.version}`);
        return next.version;
      }
      lastError = replyError;
      this.logger.warn(`app-state ${name}: patch REJECTED (attempt ${attempt}) at v${next.version - 1}: ${replyError}`);
    }
    throw new Error(`WhatsApp rejected the ${name} change: ${lastError}`);
  }

  // Read one collection from WhatsApp's server WITHOUT decrypting or changing local state:
  // which app-state key ids its snapshot/patches use, and how many sampled records each of
  // our stored keys can decrypt (ok / bad = "bad decrypt" / missing = we hold no such key).
  private async scanAppStateKeys(name: string) {
    const sock: any = this.client;
    const keys: any = sock.authState.keys;
    const keyIds = new Set<string>();
    const stats: Record<string, { ok: number; bad: number; missing: number }> = {};
    const derived: Record<string, any> = {};
    const b64 = (bytes: any) => Buffer.from(bytes || []).toString('base64');
    const addId = (bytes: any) => {
      const id = b64(bytes);
      if (id) keyIds.add(id);
    };
    const sample = async (record: any) => {
      const id = b64(record?.keyId?.id);
      if (!id) return;
      keyIds.add(id);
      const st = (stats[id] = stats[id] || { ok: 0, bad: 0, missing: 0 });
      if (st.ok + st.bad + st.missing >= 10) return;
      if (!(id in derived)) {
        const key = (await keys.get('app-state-sync-key', [id]))[id];
        derived[id] = key?.keyData ? await hkdf(Buffer.from(key.keyData), 160, { info: 'WhatsApp Mutation Keys' }) : null;
      }
      if (!derived[id]) {
        st.missing++;
        return;
      }
      try {
        const blob = Buffer.from(record.value.blob);
        aesDecrypt(blob.slice(0, -32), derived[id].slice(32, 64));
        st.ok++;
      } catch {
        st.bad++;
      }
    };

    let version = 0;
    for (let round = 0; round < 50; round++) {
      const result = await sock.query({
        tag: 'iq',
        attrs: { to: S_WHATSAPP_NET, xmlns: 'w:sync:app:state', type: 'set' },
        content: [
          {
            tag: 'sync',
            attrs: {},
            content: [
              { tag: 'collection', attrs: { name, version: version.toString(), return_snapshot: (!version).toString() } },
            ],
          },
        ],
      });
      const replyError = this.appStateReplyError(result, name);
      if (replyError) throw new Error(`WhatsApp refused to send ${name} state: ${replyError}`);
      const decoded: any = await extractSyncdPatches(result, {} as any);
      const { patches = [], hasMorePatches = false, snapshot = undefined } = decoded[name] || {};
      if (snapshot) {
        addId(snapshot.keyId?.id);
        for (const record of snapshot.records || []) await sample(record);
        version = Number(snapshot.version?.version || 0);
      }
      for (const patch of patches) {
        addId(patch.keyId?.id);
        if (patch.externalMutations) {
          const ext = await downloadExternalPatch(patch.externalMutations, {} as any);
          patch.mutations = [...(patch.mutations || []), ...(ext.mutations || [])];
        }
        for (const m of patch.mutations || []) await sample(m.record);
        version = Number(patch.version?.version || version + 1);
      }
      if (!hasMorePatches) break;
    }
    const total = Object.values(stats).reduce(
      (acc, s) => ({ ok: acc.ok + s.ok, bad: acc.bad + s.bad, missing: acc.missing + s.missing }),
      { ok: 0, bad: 0, missing: 0 },
    );
    return { version, keyIds: [...keyIds], stats, total };
  }

  // Repair a linked device whose stored app-state sync keys are wrong ("bad decrypt" when
  // resyncing regular_low etc.) WITHOUT re-linking: ask our own primary phone to re-share the
  // keys with an APP_STATE_SYNC_KEY_REQUEST — a hidden peer protocol message to our own JID,
  // the same one WhatsApp Web sends; never a chat message. Baileys stores the keys from the
  // phone's APP_STATE_SYNC_KEY_SHARE answer itself; then every collection is resynced from
  // its server snapshot. The reply says plainly whether the phone answered and what synced.
  public async requestAppStateKeys(data: { collections?: string[]; keyIds?: string[]; waitSeconds?: number }) {
    const sock: any = this.client;
    if (!sock?.authState?.creds?.me?.id || this.stateConnection?.state !== 'open') {
      throw new BadRequestException('Instance is not connected');
    }
    const keys: any = sock.authState.keys;
    const collections =
      Array.isArray(data?.collections) && data.collections.length
        ? data.collections
        : ['regular_low', 'regular_high', 'regular', 'critical_block', 'critical_unblock_low'];
    const waitMs = Math.min(Math.max(Number(data?.waitSeconds ?? 90), 5), 280) * 1000;
    const myKeyIdBefore: string = sock.authState.creds.myAppStateKeyId;

    // 1. key ids the server data uses + what we can decrypt now
    const before: Record<string, any> = {};
    const keyIdSet = new Set<string>(Array.isArray(data?.keyIds) ? data.keyIds : []);
    if (myKeyIdBefore) keyIdSet.add(myKeyIdBefore);
    for (const name of collections) {
      try {
        const scan = await this.scanAppStateKeys(name);
        scan.keyIds.forEach((id) => keyIdSet.add(id));
        before[name] = { version: scan.version, keys: scan.keyIds.length, decrypt: scan.total };
      } catch (error) {
        before[name] = { error: String(error) };
      }
    }
    const keyIds = [...keyIdSet];
    const fingerprint = async () => {
      const got = await keys.get('app-state-sync-key', keyIds);
      const out: Record<string, string | null> = {};
      for (const id of keyIds) {
        out[id] = got[id]?.keyData
          ? createHash('sha256').update(Buffer.from(got[id].keyData)).digest('hex').slice(0, 12)
          : null;
      }
      return out;
    };
    const fpBefore = await fingerprint();

    // 2. ask the phone, wait for its key share
    const shared: string[] = [];
    let shares = 0;
    const onUpsert = ({ messages }: any) => {
      for (const m of messages || []) {
        const p = m?.message?.protocolMessage;
        if (p?.type === proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_SHARE) {
          shares++;
          for (const k of p.appStateSyncKeyShare?.keys || []) {
            shared.push(Buffer.from(k?.keyId?.keyId || []).toString('base64'));
          }
        }
      }
    };
    sock.ev.on('messages.upsert', onUpsert);
    let requestId = '';
    const startedAt = Date.now();
    try {
      const meJid = jidNormalizedUser(sock.authState.creds.me.id);
      requestId = await sock.relayMessage(
        meJid,
        {
          protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.APP_STATE_SYNC_KEY_REQUEST,
            appStateSyncKeyRequest: { keyIds: keyIds.map((id) => ({ keyId: Buffer.from(id, 'base64') })) },
          },
        },
        {
          additionalAttributes: { category: 'peer', push_priority: 'high_force' },
          additionalNodes: [{ tag: 'meta', attrs: { appdata: 'default' } }],
        },
      );
      this.logger.warn(`app-state key request ${requestId} sent to own phone for ${keyIds.length} key ids`);
      while (Date.now() - startedAt < waitMs && shares === 0) await delay(2000);
      if (shares) await delay(4000); // let Baileys finish storing the keys
    } finally {
      sock.ev.off('messages.upsert', onUpsert);
    }
    const fpAfter = await fingerprint();
    const changed = keyIds.filter((id) => fpBefore[id] !== fpAfter[id]);
    const answer = {
      requestId,
      requestedKeyIds: keyIds.length,
      phoneAnswered: shares > 0,
      waitedSeconds: Math.round((Date.now() - startedAt) / 1000),
      sharedKeyIds: shared.length,
      keysChanged: changed.length,
      myAppStateKeyId: { before: myKeyIdBefore, after: sock.authState.creds.myAppStateKeyId },
    };
    this.logger.warn(`app-state key request result: ${JSON.stringify(answer)}`);
    if (!shares && !changed.length) {
      return { ok: false, ...answer, before, message: 'The phone did not answer the key request; nothing was changed.' };
    }

    // 3. resync every collection from its server snapshot with the fresh keys
    const resync: Record<string, string> = {};
    for (const name of collections) {
      this.appStateOwnWrite = true;
      try {
        await keys.set({ 'app-state-sync-version': { [name]: null } });
      } finally {
        this.appStateOwnWrite = false;
      }
      this.appStateRebuilt.delete(name);
      try {
        await sock.resyncAppState([name], true);
      } catch (error) {
        resync[name] = `error: ${error}`;
        continue;
      }
      const state = (await keys.get('app-state-sync-version', [name]))[name];
      resync[name] = state ? `synced v${state.version}` : 'FAILED (Baileys dropped it — LOG_BAILEYS=info shows why)';
    }

    // 4. decrypt check again
    const after: Record<string, any> = {};
    for (const name of collections) {
      try {
        const scan = await this.scanAppStateKeys(name);
        after[name] = { version: scan.version, decrypt: scan.total };
      } catch (error) {
        after[name] = { error: String(error) };
      }
    }
    this.logger.warn(`app-state resync after key share: ${JSON.stringify({ resync, after })}`);
    const ok = collections.every((n) => resync[n]?.startsWith('synced'));
    return { ok, ...answer, before, resync, after };
  }

  // darnabt 2026-10-01: READ-ONLY per-chat state straight from WhatsApp's app-state server copy
  // (snapshot + every patch of regular_low / regular_high), decrypted with our keys. Nothing is
  // written: no local state, no events, no webhooks. Nawah uses it to reconcile its read/unread
  // dots with what the phone and WhatsApp Web show (mark-unread, read marks, archive, pin, mute).
  // markChatAsRead.read=false = "marked unread"; read=true covers messages up to rangeLastTs (s).
  public async appStateChats(data: { collections?: string[]; trace?: string[] }) {
    const sock: any = this.client;
    if (!sock?.authState || this.stateConnection?.state !== 'open') {
      throw new BadRequestException('Instance is not connected');
    }
    const keys: any = sock.authState.keys;
    const collections =
      Array.isArray(data?.collections) && data.collections.length ? data.collections : ['regular_low', 'regular_high'];
    const num = (v: any) => (v === null || v === undefined ? null : Number(v?.toString?.() ?? v));
    const derived: Record<string, Buffer | null> = {};
    const keyFor = async (bytes: any) => {
      const id = Buffer.from(bytes || []).toString('base64');
      if (!(id in derived)) {
        const key = (await keys.get('app-state-sync-key', [id]))[id];
        derived[id] = key?.keyData ? await hkdf(Buffer.from(key.keyData), 160, { info: 'WhatsApp Mutation Keys' }) : null;
      }
      return derived[id];
    };
    type Entry = { index: string[]; value: any };
    // darnabt: `trace: [jid, ...]` lists every mutation WhatsApp holds for those chats, in order
    // (snapshot, then each patch: SET / REMOVE, value, writer key) — to debug what a device wrote.
    const traceJids = new Set(Array.isArray(data?.trace) ? data.trace.filter((j) => typeof j === 'string') : []);
    const trace: any[] = [];
    const stats: Record<string, { version: number; records: number; decrypted: number; failed: number }> = {};
    const chats: Record<string, any> = {};
    const chat = (jid: string) => (chats[jid] = chats[jid] || { jid });

    for (const name of collections) {
      const live: Record<string, Entry> = {}; // keyed by index MAC (base64) — REMOVE needs no decrypt
      const st = (stats[name] = { version: 0, records: 0, decrypted: 0, failed: 0 });
      const note = (index: string[], value: any, op: string, at: string, record: any) => {
        if (!traceJids.has(index?.[1])) return;
        const m = value?.markChatAsReadAction;
        trace.push({
          collection: name,
          at,
          op,
          type: index[0],
          jid: index[1],
          ...(m ? { read: !!m.read, rangeMessages: (m.messageRange?.messages || []).length } : {}),
          ...(value?.archiveChatAction ? { archived: !!value.archiveChatAction.archived } : {}),
          ...(value?.pinAction ? { pinned: !!value.pinAction.pinned } : {}),
          ts: num(value?.timestamp),
          keyId: Buffer.from(record?.keyId?.id || []).toString('base64'),
        });
      };
      const apply = async (record: any, remove: boolean, at: string) => {
        const indexMac = Buffer.from(record?.index?.blob || []).toString('base64');
        if (remove) {
          if (traceJids.size) {
            // a REMOVE carries the retired record: decode it for the trace only
            try {
              const expandedOld = await keyFor(record?.keyId?.id);
              const blobOld = Buffer.from(record.value.blob);
              const plainOld = aesDecrypt(blobOld.slice(0, -32), expandedOld.slice(32, 64));
              const actionOld = proto.SyncActionData.decode(plainOld);
              note(JSON.parse(Buffer.from(actionOld.index).toString()), actionOld.value, 'REMOVE', at, record);
            } catch {
              if (live[indexMac]) note(live[indexMac].index, live[indexMac].value, 'REMOVE (undecoded)', at, record);
            }
          }
          delete live[indexMac];
          return;
        }
        const expanded = await keyFor(record?.keyId?.id);
        if (!expanded) {
          st.failed++;
          return;
        }
        try {
          const blob = Buffer.from(record.value.blob);
          const plain = aesDecrypt(blob.slice(0, -32), expanded.slice(32, 64));
          const action = proto.SyncActionData.decode(plain);
          live[indexMac] = { index: JSON.parse(Buffer.from(action.index).toString()), value: action.value };
          st.decrypted++;
          if (traceJids.size) note(live[indexMac].index, action.value, 'SET', at, record);
        } catch {
          st.failed++;
        }
      };
      let version = 0;
      for (let round = 0; round < 50; round++) {
        const result = await sock.query({
          tag: 'iq',
          attrs: { to: S_WHATSAPP_NET, xmlns: 'w:sync:app:state', type: 'set' },
          content: [
            {
              tag: 'sync',
              attrs: {},
              content: [
                { tag: 'collection', attrs: { name, version: version.toString(), return_snapshot: (!version).toString() } },
              ],
            },
          ],
        });
        const replyError = this.appStateReplyError(result, name);
        if (replyError) throw new Error(`WhatsApp refused to send ${name} state: ${replyError}`);
        const decoded: any = await extractSyncdPatches(result, {} as any);
        const { patches = [], hasMorePatches = false, snapshot = undefined } = decoded[name] || {};
        if (snapshot) {
          for (const k of Object.keys(live)) delete live[k];
          version = Number(snapshot.version?.version || 0);
          for (const record of snapshot.records || []) await apply(record, false, `snapshot v${version}`);
        }
        for (const patch of patches) {
          if (patch.externalMutations) {
            const ext = await downloadExternalPatch(patch.externalMutations, {} as any);
            patch.mutations = [...(patch.mutations || []), ...(ext.mutations || [])];
          }
          version = Number(patch.version?.version || version + 1);
          for (const m of patch.mutations || []) {
            await apply(m.record, m.operation === proto.SyncdMutation.SyncdOperation.REMOVE, `patch v${version}`);
          }
        }
        if (!hasMorePatches) break;
      }
      st.version = version;
      st.records = Object.keys(live).length;

      for (const { index, value } of Object.values(live)) {
        const [type, jid] = index;
        if (!jid || typeof jid !== 'string') continue;
        const at = num(value?.timestamp);
        if (type === 'markChatAsRead' && value?.markChatAsReadAction) {
          const a = value.markChatAsReadAction;
          const r = a.messageRange || {};
          // darnabt: a chat can hold two records for one index (written under different app-state
          // keys, the older one not yet retired) — the newest write is the chat's state.
          const prevAt = chats[jid]?.readAt;
          if (typeof prevAt === 'number' && typeof at === 'number' && prevAt > at) continue;
          // the mark names the chat's last messages: their ids let the receiver find a @lid chat
          // it has no LID for (it knows the messages).
          const rangeIds = (Array.isArray(r.messages) ? r.messages : [])
            .map((m: any) => m?.key?.id)
            .filter((id: any) => typeof id === 'string' && id)
            .slice(-5);
          Object.assign(chat(jid), {
            read: !!a.read,
            readAt: at,
            rangeLastTs: num(r.lastMessageTimestamp) || num(r.lastSystemMessageTimestamp) || null,
            rangeIds,
          });
        } else if (type === 'archive' && value?.archiveChatAction) {
          const prevAt = chats[jid]?.archivedAt;
          if (typeof prevAt === 'number' && typeof at === 'number' && prevAt > at) continue;
          Object.assign(chat(jid), { archived: !!value.archiveChatAction.archived, archivedAt: at });
        } else if (type === 'pin_v1' && value?.pinAction) {
          Object.assign(chat(jid), { pinned: !!value.pinAction.pinned, pinnedAt: at });
        } else if (type === 'mute' && value?.muteAction) {
          Object.assign(chat(jid), {
            muted: !!value.muteAction.muted,
            muteEnd: num(value.muteAction.muteEndTimestamp),
          });
        }
      }
    }
    const list = Object.values(chats);
    // darnabt: name the phone JID of every @lid chat we know a pair for (Baileys' LID store), so
    // the reconcile can match chats whose LID the receiver never saw.
    const lidChats = list.filter((c) => typeof c.jid === 'string' && c.jid.endsWith('@lid'));
    for (let i = 0; i < lidChats.length; i += 50) {
      await Promise.all(
        lidChats.slice(i, i + 50).map(async (c) => {
          try {
            const pn = await sock.signalRepository.lidMapping.getPNForLID(c.jid);
            if (pn) c.pn = jidNormalizedUser(pn);
          } catch {
            // unknown pair — left without a phone
          }
        }),
      );
    }
    this.logger.info(`app-state chats read: ${list.length} chats ${JSON.stringify(stats)}`);
    return { ok: true, collections: stats, chats: list, ...(traceJids.size ? { trace } : {}) };
  }

  public async archiveChat(data: ArchiveChatDto) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Math.floor(Date.now() / 1000);
        number = last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      const appStateVersion = await this.sendAppPatchChecked(
        chatModificationToAppPatch({ archive: data.archive, lastMessages: [last_message] } as any, createJid(number)),
      );

      return { chatId: number, archived: true, appStateVersion };
    } catch (error) {
      throw new InternalServerErrorException({
        archived: false,
        message: ['An error occurred while archiving the chat. Open a calling.', error.toString()],
      });
    }
  }

  public async markChatUnread(data: MarkChatUnreadDto) {
    return this.setChatReadState(data, false);
  }

  // darnabt: "Mark as read" from Nawah. Read receipts alone never clear a chat that was marked
  // unread by hand (phone / WhatsApp Web / Nawah) — that flag is app-state, so clear it there.
  public async markChatRead(data: MarkChatUnreadDto) {
    return this.setChatReadState(data, true);
  }

  private async setChatReadState(data: MarkChatUnreadDto, read: boolean) {
    try {
      let last_message = data.lastMessage;
      let number = data.chat;

      if (!last_message && number) {
        last_message = await this.getLastMessage(number);
      } else {
        last_message = data.lastMessage;
        last_message.messageTimestamp = last_message?.messageTimestamp ?? Math.floor(Date.now() / 1000);
        number = data.chat || last_message?.key?.remoteJid;
      }

      if (!last_message || Object.keys(last_message).length === 0) {
        throw new NotFoundException('Last message not found');
      }

      // darnabt: WhatsApp keeps every 1:1 chat under its @lid. A caller that only knows the phone
      // number gets the LID looked up here (Baileys' store first, then one USync query to WhatsApp —
      // no message is sent) and the answer names it, so the caller can keep the pair.
      let chatJid = createJid(number);
      let lid: string | null = null;
      if (chatJid.endsWith('@s.whatsapp.net')) {
        try {
          const found = await this.client.signalRepository.lidMapping.getLIDForPN(chatJid);
          if (found) lid = jidNormalizedUser(found);
        } catch (e) {
          this.logger.warn(`mark ${read ? 'read' : 'unread'}: LID lookup failed for ${chatJid}: ${e}`);
        }
        if (lid) chatJid = lid;
      }

      const ts = Number(last_message.messageTimestamp) || Math.floor(Date.now() / 1000);
      const nowSeconds = Math.floor(Date.now() / 1000);
      // darnabt: WhatsApp (Web: WAWebMarkChatAsReadSync + WAWebMessageRangeUtils.compareMessageRanges)
      // applies an UNREAD mark only when the mark's message range ENCLOSES the chat's own newest
      // messages: every newer message must be listed by id, or be older than lastMessageTimestamp.
      // A caller whose newest known message is not WhatsApp's newest (a message it never stored)
      // was silently ignored. "Unread as of now" encloses whatever the chat holds, so stamp the
      // range with the current time; the named message keeps its real timestamp.
      // A READ mark keeps the real timestamp (it must never cover messages nobody has seen).
      // A group message from someone else must name its sender, or WhatsApp rejects the whole range
      // (MESSAGE_RANGE_MESSAGE_KEY_PARTICIPANT_UNSET); without it the range goes by time alone.
      const key: any = { ...last_message.key, remoteJid: chatJid };
      const keyUsable = !(chatJid.endsWith('@g.us') && !key.fromMe && !key.participant);
      const messageRange = {
        lastMessageTimestamp: read ? ts : Math.max(ts, nowSeconds),
        ...(keyUsable ? { messages: [{ key, timestamp: ts }] } : {}),
      };

      const appStateVersion = await this.sendAppPatchChecked(
        chatModificationToAppPatch({ markRead: read, lastMessages: messageRange } as any, chatJid),
      );

      return read
        ? { chatId: chatJid, markedChatRead: true, appStateVersion, ...(lid ? { lid } : {}) }
        : { chatId: chatJid, markedChatUnread: true, appStateVersion, ...(lid ? { lid } : {}) };
    } catch (error) {
      throw new InternalServerErrorException({
        [read ? 'markedChatRead' : 'markedChatUnread']: false,
        message: [`An error occurred while marking the chat ${read ? 'read' : 'unread'}.`, error.toString()],
      });
    }
  }

  public async deleteMessage(del: DeleteMessage) {
    try {
      const response = await this.client.sendMessage(del.remoteJid, { delete: del });
      if (response) {
        const messageId = response.message?.protocolMessage?.key?.id;
        if (messageId) {
          const isLogicalDeleted = configService.get<Database>('DATABASE').DELETE_DATA.LOGICAL_MESSAGE_DELETE;
          let message = await this.prismaRepository.message.findFirst({
            where: { key: { path: ['id'], equals: messageId } },
          });
          if (isLogicalDeleted) {
            if (!message) return response;
            const existingKey = typeof message?.key === 'object' && message.key !== null ? message.key : {};
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: { key: { ...existingKey, deleted: true }, status: 'DELETED' },
            });
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: response.key.remoteJid,
                fromMe: response.key.fromMe,
                participant: response.key?.participant,
                status: 'DELETED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          } else {
            if (!message) return response;
            await this.prismaRepository.message.deleteMany({ where: { id: message.id } });
          }
          this.sendDataWebhook(Events.MESSAGES_DELETE, {
            id: message.id,
            instanceId: message.instanceId,
            key: message.key,
            messageType: message.messageType,
            status: 'DELETED',
            source: message.source,
            messageTimestamp: message.messageTimestamp,
            pushName: message.pushName,
            participant: message.participant,
            message: message.message,
          });
        }
      }

      return response;
    } catch (error) {
      throw new InternalServerErrorException('Error while deleting message for everyone', error?.toString());
    }
  }

  public async mapMediaType(mediaType) {
    const map = {
      imageMessage: 'image',
      videoMessage: 'video',
      documentMessage: 'document',
      stickerMessage: 'sticker',
      audioMessage: 'audio',
      ptvMessage: 'video',
    };
    return map[mediaType] || null;
  }

  public async getBase64FromMediaMessage(data: getBase64FromMediaMessageDto, getBuffer = false) {
    try {
      const m = data?.message;
      const convertToMp4 = data?.convertToMp4 ?? false;

      const msg = m?.message ? m : ((await this.getMessage(m.key, true)) as proto.IWebMessageInfo);

      if (!msg) {
        throw 'Message not found';
      }

      for (const subtype of MessageSubtype) {
        if (msg.message[subtype]) {
          msg.message = msg.message[subtype].message;
        }
      }

      if ('messageContextInfo' in msg.message && Object.keys(msg.message).length === 1) {
        this.logger.verbose('Message contains only messageContextInfo, skipping media processing');
        return null;
      }

      let mediaMessage: any;
      let mediaType: string;

      if (msg.message?.templateMessage) {
        const template =
          msg.message.templateMessage.hydratedTemplate || msg.message.templateMessage.hydratedFourRowTemplate;

        for (const type of TypeMediaMessage) {
          if (template[type]) {
            mediaMessage = template[type];
            mediaType = type;
            msg.message = { [type]: { ...template[type], url: template[type].staticUrl } };
            break;
          }
        }

        if (!mediaMessage) {
          throw 'Template message does not contain a supported media type';
        }
      } else {
        for (const type of TypeMediaMessage) {
          mediaMessage = msg.message[type];
          if (mediaMessage) {
            mediaType = type;
            break;
          }
        }

        if (!mediaMessage) {
          throw 'The message is not of the media type';
        }
      }

      if (typeof mediaMessage['mediaKey'] === 'object') {
        msg.message[mediaType].mediaKey = Uint8Array.from(Object.values(mediaMessage['mediaKey']));
      }

      let buffer: Buffer;

      try {
        buffer = await downloadMediaMessage(
          { key: msg?.key, message: msg?.message },
          'buffer',
          {},
          { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
        );
      } catch {
        this.logger.error('Download Media failed, trying to retry in 5 seconds...');
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const mediaType = Object.keys(msg.message).find((key) => key.endsWith('Message'));
        if (!mediaType) throw new Error('Could not determine mediaType for fallback');

        try {
          const media = await downloadContentFromMessage(
            {
              mediaKey: msg.message?.[mediaType]?.mediaKey,
              directPath: msg.message?.[mediaType]?.directPath,
              url: `https://mmg.whatsapp.net${msg?.message?.[mediaType]?.directPath}`,
            },
            await this.mapMediaType(mediaType),
            {},
          );
          const chunks = [];
          for await (const chunk of media) {
            chunks.push(chunk);
          }
          buffer = Buffer.concat(chunks);
          this.logger.info('Download Media with downloadContentFromMessage was successful!');
        } catch (fallbackErr) {
          this.logger.error('Download Media with downloadContentFromMessage also failed!');
          throw fallbackErr;
        }
      }
      const typeMessage = getContentType(msg.message);

      const ext = mimeTypes.extension(mediaMessage?.['mimetype']);
      const fileName = mediaMessage?.['fileName'] || `${msg.key.id}.${ext}` || `${v4()}.${ext}`;

      if (convertToMp4 && typeMessage === 'audioMessage') {
        try {
          const convert = await this.processAudioMp4(buffer.toString('base64'));

          if (Buffer.isBuffer(convert)) {
            const result = {
              mediaType,
              fileName,
              caption: mediaMessage['caption'],
              size: {
                fileLength: mediaMessage['fileLength'],
                height: mediaMessage['height'],
                width: mediaMessage['width'],
              },
              mimetype: 'audio/mp4',
              base64: convert.toString('base64'),
              buffer: getBuffer ? convert : null,
            };

            return result;
          }
        } catch (error) {
          this.logger.error('Error converting audio to mp4:');
          this.logger.error(error);
          throw new BadRequestException('Failed to convert audio to MP4');
        }
      }

      return {
        mediaType,
        fileName,
        caption: mediaMessage['caption'],
        size: { fileLength: mediaMessage['fileLength'], height: mediaMessage['height'], width: mediaMessage['width'] },
        mimetype: mediaMessage['mimetype'],
        base64: buffer.toString('base64'),
        buffer: getBuffer ? buffer : null,
      };
    } catch (error) {
      this.logger.error('Error processing media message:');
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  public async fetchPrivacySettings() {
    const privacy = await this.client.fetchPrivacySettings();

    return {
      readreceipts: privacy.readreceipts,
      profile: privacy.profile,
      status: privacy.status,
      online: privacy.online,
      last: privacy.last,
      groupadd: privacy.groupadd,
    };
  }

  public async updatePrivacySettings(settings: PrivacySettingDto) {
    try {
      await this.client.updateReadReceiptsPrivacy(settings.readreceipts);
      await this.client.updateProfilePicturePrivacy(settings.profile);
      await this.client.updateStatusPrivacy(settings.status);
      await this.client.updateOnlinePrivacy(settings.online);
      await this.client.updateLastSeenPrivacy(settings.last);
      await this.client.updateGroupsAddPrivacy(settings.groupadd);

      this.reloadConnection();

      return {
        update: 'success',
        data: {
          readreceipts: settings.readreceipts,
          profile: settings.profile,
          status: settings.status,
          online: settings.online,
          last: settings.last,
          groupadd: settings.groupadd,
        },
      };
    } catch (error) {
      throw new InternalServerErrorException('Error updating privacy settings', error.toString());
    }
  }

  public async fetchBusinessProfile(number: string): Promise<NumberBusiness> {
    try {
      const jid = number ? createJid(number) : this.instance.wuid;

      const profile = await this.client.getBusinessProfile(jid);

      if (!profile) {
        const info = await this.whatsappNumber({ numbers: [jid] });

        return { isBusiness: false, message: 'Not is business profile', ...info?.shift() };
      }

      return { isBusiness: true, ...profile };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileName(name: string) {
    try {
      await this.client.updateProfileName(name);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile name', error.toString());
    }
  }

  public async updateProfileStatus(status: string) {
    try {
      await this.client.updateProfileStatus(status);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile status', error.toString());
    }
  }

  public async updateProfilePicture(picture: string) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture)) {
        pic = Buffer.from(picture, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }

      await this.client.updateProfilePicture(this.instance.wuid, pic);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating profile picture', error.toString());
    }
  }

  public async removeProfilePicture() {
    try {
      await this.client.removeProfilePicture(this.instance.wuid);

      this.reloadConnection();

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error removing profile picture', error.toString());
    }
  }

  public async blockUser(data: BlockUserDto) {
    try {
      const { number } = data;

      const isWA = (await this.whatsappNumber({ numbers: [number] }))?.shift();

      if (!isWA.exists && !isJidGroup(isWA.jid) && !isWA.jid.includes('@broadcast')) {
        throw new BadRequestException(isWA);
      }

      const sender = isWA.jid;

      await this.client.updateBlockStatus(sender, data.status);

      return { block: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error blocking user', error.toString());
    }
  }

  private async formatUpdateMessage(data: UpdateMessageDto) {
    try {
      if (!this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        return data;
      }

      const msg: any = await this.getMessage(data.key, true);

      if (msg?.messageType === 'conversation' || msg?.messageType === 'extendedTextMessage') {
        return { text: data.text };
      }

      if (msg?.messageType === 'imageMessage') {
        return { image: msg?.message?.imageMessage, caption: data.text };
      }

      if (msg?.messageType === 'videoMessage') {
        return { video: msg?.message?.videoMessage, caption: data.text };
      }

      return null;
    } catch (error) {
      this.logger.error(error);
      throw new BadRequestException(error.toString());
    }
  }

  public async updateMessage(data: UpdateMessageDto) {
    const jid = createJid(data.number);

    const options = await this.formatUpdateMessage(data);

    if (!options) {
      this.logger.error('Message not compatible');
      throw new BadRequestException('Message not compatible');
    }

    try {
      const oldMessage: any = await this.getMessage(data.key, true);
      if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
        if (!oldMessage) throw new NotFoundException('Message not found');
        if (oldMessage?.key?.remoteJid !== jid) {
          throw new BadRequestException('RemoteJid does not match');
        }
        if (oldMessage?.messageTimestamp > Date.now() + 900000) {
          // 15 minutes in milliseconds
          throw new BadRequestException('Message is older than 15 minutes');
        }
      }

      const messageSent = await this.client.sendMessage(jid, { ...(options as any), edit: data.key });
      if (messageSent) {
        const editedMessage =
          messageSent?.message?.protocolMessage || messageSent?.message?.editedMessage?.message?.protocolMessage;

        if (editedMessage) {
          this.sendDataWebhook(Events.SEND_MESSAGE_UPDATE, editedMessage);
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
            this.chatwootService.eventWhatsapp(
              'send.message.update',
              { instanceName: this.instance.name, instanceId: this.instance.id },
              editedMessage,
            );

          const messageId = messageSent.message?.protocolMessage?.key?.id;
          if (messageId && this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            let message = await this.prismaRepository.message.findFirst({
              where: { key: { path: ['id'], equals: messageId } },
            });
            if (!message) throw new NotFoundException('Message not found');

            if (!(message.key.valueOf() as any).fromMe) {
              new BadRequestException('You cannot edit others messages');
            }
            if ((message.key.valueOf() as any)?.deleted) {
              new BadRequestException('You cannot edit deleted messages');
            }

            if (oldMessage.messageType === 'conversation' || oldMessage.messageType === 'extendedTextMessage') {
              oldMessage.message.conversation = data.text;
            } else {
              oldMessage.message[oldMessage.messageType].caption = data.text;
            }
            message = await this.prismaRepository.message.update({
              where: { id: message.id },
              data: {
                message: oldMessage.message,
                status: 'EDITED',
                messageTimestamp: Math.floor(Date.now() / 1000), // Convert to int32 by dividing by 1000 to get seconds
              },
            });

            if (this.configService.get<Database>('DATABASE').SAVE_DATA.MESSAGE_UPDATE) {
              const messageUpdate: any = {
                messageId: message.id,
                keyId: messageId,
                remoteJid: messageSent.key.remoteJid,
                fromMe: messageSent.key.fromMe,
                participant: messageSent.key?.participant,
                status: 'EDITED',
                instanceId: this.instanceId,
              };
              await this.prismaRepository.messageUpdate.create({ data: messageUpdate });
            }
          }
        }
      }

      return messageSent;
    } catch (error) {
      this.logger.error(error);
      throw error;
    }
  }

  public async fetchLabels(): Promise<LabelDto[]> {
    const labels = await this.prismaRepository.label.findMany({ where: { instanceId: this.instanceId } });

    return labels.map((label) => ({
      color: label.color,
      name: label.name,
      id: label.labelId,
      predefinedId: label.predefinedId,
    }));
  }

  public async handleLabel(data: HandleLabelDto) {
    const whatsappContact = await this.whatsappNumber({ numbers: [data.number] });
    if (whatsappContact.length === 0) {
      throw new NotFoundException('Number not found');
    }
    const contact = whatsappContact[0];
    if (!contact.exists) {
      throw new NotFoundException('Number is not on WhatsApp');
    }

    try {
      if (data.action === 'add') {
        await this.sendAppPatchChecked(
          chatModificationToAppPatch({ addChatLabel: { labelId: data.labelId } } as any, contact.jid),
        );
        await this.addLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, add: true };
      }
      if (data.action === 'remove') {
        await this.sendAppPatchChecked(
          chatModificationToAppPatch({ removeChatLabel: { labelId: data.labelId } } as any, contact.jid),
        );
        await this.removeLabel(data.labelId, this.instanceId, contact.jid);

        return { numberJid: contact.jid, labelId: data.labelId, remove: true };
      }
    } catch (error) {
      throw new BadRequestException(`Unable to ${data.action} label to chat`, error.toString());
    }
  }

  // Group
  private async updateGroupMetadataCache(groupJid: string) {
    try {
      const meta = await this.client.groupMetadata(groupJid);

      const cacheConf = this.configService.get<CacheConf>('CACHE');

      if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
        this.logger.verbose(`Updating cache for group: ${groupJid}`);
        await groupMetadataCache.set(groupJid, { timestamp: Date.now(), data: meta });
      }

      return meta;
    } catch (error) {
      this.logger.error(error);
      return null;
    }
  }

  private getGroupMetadataCache = async (groupJid: string) => {
    if (!isJidGroup(groupJid)) return null;

    const cacheConf = this.configService.get<CacheConf>('CACHE');

    if ((cacheConf?.REDIS?.ENABLED && cacheConf?.REDIS?.URI !== '') || cacheConf?.LOCAL?.ENABLED) {
      if (await groupMetadataCache?.has(groupJid)) {
        console.log(`Cache request for group: ${groupJid}`);
        const meta = await groupMetadataCache.get(groupJid);

        if (Date.now() - meta.timestamp > 3600000) {
          await this.updateGroupMetadataCache(groupJid);
        }

        return meta.data;
      }

      console.log(`Cache request for group: ${groupJid} - not found`);
      return await this.updateGroupMetadataCache(groupJid);
    }

    return await this.findGroup({ groupJid }, 'inner');
  };

  public async createGroup(create: CreateGroupDto) {
    try {
      const participants = (await this.whatsappNumber({ numbers: create.participants }))
        .filter((participant) => participant.exists)
        .map((participant) => participant.jid);
      const { id } = await this.client.groupCreate(create.subject, participants);

      if (create?.description) {
        await this.client.groupUpdateDescription(id, create.description);
      }

      if (create?.promoteParticipants) {
        await this.updateGParticipant({ groupJid: id, action: 'promote', participants: participants });
      }

      const group = await this.client.groupMetadata(id);

      return group;
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException('Error creating group', error.toString());
    }
  }

  public async updateGroupPicture(picture: GroupPictureDto) {
    try {
      let pic: WAMediaUpload;
      if (isURL(picture.image)) {
        const timestamp = new Date().getTime();
        const parsedURL = new URL(picture.image);
        parsedURL.searchParams.set('timestamp', timestamp.toString());
        const url = parsedURL.toString();

        let config: any = { responseType: 'arraybuffer' };

        if (this.localProxy?.enabled) {
          config = {
            ...config,
            httpsAgent: makeProxyAgent({
              host: this.localProxy.host,
              port: this.localProxy.port,
              protocol: this.localProxy.protocol,
              username: this.localProxy.username,
              password: this.localProxy.password,
            }),
          };
        }

        pic = (await axios.get(url, config)).data;
      } else if (isBase64(picture.image)) {
        pic = Buffer.from(picture.image, 'base64');
      } else {
        throw new BadRequestException('"profilePicture" must be a url or a base64');
      }
      await this.client.updateProfilePicture(picture.groupJid, pic);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error update group picture', error.toString());
    }
  }

  public async updateGroupSubject(data: GroupSubjectDto) {
    try {
      await this.client.groupUpdateSubject(data.groupJid, data.subject);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group subject', error.toString());
    }
  }

  public async updateGroupDescription(data: GroupDescriptionDto) {
    try {
      await this.client.groupUpdateDescription(data.groupJid, data.description);

      return { update: 'success' };
    } catch (error) {
      throw new InternalServerErrorException('Error updating group description', error.toString());
    }
  }

  public async findGroup(id: GroupJid, reply: 'inner' | 'out' = 'out') {
    try {
      const group = await this.client.groupMetadata(id.groupJid);

      if (!group) {
        this.logger.error('Group not found');
        return null;
      }

      const picture = await this.profilePicture(group.id);

      return {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        participants: group.participants,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };
    } catch (error) {
      if (reply === 'inner') {
        return;
      }
      throw new NotFoundException('Error fetching group', error.toString());
    }
  }

  public async fetchAllGroups(getParticipants: GetParticipant) {
    const fetch = Object.values(await this?.client?.groupFetchAllParticipating());

    let groups = [];
    for (const group of fetch) {
      const picture = await this.profilePicture(group.id);

      const result = {
        id: group.id,
        subject: group.subject,
        subjectOwner: group.subjectOwner,
        subjectTime: group.subjectTime,
        pictureUrl: picture?.profilePictureUrl,
        size: group.participants.length,
        creation: group.creation,
        owner: group.owner,
        desc: group.desc,
        descId: group.descId,
        restrict: group.restrict,
        announce: group.announce,
        isCommunity: group.isCommunity,
        isCommunityAnnounce: group.isCommunityAnnounce,
        linkedParent: group.linkedParent,
      };

      if (getParticipants.getParticipants == 'true') {
        result['participants'] = group.participants;
      }

      groups = [...groups, result];
    }

    return groups;
  }

  public async inviteCode(id: GroupJid) {
    try {
      const code = await this.client.groupInviteCode(id.groupJid);
      return { inviteUrl: `https://chat.whatsapp.com/${code}`, inviteCode: code };
    } catch (error) {
      throw new NotFoundException('No invite code', error.toString());
    }
  }

  public async inviteInfo(id: GroupInvite) {
    try {
      return await this.client.groupGetInviteInfo(id.inviteCode);
    } catch {
      throw new NotFoundException('No invite info', id.inviteCode);
    }
  }

  public async sendInvite(id: GroupSendInvite) {
    try {
      const inviteCode = await this.inviteCode({ groupJid: id.groupJid });

      const inviteUrl = inviteCode.inviteUrl;

      const numbers = id.numbers.map((number) => createJid(number));
      const description = id.description ?? '';

      const msg = `${description}\n\n${inviteUrl}`;

      const message = { conversation: msg };

      for await (const number of numbers) {
        await this.sendMessageWithTyping(number, message);
      }

      return { send: true, inviteUrl };
    } catch {
      throw new NotFoundException('No send invite');
    }
  }

  public async acceptInviteCode(id: AcceptGroupInvite) {
    try {
      const groupJid = await this.client.groupAcceptInvite(id.inviteCode);
      return { accepted: true, groupJid: groupJid };
    } catch (error) {
      throw new NotFoundException('Accept invite error', error.toString());
    }
  }

  public async revokeInviteCode(id: GroupJid) {
    try {
      const inviteCode = await this.client.groupRevokeInvite(id.groupJid);
      return { revoked: true, inviteCode };
    } catch (error) {
      throw new NotFoundException('Revoke error', error.toString());
    }
  }

  public async findParticipants(id: GroupJid) {
    try {
      const participants = (await this.client.groupMetadata(id.groupJid)).participants;
      const contacts = await this.prismaRepository.contact.findMany({
        where: { instanceId: this.instanceId, remoteJid: { in: participants.map((p) => p.id) } },
      });
      const parsedParticipants = participants.map((participant) => {
        const contact = contacts.find((c) => c.remoteJid === participant.id);
        return {
          ...participant,
          name: participant.name ?? contact?.pushName,
          imgUrl: participant.imgUrl ?? contact?.profilePicUrl,
        };
      });

      const usersContacts = parsedParticipants.filter((c) => c.id.includes('@s.whatsapp'));
      if (usersContacts) {
        await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.id })));
      }

      return { participants: parsedParticipants };
    } catch (error) {
      console.error(error);
      throw new NotFoundException('No participants', error.toString());
    }
  }

  public async updateGParticipant(update: GroupUpdateParticipantDto) {
    try {
      const participants = update.participants.map((p) => createJid(p));
      const updateParticipants = await this.client.groupParticipantsUpdate(
        update.groupJid,
        participants,
        update.action,
      );
      return { updateParticipants: updateParticipants };
    } catch (error) {
      throw new BadRequestException('Error updating participants', error.toString());
    }
  }

  public async updateGSetting(update: GroupUpdateSettingDto) {
    try {
      const updateSetting = await this.client.groupSettingUpdate(update.groupJid, update.action);
      return { updateSetting: updateSetting };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async toggleEphemeral(update: GroupToggleEphemeralDto) {
    try {
      await this.client.groupToggleEphemeral(update.groupJid, update.expiration);
      return { success: true };
    } catch (error) {
      throw new BadRequestException('Error updating setting', error.toString());
    }
  }

  public async leaveGroup(id: GroupJid) {
    try {
      await this.client.groupLeave(id.groupJid);
      return { groupJid: id.groupJid, leave: true };
    } catch (error) {
      throw new BadRequestException('Unable to leave the group', error.toString());
    }
  }

  public async templateMessage() {
    throw new Error('Method not available in the Baileys service');
  }

  private deserializeMessageBuffers(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }

    if (typeof obj === 'object' && !Array.isArray(obj) && !Buffer.isBuffer(obj)) {
      const keys = Object.keys(obj);
      const isIndexedObject = keys.every((key) => !isNaN(Number(key)));

      if (isIndexedObject && keys.length > 0) {
        const values = keys.sort((a, b) => Number(a) - Number(b)).map((key) => obj[key]);
        return new Uint8Array(values);
      }
    }

    // Is Buffer?, converter to Uint8Array
    if (Buffer.isBuffer(obj)) {
      return new Uint8Array(obj);
    }

    // Process arrays recursively
    if (Array.isArray(obj)) {
      return obj.map((item) => this.deserializeMessageBuffers(item));
    }

    // Process objects recursively
    if (typeof obj === 'object') {
      const converted: any = {};
      for (const key in obj) {
        if (Object.prototype.hasOwnProperty.call(obj, key)) {
          converted[key] = this.deserializeMessageBuffers(obj[key]);
        }
      }
      return converted;
    }

    return obj;
  }

  private prepareMessage(message: proto.IWebMessageInfo): any {
    const contentType = getContentType(message.message);
    const contentMsg = message?.message[contentType] as any;

    const messageRaw = {
      key: message.key, // Save key exactly as it comes from Baileys
      pushName:
        message.pushName ||
        (message.key.fromMe
          ? 'Você'
          : message?.participant || (message.key?.participant ? message.key.participant.split('@')[0] : null)),
      status: status[message.status],
      message: this.deserializeMessageBuffers({ ...message.message }),
      contextInfo: this.deserializeMessageBuffers(contentMsg?.contextInfo),
      messageType: contentType || 'unknown',
      messageTimestamp: Long.isLong(message.messageTimestamp)
        ? message.messageTimestamp.toNumber()
        : (message.messageTimestamp as number),
      instanceId: this.instanceId,
      source: getDevice(message.key.id),
    };

    if (!messageRaw.status && message.key.fromMe === false) {
      messageRaw.status = status[3]; // DELIVERED MESSAGE
    }

    if (messageRaw.message.extendedTextMessage) {
      messageRaw.messageType = 'conversation';
      messageRaw.message.conversation = messageRaw.message.extendedTextMessage.text;
      delete messageRaw.message.extendedTextMessage;
    }

    if (messageRaw.message.documentWithCaptionMessage) {
      messageRaw.messageType = 'documentMessage';
      messageRaw.message.documentMessage = messageRaw.message.documentWithCaptionMessage.message.documentMessage;
      delete messageRaw.message.documentWithCaptionMessage;
    }

    const quotedMessage = messageRaw?.contextInfo?.quotedMessage;
    if (quotedMessage) {
      if (quotedMessage.extendedTextMessage) {
        quotedMessage.conversation = quotedMessage.extendedTextMessage.text;
        delete quotedMessage.extendedTextMessage;
      }

      if (quotedMessage.documentWithCaptionMessage) {
        quotedMessage.documentMessage = quotedMessage.documentWithCaptionMessage.message.documentMessage;
        delete quotedMessage.documentWithCaptionMessage;
      }
    }

    return messageRaw;
  }

  private async syncChatwootLostMessages() {
    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
      const chatwootConfig = await this.findChatwoot();
      const prepare = (message: any) => this.prepareMessage(message);
      this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);

      // Generate ID for this cron task and store in cache
      const cronId = cuid();
      const cronKey = `chatwoot:syncLostMessages`;
      await this.chatwootService.getCache()?.hSet(cronKey, this.instance.name, cronId);

      const task = cron.schedule('0,30 * * * *', async () => {
        // Check ID before executing (only if cache is available)
        const cache = this.chatwootService.getCache();
        if (cache) {
          const storedId = await cache.hGet(cronKey, this.instance.name);
          if (storedId && storedId !== cronId) {
            this.logger.info(`Stopping syncChatwootLostMessages cron - ID mismatch: ${cronId} vs ${storedId}`);
            task.stop();
            return;
          }
        }
        this.chatwootService.syncLostMessages({ instanceName: this.instance.name }, chatwootConfig, prepare);
      });
      task.start();
    }
  }

  private async updateMessagesReadedByTimestamp(remoteJid: string, timestamp?: number): Promise<number> {
    if (timestamp === undefined || timestamp === null) return 0;

    // Use raw SQL to avoid JSON path issues
    const result = await this.prismaRepository.$executeRaw`
      UPDATE "Message"
      SET "status" = ${status[4]}
      WHERE "instanceId" = ${this.instanceId}
      AND "key"->>'remoteJid' = ${remoteJid}
      AND ("key"->>'fromMe')::boolean = false
      AND "messageTimestamp" <= ${timestamp}
      AND ("status" IS NULL OR "status" = ${status[3]})
    `;

    if (result) {
      if (result > 0) {
        this.updateChatUnreadMessages(remoteJid);
      }

      return result;
    }

    return 0;
  }

  private async updateChatUnreadMessages(remoteJid: string): Promise<number> {
    const [chat, unreadMessages] = await Promise.all([
      this.prismaRepository.chat.findFirst({ where: { remoteJid } }),
      // Use raw SQL to avoid JSON path issues
      this.prismaRepository.$queryRaw`
        SELECT COUNT(*)::int as count FROM "Message"
        WHERE "instanceId" = ${this.instanceId}
        AND "key"->>'remoteJid' = ${remoteJid}
        AND ("key"->>'fromMe')::boolean = false
        AND "status" = ${status[3]}
      `.then((result: any[]) => result[0]?.count || 0),
    ]);

    if (chat && chat.unreadMessages !== unreadMessages) {
      await this.prismaRepository.chat.update({ where: { id: chat.id }, data: { unreadMessages } });
    }

    return unreadMessages;
  }

  private async addLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();

    await this.prismaRepository.$executeRawUnsafe(
      `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
       VALUES ($4, $2, $3, to_jsonb(ARRAY[$1]::text[]), NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
     DO
      UPDATE
          SET "labels" = (
          SELECT to_jsonb(array_agg(DISTINCT elem))
          FROM (
          SELECT jsonb_array_elements_text("Chat"."labels") AS elem
          UNION
          SELECT $1::text AS elem
          ) sub
          ),
          "updatedAt" = NOW();`,
      labelId,
      instanceId,
      chatId,
      id,
    );
  }

  private async removeLabel(labelId: string, instanceId: string, chatId: string) {
    const id = cuid();

    await this.prismaRepository.$executeRawUnsafe(
      `INSERT INTO "Chat" ("id", "instanceId", "remoteJid", "labels", "createdAt", "updatedAt")
       VALUES ($4, $2, $3, '[]'::jsonb, NOW(), NOW()) ON CONFLICT ("instanceId", "remoteJid")
     DO
      UPDATE
          SET "labels" = COALESCE (
          (
          SELECT jsonb_agg(elem)
          FROM jsonb_array_elements_text("Chat"."labels") AS elem
          WHERE elem <> $1
          ),
          '[]'::jsonb
          ),
          "updatedAt" = NOW();`,
      labelId,
      instanceId,
      chatId,
      id,
    );
  }

  public async baileysOnWhatsapp(jid: string) {
    const response = await this.client.onWhatsApp(jid);

    return response;
  }

  public async baileysProfilePictureUrl(jid: string, type: 'image' | 'preview', timeoutMs: number) {
    const response = await this.client.profilePictureUrl(jid, type, timeoutMs);

    return response;
  }

  public async baileysAssertSessions(jids: string[]) {
    const response = await this.client.assertSessions(jids);

    return response;
  }

  public async baileysCreateParticipantNodes(jids: string[], message: proto.IMessage, extraAttrs: any) {
    const response = await this.client.createParticipantNodes(jids, message, extraAttrs);

    const convertedResponse = {
      ...response,
      nodes: response.nodes.map((node: any) => ({
        ...node,
        content: node.content?.map((c: any) => ({
          ...c,
          content: c.content instanceof Uint8Array ? Buffer.from(c.content).toString('base64') : c.content,
        })),
      })),
    };

    return convertedResponse;
  }

  public async baileysSendNode(stanza: any) {
    console.log('stanza', JSON.stringify(stanza));
    const response = await this.client.sendNode(stanza);

    return response;
  }

  public async baileysGetUSyncDevices(jids: string[], useCache: boolean, ignoreZeroDevices: boolean) {
    const response = await this.client.getUSyncDevices(jids, useCache, ignoreZeroDevices);

    return response;
  }

  public async baileysGenerateMessageTag() {
    const response = await this.client.generateMessageTag();

    return response;
  }

  public async baileysSignalRepositoryDecryptMessage(jid: string, type: 'pkmsg' | 'msg', ciphertext: string) {
    try {
      const ciphertextBuffer = Buffer.from(ciphertext, 'base64');

      const response = await this.client.signalRepository.decryptMessage({ jid, type, ciphertext: ciphertextBuffer });

      return response instanceof Uint8Array ? Buffer.from(response).toString('base64') : response;
    } catch (error) {
      this.logger.error('Error decrypting message:');
      this.logger.error(error);
      throw error;
    }
  }

  public async baileysGetAuthState() {
    const response = { me: this.client.authState.creds.me, account: this.client.authState.creds.account };

    return response;
  }

  //Business Controller
  public async fetchCatalog(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit || 10;
    const cursor = null;

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);

      let catalog = await this.getCatalog({ jid: info?.jid, limit, cursor });
      let nextPageCursor = catalog.nextPageCursor;
      let nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
      let pagination = nextPageCursorJson?.pagination_cursor
        ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
        : null;
      let fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;

      let productsCatalog = catalog.products || [];
      let countLoops = 0;
      while (fetcherHasMore && countLoops < 4) {
        catalog = await this.getCatalog({ jid: info?.jid, limit, cursor: nextPageCursor });
        nextPageCursor = catalog.nextPageCursor;
        nextPageCursorJson = nextPageCursor ? JSON.parse(atob(nextPageCursor)) : null;
        pagination = nextPageCursorJson?.pagination_cursor
          ? JSON.parse(atob(nextPageCursorJson.pagination_cursor))
          : null;
        fetcherHasMore = pagination?.fetcher_has_more === true ? true : false;
        productsCatalog = [...productsCatalog, ...catalog.products];
        countLoops++;
      }

      return {
        wuid: info?.jid || jid,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        catalogLength: productsCatalog.length,
        catalog: productsCatalog,
      };
    } catch (error) {
      console.log(error);
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCatalog({
    jid,
    limit,
    cursor,
  }: GetCatalogOptions): Promise<{ products: Product[]; nextPageCursor: string | undefined }> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const catalog = await this.client.getCatalog({ jid, limit: limit, cursor: cursor });

      if (!catalog) {
        return { products: undefined, nextPageCursor: undefined };
      }

      return catalog;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchCollections(instanceName: string, data: getCollectionsDto) {
    const jid = data.number ? createJid(data.number) : this.client?.user?.id;
    const limit = data.limit <= 20 ? data.limit : 20; //(tem esse limite, não sei porque)

    const onWhatsapp = (await this.whatsappNumber({ numbers: [jid] }))?.shift();

    if (!onWhatsapp.exists) {
      throw new BadRequestException(onWhatsapp);
    }

    try {
      const info = (await this.whatsappNumber({ numbers: [jid] }))?.shift();
      const business = await this.fetchBusinessProfile(info?.jid);
      const collections = await this.getCollections(info?.jid, limit);

      return {
        wuid: info?.jid || jid,
        name: info?.name,
        numberExists: info?.exists,
        isBusiness: business.isBusiness,
        collectionsLength: collections?.length,
        collections: collections,
      };
    } catch {
      return { wuid: jid, name: null, isBusiness: false };
    }
  }

  public async getCollections(jid?: string | undefined, limit?: number): Promise<CatalogCollection[]> {
    try {
      jid = jid ? createJid(jid) : this.instance.wuid;

      const result = await this.client.getCollections(jid, limit);

      if (!result) {
        return [{ id: undefined, name: undefined, products: [], status: undefined }];
      }

      return result.collections;
    } catch (error) {
      throw new InternalServerErrorException('Error getCatalog', error.toString());
    }
  }

  public async fetchMessages(query: Query<Message>) {
    const keyFilters = query?.where?.key as ExtendedIMessageKey;

    const timestampFilter = {};
    if (query?.where?.messageTimestamp) {
      if (query.where.messageTimestamp['gte'] && query.where.messageTimestamp['lte']) {
        timestampFilter['messageTimestamp'] = {
          gte: Math.floor(new Date(query.where.messageTimestamp['gte']).getTime() / 1000),
          lte: Math.floor(new Date(query.where.messageTimestamp['lte']).getTime() / 1000),
        };
      }
    }

    const count = await this.prismaRepository.message.count({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participant ? { key: { path: ['participant'], equals: keyFilters?.participant } } : {},
          {
            OR: [
              keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
              keyFilters?.remoteJidAlt ? { key: { path: ['remoteJidAlt'], equals: keyFilters?.remoteJidAlt } } : {},
            ],
          },
        ],
      },
    });

    if (!query?.offset) {
      query.offset = 50;
    }

    if (!query?.page) {
      query.page = 1;
    }

    const messages = await this.prismaRepository.message.findMany({
      where: {
        instanceId: this.instanceId,
        id: query?.where?.id,
        source: query?.where?.source,
        messageType: query?.where?.messageType,
        ...timestampFilter,
        AND: [
          keyFilters?.id ? { key: { path: ['id'], equals: keyFilters?.id } } : {},
          keyFilters?.fromMe ? { key: { path: ['fromMe'], equals: keyFilters?.fromMe } } : {},
          keyFilters?.participant ? { key: { path: ['participant'], equals: keyFilters?.participant } } : {},
          {
            OR: [
              keyFilters?.remoteJid ? { key: { path: ['remoteJid'], equals: keyFilters?.remoteJid } } : {},
              keyFilters?.remoteJidAlt ? { key: { path: ['remoteJidAlt'], equals: keyFilters?.remoteJidAlt } } : {},
            ],
          },
        ],
      },
      orderBy: { messageTimestamp: 'desc' },
      skip: query.offset * (query?.page === 1 ? 0 : (query?.page as number) - 1),
      take: query.offset,
      select: {
        id: true,
        key: true,
        pushName: true,
        messageType: true,
        message: true,
        messageTimestamp: true,
        instanceId: true,
        source: true,
        contextInfo: true,
        MessageUpdate: { select: { status: true } },
      },
    });

    const formattedMessages = messages.map((message) => {
      const messageKey = message.key as { fromMe: boolean; remoteJid: string; id: string; participant?: string };

      if (!message.pushName) {
        if (messageKey.fromMe) {
          message.pushName = 'Você';
        } else if (message.contextInfo) {
          const contextInfo = message.contextInfo as { participant?: string };
          if (contextInfo.participant) {
            message.pushName = contextInfo.participant.split('@')[0];
          } else if (messageKey.participant) {
            message.pushName = messageKey.participant.split('@')[0];
          }
        }
      }

      return message;
    });

    return {
      messages: {
        total: count,
        pages: Math.ceil(count / query.offset),
        currentPage: query.page,
        records: formattedMessages,
      },
    };
  }
}
