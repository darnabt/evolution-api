import { ConfigService } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { PrismaClient } from '@prisma/client';

export class Query<T> {
  where?: T;
  sort?: 'asc' | 'desc';
  page?: number;
  offset?: number;
}

/**
 * darnabt: listeners told about every error Prisma reports on a query (the liveness self-check uses this
 * to learn about a short database outage from queries that already ran, with no extra reads).
 */
export const prismaErrorListeners: Array<(message: string) => void> = [];

export class PrismaRepository extends PrismaClient {
  constructor(private readonly configService: ConfigService) {
    // emit: 'event' only hands errors to $on below; nothing new is printed.
    super({ log: [{ emit: 'event', level: 'error' }] });
    (this as any).$on('error', (event: any) => {
      const message = String(event?.message ?? '');
      for (const listener of prismaErrorListeners) {
        try {
          listener(message);
        } catch {
          // a listener must never break a query path
        }
      }
    });
  }

  private readonly logger = new Logger('PrismaRepository');

  public async onModuleInit() {
    await this.$connect();
    this.logger.info('Repository:Prisma - ON');
  }

  public async onModuleDestroy() {
    await this.$disconnect();
    this.logger.warn('Repository:Prisma - OFF');
  }
}
