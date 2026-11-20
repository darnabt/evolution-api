// darnabt: liveness routes (study SCAN-ROOT-CAUSE-v1 §5 (c) F5 and (d) F6). Mounted under /instance.
//   GET|POST /instance/socketPing/:instanceName    real w:p round trip on the number's socket (5 s);
//                                                   ok true | false (claims open, does not answer) | null (not open yet)
//   GET      /instance/deviceListed/:instanceName  does WhatsApp still list our linked device (true/false/unknown)
//   POST     /instance/processRestart              bridge exits, Railway restarts it (global key; 429 within 10 min;
//                                                   409 while a number is pairing or just paired)
//                                                   ONLY when LIVENESS_PROCESS_RESTART=true (default off:
//                                                   not registered → the framework's own 404, as before)
//   POST     /instance/appStateRepair/:instanceName  one-shot app-state repair (global key; 409 not open; 429 within 6 h)
// None of them logs out, deletes a session or pairs a device.
import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { authGuard } from '@api/guards/auth.guard';
import { livenessService, processRestartEnabled } from '@api/services/liveness.service';
import { Auth, configService } from '@config/env.config';
import { RequestHandler, Router } from 'express';

import { HttpStatus } from './index.router';

export class LivenessRouter extends RouterBroker {
  constructor(...guards: RequestHandler[]) {
    super();

    const socketPing: RequestHandler = async (req, res) => {
      const response = await this.dataValidate<InstanceDto>({
        request: req,
        schema: null,
        ClassRef: InstanceDto,
        execute: (instance) => livenessService.socketPing(instance.instanceName),
      });

      return res.status(HttpStatus.OK).json(response);
    };

    this.router
      .get(this.routerPath('socketPing'), ...guards, socketPing)
      .post(this.routerPath('socketPing'), ...guards, socketPing)
      .get(this.routerPath('deviceListed'), ...guards, async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: (instance) => livenessService.deviceListed(instance.instanceName),
        });

        return res.status(HttpStatus.OK).json(response);
      });

    // Default OFF: live Nawah already calls this route and reads its 404 as «not available».
    // Global API key only (no instance in the path, so an instance token can never pass authGuard here).
    if (processRestartEnabled()) {
      this.router.post(this.routerPath('processRestart', false), authGuard['apikey'], async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: () => livenessService.processRestart('api'),
        });

        return res.status(202).json(response);
      });
    }

    // darnabt 2026-11-20 F4: one-shot app-state repair. Global API key only (authGuard['apikey'] alone, like
    // processRestart — an instance token never passes). 409 not open, 429 within 6 h, else 202 at once.
    const globalKeyOnly: RequestHandler = (req, res, next) => {
      // authGuard['apikey'] also lets the instance's own token in when the path names the instance
      if (req.get('apikey') !== configService.get<Auth>('AUTHENTICATION').API_KEY.KEY) {
        return res.status(401).json({ status: 401, error: 'Unauthorized', message: ['Global API key required'] });
      }
      return next();
    };
    this.router.post(this.routerPath('appStateRepair'), globalKeyOnly, async (req, res) => {
      const response = await this.dataValidate<InstanceDto>({
        request: req,
        schema: null,
        ClassRef: InstanceDto,
        execute: (instance) => livenessService.appStateRepair(instance.instanceName),
      });

      return res.status(202).json(response);
    });

    livenessService.startSelfCheck();
  }

  public readonly router: Router = Router();
}
