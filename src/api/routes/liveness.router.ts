// darnabt: liveness routes (study SCAN-ROOT-CAUSE-v1 §5 (c) F5 and (d) F6). Mounted under /instance.
//   GET|POST /instance/socketPing/:instanceName    real w:p round trip on the number's socket (5 s);
//                                                   ok true | false (claims open, does not answer) | null (not open yet)
//   GET      /instance/deviceListed/:instanceName  does WhatsApp still list our linked device (true/false/unknown)
//   POST     /instance/processRestart              bridge exits, Railway restarts it (global key; 429 within 10 min)
// None of them logs out, deletes a session or pairs a device.
import { RouterBroker } from '@api/abstract/abstract.router';
import { InstanceDto } from '@api/dto/instance.dto';
import { authGuard } from '@api/guards/auth.guard';
import { livenessService } from '@api/services/liveness.service';
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
      })
      // Global API key only (no instance in the path, so an instance token can never pass authGuard here).
      .post(this.routerPath('processRestart', false), authGuard['apikey'], async (req, res) => {
        const response = await this.dataValidate<InstanceDto>({
          request: req,
          schema: null,
          ClassRef: InstanceDto,
          execute: () => livenessService.processRestart('api'),
        });

        return res.status(202).json(response);
      });

    livenessService.startSelfCheck();
  }

  public readonly router: Router = Router();
}
