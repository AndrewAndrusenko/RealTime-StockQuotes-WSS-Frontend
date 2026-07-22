/* eslint-disable @typescript-eslint/no-unused-expressions */
import { DestroyRef, inject, Injectable } from '@angular/core';
import { ConfigService } from '../core/config.service';
import { WebSocketService } from '../core/websocket.service';
import { IServerCommand } from '../core/websocket.types';
import { filter, takeWhile } from 'rxjs';
import { SnacksService } from '../shared/snacks.service';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
@Injectable()
export class TestingMngService {
  //Service to handle testing functionaly
  private readonly destroyRef = inject(DestroyRef)
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG
  private readonly wssCore = inject(WebSocketService)
  private readonly snack = inject(SnacksService)

  public sendMessageToServer(cmd: IServerCommand) {
    this.wssCore.connectionState === 'connected' ? this.wssCore.sendCommandToServer(cmd) : this.connectAndExecute(cmd);
  }
  private connectAndExecute(cmd: IServerCommand) {
    this.wssCore.connectServer(this.CONFIG.TEST_WS_ENDPOINT + '/manage_connection')
    this.wssCore.connectionState$.pipe(
      filter(status => status === 'connected'),
      takeWhile(status => status === 'connected',true),
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(()=>{
      this.wssCore.sendCommandToServer(cmd);
      this.snack.openSnack('Connected. Command has been sent to server','Okay','success-snackBar')
    })
  }
}