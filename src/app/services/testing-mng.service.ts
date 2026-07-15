/* eslint-disable @typescript-eslint/no-unused-expressions */
import { inject, Injectable } from '@angular/core';
import {
  BehaviorSubject,
  catchError,
  EMPTY,
  interval,
  of,
  repeat,
  Subscription,
  switchMap,
  tap,
  throwError,
} from 'rxjs';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { SnacksService } from './snacks.service';
import { SERVER_ERRORS } from '../types/errors-model';
import { ENV } from '../../environments/environment';
import { JwtHandlerService } from './jwt-handler.service';
export interface IServerCommand {
  cmd: string; //command to server: start, stop
  timeToWork: number; //time of emmiting values in milliseconds
  intervalToEmit: number; //interval between emits in milliseconds
  market: string;
}
@Injectable({
  providedIn: 'root',
})
export class TestingMngService {
  //Service to handle testing functionaly
  private readonly snacksService = inject(SnacksService)
  private readonly jwtService =inject(JwtHandlerService)
  private readonly _streamStarted$ = new BehaviorSubject<boolean>(false);
  private readonly _serverConnection$ = new BehaviorSubject<boolean>(false);
  private _webSocketTest$: WebSocketSubject<{ message: string } | IServerCommand> | undefined = undefined;
  private cmdCurrent: IServerCommand | undefined = undefined;
  private closeConnectionErrorCode: number = 0;
  private conecctionRetryCount: number = 2;
  private connectionAttemptN: number = 0;
  private pingInterval = interval(60000).pipe(tap(() => this._webSocketTest$?.next({ message: 'ping' })));
  private pingIntervalSub = new Subscription();

  ngOnDestroy(): void {
    this._webSocketTest$?.closed ? null : this._webSocketTest$?.unsubscribe();
  }
  sendMessageToServer(cmd: IServerCommand) {
    this.cmdCurrent = cmd;
    this._webSocketTest$ && !this._webSocketTest$.closed ? this._webSocketTest$.next(cmd) : this.createTestingStream();
  }
  private createTestingStream() {
    //Creating stream of quotes for testing
    this._webSocketTest$ = webSocket({
      url: ENV.TEST_WS_ENDPOINT + '/manage_connection',
      openObserver: {
        next: () => {
          this._serverConnection$.next(true);
          this.connectionAttemptN = 0;
          this.cmdCurrent ? this._webSocketTest$?.next(this.cmdCurrent) : null;
          this.pingIntervalSub = this.pingInterval.subscribe();
        },
      },
      closeObserver: {
        next: (event) => {
          console.log('createTestingStream closed. code:', event.code);
          this.closeConnectionErrorCode = event.code;
          this._streamStarted$.next(false);
          this._serverConnection$.next(false);
          this.pingIntervalSub.unsubscribe();
        },
      },
    });
    this._webSocketTest$
      .pipe(
        catchError((err) => {
          console.log('catchError', err);
          if (SERVER_ERRORS.get(this.closeConnectionErrorCode)?.retryConnection) {
            throwError(() => err);
            this._serverConnection$.next(false);
            this._streamStarted$.next(false);
          }
          return EMPTY;
        }),
        repeat({
          delay: () => {
            this.connectionAttemptN++;
            return of(SERVER_ERRORS.get(this.closeConnectionErrorCode)?.authErr === true).pipe(
              tap((jwtError) =>
                jwtError === true ? setTimeout(() => this.jwtService.refreshToken(), 10) : null,
              ),
              switchMap((jwtError) => (jwtError === true ? this.jwtService.refreshTokenReady$ : of(false))),
              switchMap(() =>
                SERVER_ERRORS.get(this.closeConnectionErrorCode)?.retryConnection === false ? EMPTY : of(true),
              ),
              tap(() => {
                if (this.conecctionRetryCount + 1 === this.connectionAttemptN) {
                  this.connectionAttemptN = 0;
                  console.log('this._webSocketTest$.closed', this._webSocketTest$?.closed);
                  this._webSocketTest$?.closed ? null : this._webSocketTest$?.unsubscribe();
                  this._serverConnection$.next(false);
                  SERVER_ERRORS.get(this.closeConnectionErrorCode)?.errmsgIgnore
                    ? null
                    : this.snacksService.openSnack(
                        `Error code: ${this.closeConnectionErrorCode}. ${SERVER_ERRORS.get(1)?.messageToUI} `,
                        'Okay',
                        'error-snackBar',
                      );
                  return EMPTY;
                }
                console.log(
                  `TestingMngService: Trying to reconnect due to error ${this.closeConnectionErrorCode}. Attempt ${this.connectionAttemptN} out of ${this.conecctionRetryCount}`,
                );
                return of({});
              }),
            );
          },
        }),
      )
      .subscribe({
        next: (msg) => {
          this.connectionAttemptN = 0;
          switch ((msg as { message: string; detail: string }).message) {
            case 'stream_stopped':
              console.log('stream_stopped');
              this._streamStarted$.next(false);
              this.cmdCurrent = undefined;
              break;
            case 'stream_started':
              console.log('stream_started');
              this._streamStarted$.next(true);
              break;
            default:
              console.log('msg', msg);
          }
        },
      });
  }

  get serverConnection$ () {
    return this._serverConnection$.asObservable()
  }
  get streamStarted$ () {
    return this._streamStarted$.asObservable()
  }
  get streamStarted () {
    return this._streamStarted$.value
  }
  get webSocketTest$ () {
    return this._webSocketTest$
  }
}