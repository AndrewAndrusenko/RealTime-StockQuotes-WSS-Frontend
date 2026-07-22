import { inject, Injectable } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import {
  switchMap,
  filter,
  repeat,
  delay,
  retry,
  takeUntil,
  share,
  tap,
  ignoreElements,
} from 'rxjs/operators';
import { BehaviorSubject, EMPTY, merge, MonoTypeOperatorFunction, Observable, of, Subject, timer } from 'rxjs';
import { SnacksService } from '../shared/snacks.service';
import { TConnectionStatus } from '../types/shared-models';
import { IErrorHandler, SERVER_ERRORS } from '../types/errors-model';
import { JwtHandlerService } from './jwt.service';
import { ConfigService } from './config.service';
import { IRate, IServerCommand, TwsServerResponse } from './websocket.types';

@Injectable()
export class WebSocketService {
  //Service to handle data
  private readonly snacksService = inject(SnacksService);
  private readonly jwtService = inject(JwtHandlerService);
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG
  private wsServer$: WebSocketSubject<TwsServerResponse> | undefined = undefined;
  //WSS connection status for UI
  private readonly _connectionState$ = new BehaviorSubject<TConnectionStatus>('disconnected');
  public readonly connectionState$ = this._connectionState$.asObservable();
  get connectionState() {return this._connectionState$.value}
  //Stream status for UI
  private readonly _streamActive$ = new BehaviorSubject<boolean>(false);
  public readonly streamActive$ = this._streamActive$.asObservable();
  get streamActive() {return this._streamActive$.value};
  //WSS data stream
  private _serverStream$ = new Subject<IRate[]>()
  public serverStream$ = this._serverStream$.asObservable()
  //WSS service messages stream
  private _serverMessageStream$ = new Subject<{message: string}>()
  public serverMessageStream$ = this._serverMessageStream$.asObservable()
  //Reconnecting data
  private readonly _connectionRepeat$ = new BehaviorSubject<{ current: number; total: number } | null>(null);
  public readonly connectionRepeat$ = this._connectionRepeat$.asObservable();
  //Network latency
  private readonly _networkLatency$ = new BehaviorSubject<number|null>(null)
  public readonly networkLatency$ = this._networkLatency$.asObservable();
  private pingStartTime:number = 0;

  private closeConnectionErrorCode: number | null = null;
  private readonly destroyStreams$ = new Subject<void>();

  public connectServer(endpoint = this.CONFIG.TEST_WS_ENDPOINT) {
    const currentConnectionState = this._connectionState$.value;
    if (currentConnectionState !== 'disconnected') {
      console.warn(`connectToWSServer: Creating new connection was blocked. Current connection state is ${currentConnectionState}`);
      return;
    }
    this._connectionState$.next('Connecting');
    this.destroyStreams$.next();

    this.wsServer$ = webSocket({
      url: endpoint,
      closeObserver: {
        next: (event) => {
          this.closeConnectionErrorCode = this.closeConnectionErrorCode || event.code;
          console.log('UI connection is closed with code: in code', this.closeConnectionErrorCode);
        },
      },
    });
    const reconnectingSocket$ = this.wsServer$.pipe(this.reconnecting())
    //ui heartbeat ping
    const uiPing$ = timer(this.CONFIG.PING_HEARTBEAT_INTERVAL, this.CONFIG.PING_HEARTBEAT_INTERVAL).pipe(
      tap(() => {
        if (this.wsServer$ && this.wsServer$.closed === false) {
          this.pingStartTime = performance.now();
          this.wsServer$.next({ cmd: 'ping' });
        }
      }),
      ignoreElements(),
    );
    //core source stream
    const sharedServerStream$ = merge(reconnectingSocket$, uiPing$).pipe(
      takeUntil(this.destroyStreams$),
      share(),
    );
    this.createSystemStream(sharedServerStream$);
  }
  public disconnectServer(error?: IErrorHandler, errorCode?: number) {
    if (error?.errmsgIgnore === false) {
      let message = error?.messageToUI || 'Unknown connection error';
      this.snacksService.openSnack(`Error code:${errorCode}. ${message} `, 'Okay', 'error-snackBar');
    }
    this.destroyStreams$.next();
    if (this.wsServer$) {
      this.wsServer$.complete();
      this.wsServer$ = undefined;
    }
    this._connectionState$.next('disconnected');

    this._serverStream$.complete();
    this._serverStream$ = new Subject<IRate[]>()
    this.serverStream$ = this._serverStream$.asObservable();

    this._streamActive$.next(false); 
    this._connectionRepeat$.next(null);
    this._networkLatency$.next(null)

    this.closeConnectionErrorCode = null;
    this.pingStartTime = 0;
  }
  private reconnecting<T>(): MonoTypeOperatorFunction<T> {
    let retryAttemptNum: number = 0;
    const reconnectStateShare = () => {
      this._connectionRepeat$.next({ current: retryAttemptNum, total: this.CONFIG.RETRY_ATTEMPTS });
      console.log(
        `QuotesDataService: Trying to reconnect due to error ${this.closeConnectionErrorCode}. Attempt ${retryAttemptNum} out of ${this.CONFIG.RETRY_ATTEMPTS}`,
      );
      this._connectionState$.next('Reconnecting');
      this.closeConnectionErrorCode = null;
    }
    const retryDelay = () => {
      retryAttemptNum++;
      const errorCode = this.closeConnectionErrorCode || 503;
      const error = SERVER_ERRORS.get(errorCode)!;
      // Max attempts reached block or no retry
      if (error?.retryConnection === false || this.CONFIG.RETRY_ATTEMPTS + 1 === retryAttemptNum) {
        this.disconnectServer(error, errorCode);
        return EMPTY;
      }
      //JWT has been expired
      if (error?.authErr) {
        this.pingStartTime = 0;
        return this.jwtService.refreshTokenAndWait$().pipe(
          switchMap((done) => {
            if (done) {
              reconnectStateShare();
              return of(1);
            } else {
              console.error('Token refresh error');
              this._connectionState$.next('disconnected');
              return EMPTY;
            }
          }),
        );
      }
      //Error requires reconnection
      if (error?.retryConnection) {
        const exponentDelay = Math.pow(2, retryAttemptNum) * this.CONFIG.RETRY_INTERVAL;
        const jitterRate = 0.7 + Math.random() * 0.6;
        const finalDelay = Math.round(exponentDelay * jitterRate);
        reconnectStateShare();
        return of(1).pipe(delay(finalDelay));
      }
      console.warn(`QuotesDataService has been unable to handle error:${errorCode}`);
      this.disconnectServer(error, errorCode);
      return EMPTY;
    };
    return (source$) =>
      source$.pipe(
        repeat({ delay: retryDelay }), // reconnect when source completes
        retry({ delay: retryDelay }), // reconnect when there is an error in the source
      );
  }
  private createSystemStream(sharedServerStream$: Observable<TwsServerResponse>): void {
    sharedServerStream$
      .pipe(
        tap((data) => 'message' in Object(data) === false ? this._serverStream$.next(data as IRate[]) : null),
        filter((data) => 'message' in Object(data)),
        tap((data) => this._serverMessageStream$.next(data as {message:string})),
        takeUntil(this.destroyStreams$),
      )
      .subscribe({
        complete: () => {
          this.disconnectServer();
        },
        next: (msg) => {
          switch ((msg as { message: string }).message) {
            case 'pong':
              if (this.pingStartTime > 0) {
                const latencyRaw = Math.round(performance.now() - this.pingStartTime)
                const previousEMALatency = this._networkLatency$.value
                if (previousEMALatency === null) {
                  this._networkLatency$.next(latencyRaw)
                } else {
                  const alpha = 0.3
                  const EMALatency = Math.round(alpha * latencyRaw + (1 - alpha) * previousEMALatency);
                  this._networkLatency$.next(EMALatency)
                }
                this.pingStartTime = 0;
              }
              break;
            case 'connected':
              this._connectionState$.next('connected');
              this.closeConnectionErrorCode = null;
              break;
            case 'stream_started':
              this._streamActive$.next(true);
              break;
            case 'stream_stopped':
              this._streamActive$.next(false);
              break;
            default:
              console.log('ws server message:', msg);
          }
        },
      });
  }
  public sendCommandToServer(cmd:IServerCommand) {
    this.wsServer$?.next(cmd)
  }
  public setStreamActive(isActive:boolean) {
    this._streamActive$.next(isActive) 
  }
  ngOnDestroy(): void {
    this.destroyStreams$.next()
    this.destroyStreams$.complete()
    this.disconnectServer();
  }
}