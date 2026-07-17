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
  map,
  startWith,
  distinctUntilChanged,
  bufferTime,
  tap,
  ignoreElements,
} from 'rxjs/operators';
import { BehaviorSubject, EMPTY, merge, MonoTypeOperatorFunction, Observable, of, Subject, timer } from 'rxjs';
import { SnacksService } from './snacks.service';
import { TConnectionStatus } from '../types/shared-models';
import { IErrorHandler, SERVER_ERRORS } from '../types/errors-model';
import { ENV } from '../../environments/environment';
import { JwtHandlerService } from './jwt-handler.service';
import { TitleStrategy } from '@angular/router';
export interface IRate {
  //Intreface for received quotes from server
  time: Date; //quote rate
  symbol: string; // instrument code
  bid: number; // price to buy
  ask: number; // price to sell
  open: number; // price to sell
  chgBid: number;
  chgAsk: number;
  chgUpDown: number;
}
export type TwsServerResponse = IRate[] | { message: string } | { cmd: 'close' | 'ping' };
@Injectable({
  providedIn: 'root',
})
export class QuotesDataService {
  //Service to handle data
  private readonly snacksService = inject(SnacksService);
  private readonly jwtService = inject(JwtHandlerService);

  private wsServer$: WebSocketSubject<TwsServerResponse> | undefined = undefined;
  //WSS Connection status for UI
  private readonly _connectionState$ = new BehaviorSubject<TConnectionStatus>('disconnected');
  public readonly connectionState$ = this._connectionState$.asObservable();
  //Stream status for UI
  private readonly _streamActive$ = new BehaviorSubject<boolean>(false);
  public readonly streamActive$ = this._streamActive$.asObservable();
  //Reconnecting data
  private readonly _connectionRepeat$ = new BehaviorSubject<{ current: number; total: number } | null>(null);
  public readonly connectionRepeat$ = this._connectionRepeat$.asObservable();
  //Quotes data stream
  private readonly _quotesData$ = new BehaviorSubject<IRate[]>([]);
  public readonly quotesData$ = this._quotesData$.asObservable();
  //buffer time for quotes data stream
  private readonly _quotesBufferTime$ = new BehaviorSubject<number>(500);
  public readonly quotesBufferTime$ = this._quotesBufferTime$.asObservable();
  //network latency
  private readonly _networkLatency$ = new BehaviorSubject<number|null>(null)
  public readonly networkLatency$ = this._networkLatency$.asObservable();
  private pingStartTime:number = 0;

  private closeConnectionErrorCode: number | null = null;
  private conecctionRetryCount: number = 2;
  private retryAttemptNum: number = 0;

  private readonly destroyStreams$ = new Subject<void>();
  private quotesDataMap = new Map<string, IRate>();

  get connectionState() {
    return this._connectionState$.value;
  }

  public connectToWSServer(bufferPeriod = 500, endpoint = ENV.TEST_WS_ENDPOINT) {
    const currentConnectionState = this._connectionState$.value;
    if (currentConnectionState !== 'disconnected') {
      console.warn(
        `connectToWSServer: Creating new connection was blocked. Current connection state is ${currentConnectionState}`,
      );
      return;
    }
    this._quotesBufferTime$.next(bufferPeriod);
    this._connectionState$.next('Connecting');
    this.destroyStreams$.next();

    this.wsServer$ = webSocket({
      url: endpoint + '/front',
      closeObserver: {
        next: (event) => {
          this.closeConnectionErrorCode = this.closeConnectionErrorCode || event.code;
          console.log('UI connection is closed with code: in code', this.closeConnectionErrorCode);
        },
      },
    });
    const reconnectingSocket$ = this.wsServer$.pipe(
      this.handleReconnecting()
    )
    //ui heartbeat ping
    const uiPing$ = timer(ENV.PING_HEARTBEAT_INTERVAL, ENV.PING_HEARTBEAT_INTERVAL).pipe(
      tap(() => {
        if (this.wsServer$ && this.wsServer$.closed === false) {
          this.pingStartTime = performance.now();
          this.wsServer$.next({ cmd: 'ping' });
        }
      }),
      ignoreElements(),
    );
    //core source stream
    const sharedWssStream$ = merge(reconnectingSocket$, uiPing$).pipe(
      takeUntil(this.destroyStreams$),
      share(),
    );
    this.createWssSystemStream(sharedWssStream$);

    //source stream for quotes stream and watchdog
    const rawQuoteStream$ = sharedWssStream$.pipe(
      filter((data) => 'message' in Object(data) === false),
      map((data) => data as IRate[]),
    );
    this.createWatchDogStream(rawQuoteStream$);
    this.createQuoteStream(rawQuoteStream$);
  }

  public disconnectFromServer() {
    this.destroyStreams$.next();
    if (this.wsServer$) {
      this.wsServer$.complete();
      this.wsServer$ = undefined;
    }
    this.resetServiceState();
  }
  public resetBufferTime(bufferPeriod: number) {
    this._quotesBufferTime$.next(bufferPeriod);
  }
  private handleReconnecting<T>(): MonoTypeOperatorFunction<T> {
    const retryDelay = () => {
      this.retryAttemptNum++;
      const errorCode = this.closeConnectionErrorCode || 503;
      const error = SERVER_ERRORS.get(errorCode)!;

      // Max attempts reached block or no retry
      if (error?.retryConnection === false || this.conecctionRetryCount + 1 === this.retryAttemptNum) {
        this.wssDisconnect(error, errorCode);
        return EMPTY;
      }

      //JWT has been expired
      if (error?.authErr) {
        this.pingStartTime = 0;
        return this.jwtService.refreshTokenAndWait$().pipe(
          switchMap((done) => {
            if (done) {
              this.reconnectNotify();
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
        const exponentDelay = Math.pow(2, this.retryAttemptNum) * ENV.RETRY_INTERVAL;
        const jitterRate = 0.7 + Math.random() * 0.6;
        const finalDelay = Math.round(exponentDelay * jitterRate);
        this.reconnectNotify();
        return of(1).pipe(delay(finalDelay));
      }
      console.warn(`QuotesDataService has been unable to handle error:${errorCode}`);
      this.wssDisconnect(error, errorCode);
      return EMPTY;
    };
    return (source$) =>
      source$.pipe(
        repeat({ delay: retryDelay }), // reconnect when source completes
        retry({ delay: retryDelay }), // reconnect when there is an error in the source
      );
  }

  private reconnectNotify() {
    this._connectionRepeat$.next({ current: this.retryAttemptNum, total: this.conecctionRetryCount });
    console.log(
      `QuotesDataService: Trying to reconnect due to error ${this.closeConnectionErrorCode}. Attempt ${this.retryAttemptNum} out of ${this.conecctionRetryCount}`,
    );
    this._connectionState$.next('Reconnecting');
    this.closeConnectionErrorCode = null;
  }

  private createWssSystemStream(sharedWssStream$: Observable<TwsServerResponse>): void {
    sharedWssStream$
      .pipe(
        filter((data) => 'message' in Object(data)),
        takeUntil(this.destroyStreams$),
      )
      .subscribe({
        complete: () => {
          this.resetServiceState();
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
              this.retryAttemptNum = 0;
              break;
            case 'stream_started':
              this._streamActive$.next(true);
              this.quotesDataMap.clear();
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

  private createWatchDogStream(rawQuoteStream$: Observable<IRate[]>): void {
    rawQuoteStream$
      .pipe(
        switchMap(() =>
          timer(ENV.STREAM_TIMEOUT).pipe(
            map(() => false), //stream is dead,
            startWith(true), // stream is up
          ),
        ),
        distinctUntilChanged(),
        takeUntil(this.destroyStreams$),
      )
      .subscribe((isActive) => {
        this._streamActive$.value !== isActive ? this._streamActive$.next(isActive) : null;
        if (isActive === false) {
          const errMsg = `Warning: There has been no new quote for ${ENV.STREAM_TIMEOUT / 1000} sec...`;
          this.snacksService.openSnack(errMsg, 'Okay', 'error-snackBar');
        } else {
          this.snacksService.openSnack('Stream is up and runnig', 'Okay', 'success-snackBar');
        }
      });
  }

  private createQuoteStream(rawQuoteStream$: Observable<IRate[]>): void {
    this._quotesBufferTime$
      .pipe(
        switchMap((bufferPeriod) => {
          return rawQuoteStream$.pipe(
            bufferTime(bufferPeriod),
            filter((buffer) => buffer.length > 0),
            map((bufferArrays) => {
              const flatBuffer = bufferArrays.flat();
              flatBuffer.forEach((rate) => this.quotesDataMap.set(rate.symbol, rate));
              return Array.from(this.quotesDataMap.values());
            }),
          );
        }),
        takeUntil(this.destroyStreams$),
      )
      .subscribe((quotesArray) => this._quotesData$.next(quotesArray));
  }

  private wssDisconnect(error: IErrorHandler, errorCode: number) {
    if (error?.errmsgIgnore === false) {
      let message = error?.messageToUI || 'Unknown connection error';
      this.snacksService.openSnack(`Error code:${errorCode}. ${message} `, 'Okay', 'error-snackBar');
    }
    this.resetServiceState();
  }

  private resetServiceState() {
    this._connectionState$.next('disconnected');
    this._streamActive$.next(false);
    this._connectionRepeat$.next(null);
    this._quotesData$.next([]);
    this._networkLatency$.next(null)
    this.quotesDataMap.clear();
    this.retryAttemptNum = 0;
    this.closeConnectionErrorCode = null;
    this.pingStartTime = 0;
  }
}
