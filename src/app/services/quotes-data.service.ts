import { inject, Injectable } from '@angular/core';
import {
  switchMap,
  filter,
  takeUntil,
  map,
  startWith,
  distinctUntilChanged,
  bufferTime,
  tap,
} from 'rxjs/operators';
import { BehaviorSubject, Subject, timer } from 'rxjs';
import { SnacksService } from '../shared/snacks.service';
import { ConfigService } from '../core/config.service';
import { WebSocketService } from '../core/websocket.service';
import { IRate } from '../core/websocket.types';
@Injectable()
export class QuotesDataService {
  //Service to handle data
  private readonly snacksService = inject(SnacksService);
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG;
  private readonly wssCore = inject(WebSocketService)
  //Quotes data stream
  private readonly _quotesData$ = new BehaviorSubject<IRate[]>([]);
  public readonly quotesData$ = this._quotesData$.asObservable();
  //buffer time for quotes data stream
  private readonly _quotesBufferTime$ = new BehaviorSubject<number>(500);
  public readonly quotesBufferTime$ = this._quotesBufferTime$.asObservable();

  private isStreamInitialized = false;
  private readonly destroyQuoteStreams$ = new Subject<void>();
  private quotesDataMap = new Map<string, IRate>();

  public connectToQuoteStream (bufferPeriod = 500, endpoint = this.CONFIG.TEST_WS_ENDPOINT +  '/front') {
    if (this.isStreamInitialized && this.wssCore.connectionState !== 'disconnected') {
      console.warn('[QuotesDataService] Stream already active. Connection request ignored.');
      return;
    }
    this.destroyQuoteStreams$.next();
    this.isStreamInitialized = true;
    this.wssCore.connectServer(endpoint)
    this._quotesBufferTime$.next(bufferPeriod);
    this.createWatchDogStream();
    this.createQuoteStream();
    this.createAutoReset();
    this.createNewStreamStarted();
  }
  public disconnectFromQuoteStream() {
    this.isStreamInitialized = false;
    this.destroyQuoteStreams$.next()
    this.wssCore.disconnectServer();
    this.resetQuoteServiceState()
  }
  public resetBufferTime(bufferPeriod: number) {
    this._quotesBufferTime$.next(bufferPeriod);
  }
  private createWatchDogStream(): void {
    this.wssCore.serverStream$
      .pipe(
        switchMap(() =>
          timer(this.CONFIG.STREAM_TIMEOUT).pipe(
            map(() => false), //stream is dead,
            startWith(true), // stream is up
          ),
        ),
        distinctUntilChanged(),
        takeUntil(this.destroyQuoteStreams$),
      )
      .subscribe((isActive) => {
        this.wssCore.streamActive !== isActive ? this.wssCore.setStreamActive(isActive) : null;
        if (isActive === false) {
          const errMsg = `Warning: There has been no new quote for ${this.CONFIG.STREAM_TIMEOUT / 1000} sec...`;
          this.snacksService.openSnack(errMsg, 'Okay', 'error-snackBar');
        } else {
          this.snacksService.openSnack('Stream is up and runnig', 'Okay', 'success-snackBar');
        }
      });
  }
  private createQuoteStream(): void {
    this._quotesBufferTime$
      .pipe(
        switchMap((bufferPeriod) => {
          return this.wssCore.serverStream$.pipe(
            bufferTime(bufferPeriod),
            filter((buffer) => buffer.length > 0),
            map((bufferArrays) => {
              const flatBuffer = bufferArrays.flat();
              flatBuffer.forEach((rate) => this.quotesDataMap.set(rate.symbol, rate));
              return Array.from(this.quotesDataMap.values());
            }),
          );
        }),
        takeUntil(this.destroyQuoteStreams$),
      )
      .subscribe((quotesArray) => this._quotesData$.next(quotesArray));
  }
  private createAutoReset():void {
    this.wssCore.connectionState$
    .pipe(
      filter(state=>state === 'disconnected'),
      tap(()=>this.resetQuoteServiceState()),
      takeUntil(this.destroyQuoteStreams$)
    ).subscribe()
  }
  private createNewStreamStarted():void {
    this.wssCore.serverMessageStream$
    .pipe(
      filter(msg=>msg.message==='stream_started'),
      tap(()=>this.resetQuoteServiceState()),
      takeUntil(this.destroyQuoteStreams$)
    ).subscribe()
  }
  private resetQuoteServiceState() {
    this._quotesData$.next([]);
    this.quotesDataMap.clear();
  }
  ngOnDestroy(): void {
    this.destroyQuoteStreams$.next()
    this.destroyQuoteStreams$.complete()
    this.resetQuoteServiceState();
  }
}
