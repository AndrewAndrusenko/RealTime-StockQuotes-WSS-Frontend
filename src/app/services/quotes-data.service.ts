/* eslint-disable @typescript-eslint/no-unused-expressions */
import { inject, Injectable } from '@angular/core';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import {
  throttleTime,
  switchMap,
  filter,
  timeout,
  catchError,
  repeat,
  delay,
  retry,
} from 'rxjs/operators';
import { BehaviorSubject, EMPTY, MonoTypeOperatorFunction, Observable, of, throwError } from 'rxjs';
import { SnacksService } from './snacks.service';
import { TConnectionStatus } from '../types/shared-models';
import { IErrorHandler, SERVER_ERRORS } from '../types/errors-model';
import { ENV } from '../../environments/environment';
import { JwtHandlerService } from './jwt-handler.service';
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
@Injectable({
  providedIn: 'root',
})
export class QuotesDataService {
  //Service to handle data
  private readonly snacksService = inject(SnacksService);
  private readonly jwtService = inject(JwtHandlerService);
  private wsServer$: WebSocketSubject<IRate[] | { message: string } | { cmd: 'close' }> | undefined = undefined;
  private readonly _connectionState$ = new BehaviorSubject<TConnectionStatus>('disconnected'); //Connection status for UI
  //Connection status for UI
  private readonly _connectionRepeat$ = new BehaviorSubject<{ current: number; total: number } | null>(null);
  private readonly _streamActive$ = new BehaviorSubject<boolean>(false); //Stream status for UI
  private quotesDataArray: IRate[] = []; // Quotes array to be displayed in the template
  private closeConnectionErrorCode: number | null = null;
  private conecctionRetryCount: number = 2;
  private connectionAttemptN: number = 0;

  public connectToWSServer(endpoint = ENV.TEST_WS_ENDPOINT) {
    this._connectionState$.next('Connecting');
    this.wsServer$ = webSocket({
      url: endpoint + '/front',
/*       openObserver: {
        next: () => {
          console.log('openObserver webSocket');
        },
      }, */
      closeObserver: {
        next: (event) => {
          this.closeConnectionErrorCode = this.closeConnectionErrorCode || event.code;
          console.log('UI connection is closed with code: in code', this.closeConnectionErrorCode);
        },
      },
    });

    this.wsServer$
      .pipe(
        this.handleReconnecting(),
        filter((data) => 'message' in Object(data)),
      )
      .subscribe({
        complete: () => {
          this._connectionState$.next('disconnected');
          this._streamActive$.next(false);
        },
        next: (msg) => {
          switch ((msg as { message: string }).message) {
            case 'connected':
              this._connectionState$.next('connected');
              this.closeConnectionErrorCode = null;
              this.connectionAttemptN = 0;
              break;
            case 'stream_started':
              this._streamActive$.next(true);
              this.quotesDataArray = [];
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

  private handleReconnecting<T>(): MonoTypeOperatorFunction<T> {
    const retryDelay = () =>{
      this.connectionAttemptN++;
      const errorCode = this.closeConnectionErrorCode || 503
      const error = SERVER_ERRORS.get(errorCode)!;

      // Max attempts reached block or no retry
      if ((error?.retryConnection === false) || (this.conecctionRetryCount + 1 === this.connectionAttemptN)) {
        this.wssDisconnect(error,errorCode)
        return EMPTY;
      }
      
      //JWT has been expired
      if (error?.authErr) {
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
        const exponentDelay = Math.pow(2, this.connectionAttemptN) * ENV.RETRY_INTERVAL;
        const jitterRate = 0.7 + Math.random() * 0.6; 
        const finalDelay = Math.round(exponentDelay * jitterRate);
        this.reconnectNotify();
        return of(1).pipe(delay(finalDelay));
      }
      console.warn(`QuotesDataService has been unable to handle error:${errorCode}`)
      this.wssDisconnect(error,errorCode);
      return EMPTY
    }
    return (source$) => source$.pipe(
      repeat({delay:retryDelay}), // reconnect when source completes 
      retry({delay:retryDelay}), // reconnect when there is an error in the source
/*    catchError(()=>{ // reconnect when there is an error in the source (old recurrsive)
        return retryDelay().pipe(
          switchMap(()=>source$.pipe(this.handleReconnecting()))
        )
      }) */
    )
  }

  private reconnectNotify() {
    this._connectionRepeat$.next({ current: this.connectionAttemptN, total: this.conecctionRetryCount });
    console.log(
      `QuotesDataService: Trying to reconnect due to error ${this.closeConnectionErrorCode}. Attempt ${this.connectionAttemptN} out of ${this.conecctionRetryCount}`,
    );
    this._connectionState$.next('Reconnecting');
    this.closeConnectionErrorCode = null;
  }

  private wssDisconnect(error:IErrorHandler, errorCode:number) {
    if (error?.errmsgIgnore === false) {
      let message = error?.messageToUI || 'Unknown connection error'
      this.snacksService.openSnack(`Error code:${errorCode}. ${message} `,'Okay','error-snackBar');
    }
    this.closeConnectionErrorCode = null;
    this.connectionAttemptN = 0;
    this._connectionRepeat$.next(null);
    this.wsServer$?.closed ? null : this.wsServer$?.unsubscribe();
    this._connectionState$.next('disconnected');
  }
  
  public quotesStream$(cachingTime = 500): Observable<IRate[]> {
    let bufferRates: IRate[] = [];
    return of(!this.wsServer$ || this.wsServer$.closed).pipe(
      switchMap(() =>
        this.wsServer$!.pipe(
          filter((data) => !('message' in Object(data))),
          //timeout({ each: ENV.STREAM_TIMEOUT }),
          switchMap((newSet) => {
            const newSetSymbols = (newSet as IRate[]).map((newRate) => newRate.symbol);
            return of(
              (bufferRates = (newSet as IRate[]).concat(
                bufferRates.filter((oldRate) => !newSetSymbols.includes(oldRate.symbol)),
              )),
            );
          }),
          throttleTime(cachingTime),
          switchMap((newSetFull) => {
            bufferRates = [];
            newSetFull.length && !this._streamActive$.value ? this._streamActive$.next(true) : null;
            newSetFull.forEach((newRate) => {
              const index = this.quotesDataArray.findIndex((rateRow) => rateRow.symbol === newRate.symbol);
              index > -1 ? (this.quotesDataArray[index] = newRate) : this.quotesDataArray.push(newRate);
            });
            return of(this.quotesDataArray);
          }),
          catchError((err) => {
            console.error('error quotesStream$', err);
            this._streamActive$.next(false);
            let errMsg = 'Server error';
            switch (err.name) {
              case 'TimeoutError':
                errMsg = `Warning: There has been no new quote for ${ENV.STREAM_TIMEOUT / 1000} sec...`;
                //this.disconnectFromServer(); //??
                break;
            }
            this.wsServer$?.closed === false && this._connectionState$.getValue() !== 'Reconnecting'
              ? this.snacksService.openSnack(errMsg, 'Okay', 'error-snackBar')
              : null;
            //return of([]); //??
            return of(this.quotesDataArray)
          }),
        ),
      ),
    );
  }

  public disconnectFromServer() {
    if (!this.wsServer$) {
      return;
    }
    if (this.wsServer$.closed === false) {
      this._connectionState$.next('Disconnecting');
      this.wsServer$.next({ cmd: 'close' });
    }
  }

  get connectionState() {
    return this._connectionState$.value;
  }
  get connectionState$() {
    return this._connectionState$.asObservable();
  }
  get connectionRepeat$() {
    return this._connectionRepeat$.asObservable();
  }
  get streamActive$() {
    return this._streamActive$.asObservable();
  }
}
