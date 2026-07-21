import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { catchError, exhaustMap, map, Observable, of, Subject, take, tap } from 'rxjs';
import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root',
})
export class JwtHandlerService {
  private readonly http = inject(HttpClient)
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG
  private refreshTokenSub$: Subject<boolean> = new Subject();
  private _refreshTokenState$: Subject<boolean> = new Subject();
  private _refreshReqCount: number = 0;
  constructor() {
    this.refreshTokenSub$
      .pipe(
        tap(() => console.log('Request to refresh token', ++this._refreshReqCount)),
        exhaustMap(() => this._refreshToken()),
        tap(()=>this._refreshTokenState$.next(true))
      )
      .subscribe(()=>this._refreshReqCount = 0);
  }
  get refreshReqCount() {return this._refreshReqCount}
  get refreshTokenReady$():Observable<boolean> {return this._refreshTokenState$.asObservable()}
  
  private _refreshToken(): Observable<boolean | Error> {
    return this.http.get<boolean>(this.CONFIG.AUTH_SERVER_ENDPOINT + '/refresh', { withCredentials: true }).pipe(
      map(() => true),
      tap(() => console.log('REFRESHED token')),
      catchError((err) => {
        console.log('catchError refreshToken', err);
        return of(false)
      }),
    );
  }
  public refreshToken():void {
    this.refreshTokenSub$.next(true)
  }
  public refreshTokenAndWait$():Observable<boolean> {
    this.refreshTokenSub$.next(true)
    return this._refreshTokenState$.pipe(take(1))
  }
}
