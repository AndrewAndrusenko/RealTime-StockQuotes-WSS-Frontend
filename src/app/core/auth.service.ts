import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { BehaviorSubject, catchError, map, Observable, of, switchMap, tap } from 'rxjs';
import { StorageService, StorageType } from './storage.service';
import { ConfigService } from './config.service';
import { IJWTInfo, IJWTInfoExt, IJWTStorage } from './jwt.types';

@Injectable({
  providedIn: 'root',
})
export class AuthService {
  private readonly http = inject(HttpClient);
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG
  private appStorage = inject(StorageService).storage(StorageType.IndexDB);
  private _userData$: BehaviorSubject<IJWTInfo> = new BehaviorSubject({ userId: '', _id: 'null', role: 'null' });

  get userData$():Observable<IJWTInfo> {return this._userData$.asObservable()}
  get userData():IJWTInfo {return this._userData$.value}

  httpGetUserData():Observable<IJWTInfoExt> {
    return this.http
    .get<IJWTInfoExt>(this.CONFIG.AUTH_SERVER_ENDPOINT + 'userData',{ withCredentials: true })
    .pipe(
      switchMap(userData=>this.appStorage.setStorageData<IJWTStorage>('jwt', { code: 'jwt', data: userData })),
      map(data=>(data as IJWTStorage).data),
      tap(data=>this._userData$.next(data))
    )
  }

  logOut(userId: string):Observable<boolean> {
    return this.http
      .post<boolean>(this.CONFIG.AUTH_SERVER_ENDPOINT + 'logout', { userId: userId }, { withCredentials: true })
      .pipe(
        switchMap(()=>this.appStorage.deleteStorageData('jwt')),
        tap(() =>this._userData$.next({ userId: '', role: '', _id: '' })),
        map(()=>true),
        catchError(()=> of(false))
      )
  }
}