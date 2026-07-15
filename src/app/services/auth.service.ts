import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { BehaviorSubject, catchError, EMPTY, map, Observable, of, switchMap, tap } from 'rxjs';
import { StorageService, StorageType } from './storage.service';
import { ENV } from '../../environments/environment';
import { JwtHandlerService } from './jwt-handler.service';
export interface IJWTInfo {
  role: string;
  userId: string;
  _id: string;
}
export interface IJWTInfoExt extends IJWTInfo {
  exp?: number | undefined;
  iat?: number | undefined;
  iss?: string | undefined;
}
export interface IJWTStorage {
  code: 'jwt',
  data: IJWTInfoExt 
}
@Injectable({
  providedIn: 'root',
})
export class AuthService {
  private readonly http = inject(HttpClient);
  private readonly jwtService = inject(JwtHandlerService);
  private appStorage = inject(StorageService).storage(StorageType.IndexDB);
  private _userData$: BehaviorSubject<IJWTInfo> = new BehaviorSubject({ userId: '', _id: 'null', role: 'null' });

  get userData$():Observable<IJWTInfo> {return this._userData$.asObservable()}
  get userData():IJWTInfo {return this._userData$.value}

  httpGetUserData():Observable<IJWTInfoExt> {
    return this.http
    .get<IJWTInfoExt>(ENV.AUTH_SERVER_ENDPOINT + 'userData',{ withCredentials: true })
    .pipe(
      switchMap(userData=>this.appStorage.setStorageData<IJWTStorage>('jwt', { code: 'jwt', data: userData })),
      map(data=>(data as IJWTStorage).data)
    )
  }

  getUserData(): Observable<IJWTInfoExt | Error> {
    return this.appStorage.getStorageData<IJWTStorage|undefined>('jwt')
    .pipe(
      map(data=>data as IJWTStorage),
      switchMap(data=>data?.data? of(data.data) : this.httpGetUserData()),
      tap(data=>this._userData$.next(data)),
      catchError((err)=>{
        console.error('getStorageData error',err )
        return EMPTY
      })
    )
  }

  logOut(userId: string):Observable<boolean> {
    return this.http
      .post<boolean>(ENV.AUTH_SERVER_ENDPOINT + 'logout', { userId: userId }, { withCredentials: true })
      .pipe(
        switchMap(()=>this.appStorage.deleteStorageData('jwt')),
        tap(() =>this._userData$.next({ userId: '', role: '', _id: '' })),
        map(()=>true),
        catchError(()=> of(false))
      )
  }
}