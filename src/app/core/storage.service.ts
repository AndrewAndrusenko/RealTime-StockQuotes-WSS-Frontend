/* eslint-disable @typescript-eslint/no-unused-vars */
import { inject, Injectable } from '@angular/core';
import { NgxIndexedDBService } from 'ngx-indexed-db';
import { catchError, map, Observable, of, throwError } from 'rxjs';
import { CookieService } from 'ngx-cookie-service';
import { IndexDBConfig } from '../app.module';
export enum StorageType {
  Cookie,
  IndexDB,
}
interface StorageStrategy {
  getData<T>(key: string):Observable<T>,
  setData<T>(key: string, data: T):Observable<T>,
  deleteData(key: string):Observable<boolean>
}
class StrategyCookie implements StorageStrategy {
  constructor(private cookiesService: CookieService) {}
  getData<T>(key: string): Observable<T> {
    try {
      const cookieData = JSON.parse(this.cookiesService.get(key));
      if (!cookieData) {
        throw new Error (`Cookie with key: ${key} has not been found` )
      }
      const res = JSON.parse(cookieData) as T
      return of(res)
    } catch (error) {
      console.log('Cookie get error', error);
      return throwError(() => error instanceof Error? error : new Error(String(error)))
    }
  }
  setData<T>(key: string, data: T): Observable<T> {
    const stringifiedData = JSON.stringify(data)
    try {
      this.cookiesService.set(key,stringifiedData);
      if (this.cookiesService.get(key) === stringifiedData) {
        return of(data)
      }
      return throwError(()=>new Error('Error saving cookie: stringified data does not match'))
    } catch (error) {
      return throwError(()=>error)
    }
  }
  deleteData(key: string): Observable<boolean> {
    try {
      this.cookiesService.delete(key);
      if (!this.cookiesService.get((key))) {
        of(true)
      }
      return throwError(()=>new Error('Error deleting cookie: Cookie still exists'))
    } catch (error) {
      return throwError(()=>error)
    }
  }
}
class StrategyIndexDB implements StorageStrategy {
  constructor(private indexDBservice: NgxIndexedDBService) {}
  getData<T>(key: string): Observable<T> {
    return this.indexDBservice
      .getByIndex<T>(
        IndexDBConfig.objectStoresMeta[0].store,
        IndexDBConfig.objectStoresMeta[0].storeConfig.keyPath as string,
        key,
      )
      .pipe(
        catchError((err) => {
          console.log('er', err);
          return of(err);
        }),
      );
  }
  setData<T>(key: string, data: T): Observable<T> {
    return this.indexDBservice.update<T>(IndexDBConfig.objectStoresMeta[0].store, data).pipe(
      catchError(error => {
        console.log(`IndexDB writing error for key ${key}:`, error)
        return throwError(()=>error)
      }),
    );
  }
  deleteData(key: string): Observable<boolean> {
    return this.indexDBservice.deleteByKey(IndexDBConfig.objectStoresMeta[0].store,key).pipe(
      map(()=>true),
      catchError((error) => {
        console.log(`IndexDB delete error for key ${key}:`, error);
        return throwError(()=>error);
      }),
    );
  }
}
export class AppStorage {
  constructor(private strategy: StorageStrategy) {}
  getStorageData<T>(key: string): Observable<T> {
    return this.strategy.getData<T>(key).pipe(
    );
  }
  setStorageData<T>(key: string, data: T): Observable<T> {
    return this.strategy.setData(key, data);
  }
  deleteStorageData(key: string): Observable<boolean> {
    return this.strategy.deleteData(key);
  }
}
@Injectable({
  providedIn: 'root',
})
export class StorageService {
  private readonly indexDBservice =inject(NgxIndexedDBService);
  private readonly cookiesService = inject (CookieService)
  private _storages:Map<StorageType,AppStorage> = new Map()
  private initStorageObj(storageType: StorageType): AppStorage {
    switch (storageType) {
      case StorageType.Cookie:
        this._storages.set(storageType, new AppStorage(new StrategyCookie(this.cookiesService)));
        break;
      case StorageType.IndexDB:
        this._storages.set(storageType, new AppStorage(new StrategyIndexDB(this.indexDBservice)));
        break;
    }
    return this._storages.get(storageType)!
  }
  public storage(storageType:StorageType):AppStorage {
    return this._storages.get(storageType) || this.initStorageObj(storageType)
  }
}