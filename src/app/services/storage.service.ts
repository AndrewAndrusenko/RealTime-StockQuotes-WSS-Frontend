/* eslint-disable @typescript-eslint/no-unused-vars */
import { inject, Injectable } from '@angular/core';
import { NgxIndexedDBService } from 'ngx-indexed-db';
import { catchError, filter, map, Observable, of, tap } from 'rxjs';
import { CookieService } from 'ngx-cookie-service';
import { IndexDBConfig } from '../app.module';
export enum StorageType {
  Cookie,
  IndexDB,
}
class Strategy {
  getData(key: string) {}
  setData<T>(key: string, data: T) {}
  deleteData(key: string) {}
}
class StrategyCookie extends Strategy {
  constructor(private cookiesService: CookieService) {
    super();
  }
  override getData<T>(key: string): Observable<T | Error> {
    let result: T | Error;
    try {
      result = JSON.parse(this.cookiesService.get(key));
    } catch (error) {
      console.log('err', error);
      result = error as Error;
    }
    return of<T | Error>(result as T | Error).pipe(filter((data) => !(data instanceof Error)));
  }
  override setData<T>(key: string, data: T): Observable<T | Error> {
    this.cookiesService.set(key, JSON.stringify(data));
    return this.cookiesService.get(key) === JSON.stringify(data) ? of(data) : of(new Error('Error saving cookies'));
  }
  override deleteData<T>(key: string): Observable<boolean | Error> {
    this.cookiesService.delete(key);
    return !!this.cookiesService.get(key)? of(true) : of(new Error('Error deleting cookie'));
  }
}
class StrategyIndexDB extends Strategy {
  constructor(private indexDBservice: NgxIndexedDBService) {
    super();
  }
  override getData<T>(key: string): Observable<T> {
    return this.indexDBservice
      .getByIndex<T>(
        IndexDBConfig.objectStoresMeta[0].store,
        IndexDBConfig.objectStoresMeta[0].storeConfig.keyPath as string,
        key,
      )
      .pipe(
        //tap(d=>console.log('d',d )),
        //filter((data) => data !== undefined),
        catchError((err) => {
          console.log('er', err);
          return of(err);
        }),
      );
  }
  override setData<T>(key: string, data: T): Observable<T | Error> {
    return this.indexDBservice.update<T | Error>(IndexDBConfig.objectStoresMeta[0].store, data).pipe(
      catchError((err) => {
        console.log('er', err);
        return of(err);
      }),
    );
  }
  override deleteData<T>(key: string): Observable<boolean | Error> {
    return this.indexDBservice.deleteByKey(IndexDBConfig.objectStoresMeta[0].store,key).pipe(
      map(()=>true),
      catchError((err) => {
        console.log('er', err);
        return of(err);
      }),
    );
  }
}
export class AppStorage {
  constructor(private strategy: StrategyIndexDB | StrategyCookie) {
    this.strategy = strategy;
  }
  getStorageData<T>(key: string): Observable<T | Error> {
    return this.strategy.getData<T | Error>(key).pipe(
    );
  }
  setStorageData<T>(key: string, data: T): Observable<T | Error> {
    return this.strategy.setData(key, data);
  }
  deleteStorageData<T>(key: string): Observable<boolean | Error> {
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
