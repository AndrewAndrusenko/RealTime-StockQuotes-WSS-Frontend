import { HttpClient } from "@angular/common/http";
import { DOCUMENT, inject, Injectable } from "@angular/core";
import { catchError, map, Observable, of, take, tap } from "rxjs";
interface IConfigFile {
  "production": boolean,
  "TEST_WS_ENDPOINT": string,
  "AUTH_SERVER_ENDPOINT":string,
  "AUTH_SERVER_UI_ADDRESS":string,
  "RETRY_INTERVAL":number,
  "RETRY_ATTEMPTS":number,
  "STREAM_TIMEOUT":number,
  "PING_HEARTBEAT_INTERVAL":number,
  "SUCCESS_TIME_OUT":number
}
const DEFFULT_CONFIG:IConfigFile = {
  "production": false,
  "TEST_WS_ENDPOINT":"wss://ppklrx85-3003.euw.devtunnels.ms",
  "AUTH_SERVER_ENDPOINT":"https://ppklrx85-3010.euw.devtunnels.ms/users/",
  "AUTH_SERVER_UI_ADDRESS":"https://ppklrx85-5001.euw.devtunnels.ms/apps/ssngrx/register",
  "RETRY_INTERVAL":1000,
  "RETRY_ATTEMPTS":2,
  "STREAM_TIMEOUT":5500,
  "PING_HEARTBEAT_INTERVAL":15000,
  "SUCCESS_TIME_OUT":2000
}
@Injectable({
  providedIn:'root'
})
export class ConfigService {
  private readonly http = inject(HttpClient);
  private readonly document = inject(DOCUMENT)
  private _ENV_CONFIG:IConfigFile = DEFFULT_CONFIG 
  get ENV_CONFIG() {return this._ENV_CONFIG}
  public loadConfigFile():Observable<boolean> {
    console.log('Loading config data from file: env.config.prod.json')
    let configURL = new URL('env.config.prod.json', this.document.baseURI).href
    return this.http.get<IConfigFile>(configURL).pipe(
      take(1),
      tap(res=>this._ENV_CONFIG = res),
      map(()=>true),
      catchError(()=>{
        console.error ('Error: ConfigService has not managed to load env.config.prod.json. Instead applied default config data')
        console.warn (this._ENV_CONFIG)
        return of(false)
      })
    )
  }
}