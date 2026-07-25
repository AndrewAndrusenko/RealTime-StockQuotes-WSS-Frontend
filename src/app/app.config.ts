import { provideHttpClient, withInterceptors } from "@angular/common/http";
import { ApplicationConfig, inject, provideAppInitializer, provideZonelessChangeDetection } from "@angular/core";
import { errorsIterceptor } from "./core/errors.interceptor";
import { ConfigService } from "./core/config.service";
import { DBConfig, provideIndexedDb } from "ngx-indexed-db";
import { provideRouter, withComponentInputBinding } from "@angular/router";
import { appRoutes } from "./app.routes";
export const IndexDBConfig: DBConfig = {
  name: 'RTQ',
  version: 1,
  objectStoresMeta: [
    {
      store: 'rtq',
      storeConfig: { keyPath: 'code', autoIncrement: false },
      storeSchema: [{ name: 'code', keypath: 'code', options: { unique: true } }],
    },
  ],
};
export const appConfig:ApplicationConfig = {
  providers:[
    provideZonelessChangeDetection(),
    provideAppInitializer(()=> {
      const configService = inject(ConfigService)
      return configService.loadConfigFile()
    }),
    provideIndexedDb(IndexDBConfig),
    provideHttpClient(
      withInterceptors([errorsIterceptor])
    ),
    provideRouter(
      appRoutes,
      withComponentInputBinding()
    )
  ]
}