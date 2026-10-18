import { DBConfig } from "ngx-indexed-db";

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