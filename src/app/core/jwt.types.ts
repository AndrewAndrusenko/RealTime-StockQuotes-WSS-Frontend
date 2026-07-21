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