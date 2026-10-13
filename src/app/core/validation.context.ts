import { HttpContextToken } from "@angular/common/http";
import { ZodType } from 'zod';
export const VALIDATION_SCHEMA = new HttpContextToken<ZodType<any> | null> (()=>null)