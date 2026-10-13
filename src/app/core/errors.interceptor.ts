import { inject } from '@angular/core';
import { HttpRequest,  HttpErrorResponse, HttpInterceptorFn, HttpHandlerFn, HttpResponse } from '@angular/common/http';
import { catchError, map, Observable, switchMap, take, tap, throwError } from 'rxjs';
import { Router } from '@angular/router';
import { SnacksService } from '../shared/snacks.service';
import { Location } from '@angular/common';
import { JwtHandlerService } from './jwt.service';
import { ConfigService } from './config.service';
import { errorsCode, errorsInfo, IErrorCode } from './error-codes-maps';
import { VALIDATION_SCHEMA } from './validation.context';
import z from 'zod';

export const errorsIterceptor:HttpInterceptorFn = (
  request: HttpRequest<unknown>, 
  next: HttpHandlerFn
) => {
  const CONFIG = inject (ConfigService).ENV_CONFIG
  const router = inject(Router);
  const location = inject(Location);
  const snacksService = inject(SnacksService);
  const jwtService = inject(JwtHandlerService);
  const schema = request.context.get(VALIDATION_SCHEMA)
  return next(request).pipe(
    map(event => {
      if ((event instanceof HttpResponse) === false ) {
        return event
      }
      if (event.status === 200 && schema) {
        const verficationRes = schema.safeParse(event.body)
        if (verficationRes.success === false) {
          console.error('API Contract Error:', z.treeifyError(verficationRes.error))
          throw new HttpErrorResponse ({
            error:'JSON schema validation error',
            status:422,
            url:request.url
          })
        }
      }
      return event
    }),
    catchError((error: HttpErrorResponse) => {
      if (error.status === 511) {
        jwtService.refreshToken();
        return jwtService.refreshTokenReady$.pipe(
          take(1),
          switchMap(() => next(request)),
        );
      }
      return handleErrorCode(error);
    }),
  );

  function handleErrorCode(error: HttpErrorResponse): Observable<never> {
    switch (error.status) {
      case 422:
        return showError(422, `${error?.error} \n URL: ${error?.url}`);
      case 401:
        return showError(401, error?.error);
      case 403:
        return showError(403);
      case 0:
        return showError(0);
      default:
        console.log('def err', error);
        snacksService.openSnack(
          `Module:${error.error.ml} | Code: ${errorsInfo.get(error.error.msg) || error.error.msg}`,
          'Okay',
          'error-snackBar',
        );
        return throwError(() => error);
    }
  }
  
  function showError(code: number, msg: string | null = ''): Observable<never> {
    const errorOptions = errorsCode(CONFIG.AUTH_SERVER_UI_ADDRESS).get(code) as IErrorCode;
    return snacksService
      .openSnackObserve(msg||errorOptions?.message , errorOptions?.buttonName, 'error-snackBar')
      .pipe(
        tap(() => {
          console.log('errorOptions', errorOptions);
          if (errorOptions.redirect) {
            if (errorOptions.externalRoute) {
              window.location.href = errorOptions?.route;
            } else {
              router.navigate([errorOptions?.route]);
            }
          } else if (errorOptions.route === 'back') {
            location.back();
          }
        }),
        switchMap(() => throwError(() => new Error(`Error ${code}) has been handled`))),
      );
  }
}