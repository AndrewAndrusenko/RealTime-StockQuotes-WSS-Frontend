import { inject } from '@angular/core';
import { HttpRequest,  HttpErrorResponse, HttpInterceptorFn, HttpHandlerFn } from '@angular/common/http';
import { catchError, Observable, switchMap, take, tap, throwError } from 'rxjs';
import { Router } from '@angular/router';
import { SnacksService } from '../shared/snacks.service';
import { Location } from '@angular/common';
import { JwtHandlerService } from './jwt.service';
import { ConfigService } from './config.service';
import { errorsCode, errorsInfo, IErrorCode } from './error-codes-maps';

export const errorsIterceptor:HttpInterceptorFn = (
  request: HttpRequest<unknown>, 
  next: HttpHandlerFn
) => {
  const CONFIG = inject (ConfigService).ENV_CONFIG
  const router = inject(Router);
  const location = inject(Location);
  const snacksService = inject(SnacksService);
  const jwtService = inject(JwtHandlerService);
  return next(request).pipe(
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
      .openSnackObserve(errorOptions?.message , errorOptions?.buttonName, 'error-snackBar')
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