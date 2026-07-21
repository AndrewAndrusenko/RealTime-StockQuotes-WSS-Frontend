import { inject, Injectable } from '@angular/core';
import { HttpRequest, HttpHandler, HttpEvent, HttpInterceptor, HttpErrorResponse } from '@angular/common/http';
import { catchError, Observable, switchMap, take, tap, throwError } from 'rxjs';
import { Router } from '@angular/router';
import { SnacksService } from '../shared/snacks.service';
import { errorsCode, errorsInfo, IErrorCode } from '../types/errors-model';
import { Location } from '@angular/common';
import { JwtHandlerService } from './jwt.service';
import { ConfigService } from './config.service';

@Injectable()
export class HttpErrorsHandlerInterceptor implements HttpInterceptor {
  private router = inject(Router);
  private snacksService = inject(SnacksService);
  private location = inject(Location);
  private jwtService = inject(JwtHandlerService);
  private CONFIG = inject (ConfigService).ENV_CONFIG
  private readonly errorCodesMap = errorsCode(this.CONFIG.AUTH_SERVER_UI_ADDRESS)

  intercept(request: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    return next.handle(request).pipe(
      catchError((error: HttpErrorResponse) => {
        if (error.status === 511) {
          this.jwtService.refreshToken();
          return this.jwtService.refreshTokenReady$.pipe(
            take(1),
            switchMap(() => next.handle(request)),
          );
        }
        return this.handleErrorCode(error);
      }),
    );
  }

  handleErrorCode(error: HttpErrorResponse): Observable<never> {
    switch (error.status) {
      case 401:
        return this.showError(401, error?.error);
      case 403:
        return this.showError(403);
      case 0:
        return this.showError(0);
      default:
        console.log('def err', error);
        this.snacksService.openSnack(
          `Module:${error.error.ml} | Code: ${errorsInfo.get(error.error.msg) || error.error.msg}`,
          'Okay',
          'error-snackBar',
        );
        return throwError(() => error);
    }
  }

  showError(code: number, msg: string | null = ''): Observable<never> {
    const errorOptions = this.errorCodesMap.get(code) as IErrorCode;
    return this.snacksService
      .openSnackObserve(errorOptions?.message + '\n ' + msg, errorOptions?.buttonName, 'error-snackBar')
      .pipe(
        tap(() => {
          console.log('errorOptions', errorOptions);
          if (errorOptions.redirect) {
            if (errorOptions.externalRoute) {
              window.location.href = errorOptions?.route;
            } else {
              this.router.navigate([errorOptions?.route]);
            }
          } else if (errorOptions.route === 'back') {
            this.location.back();
          }
        }),
        switchMap(() => throwError(() => new Error(`Error ${code}) has been handled`))),
      );
  }
}
