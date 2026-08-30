/* eslint-disable @typescript-eslint/no-unused-expressions */
import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { debounceTime, map, Observable, startWith, switchMap, take } from 'rxjs';
import { QuotesDataService } from './data-access/quotes-data.service';
import { FormControl, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { AppStorage, StorageService, StorageType } from '../../core/storage.service';
import { AuthService } from '../../core/auth.service';
import { SnacksService } from '../../shared/snacks.service';
import { ConfigService } from '../../core/config.service';
import { IRate } from '../../core/websocket.types';
import { WebSocketService } from '../../core/websocket.service';
import { MatIconModule } from '@angular/material/icon';
import { ServerManagementPanelComponent } from '../server-management-panel/server-management.component';
import { AsyncPipe, DatePipe, NgClass, PercentPipe } from '@angular/common';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatMenuModule } from '@angular/material/menu';
import { MatSliderModule } from '@angular/material/slider';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { toSignal } from '@angular/core/rxjs-interop';
import { ScrollingModule } from '@angular/cdk/scrolling';
@Component({
  selector: 'app-rt-quotes-stream',
  templateUrl: './rt-quotes-stream.component.html',
  styleUrls: ['./rt-quotes-stream.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    AsyncPipe,
    MatTooltipModule,
    MatMenuModule,
    MatSliderModule,
    FormsModule,
    MatProgressBarModule,
    MatFormFieldModule,
    MatInputModule,
    MatAutocompleteModule,
    ReactiveFormsModule,
    NgClass,
    PercentPipe,
    DatePipe,
    MatIconModule,
    MatButtonModule,
    ScrollingModule,
    ServerManagementPanelComponent,
  ],
  providers: [WebSocketService, QuotesDataService],
})
export class RealTimeQuotesStreamComponent {
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG;
  private snack = inject(SnacksService);
  private appStorage: AppStorage = inject(StorageService).storage(StorageType.IndexDB);
  public authService = inject(AuthService);
  public wssCore = inject(WebSocketService);
  public quotesService = inject(QuotesDataService);

  public showPanels = signal<boolean>(true);
  public savedFilters = signal<string[]>([]);
  public quotesFilterFC = new FormControl('');

  private quotesFilterInputSignal = toSignal(
    this.quotesFilterFC.valueChanges.pipe(startWith(this.quotesFilterFC.value), debounceTime(200)),
    { initialValue: '' },
  );
  public bufferdTime = signal<number>(this.CONFIG.BUFFER_TIME_DEFAULT);
  public quotesData$!: Observable<IRate[]>; //Subsction to the quotes stream

  public isNewFilterUnique = computed(() => {
    const rawValue = this.quotesFilterInputSignal();
    if (!rawValue) {
      return false;
    }
    const formattedValue = rawValue
      .split(',')
      .flatMap((el) => (el.trim() ? [el.trim().toUpperCase()] : []))
      .join(',');
    return formattedValue.length > 0 && this.savedFilters().includes(formattedValue) === false;
  });

  ngOnInit(): void {
    this.authService.httpGetUserData().pipe(take(1)).subscribe();
    this.appStorage
      .getStorageData('custom-filter')
      .pipe(take(1))
      .subscribe((filters) => {
        this.savedFilters.set((filters as { code: string; filter: string[] }).filter);
      });
  }
  manageStream() {
    this.wssCore.connectionState === 'connected' ? this.disconnectedFromStream() : this.getQuotesStream();
  }
  resetBufferTime(bufferInput: HTMLInputElement) {
    const newBufferTime = Number(bufferInput.value);
    if (this.bufferdTime() !== newBufferTime) {
      this.bufferdTime.set(Math.max(newBufferTime, this.CONFIG.MIN_BUFFER_TIME));
      this.quotesService.resetBufferTime(newBufferTime);
    }
  }
  trackQuotes(index:number,item:IRate):string {
    return item.symbol + item.time
  }
  getQuotesStream() {
    //Subscribe to the stream of quotes and handle update of quotes array
    this.quotesService.connectToQuoteStream(this.bufferdTime());
    this.quotesData$ = this.quotesFilterFC.valueChanges.pipe(
      startWith(this.quotesFilterFC.value),
      debounceTime(100),
      switchMap((currentFilter) => {
        const filterArray = (currentFilter || '')
          ?.toLowerCase()
          .split(',')
          .map((el) => el.trim())
          .filter((arr) => arr.length > 0);
        return this.quotesService.quotesData$.pipe(
          map((data) => {
            if (filterArray.length > 0) {
              return data.filter((quote) => filterArray.includes(quote.symbol.toLowerCase()));
            } else {
              return data;
            }
          }),
        );
      }),
    );
  }
  disconnectedFromStream() {
    // stop receiving quotes data
    this.quotesService.disconnectFromQuoteStream();
  }
  saveFilter(newFilter: string) {
    //Saving user custom filter in indexDBB
    const formatted = newFilter
      .trim()
      .split(',')
      .flatMap((el) => (el ? [el.trim().toUpperCase()] : []))
      .join(',');
    if (!formatted) {
      return;
    }
    this.savedFilters.update((prev) => [...prev, formatted]);
    this.appStorage
      .setStorageData('filterList', { code: 'custom-filter', filter: this.savedFilters() })
      .pipe(take(1))
      .subscribe();
  }
  deleteFilter(event: MouseEvent, oldFilter: string) {
    //Deleting user custom filter from indexDBB
    event.stopPropagation(); //prevent closing of autocomplete list
    this.savedFilters.update((prev) => prev.filter((el) => el !== oldFilter));
    this.appStorage
      .setStorageData('filterList', { code: 'custom-filter', filter: this.savedFilters() })
      .pipe(take(1))
      .subscribe();
  }
  logOut(loginAgain: boolean) {
    this.authService
      .logOut(this.authService.userData.userId)
      .pipe(take(1))
      .subscribe((res) => {
        res && loginAgain ? (window.location.href = this.CONFIG.AUTH_SERVER_UI_ADDRESS) : null;
        res === false ? this.snack.openSnack('Logout error', 'Okay', 'error-snackBar') : null;
      });
  }
}
