/* eslint-disable @typescript-eslint/no-unused-expressions */
import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { debounceTime, filter, Observable, of, Subscription, switchMap } from 'rxjs';
import { QuotesDataService } from '../../services/quotes-data.service';
import { FormControl } from '@angular/forms';
import { AppStorage, StorageService, StorageType } from '../../core/storage.service';
import { AuthService } from '../../core/auth.service';
import { SnacksService } from '../../shared/snacks.service';
import { ConfigService } from '../../core/config.service';
import { IRate } from '../../core/websocket.types';
import { WebSocketService } from '../../core/websocket.service';
@Component({
  selector: 'app-rt-quotes-table',
  templateUrl: './rt-quotes-table.component.html',
  styleUrls: ['./rt-quotes-table.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false,
  providers:[WebSocketService,QuotesDataService]
})
export class RTQuotesTableComponent {
  private readonly CONFIG = inject(ConfigService).ENV_CONFIG
  private snack = inject(SnacksService);
  private appStorage: AppStorage = inject(StorageService).storage(StorageType.IndexDB);
  private subsriptions = new Subscription();
  public authService = inject(AuthService);
  public wssCore = inject(WebSocketService);
  public quotesService = inject(QuotesDataService);
  public showPanels: boolean = true;
  public filterQuotesList = new FormControl('');
  public savedFilters: string[] = [];
  public bufferdTime = 500;
  public quotesData$!: Observable<IRate[]>; //Subsction to the quotes stream
  public newFilter: Observable<boolean> | undefined = undefined;

  ngOnInit(): void {
    this.subsriptions.add(this.authService.getUserData().subscribe());
    this.subsriptions.add(
      this.appStorage.getStorageData('custom-filter').subscribe((filters) => {
        this.savedFilters = (filters as { code: string; filter: string[] }).filter;
      }),
    );
    this.subsriptions.add(
      this.wssCore.connectionState$.pipe(filter((st) => st === 'connected')).subscribe((st) => {
        this.getQuotesStream();
      }),
    );
    this.newFilter = this.filterQuotesList.valueChanges.pipe(
      debounceTime(300),
      switchMap((newFilter) => of(!this.savedFilters.includes(newFilter!.toLocaleUpperCase()))),
    );
  }
  ngOnDestroy(): void {
    this.subsriptions.unsubscribe();
  }
  ngAfterViewInit(): void {
    //this.manageStream();
  }
  manageStream() {
    this.wssCore.connectionState === 'connected'
      ? this.disconnectedFromStream()
      : this.quotesService.connectToQuoteStream(this.bufferdTime);
  }
  resetBufferTime(bufferInput: HTMLInputElement) {
    const bufferTime = this.bufferdTime !== Number(bufferInput.value) ? Number(bufferInput.value) : this.bufferdTime
    this.quotesService.resetBufferTime(bufferTime)
  }
  getQuotesStream() {
    //Subscribe to the stream of quotes and handle update of quotes array
    this.quotesData$ = this.quotesService.quotesData$.pipe(
      switchMap((data) => {
        const filterArray = this.filterQuotesList
          ?.getRawValue()
          ?.toLocaleLowerCase()
          .split(',')
          .map((el) => el.trim());
        // return of( data.slice(1,51))
        return of(
          filterArray![0].length > 0
            ? data.filter((row) => filterArray?.includes(row.symbol.toLocaleLowerCase()))
            : data.slice(1, 51),
        );
      }),
    );
  }
  disconnectedFromStream() {
    // stop receiving quotes data
    this.quotesService.disconnectFromQuoteStream();
  }
  trackByfn(index: number, item: IRate) {
    //trackBy to avoid whole list rendering on update
    return item.symbol + item.time; // quote has to be updated in the view if for given symbol changed time stamp
  }
  saveFilter(newFilter: string) {
    //Saving user custom filter in indexDBB
    this.savedFilters.push(newFilter.toLocaleUpperCase());
    this.subsriptions.add(
      this.appStorage.setStorageData('filterList', { code: 'custom-filter', filter: this.savedFilters }).subscribe(),
    );
    this.filterQuotesList.updateValueAndValidity();
  }
  deleteFilter(event: MouseEvent, oldFilter: string) {
    //Deleting user custom filter from indexDBB
    event.stopPropagation(); //prevent closing of autocomplete list
    this.savedFilters.splice(
      this.savedFilters.findIndex((el) => el === oldFilter),
      1,
    );
    this.subsriptions.add(
      this.appStorage.setStorageData('filterList', { code: 'custom-filter', filter: this.savedFilters }).subscribe(),
    );
  }
  logOut(loginAgain: boolean) {
    this.subsriptions.add(
      this.authService.logOut(this.authService.userData.userId).subscribe((res) => {
        res && loginAgain ? (window.location.href = this.CONFIG.AUTH_SERVER_UI_ADDRESS) : null;
        res === false ? this.snack.openSnack('Logout error', 'Okay', 'error-snackBar') : null;
      }),
    );
  }
}