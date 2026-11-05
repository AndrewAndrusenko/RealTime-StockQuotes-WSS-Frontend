import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RealTimeQuotesStreamComponent } from './rt-quotes-stream.component';
import { BehaviorSubject } from 'rxjs';
import { IRate, TConnectionStatus } from '../../core/websocket.types';
import { IJWTInfo } from '../../core/jwt.types';
import { QuotesDataService } from './data-access/quotes-data.service';
import { WebSocketService } from '../../core/websocket.service';
import { AuthService } from '../../core/auth.service';
import { By } from '@angular/platform-browser';
import { ConfigService } from '../../core/config.service';
import { SnacksService } from '../../shared/snacks.service';
import { StorageService } from '../../core/storage.service';
import { AsyncPipe, DatePipe, NgClass, PercentPipe } from '@angular/common';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatSliderModule } from '@angular/material/slider';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { CdkVirtualScrollViewport, ScrollingModule } from '@angular/cdk/scrolling';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { Component } from '@angular/core';
@Component({
  selector:'app-server-management-panel',
  standalone:true,
  template:'<div>Mock Server Panel</div>'
})
class MockServerManagementPanelComponent {}

describe('RealTimeQuotesStreamComponent', () => {
  let component: RealTimeQuotesStreamComponent;
  let fixture: ComponentFixture<RealTimeQuotesStreamComponent>;
  let mockQuotesDataService: jest.Mocked<QuotesDataService>;
  let mockQuotesData$: BehaviorSubject<IRate[]>;

  let mockWebSocketService: jest.Mocked<WebSocketService>;
  let mockConnectionState$:  BehaviorSubject<TConnectionStatus>;
  let mockStreamActive$: BehaviorSubject<boolean>;
  let mockConnectionRepeat$: BehaviorSubject<{ current: number; total: number } | null>;
  let mockNetworkLatency$: BehaviorSubject<number|null>;
  
  let mockAuthService: jest.Mocked<AuthService>;
  let mockUserData$: BehaviorSubject<IJWTInfo> ;
  let httpGetUserData$: BehaviorSubject<IJWTInfo> ;
  let mockConfigService: jest.Mocked<ConfigService>;
  
  let mockSnackService: jest.Mocked<SnacksService>;
  let mockStorageService: jest.Mocked<StorageService>;
  let getStorageData$ : BehaviorSubject<{code:string, filter:string[]}>
  let setStorageData$ : BehaviorSubject<{code:string, filter:string[]}>
  beforeEach(async () => {
    mockQuotesData$ = new BehaviorSubject<IRate[]>([]);

    mockQuotesDataService = {
      quotesData$: mockQuotesData$,
      resetBufferTime:jest.fn(),
      connectToQuoteStream:jest.fn(),
      disconnectFromQuoteStream:jest.fn()
    } as any;
    mockConfigService = {
      ENV_CONFIG : {
        "BUFFER_TIME_DEFAULT":300,
        "MIN_BUFFER_TIME":5,
        "AUTH_SERVER_UI_ADDRESS":"https://ppklrx85-5001.euw.devtunnels.ms/apps/ssngrx/register/logout/:logout",
      }
    } as any;
    mockConnectionState$ =  new BehaviorSubject<TConnectionStatus>('disconnected');
    mockStreamActive$ = new BehaviorSubject<boolean>(false);
    mockConnectionRepeat$ = new BehaviorSubject<{ current: number; total: number } | null> (null);
    mockNetworkLatency$ = new BehaviorSubject<number|null>(null);
    mockWebSocketService = {
      connectionState$: mockConnectionState$,
      streamActive$:mockStreamActive$,
      connectionRepeat$: mockConnectionRepeat$,
      networkLatency$:mockNetworkLatency$,
    } as any;
    getStorageData$ = new BehaviorSubject<{code:string, filter:string[]}>({code:'custom-filter', filter: [
      'ins1,ins2,ins3','stock1,stock2'
    ]})
    setStorageData$ = new BehaviorSubject<{code:string, filter:string[]}>({code:'custom-filter', filter: [
      'ins1,ins2,ins3','stock1,stock2'
    ]})

    mockStorageService = {
      storage :jest.fn(()=>{
        return {
          getStorageData: jest.fn(() => getStorageData$.asObservable()),
          setStorageData: jest.fn(() => setStorageData$.asObservable())
        }})
    } as any;
    mockUserData$ =  new BehaviorSubject({ userId: '', _id: 'null', role: 'null' });
    httpGetUserData$ =  new BehaviorSubject({ userId: '', _id: 'null', role: 'null' });
    mockAuthService = {
      userData$:mockUserData$,
      httpGetUserData:jest.fn(()=>httpGetUserData$)
    } as any;
    mockSnackService = {
      openSnack:jest.fn()
    } as any;
    await TestBed.configureTestingModule({
      imports:[RealTimeQuotesStreamComponent],
      providers:[
        {provide:AuthService, useValue:mockAuthService},
        {provide:ConfigService, useValue:mockConfigService},
        {provide:StorageService, useValue:mockStorageService},
        {provide:SnacksService, useValue:mockSnackService},
      ]
    })
    .overrideComponent(RealTimeQuotesStreamComponent,{
      set:{
        providers: [
          {provide:QuotesDataService, useValue:mockQuotesDataService},
          {provide:WebSocketService, useValue:mockWebSocketService},
        ],
          imports: [
            AsyncPipe, MatTooltipModule, MatMenuModule,  MatSliderModule, FormsModule,
            MatProgressBarModule,MatFormFieldModule,MatInputModule,MatAutocompleteModule,ReactiveFormsModule,
            NgClass,PercentPipe,DatePipe,MatIconModule,MatButtonModule,ScrollingModule, 
            MockServerManagementPanelComponent
          ],
      }
    })
    .compileComponents()
    
    fixture = TestBed.createComponent(RealTimeQuotesStreamComponent);
    component = fixture.componentInstance;
    fixture.detectChanges()
    await fixture.whenStable()
  });
  afterEach(() => {});
  describe('Initial Render & Layout', () => {
    const uiElementToVerify:{id:string, result:boolean}[] = [
      {id:'[data-testid="app-server-management-panel"]', result:true},
      {id:'[data-testid="mng-panel"]', result:true},
      {id:'[data-testid="mng-panel__ui__button"]', result:true},
      {id:'[data-testid="mng-panel__ui__buffer-slider"]', result:true},
      {id:'[data-testid="mng-panel__ui__button_reset"]', result:true},
      {id:'[data-testid="mng-panel__header__user_button"]', result:true},
      {id:'[data-testid="content__filter"]', result:true},
      {id:'[data-testid="virtual-scroll-container"]', result:true},
      {id:'[data-testid="quote-data-row"]', result:false},
      {id:'[data-testid="ask-price"]', result:false}
    ]
    it.each(uiElementToVerify) (
      'initial render: element with $id has to be in dom ($result) ',
     ({id,result}) => expect(!!fixture.debugElement.query(By.css(id))).toBe(result)
    );
    it('connect button is not disabled', () =>{
       const connectButton =  fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
       const buttonState = connectButton.getAttribute('data-state')
       expect(connectButton.disabled).toBeFalsy()
       expect(buttonState).toEqual('disconnected')
    })
    it('default buffer time rendered', ()=>{
      const bufferTime = fixture.debugElement.query(By.css('[data-testid="buffer-time-value"]'))?.nativeElement as HTMLInputElement;
      expect(bufferTime.value).toEqual(mockConfigService.ENV_CONFIG.BUFFER_TIME_DEFAULT.toString())
    })
    it('no ping before connection', ()=>{
      expect(fixture.debugElement.query(By.css('[data-testid="mng-panel__header__info_ping"]'))).toBeNull()
    })
  });
  describe('Stream Connections', () => {
    it('connect to steam via ui button', async () => {
      const connectButton = fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
      //1. No data stream pipe
      expect(component.quotesData$).toBeFalsy()
      connectButton.click();
      fixture.detectChanges();
      await fixture.whenStable();
      //2. Data stream pipe activated
      expect(component.quotesData$).toBeTruthy()
    })
    it('no stream state - there is no data from wss', async () => {
      const connectButton = fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
      connectButton.click();
      fixture.detectChanges();
      await fixture.whenStable();
      //2. Data stream pipe activated
      expect(component.quotesData$).toBeTruthy();
      //3. Stream state verification
      const streamState = fixture.debugElement.query(By.css('[data-testid="mng-panel__header__status"]')).nativeElement as HTMLElement;
      expect(streamState.getAttribute('data-state')).toEqual('false')
    })

  });
  describe('Buffer Time Settings', () => {
    it('input changed => slider - two-way biding', async () => {
      const sliderBufferValue = fixture.debugElement.query(By.css('[data-testid="buffer-time-value"]')).nativeElement as HTMLInputElement;
      const inputBuffer = fixture.debugElement.query(By.css('[data-testid="buffer-time-value-manual"]')).nativeElement as HTMLInputElement;
      //1.Change input buffer time ui element value
      inputBuffer.value = '100';
      inputBuffer.dispatchEvent(new Event('input'));
      fixture.detectChanges()
      await fixture.whenStable()
      //2.Check buffer slider has changed
      expect(sliderBufferValue.value).toEqual('100')
    })
    it('slider changed => input - two-way biding', async () => {
      const sliderBufferValue = fixture.debugElement.query(By.css('[data-testid="buffer-time-value"]')).nativeElement as HTMLInputElement;
      const inputBuffer = fixture.debugElement.query(By.css('[data-testid="buffer-time-value-manual"]')).nativeElement as HTMLInputElement;
      //1.Change slider buffer time ui element value
      sliderBufferValue.value = '800';
      sliderBufferValue.dispatchEvent(new Event('input'));
      fixture.detectChanges()
      await fixture.whenStable()
      //2.Check buffer input has changed
      expect(inputBuffer.value).toEqual('800')
    })
    it('reset buffer timer trigger', async () =>{
      const sliderBufferValue = fixture.debugElement.query(By.css('[data-testid="buffer-time-value"]')).nativeElement as HTMLInputElement;
      const inputBuffer = fixture.debugElement.query(By.css('[data-testid="buffer-time-value-manual"]')).nativeElement as HTMLInputElement;
      //1. Buffer time changed via slider elem
      sliderBufferValue.value = '800';
      sliderBufferValue.dispatchEvent(new Event('input'));
      fixture.detectChanges()
      await fixture.whenStable();
      //2. Reset stream buttom clicked
      jest.spyOn(component,'resetBufferTime')
      const resetButton = fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button_reset"]')).nativeElement as HTMLButtonElement;
      resetButton.click()
      fixture.detectChanges()
      await fixture.whenStable();
      expect(inputBuffer.value).toEqual('800')
      expect(component.resetBufferTime).toHaveBeenCalledTimes(1)
      expect(component.resetBufferTime).toHaveBeenCalledWith(inputBuffer)
    })
  });
  describe('Connection Status & Metrics', () => {
    it('connect button state connected or disconnected', async () =>{
      const connectButton =  fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
      //1.Connect to the stream
      mockConnectionState$.next('connected');
      fixture.detectChanges();
      await fixture.whenStable();
      let buttonState = connectButton.getAttribute('data-state')
      expect(connectButton.disabled).toBeFalsy()
      expect(buttonState).toEqual('connected');

      //2.Disconnect from the stream
      mockConnectionState$.next('disconnected');
      fixture.detectChanges();
      await fixture.whenStable();
      buttonState = connectButton.getAttribute('data-state')
      expect(connectButton.disabled).toBeFalsy()
      expect(buttonState).toEqual('disconnected')
    });
    it('latency data', async() => {
      let latencySpan =  fixture.debugElement.query(By.css('[data-testid="networkLatency"]'))?.nativeElement as HTMLButtonElement;
      expect(!!latencySpan).toBeFalsy();
      mockNetworkLatency$.next(180);
      fixture.detectChanges();
      await fixture.whenStable();
      latencySpan =  fixture.debugElement.query(By.css('[data-testid="networkLatency"]'))?.nativeElement as HTMLButtonElement;
      expect(latencySpan.innerHTML).toContain('180');
    })
  });
  describe('Stream filters management', () => {
    it('on click list of saved filters are shown', ()=> {
      expect(component.savedFilters()[1]).toEqual('stock1,stock2')
    });

    it('saved filters list is shown on a focus', async ()=> {
      const filterInput = fixture.debugElement.query(By.css('[data-testid="content__filter__input"]')).nativeElement as HTMLInputElement;
      filterInput.focus()
      const filtersRows = fixture.debugElement.queryAll(By.css('[data-testid="content__filter__option-item"]'))
      expect(filtersRows.length).toEqual(2);
      expect((filtersRows[1].nativeElement as HTMLElement).innerHTML.trim()).toEqual('stock1,stock2')
    });

    it('new unique filter => save button rendered', async ()=> {
      const filterInput = fixture.debugElement.query(By.css('[data-testid="content__filter__input"]')).nativeElement as HTMLInputElement;
      //1. Open auto-complete
      filterInput.focus()
      
      //2. Input value existed in auto-complete
      filterInput.value = 'stock1,stock2';
      filterInput.dispatchEvent(new Event('input'))
      fixture.detectChanges()
      await fixture.whenStable()
      let saveFilterButton =  fixture.debugElement.query(By.css('[data-testid="content__filter__button-save"]'))
      
      //3. Input new, unique filter value
      filterInput.value = 'stock1,stock2,stock3';
      filterInput.dispatchEvent(new Event('input'))
      fixture.detectChanges()
      await fixture.whenStable()
      saveFilterButton =  fixture.debugElement.query(By.css('[data-testid="content__filter__button-save"]'))
      expect(!!saveFilterButton).toBeTruthy();

      //4. Save new filter
      jest.spyOn(component,'saveFilter');
      (saveFilterButton.nativeElement as HTMLButtonElement).click()
      fixture.detectChanges()
      await fixture.whenStable()
      expect(component.savedFilters().length).toEqual(3)
      expect(component.saveFilter).toHaveBeenCalledWith('stock1,stock2,stock3')
    });

    it('delete filter => save button rendered', async ()=> {
      const filterInput = fixture.debugElement.query(By.css('[data-testid="content__filter__input"]')).nativeElement as HTMLInputElement;
      //1. Open auto-complete
      filterInput.focus()
      const deleteButton = fixture.debugElement.queryAll(By.css('[data-testid="delete-filter-button"]'))[1].nativeElement as HTMLButtonElement;

      //2. Delete button click
      jest.spyOn(component,'deleteFilter');
      deleteButton.click()
      fixture.detectChanges()
      await fixture.whenStable();
      expect(component.savedFilters().length).toEqual(1)
      expect(component.deleteFilter).toHaveBeenCalledWith(expect.any(Object),'stock1,stock2')
    });

    it('filter applied => stream data is filtered', async ()=> {
      component.getQuotesStream();
      const rates: IRate[] = [
        {time: new Date(), symbol: 'stock1', bid: 20, ask: 30, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'stock4', bid: 50, ask: 60, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'stock3', bid: 60, ask: 70, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'stock2', bid: 70, ask: 80, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      mockQuotesData$.next(rates)
      const filterInput = fixture.debugElement.query(By.css('[data-testid="content__filter__input"]')).nativeElement as HTMLInputElement;
      //1. Open auto-complete
      filterInput.focus()
      //2. Select second filter
      const filter = fixture.debugElement.queryAll(By.css('[data-testid="content__filter__mat-option-item"]'))[1].nativeElement as HTMLElement;
      filter.focus()
      filter.dispatchEvent(new Event('click'))
      fixture.detectChanges()
      await fixture.whenStable();
      expect(component.quotesFilterFC.value).toEqual('stock1,stock2')
      //3. Emit 4 rows and only 2 lands on ui
      mockQuotesData$.next(rates)
      fixture.detectChanges()
      await fixture.whenStable();
      const quotesRows = fixture.debugElement.queryAll(By.css('[data-testid="quote-data-row"]'))
      expect(quotesRows.length).toEqual(2)
    })
  });
  describe('Quotes Rendering & Virtual Scroll', () => {
    it('initial render of the the first data emit', (async() => {
      const connectButton = fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
      connectButton.click();
      fixture.detectChanges()
      await fixture.whenStable();

      const rates: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 20, ask: 30, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'INS2', bid: 50, ask: 60, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'INS3', bid: 60, ask: 70, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'INS4', bid: 70, ask: 80, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      mockQuotesData$.next(rates);
      await fixture.whenStable();
      fixture.detectChanges()

      const quotesRows = fixture.debugElement.queryAll(By.css('[data-testid="quote-data-row"]'))
      expect(quotesRows.length).toEqual(4)
    }) )
    it('virtual scroll: dom contains only rows in viewport + buffer', (async() => {
      const SCROLL_HEIGHT = 800;
      //1. Prepare virtual scroll styles 
      const virtualScrollDebug = fixture.debugElement.query(By.directive(CdkVirtualScrollViewport));
      const virtualScrollInstance = virtualScrollDebug.componentInstance as CdkVirtualScrollViewport;
      const virtualScrollElement = virtualScrollDebug.nativeElement as HTMLElement;
      virtualScrollElement.scrollTo = jest.fn()

      Object.defineProperty(virtualScrollElement,'clientHeight', {
        configurable:true, 
        value:SCROLL_HEIGHT
      });
      virtualScrollInstance.checkViewportSize();
      //2.Connect to stream
      const connectButton = fixture.debugElement.query(By.css('[data-testid="mng-panel__ui__button"]')).nativeElement as HTMLButtonElement;
      connectButton.click();

      fixture.detectChanges()
      await fixture.whenStable();
      //3.Simulate rate flow. 1000 rows => send them and update UI
      const rates: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 20, ask: 30, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      for (let i = 0; i < 1000; i++) {
        rates.push({...rates[i], bid:rates[i].bid +1, ask:rates[i].ask +1 })
      }

      mockQuotesData$.next(rates);
      fixture.detectChanges()
      await fixture.whenStable();

      //4.Calculate expeted row quantitiy based on virtual scroll attrs and style => verify data
      let scrollAttrs = virtualScrollDebug.attributes as any
      let expectedRows = (SCROLL_HEIGHT + (Number(scrollAttrs?.minBufferPx)||0) + (Number(scrollAttrs?.maxBufferPx)||0)) / (Number(scrollAttrs?.itemSize)||35) + 1

      const quotesRowsBeforeScroll = fixture.debugElement.queryAll(By.css('[data-testid="quote-data-row"]'))
      let firstBeforeScroll = (quotesRowsBeforeScroll[0].query(By.css('[data-testid="ask-price"]')).nativeElement as HTMLElement).innerHTML 
      
      //5.Trigger scroll behavior
      Object.defineProperty(virtualScrollElement,'scrollTop',{configurable:true,value:1000});
      virtualScrollElement.dispatchEvent(new Event('scroll'));
      await new Promise(resolve => requestAnimationFrame(resolve));
      fixture.detectChanges();
      await fixture.whenStable();
      
      const quotesRowsAfterScroll = fixture.debugElement.queryAll(By.css('[data-testid="quote-data-row"]'))
      let firstAfterScroll = (quotesRowsAfterScroll[0].query(By.css('[data-testid="ask-price"]')).nativeElement as HTMLElement).innerHTML 
      //6.Scroll happened. First row changed
      expect(firstAfterScroll !== firstBeforeScroll).toBeTruthy()
      //7.Dom contains only viewport + buffer. Render optimized
      expect(quotesRowsAfterScroll.length).toBeLessThanOrEqual(expectedRows)
    }) )
  })
})