import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { QuotesDataService } from './quotes-data.service';
import { WebSocketService } from '../../../core/websocket.service';
import { BehaviorSubject, Subject } from 'rxjs';
import { IRate, TConnectionStatus } from '../../../core/websocket.types';
import { SnacksService } from '../../../shared/snacks.service';
import { ConfigService } from '../../../core/config.service';
describe('QuotesDataService', () => {
  let service: QuotesDataService;
  let mockWsService: jest.Mocked<WebSocketService>;
  let mockSnackService: jest.Mocked<SnacksService>;
  let mockConfigService: jest.Mocked<ConfigService>;
  let wsServerStream$: Subject<IRate[]>;
  let connectionState$: BehaviorSubject<TConnectionStatus>;
  let serverMessageStream$: Subject<{message: string}>;
  let streamActive$:BehaviorSubject<boolean>
  beforeEach(() => {
    wsServerStream$ = new Subject<IRate[]>();
    connectionState$ = new BehaviorSubject<TConnectionStatus>('disconnected');
    streamActive$ = new BehaviorSubject<boolean>(false);
    serverMessageStream$ = new Subject<{message: string}>();
    mockWsService = {
      serverStream$: wsServerStream$,
      connectionState$: connectionState$,
      serverMessageStream$: serverMessageStream$,
      get connectionState() {
        return connectionState$.value
      },
      get streamActive () {
        return streamActive$.value
      },
      setStreamActive:jest.fn((isActive)=> streamActive$.next(isActive)),
      connectServer: jest.fn(),
      disconnectServer: jest.fn(),
    } as any;
    mockSnackService = {
      openSnack:jest.fn(),
      openSnackObserve:jest.fn()
    } as any;
    mockConfigService = {
      ENV_CONFIG: {"STREAM_TIMEOUT":5500},
    } as any;
    TestBed.configureTestingModule({
      providers: [QuotesDataService, 
        { provide: WebSocketService, useValue: mockWsService },
        { provide: SnacksService, useValue: mockSnackService },
        { provide: ConfigService, useValue: mockConfigService },
      ],
    });

    service = TestBed.inject(QuotesDataService);
  });

  afterEach(() => {
    wsServerStream$.complete();
    service.disconnectFromQuoteStream();
  });

  describe('Connection and Lifecycle',() => {
    it('Connection initialization:params are correct',() => {
      const bufferPeriod = 200;
      const wssEndpoint = "wss://test.test"
      service.connectToQuoteStream(bufferPeriod,wssEndpoint)
      expect(mockWsService.connectServer).toHaveBeenCalledWith(wssEndpoint)
      let bufferTimeSubjectValue:number|undefined
      service.quotesBufferTime$.subscribe(value => bufferTimeSubjectValue = value)
      expect(bufferTimeSubjectValue).toEqual(200);
    });
    it('Prevent connection duplicating',() => {
      // 1st call to create stream and connect to wss
      service.connectToQuoteStream();
      expect(mockWsService.connectServer).toHaveBeenCalledTimes(1);
      let bufferTimeSubjectValue:number|undefined
      service.quotesBufferTime$.subscribe(value => bufferTimeSubjectValue = value)
      expect(bufferTimeSubjectValue).toEqual(500);
      connectionState$.next('connected')
      mockWsService.connectServer.mockClear();

      //2d call which must be blocked
      service.connectToQuoteStream();
      expect(mockWsService.connectServer).toHaveBeenCalledTimes(0)
    })
    it('Manual disconnect from quote stream',fakeAsync (() => {
      //1. Call to create stream and connect to wss
      service.connectToQuoteStream();
      expect(mockWsService.connectServer).toHaveBeenCalledTimes(1);
      connectionState$.next('connected');

      //2. Push quote to a stream and fastforward time
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data)
      const rates1: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 }
      ];
      wsServerStream$.next(rates1);
      tick(550);
      expect(quotesData.length).toEqual(1);

      //3. Disconnect from the stream
      service.disconnectFromQuoteStream()
      expect(wsServerStream$.observed).toBeFalsy()
      expect(mockWsService.disconnectServer).toHaveBeenCalledTimes(1)
      expect(quotesData).toEqual([]);
    }))
  });

  describe('Quotes Stream',() => {
    it('Accumulation and Buffering: only last unique quotes emitted', fakeAsync(() => {
      
      //1. Create quote stream
      service.connectToQuoteStream()
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data)
  
      const time = new Date();
      const rates1: IRate[] = [
        {time: time, symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS2', bid: 200, ask: 250, open: 100, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 50, ask: 80, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS4', bid: 300, ask: 400, open: 250, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates2: IRate[] = [
        {time: time, symbol: 'INS2', bid: 300, ask: 350, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 150, ask: 180, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates3: IRate[] = [
        {time: time, symbol: 'INS1', bid: 500, ask: 600, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS2', bid: 30, ask: 50, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 750, ask: 780, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS5', bid: 750, ask: 780, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates4: IRate[] = [
        {time: time, symbol: 'INS4', bid: 11, ask: 22, open: 1, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      //2. First 500 buffer time window
      wsServerStream$.next(rates1)
      wsServerStream$.next(rates2)
      wsServerStream$.next(rates3)
  
      tick(550);
      expect(quotesData.length).toEqual(5);
      //3. Second 500 buffer time window started but not finished      
      wsServerStream$.next(rates4);
      let INS1 = quotesData.find(el => el.symbol === 'INS1')
      expect(INS1?.ask).toEqual(600);

      tick(550)
      //4. Second buffer time window closed
      expect(quotesData.length).toEqual(5);
      let INS4 = quotesData.find(el => el.symbol === 'INS4')
      expect(INS4?.ask).toEqual(22);

     }));
    it('BufferTime change - accumulation and buffering with new period ', fakeAsync(() => {
      
      //1. Create quote stream
      service.connectToQuoteStream()
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data)
  
      const time = new Date();
      const rates1: IRate[] = [
        {time: time, symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS2', bid: 200, ask: 250, open: 100, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 50, ask: 80, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS4', bid: 300, ask: 400, open: 250, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates2: IRate[] = [
        {time: time, symbol: 'INS2', bid: 300, ask: 350, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 150, ask: 180, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates3: IRate[] = [
        {time: time, symbol: 'INS4', bid: 11, ask: 22, open: 1, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates4: IRate[] = [
        {time: time, symbol: 'INS1', bid: 500, ask: 600, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS2', bid: 30, ask: 50, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS3', bid: 750, ask: 780, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: time, symbol: 'INS5', bid: 750, ask: 780, open: 50, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      
      //2. First 500 buffer time window
      wsServerStream$.next(rates1)
      wsServerStream$.next(rates2)
      tick(550);
      expect(quotesData.length).toEqual(4);
      //3. First 500 buffer time window closed
      wsServerStream$.next(rates3);
      //4. Buffer time changed
      service.resetBufferTime(200);
      wsServerStream$.next(rates4)
      tick(210);
      expect(quotesData.length).toEqual(5);
      //emmit after 1st buffer window closed and buffer changed to 200 not get lost 
      let INS4 = quotesData.find(el => el.symbol === 'INS4')
      expect(INS4?.ask).toEqual(22);
      //ins3 has last actual quote from the rates4
      let INS3 = quotesData.find(el => el.symbol === 'INS3')
      expect(INS3?.ask).toEqual(780);

     }));
  });

  describe('WatchDog',() => {
    it('Timeout is hit: big gap in data flow', fakeAsync(() => {
      //1. Create quote stream
      service.connectToQuoteStream()
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data)
      const rates1: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      //2. First 500 buffer time window
      wsServerStream$.next(rates1)
      tick(550);
      expect(quotesData.length).toEqual(1);
      //3. No quotes window   
      mockSnackService.openSnack.mockClear();
      tick(4000) 
      expect(mockWsService.streamActive).toBeTruthy()
      tick(2000) 
      expect(mockWsService.streamActive).toBeFalsy()
      expect(mockSnackService.openSnack).toHaveBeenCalledWith(expect.any(String), expect.any(String),'error-snackBar')
     }));
    it('Timeout is hit and then reset: gap in data flow and then stream is restored', fakeAsync(() => {
      //1. Create quote stream
      service.connectToQuoteStream()
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data);
      const rates1: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      const rates2: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 20, ask: 30, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
        {time: new Date(), symbol: 'INS2', bid: 50, ask: 60, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      //2. First 500 buffer time window
      wsServerStream$.next(rates1);
      tick(550);
      //3. No quotes window   
      tick(6000);
      expect(mockSnackService.openSnack).toHaveBeenCalledWith(expect.any(String),expect.any(String),'error-snackBar');
      mockSnackService.openSnack.mockClear();
      //4. Stream is up
      wsServerStream$.next(rates2);
      tick(0);
      expect(mockSnackService.openSnack).toHaveBeenCalledWith(expect.any(String),expect.any(String),'success-snackBar');
      tick(550);
      expect(quotesData.length).toBe(2);
     }));
  });

  describe('Auto-Reset and Cleanup',() => {
    it('WSS disconnected. Cleanup is executed', fakeAsync(() => {
      //1. Create quote stream. Connected
      service.connectToQuoteStream()
      connectionState$.next('connected')
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data);
      const rates1: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      tick(10);
      expect(mockWsService.connectionState).toEqual('connected')
      wsServerStream$.next(rates1)
      tick(500);
      expect(quotesData.length).toEqual(1)
      //2.Server disconnected
      connectionState$.next('disconnected')
      tick(0)
      expect(quotesData.length).toEqual(0)
    }))
    it('New Stream Started', fakeAsync(() => {
      //1. Create quote stream. Connected
      service.connectToQuoteStream()
      connectionState$.next('connected')
      let quotesData:IRate[] = [];
      service.quotesData$.subscribe(data => quotesData = data);
      const rates1: IRate[] = [
        {time: new Date(), symbol: 'INS1', bid: 100, ask: 200, open: 80, chgBid: 0.00, chgAsk: 1,  chgUpDown: 1 },
      ];
      tick(10);
      expect(mockWsService.connectionState).toEqual('connected')
      wsServerStream$.next(rates1)
      tick(500);
      expect(quotesData.length).toEqual(1)
      //2.New stream launched
      serverMessageStream$.next({message:'stream_started'})
      tick(0)
      expect(quotesData.length).toEqual(0)
    }))
  })
});