### Real-Time Stream Processing Dashboard

A high-performance, **Zoneless Angular 20** application designed for real-time financial data streaming, processing, and viewport rendering optimization. This system serves as the client layer within a broader **custom microservice ecosystem**, connecting seamlessly to independent, custom-built Auth and high-frequency WebSocket streaming servers. 

The client ingests continuous data points via **WebSockets** at a rapid **50ms stream rate**, utilizing sophisticated **RxJS buffering**, **Angular Signals**, and **Virtual Scrolling** to maintain a butter-smooth 60 FPS UI under extreme data loads. 

### 🚀 Key Features

* **⚡ Angular 20 Zoneless & Signals**: Completely free from Zone.js overhead. Uses native Change Detection driven entirely by Angular Signals and RxJS streams for granular, high-performance UI updates.
* **📜 Smooth Virtual Scrolling**: DOM rendering is optimized by recycling views inside the viewport via ScrollingModule. Even with thousands of active tickers, the browser only renders elements currently visible on the screen, drastically reducing rendering overhead.
* **⏱️ 50ms Real-Time Ingestion**: Built to handle high-frequency data streams with a guaranteed microsecond processing pipeline.
* **📊 Smart RxJS Buffering**: Prevents UI freezing by buffering incoming WebSocket emissions and releasing them based on dynamic runtime refresh rates.
* **🔌 Resilient WebSockets**: Features automatic connection retry, backoff strategy, heartbeat ping/pong, and stream repetition on unexpected dropouts.
* **🔒 Secure JWT Auto-Refresh**: Seamlessly intercepts HTTP/WS requests to refresh expired tokens in the background, preventing session interruption.

### 🏗️ Architecture & Optimizations

### 1. Zoneless, Signals & Viewport Optimization

Since the application runs in **Zoneless mode** (completely free from Zone.js overhead), it avoids triggering heavy global change detection cycles on every 50ms WebSocket tick. Instead, UI updates are strictly reactive, driven by a combination of Angular Signals, RxJS streams, and DOM recycling. 

* **Granular Change Detection Control**: The component utilizes ChangeDetectionStrategy.OnPush and natively hooks into the Zoneless engine via the async pipe and dynamic Signals (bufferdTime, savedFilters).
* **RxJS-to-Signal Bridge**: User input from FormControl is seamlessly bridged into the reactive system using toSignal(). This enables lean, declarative tracking of state mutations (like calculating if a newly entered ticker filter is unique) without raw subscription overhead.
* **Viewport View Recycling**: ScrollingModule (Virtual Scrolling) ensures that regardless of the incoming stream volume or total table size, the DOM footprint remains constant. The browser only templates and tracks the nodes currently inside the active viewport.
* **Micro-Optimized DOM Updates**: To prevent the virtual scroll container from completely rebuilding DOM nodes when an updated batch arrives, a precise compound tracking function is implemented. It tracks elements by combining the unique asset symbol and its latest timestamp, allowing Angular to mutate only the text nodes that actually changed.

### Code Insight: Reactive Filters & Micro-Tracking

```// Bridging FormControls into the Signal graph for pure reactive states
private quotesFilterInputSignal = toSignal(
  this.quotesFilterFC.valueChanges.pipe(
    startWith(this.quotesFilterFC.value), 
    debounceTime(200)
  ),
  { initialValue: '' },
);

// Deriving pure calculated state from the UI input signal
public isNewFilterUnique = computed(() => {
  const rawValue = this.quotesFilterInputSignal();
  if (!rawValue) return false;
  
  const formattedValue = rawValue
    .split(',')
    .flatMap((el) => (el.trim() ? [el.trim().toUpperCase()] : []))
    .join(',');
    
  return formattedValue.length > 0 && !this.savedFilters().includes(formattedValue);
});

// Precision DOM node recycling for high-frequency 50ms streaming
trackQuotes(index: number, item: IRate): string {
  return item.symbol + item.time;
}
```

### 2. Business Logic Orchestration & RxJS Buffering Strategy

Data stream management is separated from the network transport layer into a dedicated QuotesDataService. It orchestrates high-frequency data ingestion, state flattening, and stream health monitoring using advanced RxJS reactive patterns. 

* **Dynamic Reactively Switched Buffering**: The application allows users to dynamically adjust the UI refresh rate at runtime. By piping a BehaviorSubject of the buffer time into a switchMap, the downstream buffer window updates instantly without dropping or resetting the underlying WebSocket connection.
* **High-Volume Flattening & Deduplication**: To maximize rendering performance, rapid raw data packets arriving every 50ms are accumulated inside bufferTime. The buffered batches are then flattened, and historical keys are overwritten inside a local stateful Map, ensuring only the latest unique financial rates are pushed to the UI.
* **Reactive Watchdog Pattern**: A silent health-check stream monitors data frequency. Every emission resets an internal RxJS timer. If the server stops producing data points for longer than the configured threshold (e.g., 5500ms), the watchdog instantly triggers a UI notification and shifts the stream status, even if the TCP connection remains technically alive.

### Code Insight: Buffer Toggling & Watchdog Execution
```
// 1. Dynamic Buffer Switching & State Flattening
private createQuoteStream(): void {
  this._quotesBufferTime$
    .pipe(
      switchMap((bufferPeriod) => {
        return this.wssCore.serverStream$.pipe(
          bufferTime(bufferPeriod),
          filter((buffer) => buffer.length > 0),
          map((bufferArrays) => {
            // Flatten rapid batches and deduplicate by unique symbol
            const flatBuffer = bufferArrays.flat();
            flatBuffer.forEach((rate) => this.quotesDataMap.set(rate.symbol, rate));
            return Array.from(this.quotesDataMap.values());
          }),
        );
      }),
      takeUntil(this.destroyQuoteStreams$),
    )
    .subscribe((quotesArray) => this._quotesData$.next(quotesArray));
}

// 2. Stream Inactivity Watchdog (Heartbeat Failure Detection)
private createWatchDogStream(): void {
  this.wssCore.serverStream$
    .pipe(
      switchMap(() =>
        timer(this.CONFIG.STREAM_TIMEOUT).pipe(
          map(() => false), // Stream is considered dead due to inactivity timeout
          startWith(true),  // Stream is healthy on fresh data point
        ),
      ),
      distinctUntilChanged(),
      takeUntil(this.destroyQuoteStreams$),
    )
    .subscribe((isActive) => {
      this.wssCore.streamActive !== isActive ? this.wssCore.setStreamActive(isActive) : null;
      if (!isActive) {
        this.snacksService.openSnack('Warning: No new data received...', 'Okay', 'error-snackBar');
      }
    });
}
```

### 3. Connection Resilience & WebSocket Lifecycle

The network layer is managed by a standalone WebSocketService built using rxjs/webSocket. Instead of basic reconnect loops, it implements production-grade resilience strategies to communicate with our standalone backend nodes: 

* **⚡ Exponential Backoff with Jitter**: Reconnection delays grow exponentially (Math.pow(2, attempt)) combined with a random jitter factor (0.7 to 1.3) to prevent thundering herd problems on the server.
* **🔒 Inline JWT Token Refresh**: If a connection drops due to token expiration, the reconnect pipeline intercepts the failure, triggers a silent background refresh flow via JwtHandlerService, and delays the reconnection attempt until a new token is obtained.
* **⏱️ EMA Network Latency Tracking**: Keeps the connection alive using a strict Ping/Pong heartbeat interval. To prevent erratic metric jumps in the UI, network latency is smoothed out in real-time using an **Exponential Moving Average (EMA)** algorithm.

### Code Insight: The Resilient Reconnect Loop
```
private reconnecting<T>(): MonoTypeOperatorFunction<T> {
  let retryAttemptNum = 0;
  
  const retryDelay = () => {
    retryAttemptNum++;
    const errorCode = this.closeConnectionErrorCode || 503;
    const error = SERVER_ERRORS.get(errorCode)!;

    // Max attempts reached or terminal server error
    if (error?.retryConnection === false || this.CONFIG.RETRY_ATTEMPTS + 1 === retryAttemptNum) {
      this.disconnectServer(error, errorCode);
      return EMPTY;
    }

    // Intercept connection loss due to expired JWT
    if (error?.authErr) {
      return this.jwtService.refreshTokenAndWait$().pipe(
        switchMap((tokenRefreshed) => {
          if (tokenRefreshed) {
            this._connectionState$.next('Reconnecting');
            return of(1); // Resume stream connection loop
          }
          this.disconnectServer(error, errorCode);
          return EMPTY;
        })
      );
    }

    // Standard reconnection with Exponential Backoff & Jitter
    if (error?.retryConnection) {
      const exponentDelay = Math.pow(2, retryAttemptNum) * this.CONFIG.RETRY_INTERVAL;
      const jitterRate = 0.7 + Math.random() * 0.6; // 0.7 - 1.3 jitter
      const finalDelay = Math.round(exponentDelay * jitterRate);
      
      this._connectionState$.next('Reconnecting');
      return of(1).pipe(delay(finalDelay));
    }

    this.disconnectServer(error, errorCode);
    return EMPTY;
  };

  return (source$) =>
    source$.pipe(
      repeat({ delay: retryDelay }), // Reconnect when stream completes cleanly
      retry({ delay: retryDelay })   // Reconnect when stream throws an error
    );
}
```

### ⚙️ Runtime Configuration

The application uses a **runtime configuration pattern** instead of build-time environment variables. This allows swapping environment targets, backend endpoints, and stream thresholds dynamically without rebuilding the Angular application artifact. 

The configuration is loaded at startup from public/env.config.prod.json and points directly to our active development mesh services: 
```
{
  "production": false,
  "TEST_WS_ENDPOINT": "wss://ppklrx85-3003.euw.devtunnels.ms",
  "AUTH_SERVER_ENDPOINT": "https://ppklrx85-3010.euw.devtunnels.ms/users/",
  "AUTH_SERVER_UI_ADDRESS": "https://ppklrx85-5001.euw.devtunnels.ms/apps/ssngrx/register/logout/:logout",
  "RETRY_INTERVAL": 1000,
  "RETRY_ATTEMPTS": 2,
  "STREAM_TIMEOUT": 5500,
  "PING_HEARTBEAT_INTERVAL": 15000,
  "SUCCESS_TIME_OUT": 2000,
  "BUFFER_TIME_DEFAULT": 500,
  "MIN_BUFFER_TIME": 50
}
```
### Key Parameter Breakdown

* **⚡ Stream Tuning**: 

  * BUFFER_TIME_DEFAULT & MIN_BUFFER_TIME: Controls the RxJS bufferTime window for bundling rapid high-frequency WebSocket updates before triggering UI Change Detection.
  * STREAM_TIMEOUT: The maximum allowed inactivity period before the stream is considered dead and triggers a fallback.
* **🔌 Resilient Connection**: 

  * RETRY_INTERVAL & RETRY_ATTEMPTS: Defines the backoff interval and maximum reconnection attempts for the WebSocket stream.
  * PING_HEARTBEAT_INTERVAL: Keeps the connection alive and instantly detects silent drops.

### 🛠️ Getting Started

### Prerequisites

* **Node.js**: v22+
* **Angular CLI**: v20.0.0+

### Available Scripts

* **npm run start**: Runs the app in development mode at http://localhost:4200/ with hot-reloading.
* **npm run build**: Compiles the application into production-ready static assets in the dist/ directory, optimized for maximum performance.

### 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
