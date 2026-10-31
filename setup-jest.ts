import { setupZoneTestEnv } from 'jest-preset-angular/setup-env/zone';
setupZoneTestEnv();
const originalConsoleError = console.error;
console.error = (...args) => {
  if (typeof args[0] === 'object' && args[0].message?.includes('Could not parse CSS stylesheet')) {
    return;
  }
  originalConsoleError(...args);
};