export interface IRate {
  //Intreface for received quotes from server
  time: Date; //quote rate
  symbol: string; // instrument code
  bid: number; // price to buy
  ask: number; // price to sell
  open: number; // price to sell
  chgBid: number;
  chgAsk: number;
  chgUpDown: number;
}
export type TwsServerResponse = IRate[] | { message: string } | { cmd: 'close' | 'ping' };