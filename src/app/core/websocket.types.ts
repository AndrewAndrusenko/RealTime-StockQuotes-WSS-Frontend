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
export interface IServerCommand {
  cmd: 'start'|'stop'|'ping'
  timeToWork?: number; //time of emmiting values in milliseconds
  intervalToEmit?: number; //interval between emits in milliseconds
  market?: string;
}
export type TwsServerResponse = IRate[] | { message: string } | IServerCommand;