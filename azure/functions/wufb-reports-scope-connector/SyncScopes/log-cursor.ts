export default class LogCursor {
  timeGenerated: Date;
  secondaryKey?: string;

  constructor(timeGenerated: Date, secondaryKey?: string) {
    this.timeGenerated = timeGenerated;
    this.secondaryKey = secondaryKey;
  }
}