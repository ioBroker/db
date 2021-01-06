type LogSeverity = "silly" | "debug" | "info" | "warn" | "error";
type Logger = {
  [severity in LogSeverity]: (msg: string) => void;
};

interface RedisHandlerOptions {
  log?: Logger;
  logScope?: string;
  handleAsBuffers?: boolean;
  enhancedLogging?: boolean;
}

interface WriteQueueEntry {
  id: number;
  data: Buffer | false;
}

type RespArray = (Buffer | null | number | string | RespArray)[];
type EncodedRespArray = (Buffer | EncodedRespArray)[];

type Socket = import("net").Socket;