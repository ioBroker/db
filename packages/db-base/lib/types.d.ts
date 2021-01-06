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

interface BackupOptions {
	disabled: boolean,
	files: number;
	hours: number;
	period: number;
	path: string;
}

interface InMemoryFileDBOptions {
  change?: (id, state) => void;
  connected?: (nameOfServer) => string;
  logger?: Logger;
  connection?: {
    /** relative path */
    dataDir: string;
  };
  fileDB: {
	  fileName: string;
	  backupDirName?: string;
  };
  auth?: null | undefined;
  secure?: boolean;
  certificates?: any; // TODO: as required by createServer
  port?: number;
  host?: string;
  namespace?: string;
  backup?: BackupOptions;
}
