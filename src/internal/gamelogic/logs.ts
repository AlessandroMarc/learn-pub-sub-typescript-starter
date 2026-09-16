import { appendFile } from "fs/promises";

export interface GameLog {
  currentTime: Date;
  message: string;
  username: string;
}

const logsFile = "game.log";
const writeToDiskSleep = 1000;

function block(ms: number) {
  const end = Date.now() + ms;
  while (Date.now() < end) { }
}

export async function writeLog(gameLog: GameLog): Promise<void> {
  console.log("received game log...");
  block(writeToDiskSleep);

  try {

    const date = new Date(gameLog.currentTime);
    const timestamp = date.toISOString();
    const logEntry = `${timestamp} ${gameLog.username}: ${gameLog.message}\n`;
    await appendFile(logsFile, logEntry, { flag: "a" });
  } catch (err) {
    console.error(`could not write to logs file: ${err}`);
  }
}
