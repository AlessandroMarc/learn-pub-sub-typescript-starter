import process from "process";
import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../client/pubsub/consume.js";
import { AckType, subscribeMsgPack } from "../internal/pubsub/consume.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";

const rabbitConnString = 'amqp://guest:guest@localhost:5672/'

async function main() {
  console.log("Starting Peril server...");
  const conn = await amqp.connect(rabbitConnString);

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  var channel = await conn.createConfirmChannel()
  console.log("Connection's good! You can check dashboard on http://localhost:15672/#/")

  let res = await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, "game_logs.*", SimpleQueueType.Durable, async (data: GameLog) => {
    writeLog(data)
    console.log("> ");
    return AckType.Ack;
  })

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }

  printServerHelp()


  let keep = true
  while (keep) {
    let input = await getInput()

    switch (input[0]) {
      case "pause":
        console.log('Pausing')
        publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true });
        break
      case "resume":
        console.log("Resuming")
        publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: false });
        break
      case "quit":
        console.log('Quitting')
        keep = false
        break
      default:
        console.log("Not clear")
    }
  }

  console.log("Shutting down Peril server...");
  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
