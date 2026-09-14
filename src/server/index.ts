import process from "process";
import { once } from "events";
import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../client/pubsub/consume.js";

const rabbitConnString = 'amqp://guest:guest@localhost:5672/'

async function main() {
  console.log("Starting Peril server...");
  const conn = await amqp.connect(rabbitConnString);

  var channel = await conn.createConfirmChannel()
  console.log("Connection's good! You can check dashboard on http://localhost:15672/#/")

  let res = await declareAndBind(conn, ExchangePerilTopic, GameLogSlug, "game_logs.*", SimpleQueueType.Durable)

  printServerHelp()

  await once(process, "SIGINT");


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

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
