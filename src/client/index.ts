import amqp, { type Channel, type ConfirmChannel } from "amqplib";
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "./pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/consume.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

const rabbitConnString = 'amqp://guest:guest@localhost:5672/'

async function main() {
  console.log("Starting Peril client...");
  const conn = await amqp.connect(rabbitConnString);
  var channel = await conn.createConfirmChannel()

  let username = await clientWelcome()

  let state = new GameState(username)

  let subscribePause = await subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(state))

  const armyMoveKey = `${ArmyMovesPrefix}.${username}`;
  let subscribeMove = await subscribeJSON(conn, ExchangePerilTopic, armyMoveKey, ArmyMovesPrefix + '.*', SimpleQueueType.Transient, handlerMove(state, channel))

  let subscribeWar = await subscribeJSON(conn, ExchangePerilTopic, WarRecognitionsPrefix, `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(state, channel))

  let keep = true

  while (keep) {
    let input = await getInput()

    switch (input[0]) {
      case 'spawn':
        try {
          commandSpawn(state, input)
        } catch (e) {
          console.error((e as Error).message);
        }
        break
      case 'move':
        try {
          let move = commandMove(state, input)
          publishJSON(channel, ExchangePerilTopic, armyMoveKey, move)
          console.log("Move command received")
        } catch (e) {
          console.error((e as Error).message);
        }
        break
      case 'status':
        commandStatus(state)
        break
      case 'help':
        printClientHelp()
        break
      case 'quit':
        printQuit()
        keep = false
        break
      case 'spam':
        if (input.length !== 2 || !input[1]) {
          console.log("Usage: spam <number_of_messages>")
          break
        }

        let numMessages = parseInt(input[1])
        if (isNaN(numMessages) || numMessages <= 0) {
          console.log("Please provide a valid positive number for the number of messages.")
          break
        }

        for (let i = 0; i < numMessages; i++) {
          let badMsg = getMaliciousLog()
          PublishGameLog(channel, username, badMsg)
        }
        break;

      default:
        console.log("Command not clear")
    }
  }

  console.log("Closing")
  await conn.close();
  console.log("Closed")
}


main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

export function PublishGameLog(cn: ConfirmChannel, username: string, message: string) {
  const gl: GameLog = {
    currentTime: new Date(),
    message: message,
    username: username
  }

  publishMsgPack(cn, ExchangePerilTopic, `${GameLogSlug}.${username}`, gl)
}