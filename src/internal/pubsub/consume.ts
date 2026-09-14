import { declareAndBind, type SimpleQueueType } from "../../client/pubsub/consume.js";
import amqp from "amqplib";

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => void,
): Promise<void> {
    const res = await declareAndBind(conn, exchange, queueName, key, queueType)
    const queue = res[1]
    const channel = res[0]

    const callback = (message: amqp.ConsumeMessage | null) => {
        if (!message) return

        const buffer = message.content.toString()

        const parsedMessage = JSON.parse(buffer)

        handler(parsedMessage)

        channel.ack(message)
    }

    channel.consume(queue.queue, callback)
}