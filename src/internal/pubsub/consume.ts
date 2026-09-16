import { declareAndBind, type SimpleQueueType } from "../../client/pubsub/consume.js";
import amqp from "amqplib";
import { decode } from "@msgpack/msgpack";

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard
}

export async function subscribeJSON<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const res = await declareAndBind(conn, exchange, queueName, key, queueType)
    const queue = res[1]
    const channel = res[0]
    await channel.prefetch(10);

    const callback = async (message: amqp.ConsumeMessage | null) => {
        if (!message) return

        const buffer = message.content.toString()

        const parsedMessage = JSON.parse(buffer)

        const res = await handler(parsedMessage)

        if (res === AckType.Ack) {
            console.log("Acking")
            channel.ack(message)
        } else if (res === AckType.NackRequeue) {
            console.log("Nacking Requeue")
            channel.nack(message, false, true)
        } else {
            console.log("Nacking Discard")
            channel.nack(message, false, false)
        }
    }

    await channel.consume(queue.queue, callback)
}

export async function subscribeMsgPack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
    const res = await declareAndBind(conn, exchange, queueName, key, queueType)

    const queue = res[1]
    const channel = res[0]
    await channel.prefetch(10);

    const callback = async (message: amqp.ConsumeMessage | null) => {
        if (!message) return

        const buffer = message.content

        const parsedMessage = decode(buffer) as T

        const res = await handler(parsedMessage)

        if (res === AckType.Ack) {
            console.log("Acking")
            channel.ack(message)
        } else if (res === AckType.NackRequeue) {
            console.log("Nacking Requeue")
            channel.nack(message, false, true)
        } else {
            console.log("Nacking Discard")
            channel.nack(message, false, false)
        }
    }

    await channel.consume(queue.queue, callback)
}
