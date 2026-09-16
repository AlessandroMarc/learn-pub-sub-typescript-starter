import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export async function declareAndBind(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const channel = await conn.createChannel();

    const isDurable = queueType === SimpleQueueType.Durable;
    const isTransient = queueType === SimpleQueueType.Transient;

    const queue = await channel.assertQueue(queueName, {
        durable: isDurable,
        autoDelete: isTransient,
        exclusive: isTransient,
        arguments: {
            "x-dead-letter-exchange": 'peril_dlx'
        }
    });

    await channel.bindQueue(queue.queue, exchange, key);

    return [channel, queue];
}
