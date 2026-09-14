import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const json = JSON.stringify(value);
    const body = Buffer.from(json);

    return new Promise((resolve, reject) => {
        ch.publish(
            exchange,
            routingKey,
            body,
            { contentType: "application/json" },
            (err) => {
                if (err) reject(err);
                else resolve();
            },
        );
    });
}
