import pika
import os
import time
import json

JOBS_EXCHANGE = "jobs_exchange"
RESULTS_EXCHANGE = "results_exchange"

# Pobieranie hosta RabbitMQ z ENV
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")

def on_job(ch, method, properties, body):
    payload = body.decode("utf-8", errors="replace")
    routing_key = method.routing_key

    print(f"[x] Received job via {routing_key}: {payload}")

    result = f"Processed by worker: {payload.upper()}"

    worker_id = routing_key.split(".")[1]
    result_key = f"result.{worker_id}"

    props = pika.BasicProperties(
        content_type="text/plain",
        content_encoding="utf-8"
    )

    ch.basic_publish(
        exchange=RESULTS_EXCHANGE,
        routing_key=result_key,
        body=result.encode("utf-8"),
        properties=props
    )

    print(f"[x] Sent result: {result}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST)
            )
            break
        except pika.exceptions.AMQPConnectionError:
            print(f"[!] Cannot connect to RabbitMQ at {RABBIT_HOST}, retrying in 5s...")
            time.sleep(5)

    channel = connection.channel()

    channel.exchange_declare(exchange=JOBS_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=RESULTS_EXCHANGE, exchange_type="topic", durable=True)

    channel.queue_declare(queue="jobs_queue", durable=True)
    channel.queue_bind(
        exchange=JOBS_EXCHANGE,
        queue="jobs_queue",
        routing_key="job.*"
    )

    print(" [*] Python worker waiting for jobs...")
    channel.basic_consume(queue="jobs_queue", on_message_callback=on_job)

    channel.start_consuming()


if __name__ == "__main__":
    main()
