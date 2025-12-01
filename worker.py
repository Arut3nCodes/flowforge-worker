import pika
import json

JOBS_EXCHANGE = "jobs_exchange"
RESULTS_EXCHANGE = "results_exchange"

def on_job(ch, method, properties, body):
    routing_key = method.routing_key
    payload = body.decode()

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
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
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
