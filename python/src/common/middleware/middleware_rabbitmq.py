import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

    def start_consuming(self, on_message_callback):
        def make_callback(_ch, method, _properties, body):
            def ack():
                self.channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                self.channel.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=make_callback
            )
            self.channel.start_consuming()

        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.connection.close()
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type="direct")

    def start_consuming(self, on_message_callback):
        def make_callback(_ch, method, _properties, body):
            def ack():
                self.channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                self.channel.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )

            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=make_callback
            )
            self.channel.start_consuming()

        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def send(self, message):
        try:
            for routing_keys in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_keys, body=message
                )
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.connection.close()
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.StreamLostError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise MessageMiddlewareCloseError(str(e))
