import pika
import json
from pprint import pprint
from .publisher import FirmsPublisher
import traceback
from . import export

@export
class FirmsConsumer:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None
    
    def __enter__(self):
        self.connection = self._create_connection ()
        return self
    
    def __exit__(self, *args):
        print ("connection closed")
        self.channel.stop_consuming ()
        self.connection.close ()
    
    def consume(self, message_received_callback):
        self.message_received_callback = message_received_callback
        
        self.channel = self.connection.channel ()

        # self.create_exchange(channel)
        # self.create_queue(channel)

        # channel.queue_bind(queue=self.config['queueName'],
        #                   exchange=self.config['exchangeName'],
        #                   routing_key=self.config['routingKey'])
        
        self.channel.basic_consume (self._consume_message, queue=self.config['queueName'])
        self.channel.start_consuming ()
    
    def create_exchange(self, channel):
        exchange_options = self.config['exchangeOptions']
        self.channel.exchange_declare (exchange=self.config['exchangeName'],
                                       exchange_type=self.config['exchangeType'],
                                       passive=exchange_options['passive'],
                                       durable=exchange_options['durable'],
                                       auto_delete=exchange_options['autoDelete'],
                                       internal=exchange_options['internal'])
    
    def create_queue(self, channel):
        queue_options = self.config['queueOptions']
        self.channel.queue_declare (queue=self.config['queueName'],
                                    passive=queue_options['passive'],
                                    durable=queue_options['durable'],
                                    exclusive=queue_options['exclusive'],
                                    auto_delete=queue_options['autoDelete'])
    
    def _create_connection(self):
        credentials = pika.PlainCredentials (self.config['userName'], self.config['password'])
        parameters = pika.ConnectionParameters (self.config['host'], self.config['port'],
                                                self.config['virtualHost'], credentials, ssl=False)
        return pika.BlockingConnection (parameters)
    
    def _consume_message(self, channel, method, properties, body):
        #print (method.consumer_tag)
       # print (properties.headers)
        #print(body)
        properties=properties.headers
        body=json.loads(body)
        #properties.update(body)
        try:
            res = self.message_received_callback (properties,body)
        except Exception as e:
            raise
            print ("Handler received exception {} ".format (e))
            res = None
        if res:
            self.channel.basic_ack (delivery_tag=method.delivery_tag)
        else:
            self.channel.basic_nack (delivery_tag=method.delivery_tag,requeue=True)

