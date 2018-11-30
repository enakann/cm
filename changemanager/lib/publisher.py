import pika
import traceback
import sys
import json
import uuid
from . import export

@export
class FirmsPublisher:
    def __init__(self, config):
        self.config = config
        self.connection=None
        self.channel=None
        self.msg_props=None
   
    def __enter__(self):
        self.connect()
        return self
   
    def __exit__(self,*args):
        self.close()
    
    def _create_connection(self):
        self.credentials = pika.PlainCredentials(self.config['userName'], self.config['password'])
        self.parameters = pika.ConnectionParameters(self.config['host'], self.config['port'],
                                               self.config['virtualHost'], self.credentials, ssl=False)
        return pika.BlockingConnection(self.parameters)



    def set_property(self,msg):
        #self.msg_props = pika.BasicProperties()
        self.header=msg.pop("headers")
        self.msg_props=pika.BasicProperties(
                          headers=self.header # Add a key/value header
                      )
        return self.msg_props



    def connect(self):
        try:
            self.connection = self._create_connection()
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.config['exchangeName'],
                                         passive=True)
            self.channel.add_on_return_callback(self.confirm_handler)
            self.channel.add_on_cancel_callback(self.confirm_handler)
            self.channel.confirm_delivery()
            return self
        except Exception as e:
            print(repr(e))
            traceback.print_exc()
            raise e

   
    def confirm_handler(self,*frame):
        print("handler is confirmed")
        print(frame)
        if type(frame.method) == spec.Confirm.SelectOk:
              print("Channel in 'confirm' mode.")
        elif type(frame.method) == spec.Basic.Nack:
              print("Message lost!")
        elif type(frame.method) == spec.Basic.Ack:
              print("Confirm received!")

 
    def _publish(self, message):
        self.corr_id = str(uuid.uuid4())
        self.result=self.channel.basic_publish(exchange=self.config['exchangeName'],
                                      routing_key=self.config['routingKey'],
                                      properties=self.set_property(message),
                                      mandatory=True,
                                      immediate=False,
                                      body=json.dumps(message))
 
        print("Message delivered {}".format(self.result))
        print("send message --->{}".format(message))
        if not self.result:
            raise pika.exceptions.NackError(message)
            #OR PUBLISH TO A DIFFERENT EXCHANGE
        return self.result
        
    def publish(self,message):
        try:
          return self._publish(message)
        except (pika.exceptions.ChannelClosed,pika.exceptions.ConnectionClosed,pika.exceptions.AMQPConnectionError):
           print('Error. Connection closed, and the message was never delivered.')
           self.connect()
           return self._publish(message)
        except Exception as e:
            raise e


    def close(self):
            if self.connection:
                self.connection.close()
 
if __name__ == '__main__':
    #msgs=sys.argv[1:]


    msg={"headers":                      
    {
        "username":"navi",
        "ticket-num":"srno1",
        "correlation-id":11115
    },
    "Payload":
    {
        "source":"10.10.10.1",
        "destination":"10.172.2.1",
        "port": 22,
        "protocol":"tcp",
        "input-row-id" :1
    }}

    msgs=[msg]

    with FirmsPublisher(config) as  generateInstance:
        for msg in msgs:
            generateInstance.publish(msg)
        

