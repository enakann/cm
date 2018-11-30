from . import export


from .publisher import FirmsPublisher

@export
class WorkFlowMonitor:
    def __init__(self):
        self.config={'userName':'kannan',
            'password':'divya123',
            'host':'rabbitmq-1',
            'port':'5672',
            'virtualHost':'/',
            'exchangeName':'work_flow_monitor_exchange',
            'routingKey':'monitor'
            }
    def update(self,msg):
         with FirmsPublisher(self.config) as  workFlowUpdateObject:
             result=workFlowUpdateObject.publish(msg)
             return result

