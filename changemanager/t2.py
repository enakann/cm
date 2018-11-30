from lib import FirmsPublisher
from utils import YAML

yml=YAML("publisher_config.yml","request_updater")


config=yml.get_config()




msg={"headers":{
        "username":"navi",
        "ticket_num":"srno1",
        "correlation_id":"dddddddddd",
        "type":"validator",
        "status":"ready"
      },"payload":
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

