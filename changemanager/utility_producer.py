import json
import os
import sys
from utils import YAML
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
import copy
import logging
from time import sleep
from pprint import pprint
#logger=logging.getLogger("kannan")

logger_obj=Logger("aggregator","log_config.yml")
logger=logger_obj.get_logger()

from __init__ import PROJECT_ROOT,MSG_TYPE_TABLE_MAPPING

MESSAGE_PATH="/home/navi/Desktop/changemanager/etc/messages/"


MESSAGE_PATH=os.path.join(PROJECT_ROOT,"etc/messages")

print(PROJECT_ROOT)
print(MESSAGE_PATH)

MESSAGE_FILES={
'apr':'applier_result.json',
'exp':'existing_policy.json',
'val':'validator_msg.json',
'inval':'validator_invalid_msg.json',
'red':'red_flags.json',
'prnm':'pre_approved_not_matching.json',
'prm':'pre_approved_matching.json',
'new':'new_policy.json',
'gen':'gen_summary.json'
}

PUBLISH_CONFIG={
'apr':'gen_cm',
'exp':'gen_cm',
'val':'',
'inval':'',
'red':'gen_cm',
'prnm':'gen_cm',
'prm':'gen_cm',
'new':'gen_cm',
'gen':'gen_cm'
}

DB=os.path.join(PROJECT_ROOT,r"\utils\cm.db")

def _get_config(msg_type):
   
   exch=PUBLISH_CONFIG.get(msg_type,None)
   logger.info("In _get_config retrieved publisher type is {}".format(exch))
   yml=YAML("publisher_config.yml",exch)
   config=yml.get_config()
   return config

def _get_msg(msg_type):
   #import pdb;pdb.set_trace() 
   file_name=os.path.join(MESSAGE_PATH,MESSAGE_FILES.get(msg_type))
   with open(file_name,'r') as fh:
           data=json.loads(fh.read())
           return data




def _get_data_from_datastore(*args,**kwargs):

     def select_corrid(*kwargs):
         with DataStore (DB) as dbobj:
             print(kwargs[0]['corrid'])
             ret = dbobj.select_data ("select * from validator where correlation_id=:1",(kwargs[0]['corrid'],))
             return ret

     def delete_all_rows(*kwargs):
           with DataStore (DB) as dbobj:
              dbobj.delete_all_rows()

     def select_all_rows(*kwargs):
            with DataStore (DB) as dbobj:
              ret = dbobj.select_data ("select * from validator")
              return ret

     actions={'select_corrid':select_corrid,
              'select_all_rows':select_all_rows,
              'delete_all_rows':delete_all_rows}
     return actions.get(args[0])(kwargs)

def pre_process():
    #_get_data_from_datastore("select_all_rows"))
    print(_get_data_from_datastore("delete_all_rows"))
    print(_get_data_from_datastore("select_all_rows"))

def post_process(corrid):
   logger.info("verifying message in table in post proicess")
   pprint(_get_data_from_datastore("select_corrid",corrid=corrid))



def publish_consume_store_verify_process(msg_type):
     ls=[]
     
     logger.info("getting config for publishing")
     config=_get_config(msg_type)

     logger.info("getting message to be publisehd")
     msg=_get_msg(msg_type)
     msg_bk=copy.deepcopy(msg)

     logger.info("Emptying the table")
     #pre_process()

     logger.info("Publishing the message")
     with FirmsPublisher(config) as  generateInstance:
           generateInstance.publish(msg)

    # sleep(1)
    # corrid=msg_bk["headers"]["correlation_id"]

     #logger.info("verfying message in datastore for corrid {}".format(corrid))
     #post_process(corrid)











if __name__ == '__main__':
    index=sys.argv[1].strip()
    msgs=index.split(',')
    print(msgs)
    for msg in msgs:
       publish_consume_store_verify_process(msg)
    #print(_get_data_from_datastore("select",table="validator",corrid="test"))
    #pre_process()

