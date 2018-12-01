#!/usr/bin/env python

import copy
import traceback
import time
import sys
from lib import FirmsConsumer
from lib import FirmsPublisher
from lib import WorkFlowMonitor
from utils import YAML
from aggregator import Validator
from utils import Logger
import traceback

logger_obj=Logger("kannan","log_config.yml")
logger=logger_obj.get_logger()
logger.info("info test")



def callback(prop,msg):
    logger.info("starting callback with prop:{} and msg:{}".format(prop,msg))
    print(prop,msg)
    _msg={}
    _msg['header']=prop
    _msg['payload']=msg['payload']
    log_message=copy.deepcopy(_msg)
    print(_msg)
    #workflow_monitor=WorkFlowMonitor().update(log_message)
    try:
        logger.info("Calling Aggregator.process_validator_msg() with msg {}".format(_msg))

        result = Validator(_msg).process()
        if result:
            print("Succesfully updated msg into validator table")
     #       workflow_monitor(" msg {} is updated in aggregator".format(msg))
            return True
    except Exception as e:
      #  workflow_monitor(" msg {} update failed  in aggregator datastore".format(msg))
        logger.exception("Error occured while calling aggregator from validator")
        traceback.print_exc()
        raise e
        return False
    return False

def func(prop,msg):
    return True





if __name__ == '__main__':
   

    logger.info("Validator started to Execute at {}".format(time.time()))

    try:

        logger.info("reading configs required for validator consumer from file {} in utils dir".format("consumer_config.yml"))  
        yml=YAML("consumer_config.yml","validator")
        config=yml.get_config()
    except Exception as e:
        raise e
        logger.exception("error opening YAML file for reading")


    logger.info("Starting to consume")
    try:
       with FirmsConsumer(config) as conn:
           conn.consume(callback)
    except KeyboardInterrupt:
        print("keyboard interrupt")
        logger.exception("KeyboardInterrupt")
    except Exception as e:
        logger.exception(e)
        traceback.print_exc()
        sys.exit(e)



