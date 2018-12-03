from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
#from aggregator import Aggreagator
from utils.data_store_driver import ConsDataStoreDrvrForChangeRecordCreator
import copy
import traceback
import time
import logging

from etc import PROJECT_ROOT,TABLE_DATA_STORE_DRIVER_CLASS,MSG_TYPE_TABLE_MAPPING

logger_obj=Logger("kannan","log_config.yml")
logger=logger_obj.get_logger()
#logger=logging.getLogger("kannan")

class MsgNotFoundInValidatorTable(Exception):
    pass

class MsgPublishTOGeneratorFailed(Exception):
    pass

class MsgDataStoreFailed(Exception):
   pass

class UnknownMsgType(Exception):
   pass

class InvalidHeader(Exception):
  pass

class YAMLFileConfigError:
    pass


class ServiceConsumerCallback:
    
    def __init__(self,prop,msg):
        self.prop=prop
        self.msg=msg
        self.new_msg=dict()


    @property
    def verifymsg(self):
        """ sometimes Looking Before leaping is not a bad idea """
        try:
            if not isinstance(self.msg,dict):
                  raise NotADictionayInstance("Passed message is not a dictionary")
            if not  ("header" in self.msg and len (self.msg["header"]) > 0):                             # to be modified into raising exception
                raise InvalidHeader("either message header is empty or invalid")
            if not ("payload" in self.msg and len (self.msg["payload"]) > 0):
                raise InvalidPayload("either payload is empty or invalid")
            if not self.prop["type"] in ['new_policy', 'red_flags', 'existing_policies', 
                                          'pre_approved_matched', 'pre_approved_not_matched', 'applier_result']:
                raise UnknownMsgType("message type is unkown")
                
        except Exception as e:
            logger.exception ("Message verification Failed for {} due to {}".format (self.prop,e.args))
            #raise e
            return False
        return True

    def get_driver(self,msg_type):
        if msg_type == 'change_record':
              return ConsDataStoreDrvrForChangeRecordCreator
                  

    def process(self):
        logger.info("strting datastore process for")
        #if not self.verifymsg:
        #    logger.error("Message verifiction failed",exc_info=True)
        #    return False
        logger.info("Message verification passed")
    
        try:
            self.new_msg['header'] = self.prop
            self.new_msg['payload'] = self.msg['payload']
        except (KeyError,ValueError) as e:
            logger.exception(e)
            raise e            # only for troubleshooting to be removed
            return False

        msg_type,table=self._get_msg_type_and_table()
        
        logger.info("msg {} of  type {} is being processed to store in {}".format(self.prop,msg_type,table))
        try:
            ret = self.get_driver(msg_type)(self.new_msg,table).store()
            if not ret:
                 raise MsgDataStoreFailed(self.new_msg)
            return True
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()         # only for troubleshooting to be removed
            return False
        return False
 
    def _get_msg_type_and_table(self):
        try:
           table=MSG_TYPE_TABLE_MAPPING.get(self.prop['type'],None)
           if not table:
               logger.error("Error in retrieving table and msg_type".format(table,self.prop["type"]))
        except Exception as e:
           raise e
        return self.prop['type'],table
 

           
    
     


def callback(prop,msg):
    
    return ServiceConsumerCallback(prop,msg).process()

def test_callback(prop,msg):
    print(prop,msg)
    return True

if __name__ == '__main__':
    
    logger.info("Validator started to Execute at {}".format(time.time()))
    try:
        logger.info("reading configs required for validator consumer from file {} in utils dir".format("consumer_config.yml"))  
        yml=YAML("consumer_config.yml","gen_cm")
        config=yml.get_config()
    except Exception as e:
        logger.exception("error opening YAML file for reading")

    logger.info("Starting to consume")
    try:
       with FirmsConsumer(config) as conn:
           conn.consume(callback)
    except KeyboardInterrupt:
        print("keyboard interrupt")
        logger.exception("KeyboardInterrupt")
    except Exception as e:
        traceback.print_exc()
        raise e
        logger.exception(e)












