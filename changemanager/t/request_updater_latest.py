#!/usr/bin/env python

from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
from aggregator import RequestUpdator
import copy
import traceback
import time
import functools
import json


logger_obj=Logger("kannan","log_config.yml")
logger=logger_obj.get_logger()
logger.info("info test")

class MsgNotFoundInValidatorTable(Exception):
    pass

class MsgPublishTOGeneratorFailed(Exception):
    pass

class YAMLFileConfigError(Exception):
    pass

def debug(func):
    """Print the function signature and return value"""
    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]                      # 1
        kwargs_repr = ["{}={}".format(k,v) for k, v in kwargs.items()]  # 2
        signature = ", ".join(args_repr + kwargs_repr)           # 3
        print("Calling {}({})".format(func.__name__,signature))
        value = func(*args, **kwargs)
        print("{} returned {}".format(func.__name__,value))           # 4
        return value
    return wrapper_debug

class ReqUpdater:
    """ doc """
   
    __slots__ = ('prop','msg','new_msg')
    
    def __init__(self,prop,msg):
        self.prop=prop
        self.msg=msg
        self.new_msg=dict()
        self.db="/home/navi/Desktop/aggregator/aggregator/utils/test.db"
    """def process_pending_msg(self):
        pending_msgs=RequestUpdator.get_all_pending_msg()
        if pending_msgs:
            for msg in pending_msgs:
                (prop,msg)=RequestUpdator.get_new_msg_format(msg)
                ReqUpdater(prop,msg).process()"""
    
    
                 
            
  
    @debug
    def process(self):
        logger.info("calling Aggregator")
        #import pdb;pdb.set_trace()    
        try:
            self.new_msg['header'] = self.prop
            self.new_msg['payload'] = self.msg['payload']
            print(self.new_msg)
        except (KeyError,ValueError) as e:
            logger.error(e)
            raise e            # only for troubleshooting to be removed
            return False

        try:
            reqUpdaterObj=RequestUpdator(self.new_msg)
            (ret_reqUpdator_upd, gen_msg) = reqUpdaterObj.process()
            logger.info("{} {} is returned from aggregato".format(ret_reqUpdator_upd, gen_msg))    #  only for troubleshooting to be removed
        except Exception as e:
            raise e            # only for troubleshooting to be removed
            return False

        try:
            if int(ret_reqUpdator_upd)==1 and gen_msg:
                  logger.info("req msg is stored ,validator msg is there now publishing to gen")
                  ret=self._publish_msg_to_generator(gen_msg)
                  if ret:
                       self.set_msg_status("requester_updater","completed",self.prop['correlation_id'])

            elif int(ret_reqUpdator_upd) == 2 and not gen_msg:
                   logger.info("As message is already processed ,new msg is ignored")
                   return True

            elif int(ret_reqUpdator_upd) == 3 and  gen_msg:
                   logger.info("Message was already in pending but now its processed")
                   ret=self._publish_msg_to_generator(gen_msg)
                   if ret:
                      self.set_msg_status("requester_updater","completed",self.prop['correlation_id'])
                      return True

            elif int(ret_reqUpdator_upd) == 3 and not gen_msg:
                 logger.info("Message is already in pending but not validator msg")
                 return True
                                                  
            elif ret_reqUpdator_upd and not gen_msg:
                   self.set_msg_status("requester_updater","pending",self.prop['correlation_id'])
        except Exception as e:
            print("exceptioooooooooooooooooooooon ocurreeeeeeeeeeedd")
            self.set_msg_status("requester_updater","pending",self.prop['correlation_id'])
            logger.error(e)
        else:
            print("elseeeeeeeeeeeeeeeeee")
            self._update_monitor("gen",bool(gen_msg))
        finally:
            return self._update_monitor ("req",int(ret_reqUpdator_upd))

    def set_msg_status(self,table,status,corrid):
        query_str="update {} set status=\"{}\" where correlation_id=\"{}\"".format(table,status,corrid)
        logger.info(query_str)
        try:
           with DataStore (self.db) as dbobj:
               ret = dbobj.update(query_str)
               return ret
        except Exception as e:
               raise e
       
    

    
    
    def _publish_msg_to_generator(self, gen_msg):
    
        logger.info ("publishing received message to the generator queue")
        try:
            yml = YAML ("publisher_config.yml", "generator")
            config = yml.get_config ()
            if not config:
                raise YAMLFileConfigError(config)
        except Exception as e:
            logger.error(e)
        
        try:
            with FirmsPublisher (config) as generateInstance:
                ret = generateInstance.publish (gen_msg)
                if not ret:
                    raise MsgPublishTOGeneratorFailed (gen_msg)
        except Exception as e:
            logging.error("sending message to generator Failed due to {}".format (e))
            return False
        return True

    def _gen_msg_unavailable_handler(self, origmsg_header):
        # TODO : May be new functionality to be added for this
        #raise MsgNotFoundInValidatorTable (origmsg_header)
        logger.info("MsgNotFoundInValidatorTable {}".format(origmsg_header))
        return False

    def _update_monitor(self, key, status):

        states = {"req":
                      {1:{
                        "msg": "{} insert into  request_updater table is successs".format (self.new_msg['header']),
                         "return": True},
                       0: {
                        "msg": "{}  insert into request_updater table is failed".format (self.new_msg['header']),
                        "return": False},
                        2:{
                        "msg": "{}  New message is not inserted into request_updater  as already msg  in completed state".format (self.new_msg['header']),
                        "return": True},
                        3:{
                          "msg": "{}  Received message is already in  request_updater table in pending ".format (self.new_msg['header']),
                          "return":True
                           },
                        },
   
                  "gen":
                      {True: {
                         "msg": "{} Pubishing to Generator is Succesfull".format (self.new_msg['header']),
                          "return": True},
                       False: {
                           "msg": "{}  Publishing to Generator is Failed".format (self.new_msg['header']),
                           "return": False}
                        }
                  }

        if status:
             logger.info(states[key][status]["msg"])
        else:
             logger.info(states[key][status]["msg"])

        return states[key][status]["return"]
    
class PendingRequestUpdatorMessages:
    def __init__(self):
        self.pending_msgs=[]
        self.db = "/home/navi/Desktop/aggregator/aggregator/utils/test.db"
        
   
    def process(self):
        self._get_pending_msgs()
        if self.pending_msgs:
            print(self.pending_msgs)
            logger.info("Processing pending messages in Reques Updater table")
            for prop,msg in self.pending_msgs:
                msg_bkup=copy.deepcopy(msg)
                result=ReqUpdater(prop, msg).process()
                if result:
                    logger.info("succesfully processed")
                else:
                    logger.error("processing failed")
        else:
            logger.info("No Pending Message Proceed to process the consumed message")
   

     
     
    def _get_pending_msgs(self):
        pending_msgs = self._get_msg_from_table_using_status("requester_updater","pending")
        if pending_msgs:
            for msg in pending_msgs:
                print(msg)
                (prop, msg) = self.get_new_msg_format(msg)
                self.pending_msgs.append((prop, msg))

    def _get_msg_from_table_using_status(self,tablename,status):
        query_str = "select * from {} where status=:1".format (tablename)
        with DataStore (self.db) as dbobj:
            ret = dbobj.select_data (query_str, (status,))
            return ret

    def delete_msg(self, table, corrid):
        query_str = "delete from {} where correlation_id={}".format (table,corrid)
        print (query_str)
        with DataStore (self.db) as dbobj:
            ret = dbobj.delete (query_str, (corrid,))
            return ret

    
    def get_new_msg_format(self, data):
        _msg = data
        _final_msg = dict ()
        _header = dict ()
        _header["correlation_id"] = _msg[2]
        _header["username"] = _msg[3]
        _header["ticket_num"] = _msg[4]
        _header["type"] = _msg[5]
        _header["status"]=_msg[8]
        _payload = json.loads(_msg[7])

        # _final_msg["headers"] = _header
        _final_msg["payload"] = _payload
    
        return _header, _final_msg




def callback(prop,msg):
    #if random.randint (0, 100) < 25:
    PendingRequestUpdatorMessages().process()
    return ReqUpdater(prop,msg).process()

################################################ NEW MODIFICATION ###############################



def callback2(prop,msg):
#    workflow_monitor = WorkFlowMonitor ()
    logger.info("starting callback with header:{} ".format(prop))
    _msg={}
    
    _msg['header']=prop
    _msg['payload']=msg['payload']
    log_message=copy.deepcopy(_msg)

    try:
        (ret_reqUpdator_upd, final_msg) = Aggreagator(_msg).process_requestupdater_msg()
        
        if ret_reqUpdator_upd:
            print ("request updater msg is updated into the aggregator datastore ")
 #           workflow_monitor ("request updater msg is updated into the aggregator datastore".format (msg))
            try:
                if final_msg:
                     logger.info("publishing received message into the generator queue")
                     yml=YAML("publisher_config.yml","generator")
                     config=yml.get_config()
                     with FirmsPublisher(config) as generateInstance:
                         generateInstance.publish(final_msg)
                     
                else:
                    logger.exception("for the message received form requpdater corresponding message not in validator queue")

            except Exception as e:
                logging.exception("sending message to generator Failed due to {}".format(e))
                traceback.print_exc()
                raise e

            else:
                logging.info("{} is published succesfully to generator queue".format(final_msg['correlation_id']))
                
            finally:
                logger.info("Msg succesfully processed sending ack to the req updated queue ")
                return True
        else:
            return False
    except Exception as e:
        logger.info("processing failed for the {} recieved from request updater".format(msg["correlation_id"]))
        raise e
   #     workflow_monitor(" msg {} update failed  in aggregator datastore due to {}".format (msg, e))
    return False


if __name__ == '__main__':

    
    logger.info("Validator started to Execute at {}".format(time.time()))

    try:
        logger.info("reading configs required for validator consumer from file {} in utils dir".format("consumer_config.yml"))
        yml=YAML("consumer_config.yml","request_updater")
        config=yml.get_config()
    except Exception as e:
        logger.exception("error opening YAML file for reading")


    logger.info("Starting to consume")    
    try:
        with FirmsConsumer(config) as conn:
            conn.consume(callback)
    except KeyboardInterrupt:
        print('keyboard interrupt')
    except Exception as e:
        traceback.print_exc()
        raise e


