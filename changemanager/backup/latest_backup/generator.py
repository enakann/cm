from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
# from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover
from utils_poller.containers  import MessageInfos,GenMessageInfos

import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict
from time import sleep


PROJECT_ROOT = "/home/navi/Desktop/changemanager"
if sys.platform == 'win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"
logger=logging.getLogger("kannan")

class RecommendPolicyNotPresent (object):
    def __init__(self, msg):
        self.msg = msg
        self.db = os.path.join (PROJECT_ROOT, r"utils\cm.db")
        
        self.gen_table = "generate"
        self.summary_table = "gen_summary"
        self.new_policy = "new_policy"
        self.red_flag = "red_flags"
        
        self.existing_policies = 'existing_policies'
        self.approver_table = 'approver'
        self.msg_types = ['recomm_for', 'existing', 'red_flags']
        self.available_messages = OrderedDict ({"new_policy": None,
                                                "existing_policies": None,
                                                "red_flags": None
                                                })
        
        self.message_to_be_collected = dict ()
        self.all_gen_mesaages = list ()
        self.final_message = dict ()
        self.gen_msginfocontainer = []
    
    def process(self):
        return self.get_new_existing_redflag_msg ()
    
    def get_new_existing_redflag_msg(self):
        logger.info ("calling accumulate_messages to get new,existing,red flags")
        self.all_gen_messages = self.accumulate_messages()
        if not self.all_gen_messages:
            logger.error ("Error in accumuate_messages", exc_info=True)
            return False
        logger.info ("getting the details about the messages to be collecteed")
        try:
            msg_status = self.get_message_tobe_collected()
            total_msg_to_be_collected = sum ([1 for k, v in msg_status.items () if v == True])
        except Exception as e:
            logger.error ("Error in getting the msg_status  {}".format (msg_status))

        if total_msg_to_be_collected == len (self.all_gen_messages):
            
            logger.info("all messages are collected from db now creating a gen_conainter")
            self.gen_msginfocontainer = GenMessageInfos.from_list (self.all_gen_messages)
            
            if not self.gen_msginfocontainer:
                logger.error("Error in creating container from gen_messages")
                return False
            
            logger.info("Succesfully created conatainer from gen messages {}".format(self.gen_msginfocontainer))
            
            logger.info("Formatting messages to json now ")
            self.final_messages = self.get_final_msg (self.gen_msginfocontainer)

            if not self.final_messages:
                logger.error ("Error in message formatting ")
                return Fasle
            #import pdb;pdb.set_trace()
            logger.info("{} for {} has returned {}".format(self.__class__.__name__,self.msg,self.final_messages))
            return self.final_messages
        
        elif len (self.all_gen_messages) != 0:
            # Todo: find the exact message message which is missing
            logger.error ("Some messages are missing")
            return False
            # Todo: I should enfornce some kind of retry mechanism

        elif len (self.all_gen_messages) == 0:
            logger.error ("None of the messages are are available")
            return False
        else:
            logger.error("Mismatch in messages to be collected  {} and available messages {}".format(total_msg_to_be_collected,self.all_gen_messages))
            return False
            
        
        
        
        

    def get_final_msg(self, gen_msg_contaner):
        _final_msg = OrderedDict ()
        
        #import pdb;pdb.set_trace()
        
        try:
            _final_msg["headers"] = self.msg.get_header ()
        except Exception as e:
            logger.error ("Error in getting header of the gen_summary")
            raise e
        try:
            _final_msg["payload"] = OrderedDict ()
            _final_msg["payload"]["gen_summary"] = self.msg.get_payload ()
            
            for gen_msg in gen_msg_contaner:
                _final_msg["payload"][gen_msg.type] = gen_msg.payload
        except Exception as e:
            logger.error ("Error in getting payload of genereate message {}".format (e), exc_info=True)
            raise e
        
        logger.info("Message formatted {}".format(_final_msg))
        return _final_msg
        
    def get_message_tobe_collected(self):
        try:
            ls = [hasattr (self.msg, x) for x in self.msg_types]
            return dict ((zip (list (self.available_messages.keys ()), ls)))
        except Exception as e:
            logger.error ("Error in get_message_tobe_collected {}".format (e))
        return {}
    
    def accumulate_messages(self):
        _status = self.get_message_tobe_collected()
        _all_gen_messages = list()
        if all ([getattr(self.msg, x) for x in ['recomm_for', 'existing', 'red_flags']]):
            _all_gen_messages = self.get_allgenerate_msgs()
            
            if not _all_gen_messages:
                logger.error("All 3 generate message is not avaiable as of now trying again in 5 sec")
                sleep(5)
                logger.info("Trying again")
                _all_gen_messages = self.get_allgenerate_msgs()
                if not _all_gen_messages:
                    logger.error("Giving up colecting 3 generate messages")
                    return False
            
            # import pdb;pdb.set_trace()
            if len (_all_gen_messages) == 3:
                self.message_to_be_collected = {k: True for k, v in self.message_to_be_collected.items ()}
                return _all_gen_messages
            else:
                logger.error ("Not all required messages are available in db ")
                return False
        else:
            if getattr(self.msg, 'recomm_for'):
                _new_policy = self.get_new_policies ()
                if _new_policy:
                    _all_gen_messages.append (_new_policy)
                    self.message_to_be_collected['new_policy'] = True
                else:
                    logger.error ("recommended policy not available in db")
                    return False
            if getattr(self.msg, 'existing'):
                _existing_policy = self.get_existing_policies ()
                if _existing_policy:
                    _all_gen_messages.append (_existing_policy)
                    self.message_to_be_collected['existing'] = True
                else:
                    logger.error ("existing_policy not available in db")
                    return False
            if getattr(self.msg, 'red_flags'):
                _red_flags = self.get_red_flags ()
                if _red_flags:
                    _all_gen_messages.append (_existing_policy)
                    self.message_to_be_collected['red_flags'] = True
                else:
                    logger.error ("red flags not available in db")
                    return False
        return _all_gen_messages
    
    def get_msg_from_datastore(self, tablename, corrid, msg_type=None):
        """ method check if message for the passed corrid in the service datastore table"""
        logger.info ("get_msg_from_datastore is called from {}".format (self.__class__.__name__))
        try:
            if msg_type:
                query_str = "select * from {} where correlation_id=:1 and type=:2".format (tablename)
                binds = (corrid, msg_type)
            else:
                query_str = "select * from {} where correlation_id=:1".format (tablename)
                binds = (corrid,)
            with DataStore (self.db) as dbobj:
                return dbobj.select_data (query_str, binds)
        except Exception as e:
            logger.exception (e)
            return False
    
    def get_allgenerate_msgs(self):
        _all_gen_messages = self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id)
        return _all_gen_messages
    
    def get_new_policies(self):
        return self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id, self.new_policy)
    
    def get_existing_policies(self):
        return self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id, self.existing_policies)
    
    def get_red_flags(self):
        return self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id, self.red_flag)
