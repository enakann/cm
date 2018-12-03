from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
# from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover
from utils.containers  import MessageInfos,GenMessageInfos
from __init__ import PROJECT_ROOT

import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict
from time import sleep
from itertools import zip_longest



logger=logging.getLogger("kannan")

class RecommendPolicyNotPresent (object):
    def __init__(self, msg):
        self.msg = msg
        self.db = os.path.join (PROJECT_ROOT, r"utils/cm.db")
        
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
        self.msgtype_method_mapping = {'recomm_for': 'get_new_policies',
                                       'existing': 'get_existing_policies',
                                       'red_flags': 'get_red_flags'}

        self.summry_gen_msg_mapping = {'recomm_for': self.new_policy,
                                       'existing': self.existing_policies,
                                       'red_flags': self.red_flag}
        
        self.map= {self.new_policy: 'get_new_policies',
                   self.existing_policies: 'get_existing_policies',
                   self.red_flag: 'get_red_flags'}


        
        self.message_to_be_collected = dict ()
        self.all_gen_mesaages = list ()
        self.final_message = dict ()
        self.gen_msginfocontainer = []
    
    def process(self):
        return self.get_new_existing_redflag_msg ()
    
    def get_new_existing_redflag_msg(self):
        logger.info ("calling accumulate_messages to get new,existing,red flags")
        
        self.gen_msginfocontainer = self.accumulate_messages()
        
        if not self.gen_msginfocontainer:
            logger.error ("Error in accumuate_messages", exc_info=True)
            return False
 
           
        logger.info("Succesfully recevied  conatainer with all genereate messages {}".format(self.gen_msginfocontainer))
        
        return self.gen_msginfocontainer
        
        logger.info("Formatting messages to json now ")
        
        
        
        self.final_messages = self.get_final_msg (self.gen_msginfocontainer)
        
        print(self.final_messages)
        

        if not self.final_messages:
            logger.error ("Error in message formatting ")
            return Fasle
        logger.info ("{} for {} has returned {}".format (self.__class__.__name__, self.msg, self.final_messages))
        return self.final_messages
            
        
        
        
        

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
        #import pdb;pdb.set_trace()
        _status = self.get_message_tobe_collected ()
        _all_gen_messages = list ()
        if all ([getattr (self.msg, x) for x in self.msg_types]):
            logger.info("All 3 message should be collected")
            _all_gen_messages = self.get_allgenerate_msgs ()
            
            
            if not _all_gen_messages:
                  logger.error ("All 3 generate message is not avaiable as of now trying again in 5 sec")
                  sleep (5)
                  logger.info ("Trying again")
                  _all_gen_messages = self.get_allgenerate_msgs ()
                  
                  logger.info(_all_gen_messages)
                  if not _all_gen_messages:
                       logger.error ("Giving up colecting 3 generate messages")
                       return False

            _collect_msgs = [x[5] for x in _all_gen_messages]

            _missing = set (self.map.keys ()) - set (_collect_msgs)
            
            for _msg in _missing:
                    logger.info("currently working on {}".format(_msg))
                    logger.info ("Trying to get {} from generate table".format (_msg))
                    sleep(1)
                    _db_ret = getattr(self,self.map.get(_msg))()
                    if _db_ret:
                            _all_gen_messages.append (_db_ret)
                    else:
                            logger.error ("Failed to fetch {} from generate table  {}".format(_msg))
            if len (_all_gen_messages) == 3:
                   try:
                       self.gen_msginfocontainer = GenMessageInfos.from_list (_all_gen_messages)
                   except Exception as e:
                       logger.error("Error creating container")
                       return False
                   logger.info ("Succesfully created conatainer from gen messages {}".format (self.gen_msginfocontainer))
                   return self.gen_msginfocontainer

        else:
            for i in ['recomm_for', 'existing', 'red_flags']:
                  if getattr (self.msg, i):
                      msg = getattr (self,self.msgtype_method_mapping.get (i)) ()
                      if msg:
                           _all_gen_messages.append (msg)
                      else:
                            logger.info("Failed to fetch {}".format(self.summry_gen_msg_mapping.get(i)))
                            logger.info("Trying again")
                            sleep (5)
                            _msg = getattr (self,self.msgtype_method_mapping.get (i)) ()
                            if _msg:
                                _all_gen_messages.append (_msg)
                            else:
                                self.msg.failed.append (i)

            _all_gen_messages_unpacked=[ y  for x in _all_gen_messages for y in x]

            if not self.msg.failed and len (_all_gen_messages_unpacked) == sum ([1 if getattr (self.msg, x) else 0 for x in self.msg_types]):
                try:
                   
                   self.gen_msginfocontainer = GenMessageInfos.from_list (_all_gen_messages_unpacked)
                except Exception as e:
                   logger.error ("Error creating container")
                   return False
                logger.info ("Succesfully created conatainer from gen messages {}".format (self.gen_msginfocontainer))
                return self.gen_msginfocontainer

            else:
                for _failed_msg in self.msg.failed:
                     logger.error("Failed collecting {}".format(self.summry_gen_msg_mapping.get(_failed_msg)))
                return False
        return False

    def get_msg_from_datastore(self, tablename, corrid, msg_type=None):
        """ method check if message for the passed corrid in the service datastore table"""
        logger.info ("get_msg_from_datastore is called from {}".format (self.__class__.__name__))
        try:
            #import pdb;pdb.set_trace()
            logger.info(self.db)
            if msg_type:
                query_str = 'select * from \"{}\" where correlation_id=:1 and type=:2'.format (tablename)
                binds = (corrid, msg_type)
            else:
                query_str = 'select * from \"{}\"  where correlation_id=:1'.format (tablename)
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
