from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
# from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary, \
    ConsumerDataStoreDriverForApprover
from generator import RecommendPolicyNotPresent
from approver_applier import RecommendPolicyPresent
# from utils_poller.containers  import GenMessages
from utils.containers import MessageInfos, GenMessageInfos

import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict
import json

logger_obj = Logger ("kannan", "log_config.yml")
logger = logger_obj.get_logger ()

from __init__ import PROJECT_ROOT



class ContainerCreationException (Exception):
    pass


class ChangeRecordCreator:
    def __init__(self,msg):
        self.msg=msg
        self.netops_requied=False
        self.netops_triggers=['red_flags','pre_approved_not_matched_count']
    def get_msg(self):
        return self.msg
    
    def get_header(self):
        return self.msg['headers']
    
    def get_payload(self):
        return self.mag['payload']
    
    def get_summary(self):
        return self.msg['payload']['summary']
    
    def process(self):
        import pdb;pdb.set_trace()
        summary=self.get_summary()
        for k,v in summary.items():
            logger.info("key {} and value {}".format(k,v))
            if k in self.netops_triggers and v > 0:
                self.netops_requied=True
        logger.info(self.netops_requied)
                
        


class Accumulator:
    pass





class Poller:
    # Todo: Create a Poller Class
    """ 1.Poller class will poll the MessageInfo Table every 10 min
        2.Get the all the pending MessageInfo & get the summary part of the message
        3.SUMMARY dict
           payload": {"summary": {
                       "total_recs": 30,
                       "recomm_for": 15,
                       "existing": 10,
                       "red_flags": 5 }
        4.Decides Which Factory class should be Invoked
            a.RecommendPolicyPresent
            b.RecommendPolicyNotPresent

        """
    
    def __init__(self):
        self.msginfocontainer = []
        self.recommendPolicyPresent = None
        self.recommendPolicyNotPresent = None
        #self.changeRecordCreator = ChangeRecordCreator ()
        self.db = os.path.join (PROJECT_ROOT, r"utils/cm.db")
        self.gen_table = "generate"
        self.summary_table = "gen_summary"
        self.tables=["generate,gen_summary,approver,applier"]
    
    def get_pending_summary_messages(self):
        """   1.Query the table ,get the data
                  ls=query_from_the_table()
              2.create the msginfocontainer by calling
              self.msginfocontainer=MessageInfos.from_list(ls)
              """
        return self.get_msg_from_datastore (self.summary_table, "pending")
    
    def get_msg_from_datastore(self, tablename, status):
        """ method check if message for the passed corrid in the service datastore table"""
        
        try:
            query_str = "select * from {} where status=:1".format (tablename)
            print (self.db)
            with DataStore (self.db) as dbobj:
                return dbobj.select_data (query_str, (status,))
        except Exception as e:
            logger.exception (e)
            return False
    
    def process_summary_of_message_info(self):
        pass
    
    def verify_message(self):
        pass

    def set_status_completed(self,msg):
        logger.info("proceding to set status as completed for {}".format(self.tables))
        with DataStore (self.db) as dbobj:
            for table in self.tables:
                query="update {}  set status = {} where correlation_id ={}".format(table,"completed",msg.correlation_id)
                ret=dbobj.update(query)
                if not ret:
                    logger.error("Failed to update status as completed in table {} for {}".format(table,msg.correlation_id))
                

    def publish_to_change_record_creator(self,msg):
        yml = YAML ("publisher_config.yml", "change_record_creator")
        config = yml.get_config ()
        with FirmsPublisher (config) as  generateInstance:
                return generateInstance.publish(msg)
        
    def transform_container_to_json(self,item,_rcp_result_verified):
        
        #import pdb;pdb.set_trace ()
        _final_msg = OrderedDict ()
        _final_msg["headers"] = item.get_header()
        _final_msg["payload"] = OrderedDict()
        _final_msg["payload"]["summary"]=OrderedDict()
        _final_msg["payload"]["summary"]["total_recs"]=item.total_recs
        _final_msg["payload"]["summary"]["existing"]=item.existing
        _final_msg["payload"]["summary"]["red_flags"] = item.red_flags
        _final_msg["payload"]["gen_summary"]= json.loads(item.get_payload ())
        
    
        for type,gen_msg in _rcp_result_verified.items():
            logger.info(type)
            logger.info(gen_msg)
            if type == 'approver_applier':
                for _msg in gen_msg:
                     if _msg.type == 'pre_approved_matched':
                           _final_msg["payload"]["summary"]["pre_approved_matched_count"]=_msg.count
                     
                     
                     elif _msg.type == 'pre_approved_not_matched':
                           _final_msg["payload"]["summary"]["pre_approved_not_matched_count"] = _msg.count
                     
                     _final_msg["payload"][_msg.type]=_msg.get_payload()
                    
            elif type == 'generator':
                print("inside generator")
                for _msg in gen_msg:
                    _final_msg["payload"][_msg.type] = json.loads(_msg.get_payload ())
       
        try:
            #import pdb;pdb.set_trace()

            _final_msg["payload"]["summary"]["pre_approved_not_matched_count"]=_final_msg["payload"]["summary"].get("pre_approved_not_matched_count",0)
        except KeyError:
            logger.debug("setting pre_approved_not_matched as 0 as its not available")
            _final_msg["payload"]["summary"]["pre_approved_not_matched_count"] = 0
            
        try:
            _final_msg["payload"]["summary"]["approved_matched_count"] = _final_msg["payload"]["summary"].get("pre_approved_matched_count",0)
        except KeyError:
            logger.debug ("setting pre_approved_matched as 0 as its not available")
            _final_msg["payload"]["summary"]["approved_matched_count"] = 0
                
                    
                
        return _final_msg

    def verify_container(self,_rcp_result,app_apr=None):
        #import pdb;pdb.set_trace()
        
        if app_apr:
              if not 'approver_applier' in _rcp_result:
                  return False
        if not 'generator' in _rcp_result:
            return False
        for key,contnr in _rcp_result.items():
            for _msg in contnr:
                if not _msg.payload:
                     logger.error("Payload is empty")
                     return False
        return True
        
        
        
    
    def process(self):
        """This method  shold drive the class"""
        # 1. Query the message info table
        # 2.Loop over all the Message
        logger.info ("In Poller process")
        
        logger.info ("Getting the Message from gen_sumary table which is at pedning.......")
        try:
            ret = self.get_pending_summary_messages ()
            if not ret:
                logger.error ("Error in getting the pending message from gen_summary table")
        except Exception as e:
            logger.error (e)
            raise e
        
        # import pdb;pdb.set_trace()
        try:
            self.msginfocontainer = MessageInfos.from_list(ret)
            if not self.msginfocontainer:
                raise ContainerCreationException ("Unable to create a container")
        except Exception as e:
            logger.error ("Error in creating container classs from gen_summary messages  {}".format(e))
            raise e
        
        # print(self.msginfocontainer)
        
        for item in self.msginfocontainer:
            # import pdb;pdb.set_trace()
            logger.info ("********working on  {}*************".format (item))
            if not item.correlation_id == 'pm_not_avl':
                continue
            
            if item.recomm_for:
                logger.info ("recomeneded policy exist starting to process for {}".format (item))
                
                self.recommendPolicyPresent = RecommendPolicyPresent(item)
                
                rcp_result = self.recommendPolicyPresent.process()
                if rcp_result:
                    logger.info ("Poller succesfully Finished processing  {} has returned {}".format (item, rcp_result))
                    
                    _verify_ret=self.verify_container(rcp_result,None)
                    
                    if _verify_ret:
                        _json_ret=self.transform_container_to_json(item,rcp_result)
                        #import pdb;pdb.set_trace()
                        _publish_ret=ChangeRecordCreator(_json_ret).process()
                    if _publish_ret:
                        self.set_status_completed(item)
                        
                    
                else:
                    logger.error ("Poller failed processing {}".format (item))
                    
                    
            else:
                logger.info ("recomeneded policy doesnt  exist starting to process for {}".format (item))
                
                self.recommendPolicyNotPresent = RecommendPolicyNotPresent(item)
                
                rcnp_result = self.recommendPolicyNotPresent.process()
                if rcnp_result:
                    logger.info ("Poller succesfully Finished processing using {} {} has returned {}".format (
                        self.recommendPolicyNotPresent, item, rcnp_result))
                    logger.info ("Messages collected generate {}".format (rcnp_result['payload'].keys ()))
                else:
                    logger.error ("Poller Failed processing {}".format (item))
            
            
            
            
            logger.info ("**************FINISEHD WORKING  *********************{}".format (item))
            
            
        

    
    def collect_messages(self):
        """ Collect message from all the table"""
    
    def create_change_details(self):
        """ Create the change details that should be passed to change Record Creator """
    
    def get_message_info_locally(self):
        """ Make a local cache of the message info """
    
    def get_message_info_from_table(self):
        """ It should use the query_table to get the message info list"""
    
    def validate_message_info(self):
        pass
    
    def get_preapproved_matching(self):
        pass
    
    def validate_preapproved_matching(self):
        """if total_recomm = number of policies in pre-approved matching message
        - DO NOT WAIT for Pre-Approved Not Matching message

        if total_recomm >= number of policies in pre-approved matching message
        - WAIT for ApplierResult from the Applier
        """
    
    def get_preapproved_not_matching(self):
        pass
    
    def validate_preapproved_not_matching(self):
        pass
    
    def get_applier_result(self):
        pass
    
    def validate_applier_result(self):
        pass


if __name__ == '__main__':
    logger.info ("****************************************************")
    logger.info ("Starting the POLLER Now...........")
    poll = Poller ()
    poll.process ()
    #cm=ChangeRecordCreator()
    #msg=cm.get_msg()
    #print(msg)
    

