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
from utils_poller.containers import MessageInfos, GenMessageInfos

import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict

logger_obj = Logger ("kannan", "log_config.yml")
logger = logger_obj.get_logger ()

PROJECT_ROOT = "/home/navi/Desktop/changemanager"
if sys.platform == 'win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"


class ContainerCreationException (Exception):
    pass


class ChangeRecordCreator:
    pass


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
        self.changeRecordCreator = ChangeRecordCreator ()
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
            if not item.correlation_id == 'sdsdsdsdsd':
                continue
            
            if item.recomm_for:
                logger.info ("recomeneded policy exist starting to process for {}".format (item))
                
                self.recommendPolicyPresent = RecommendPolicyNotPresent(item)
                
                rcp_result = self.recommendPolicyPresent.process()
                if rcp_result:
                    logger.info ("Poller succesfully Finished processing  {} has returned {}".format (item, rcp_result))
                    logger.info ("Messages collected generate:{}  and approver_applier:{}".format (
                        rcp_result['generator']['payload'].keys (), rcp_result['applier_approver']['payload'].keys ()))

                    _pubish_ret=self.publish_to_change_record_creator(rcp_result)
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

