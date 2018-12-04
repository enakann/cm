from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary, \
    ConsumerDataStoreDriverForApprover
from generator import RecommendPolicyNotPresent
from approver_applier import RecommendPolicyPresent
from utils.containers import MessageInfos, GenMessageInfos
from etc import PROJECT_ROOT,TABLE_DATA_STORE_DRIVER_CLASS,MSG_TYPE_TABLE_MAPPING

from gen_consumer import ServiceConsumerCallback

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
        #import pdb;pdb.set_trace()
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
        self.approver_table='approver'
        self.applier_table='applier'
        self.tables=["generate","gen_summary","approver","applier"]

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


    def verify_msg_is_in_pending(self,corrid):
        _completed_state=[]
        query = """select correlation_id,type,status from {} where correlation_id=:1
             union
             select correlation_id,type,status from {} where correlation_id=:1
             union
             select correlation_id,type,status from {} where correlation_id=:1""".format(self.gen_table,self.approver_table,self.applier_table)


        with DataStore (self.db) as dbobj:
             data=dbobj.select_data(query,(corrid,))
        
        if not data:
            logger.error("DB:Data not found")
            return False
        _completed_state = [(x[-2],x[-1]) for x in data if x[-1]=='completed']

        if _completed_state:
              logger.error(" {} is in competed state for the following table {}".format(corrid,_completed_state))
              return False
        logger.info("All messages are in pending state we can proceed")
        return True



    def set_status_completed(self,msg):
        #import pdb;pdb.set_trace()
        logger.info("proceding to set status as completed for {}".format(self.tables))
        with DataStore (self.db) as dbobj:
            for _table in self.tables:
                query="update {} set status=\"{}\" where correlation_id =\"{}\"" .format(_table,"pending",msg.correlation_id)
                #query="update {} set status=\"{}\" where correlation_id ={}" .format(_table,"pending",msg.correlation_id)
                logger.info(query)
                try:
                   ret=dbobj.update(query)
                   if not ret:
                       logger.error("Failed to update status as completed in table {} for {}".format(table,msg.correlation_id))
                       return False
                except Exception as e:
                       logging.error("Exception occured during setting status as completed for {}".format(msg))
                       return False
        return True


    def publish_to_change_record_creator(self,msg):
        import pdb;pdb.set_trace ()
    
        def callback(prop, msg):
            return ServiceConsumerCallback (prop, msg).process ()

        if sys.platform == 'win32':
            _msg={}
            _prop=msg['headers']
            _msg['payload']=msg['payload']
            callback(_prop,_msg)
        else:
        #import pdb;pdb.set_trace()
            yml = YAML ("publisher_config.yml", "change_record_creator")
            config = yml.get_config ()
            with FirmsPublisher (config) as  generateInstance:
                return generateInstance.publish(msg)

    def transform_container_to_json(self,item,_rcp_result_verified):
        try:
            _final_msg = OrderedDict ()
            _final_msg["headers"] = item.get_header()
            _final_msg["payload"] = OrderedDict()
            _final_msg["payload"]["summary"]=OrderedDict()
            _final_msg["payload"]["summary"]["total_recs"]=item.total_recs
            _final_msg["payload"]["summary"]["recomm_for"]=item.recomm_for
            _final_msg["payload"]["summary"]["existing"]=item.existing
            _final_msg["payload"]["summary"]["red_flags"] = item.red_flags
            _final_msg["payload"]["gen_summary"]= json.loads(item.get_payload ())
        except Exception as e:
            logger.error("Exception {} occured while creating header and summary for {}".format(e,item))
            return False
        import pdb;pdb.set_trace()
        try:
            for srvs_type,gen_msg in _rcp_result_verified.items():
                import pdb;pdb.set_trace()
                if srvs_type == 'approver_applier':
                    for _msg in gen_msg:
                        if _msg.type == 'pre_approved_matched':
                            _final_msg["payload"]["summary"]["pre_approved_matched_count"]=_msg.count
                        elif _msg.type == 'pre_approved_not_matched':
                            _final_msg["payload"]["summary"]["pre_approved_not_matched_count"] = _msg.count
<<<<<<< HEAD

                        elif _msg.type == 'applier_result':
=======
                        elif _msg.type == 'applier':
>>>>>>> 84fa23f6e74537d1700ea140b2d8eb41f44f3972
                            _final_msg["payload"]["summary"]['applier_success_count'] = _msg.total_success
                            _final_msg["payload"]["summary"]['applier_failed_count'] = _msg.total_failed
                        _final_msg["payload"][_msg.type]=_msg.get_payload()
                elif srvs_type == 'generator':
                    for _msg in gen_msg:
                        _final_msg["payload"][_msg.type] = json.loads(_msg.get_payload ())
        except Exception as e:
            logging.error("Exception {} occured while creating summary and payload for {}".format(e,item))
            return False
        """try:
            #import pdb;pdb.set_trace()

            _final_msg["payload"]["summary"]["pre_approved_not_matched_count"]=_final_msg["payload"]["summary"].get("pre_approved_not_matched_count",0)
        except KeyError:
            logger.debug("setting pre_approved_not_matched as 0 as its not available")
            _final_msg["payload"]["summary"]["pre_approved_not_matched_count"] = 0

        try:
            _final_msg["payload"]["summary"]["approved_matched_count"] = _final_msg["payload"]["summary"].get("pre_approved_matched_count",0)
        except KeyError:
            logger.debug ("setting pre_approved_matched as 0 as its not available")
            _final_msg["payload"]["summary"]["approved_matched_count"] = 0 """



        return _final_msg

    def verify_container(self,_rcp_result,item):
        #import pdb;pdb.set_trace()
        if item.recomm_for:
              logger.info("Recommended policy is there approver_applier result should be there")
              #import pdb;pdb.set_trace()
              if not 'approver_applier' in _rcp_result.keys():
                  logger.info("Approver& Applier data is not found ")
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
            rcp_result={}
            logger.info ("********working on  {}*************".format (item))
            if not item.correlation_id == 'pm_not_avl':
                continue

            if not self.verify_msg_is_in_pending (item.correlation_id):
                logger.info ("verification failed for {}as some of the messages are in completed state".format (item))
                continue

            if item.recomm_for:
                logger.info ("recomeneded policy verifying if all message are in pending {}".format (item))

                self.recommendPolicyPresent = RecommendPolicyPresent(item)

                rcp_result = self.recommendPolicyPresent.process()
                
                if rcp_result:
                    logger.info ("Poller succesfully Finished processing  {} has returned {}".format (item, rcp_result))
                else:
                    logger.error ("Poller failed processing {}".format (item))


            else:
                logger.info ("recomeneded policy doesnt  exist starting to process for {}".format (item))

                self.recommendPolicyNotPresent = RecommendPolicyNotPresent(item)

                gen_contnr = self.recommendPolicyNotPresent.process()

                rcp_result['generator']=gen_contnr

                if rcp_result:
                    logger.info ("Poller succesfully Finished processing using {} {} has returned {}".format (
                        self.recommendPolicyNotPresent, item, rcp_result))
                    import pdb;pdb.set_trace()
                    logger.info ("Messages collected generate {}".format (rcp_result['generator']))
                else:
                    logger.error ("Poller Failed processing {}".format (item))
                    continue

####################################################################################################################################################### 
            
            logger.info("COMMON PROCESS BEGIN")
            logger.info("verifying containers now")

            _verify_ret = self.verify_container(rcp_result, item)

            if not _verify_ret:
                logging.error ("Verification of conatiner failed for {}".format (item))
                continue

            logger.info("container verification succesfull")
            logger.info("Transform container to json")
 

            _json_ret = self.transform_container_to_json (item, rcp_result)

            if not _json_ret:
                logging.error ("transform_container_to_json failed for {}".format (item))
                continue

            
            logger.info("Starting the to publish")
            _publish_ret = self.publish_to_change_record_creator (_json_ret)

            if not _publish_ret:
                logging.error ("Processing is completed but publishing failed for {}".format (item))
                continue

            _set_status_ret = self.set_status_completed (item)

            if not _set_status_ret:
                logging.error ("Processing & Publishing completed but faild to status as completed for {}".format(
                    _set_status_ret))


            logger.info ("*****  FINISEHD WORKING ON --->{}  ********************".format (item))





    def collect_messages(self):
        pass

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

    #poll.verify_msg_is_in_pending(99)
    poll.process ()
    #cm=ChangeRecordCreator()
    #msg=cm.get_msg()
    #print(msg)


