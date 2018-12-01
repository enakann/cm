from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
# from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover
from generator import RecommendPolicyNotPresent
from utils_poller.containers  import MessageInfos,GenMessageInfos,ApproverInfos,ApplierInfos
import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict
from time import sleep
import logging

PROJECT_ROOT = "/home/navi/Desktop/changemanager"
if sys.platform == 'win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"

logger = logging.getLogger("kannan")


class RecommendPolicyPresent (RecommendPolicyNotPresent):
    # Todo: Create class which handle if Recommend Policy is there
    def __init__(self, msg):
        # super(RecommendPolicyPresent,self).__init__(msg)
        super ().__init__ (msg)
        
        self.db = os.path.join (PROJECT_ROOT, r"utils/cm.db")
        
        # tables
        self.gen_table = "generate"
        self.summary_table = "gen_summary"
        self.approver_table = 'approver'
        self.applier_table = 'applier'
        
        # msg_type
        self.pre_app_matched = "pre_approved_matched"
        self.pre_app_not_matched = "pre_approved_not_matched"
        self.applier_result = "applier_result"
        
        self.app_applier_msg_conatiner = []
        
        self.final_applier_appvr_msg = dict ()
        
        self.final_msg = dict ()
        
        self.data_to_be_collected = {
            'pre_approved_matched': None,
            'pre_approved_not_matched': None,
            'applier_result': None}
        
        self.msgtype_table_mapping = {
            'pre_approved_matched': 'approver',
            'pre_approved_not_matched': 'approver',
            'applier_result': 'applier',
            'gen_summary': 'generate',
            'recomm_for': 'generate',
            'existing': 'generate',
            'red_flags': 'generate'}
        
        self.approver_applier_msgs = dict ()
        self.new_existing_redflag_msgs = dict ()
    
    def get_final_msg(self, gen_msg_contaner):
        #import pdb;pdb.set_trace ()
        _final_msg = OrderedDict ()
        _final_msg["headers"] = self.msg.get_header ()
        _final_msg["payload"] = OrderedDict ()
        _final_msg["payload"]["gen_summary"] = self.msg.get_payload ()
        
        for gen_msg in gen_msg_contaner:
            _final_msg["payload"][gen_msg.type] = gen_msg.payload
        
        return _final_msg
    
    def process(self):
        ret, self.approver_applier_msgs = self.get_approver_applier_msg ()
        # import pdb;pdb.set_trace()
        if not ret:
            logger.error("Error in getting data from Approver and Applier table")
            logger.error("FIRST HALF OF TJE PROCESS FAILED")
            return False
            
        
        logger.info ("get_approver_applier_msg has returend {}".format (self.approver_applier_msgs))
        if ret:
            logger.info ("get_approver_applier_msg is succesfull")
            
            logger.info ("Creating a container of received msgs")
            _temp = []
            try:
                for k, v in self.approver_applier_msgs.items ():
                     if not v:
                        continue
                     logger.info("working on {}".format(k))
                     if k == 'applier_result':
                         _temp.append (ApplierInfos.from_list (v))
                     else:
                         _temp.append (ApproverInfos.from_list (v))
            except Excetpion as e:
                type, value, tb = sys.exc_info ()
                tb = tb.tb_next  # Skip *this* frame
                sys.last_type, sys.last_value, sys.last_traceback = type, value, tb
                logger.error(sys.last_type, sys.last_value, sys.last_traceback)
                raise e
                
                
            
            self.app_applier_msg_conatiner = [y for x in _temp for y in x]
            print (self.app_applier_msg_conatiner)
            logger.info ("creating final_applier_approver message")
            self.final_applier_appvr_msg = self.get_final_msg (self.app_applier_msg_conatiner)
            
        
        logger.info ("Starting to collect new existing and red flag messages now ...............")
        self.new_existing_redflag_msgs = self.get_new_existing_redflag_msg ()
        
        if not self.new_existing_redflag_msgs:
            #import pdb;pdb.set_trace ()
            logger.info (
                "get_new_existing_redflag_msg has  failed returned with{} ".format (self.new_existing_redflag_msgs))
            logger.info ("generate message should be there  trying again.........")
            sleep (5)
            self.new_existing_redflag_msgs = self.get_new_existing_redflag_msg ()
            if not self.new_existing_redflag_msgs:
                logger.error ("Giving up the collecting generate messages ........")
                return False
        
        logger.info ("get_new_existing_redflag_msg has returned {}".format (self.new_existing_redflag_msgs))
        
        logger.info ("collecting 1.new_existing_redflag_msgs and 2.final_app_appr_mesg is done")
        
        self.final_msg["generator"] = self.new_existing_redflag_msgs
        self.final_msg["applier_approver"] = self.final_applier_appvr_msg
        
        if not self.final_msg["generator"]:
            logger.error("Generate Message is not available")
        else:
            logger.info("Geneate Message is available")
            
        if not self.final_msg["applier_approver"]:
            logger.error("Applier Approver message it not available")
        else:
            logger.info("Applier_Aprover message is available")
        
        if  not self.final_msg["generator"] or  not self.final_msg["applier_approver"]:
            return False
        
        logger.info("collected both Generate messages and Applier_Approver messages")
        
        return self.final_msg
    
    def get_new_existing_redflag_msg(self):
        # import pdb;pdb.set_trace()
        return super ().get_new_existing_redflag_msg ()
    
    def collect_msg_from_table(self):
        for msg_type, res in self.data_to_be_collected.items ():
            if res:
                table = self.msgtype_table_mapping.get (msg_type)
    
    def get_approver_applier_msg(self):
        # get approved Matching and Not matching
        # import pdb;pdb.set_trace()
        ret_pnm=None
        pa_matching_msg = self.get_pre_approved_matching ()
        
        if pa_matching_msg:
            logger.info ("approved matching msg is available proceeding ......")
            self.data_to_be_collected['pre_approved_matched'] = pa_matching_msg
            print (pa_matching_msg)
            ret_pm = self.pre_approved_matched (pa_matching_msg)
            logger.info ("pre_approved_matched has returned {}".format (ret_pm))
            if ret_pm:
                return ret_pm, self.data_to_be_collected
        else:
            logger.error ("approved_matching is not found in db.....")
            pa_not_matching_msg = self.get_pre_approved_not_matching ()
            if pa_not_matching_msg:
                self.data_to_be_collected['pre_approved_not_matched'] = pa_not_matching_msg
                ret_pnm = self.pre_approved_not_matched (pa_not_matching_msg)
                if re_pnm:
                    return ret_pnm, self.data_to_be_collected
        logger.error ("Neither of the applier, approved data  available as of now")
        return ret_pnm, self.data_to_be_collected
    
    def pre_approved_matched(self, pa_matching_msg):
        logger.info ("in pre_approved_matched")
        #import pdb;pdb.set_trace ()
        
        applier_result_msg = self.get_applier_result ()
        
        pa_not_matching_msg = self.get_pre_approved_not_matching ()  # pre_approved_not_match
        
        print (applier_result_msg, pa_not_matching_msg)
        
        wait_for_pre_app_not_matching, wait_for_applier_result = self.wait_stratery_pre_app_match (pa_matching_msg)
        
        if wait_for_pre_app_not_matching and wait_for_applier_result:
            logger.info("pre_approved_matched ,pre_approved_matched and applier_result should be collected")
            if pa_not_matching_msg and applier_result_msg:
                self.data_to_be_collected['pre_approved_not_matched'] = pa_not_matching_msg
                self.data_to_be_collected['applier_result'] = applier_result_msg
                self.data_to_be_collected['pre_approved_matched'] = pa_matching_msg
                return True
            return False
        
        elif wait_for_applier_result and not wait_for_pre_app_not_matching:
            logger.info ("Only pre_approved_matched and applier_result should be collected")
            if applier_result_msg:
                self.data_to_be_collected['applier_result'] = applier_result_msg
                return True
            return False
        
        elif not wait_for_applier_result and not wait_for_pre_app_not_matching:
            logger.info("collecting only approved_matched")
            logger.info ("Error:if approved_matched is True ,applier_result should be there,this scenario is not possible")
            return True
        
        elif not wait_for_applier_result and wait_for_pre_app_not_matching:
            if pa_not_matching_msg:
                logger.info("collecting approved_matched and approved_not_matched")
                logger.info("Error:if approved_matched is True ,applier_result should be there")
                self.data_to_be_collected['pre_approved_not_matched'] = pa_not_matching_msg
                return True
            return False
        
        else:
            return False
    
    def pre_approved_not_matched(self, pa_not_matching_msg):
        
        applier_result_msg = self.get_applier_result (self.msg.correlation_id)
        pa_matching_msg = self.get_pre_approved_matching (self.msg.correlation_id)  # pre_approved_not_match
        
        wait_for_pre_app_matching, wait_for_applier_result = self.wait_stratery_pre_app_not_match (self.msg)
        
        if wait_for_pre_app_matching and wait_for_applier_result:
            if pa_matching_msg and applier_result_msg:
                self.data_to_be_collected['pre_approved_matched'] = pa_matching_msg
                self.data_to_be_collected['applier_result'] = applier_result_msg
                return True
        
        elif wait_for_applier_result and not wait_for_pre_app_matching:
            if applier_result_msg:
                self.data_to_be_collected['applier_result'] = applier_result_msg
                return True
        
        elif not wait_for_pre_app_matching and not wait_for_applier_result:
            return True
        
        elif not wait_for_applier_result and wait_for_pre_app_matching:
            if pa_matching_msg:
                return True
        else:
            return False
        
        # wait_for_pre_app_not_matching, wait_for_applier_result = self.wait_stratery_pre_app_match(pa_matching_msg)
    
    def wait_stratery_pre_app_match(self, _pa_matching_msg):
        wait_for_pre_app_not_matching = None
        wait_for_applier_result = None
        # import pdb;pdb.set_trace()
        if self.msg.recomm_for > _pa_matching_msg[0][6]:
            wait_for_applier_result = True
            wait_for_pre_app_not_matching = True
        # remove the below ------------------
        if self.msg.recomm_for < _pa_matching_msg[0][6]:
            wait_for_applier_result = True
            wait_for_pre_app_not_matching = True
        
        # -------------------------------
        
        if self.msg.recomm_for == _pa_matching_msg[0][6]:
            wait_for_applier_result = True
            wait_for_pre_app_not_matching = False
        
        return wait_for_pre_app_not_matching, wait_for_applier_result
    
    def wait_stratery_pre_app_not_match(self, _pa_not_matching_msg):
        wait_for_pre_app_matching = None
        wait_for_applier_result = None
        if self.msg.recomm_for == _pa_not_matching_msg[0][6]:
            wait_for_pre_app_matching = False
            wait_for_applier_result = False
        if msg.recomm_for > _pa_not_matching_msg.count:
            wait_for_pre_app_matching = True
            wait_for_applier_result = True
        return wait_for_pre_app_matching, wait_for_applier_result
    
    def get_msg_from_datastore(self, tablename, corrid, msg_type=None):
        logger.info ("get_msg_from_datastore is called from {}".format (self.__class__.__name__))
        """ method check if message for the passed corrid in the service datastore table"""
        
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
    
    def get_pre_approved_matching(self):
        pa_matching_msg = self.get_msg_from_datastore (self.approver_table, self.msg.correlation_id,
                                                       self.pre_app_matched)
        return pa_matching_msg
    
    def get_applier_result(self):
        applier_result_msg = self.get_msg_from_datastore (self.applier_table, self.msg.correlation_id,
                                                          self.applier_result)
        return applier_result_msg
    
    def get_pre_approved_not_matching(self):
        pa_not_matching_msg = self.get_msg_from_datastore (self.approver_table, self.msg.correlation_id,
                                                           self.pre_app_not_matched)
        return pa_not_matching_msg
