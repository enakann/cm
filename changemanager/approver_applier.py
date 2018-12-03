from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
# from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver, ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover
from generator import RecommendPolicyNotPresent
from utils.containers  import MessageInfos,GenMessageInfos,ApproverInfos,ApplierInfos
import copy
import traceback
import time
import logging
import os
import sys
from collections import OrderedDict
from collections import namedtuple
from time import sleep
import logging



from __init__ import PROJECT_ROOT

logger = logging.getLogger("kannan")

class Msg_type:
    def __init__(self,name,method):
        self.collect=None
        self.data=None
        self.method=method
        self.objname=name
    
    def __repr__(self):
        return "{}(collect={},method={},data={}".format(self.objname,self.collect,self.method,self.data)
        



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

        self.msg_obj = namedtuple ('msg_obj', ['collect', 'data', 'method'])

        self.app_apr_msg_types = [self.pre_app_matched, self.pre_app_not_matched, self.applier_result]

        self.app_apr_msg_mesthod_mapping = {self.pre_app_matched : 'get_pre_approved_matching',
                                       self.pre_app_not_matched : 'get_pre_approved_not_matching',
                                       self.applier_result : 'get_applier_result'}
        
        self.conatiner_mapping={self.pre_app_matched :ApproverInfos,
                                self.pre_app_not_matched:ApproverInfos,
                                self.applier_result:ApplierInfos}
        

        self.data_to_be_collected = {}

    def process(self):
        #import pdb;pdb.set_trace()
        self.construct_data_to_be_collected()

        pa_matching_msg = self.get_pre_approved_matching()

        if pa_matching_msg:
            self.data_to_be_collected["pre_approved_matched"].collect=True
            
            #self.approver_matched_conatiner_obj.add(pa_matching_msg)
            
            self.data_to_be_collected["pre_approved_matched"].data=ApproverInfos.from_list (pa_matching_msg)
            pam_ret=self.pre_approved_matched (pa_matching_msg)
            if pam_ret:
               self.collect_data()
               
               logger.info(self.data_to_be_collected)
               
               _verify_rt=self.verify_collected_data()
               if not _verify_rt:
                   return False
        else:
            pa_not_matching_msg = self.get_pre_approved_not_matching ()
            if pa_not_matching_msg:
                self.data_to_be_collected["pre_approved_not_matched"].collect = True

                #self.approver_not_matched_conatiner_obj.add(pa_not_matching_msg)
                self.data_to_be_collected["pre_approved_not_matched"].data = ApproverInfos.from_list(pa_not_matching_msg)
                
                panm_ret=self.pre_approved_not_matched(pa_not_matching_msg)
                if panm_ret:
                    self.collect_data()
                    _ret=self.verify_collected_data()
                    if not _ret:
                        return False
        logger.info ("FIRST HALF OF THE PROCESS COMPLETED")
        
        self.final_msg["approver_applier"] = list()
        for k,v in self.data_to_be_collected.items():
            if v.data:
               self.final_msg["approver_applier"].append(v.data)
            

        logger.info ("STARTING TO COLLECT GENERATE MESSAGES")
        
        self.new_existing_redflag_msgs = self.get_new_existing_redflag_msg ()


        if not self.new_existing_redflag_msgs:
            # import pdb;pdb.set_trace ()
            logger.info ("get_new_existing_redflag_msg has  failed returned with{} ".format (self.new_existing_redflag_msgs))
            logger.info ("generate message should be there  trying again.........")
            sleep (5)
            self.new_existing_redflag_msgs = self.get_new_existing_redflag_msg ()
            if not self.new_existing_redflag_msgs:
                logger.error ("Giving up the collecting generate messages ........")
                return False
        
        
        logger.info ("get_new_existing_redflag_msg has returned {}".format (self.new_existing_redflag_msgs))

        logger.info ("collecting 1.new_existing_redflag_msgs and 2.final_app_appr_mesg is done")
        
        #import pdb;pdb.set_trace()
        self.final_msg["generator"] = self.new_existing_redflag_msgs
        
        return self.final_msg
            
    def get_new_existing_redflag_msg(self):
        # import pdb;pdb.set_trace()
        return super ().get_new_existing_redflag_msg ()

    def construct_data_to_be_collected(self):
            for msg in self.app_apr_msg_types:
                self.data_to_be_collected[msg] = self.get_msg_obj(msg,self.app_apr_msg_mesthod_mapping.get(msg))

    def get_data_to_be_collected(self):
        self.construct_data_to_be_collected ()
        return self.data_to_be_collected

    def get_msg_obj(self, _msg,method_name):
        return Msg_type(_msg,method_name)
    
    def verify_collected_data(self):
        #import pdb;pdb.set_trace()
        _verify_ret=[k for k, v in self.data_to_be_collected.items() if v.collect and not v.data]
        if not _verify_ret:
            return True
        else:
            for _failed in _verify_ret:
                logger.error("failed to collect {}".format(_failed))
        return False

    def collect_data(self):
        #import pdb; pdb.set_trace ()
        for key, _item in self.data_to_be_collected.items ():
            if _item.collect:
                _db_ret = getattr (self, _item.method) ()
                if not _db_ret:
                    sleep (1)
                    _db_ret = getattr (self, _item.method) ()
                    if not  _db_ret:
                        logger.error("Giving up collecting {}".format(key))
                if key == 'applier_result':
                    appvr_container=ApplierInfos.from_list (_db_ret)
                    _item.data=appvr_container[0]
                else:
                    applr_conatiner=ApproverInfos.from_list (_db_ret)
                    _item.data=applr_conatiner[0]
                    
                    
            
            
                    
            
    
    
    def pre_approved_matched(self,pa_matching_msg):
        wait_for_pre_app_not_matching, wait_for_applier_result = self.wait_stratery_pre_app_match (pa_matching_msg)
        if wait_for_pre_app_not_matching and wait_for_applier_result:
            for _item in self.data_to_be_collected:
                _item.collect = True
            return True
        elif wait_for_applier_result and not wait_for_pre_app_not_matching:
            self.data_to_be_collected['applier_result'].collect = True
            return True
    
        elif not wait_for_applier_result and not wait_for_pre_app_not_matching:
            logger.info ("collecting only approved_matched")
            logger.info (
                "UNEXPECTED Error:if approved_matched is True ,applier_result should be there,this scenario is not possible")
            return False
    
        elif not wait_for_applier_result and wait_for_pre_app_not_matching:
            logger.info ("collecting approved_matched and approved_not_matched")
            logger.info ("UNEXPECTEDError:if approved_matched is True ,applier_result should be there")
            self.data_to_be_collected['pre_approved_not_matched'].collect = True
            return False

    def pre_approved_not_matched(self, pa_not_matching_msg):
        
        wait_for_pre_app_matching, wait_for_applier_result = self.wait_stratery_pre_app_not_match (self.msg)

        if wait_for_pre_app_matching and wait_for_applier_result:
            self.data_to_be_collected['pre_approved_matched'].collect = True
            self.data_to_be_collected['applier_result'] = True
            return True
        elif wait_for_applier_result and not wait_for_pre_app_matching:
            logger.error (
                "UnexpectedError:This scenario is no possible as without pre_approved_matched there cannot be applier result")
            if applier_result_msg:
                self.data_to_be_collected['applier_result'].collect = True
                return True

        elif not wait_for_pre_app_matching and not wait_for_applier_result:
            return True

        elif not wait_for_applier_result and wait_for_pre_app_matching:
            logger.error (
                "UnexpectedError:This scenario is no possible if if pre_app matching is there,should wait for applier result")
            return False
        else:
            return False


    def wait_stratery_pre_app_match(self, _pa_matching_msg):
        # import pdb;pdb.set_trace()
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
            logger.error ("Pre approved matching cannot be more than Total Recommended Policy")
        # -------------------------------

        if self.msg.recomm_for == _pa_matching_msg[0][6]:
            wait_for_applier_result = True
            wait_for_pre_app_not_matching = False

        return wait_for_pre_app_not_matching, wait_for_applier_result

    def wait_stratery_pre_app_not_match(self, _pa_not_matching_msg):
                # import pdb;pdb.set_trace()
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
        __pa_matching_msg = self.get_msg_from_datastore (self.approver_table, self.msg.correlation_id,
                                                         self.pre_app_matched)
        return __pa_matching_msg

    def get_applier_result(self):
        __applier_result_msg = self.get_msg_from_datastore (self.applier_table, self.msg.correlation_id,
                                                            self.applier_result)
        return __applier_result_msg

    def get_pre_approved_not_matching(self):
        __pa_not_matching_msg = self.get_msg_from_datastore (self.approver_table, self.msg.correlation_id,
                                                             self.pre_app_not_matched)
        return __pa_not_matching_msg











