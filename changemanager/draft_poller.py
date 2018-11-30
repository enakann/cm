from lib import FirmsConsumer
from lib import FirmsPublisher
from utils import DataStore
from utils import Logger
from utils import YAML
from lib import WorkFlowMonitor
from collections import OrderedDict
#from aggregator import Aggreagator
from utils.data_store_driver import ConsumerDataStoreDriver,ConsumerDataStoreDriverForGenSummary,ConsumerDataStoreDriverForApprover
import copy
import traceback
import time
import logging
import os
import sys

logger_obj=Logger("kannan","log_config.yml")
logger=logger_obj.get_logger()

PROJECT_ROOT="/home/navi/Desktop/changemanager"
if sys.platform=='win32':
    PROJECT_ROOT = r"C:\Users\navkanna\PycharmProjects\cm\changemanager"


class Summary:
    def __init__(self, msg):
        self.msg=msg
        self.correlation_id=self.msg[2]
        self.username=self.msg[3]
        self.ticket_no=self.msg[4]
        self.type=self.msg[5]
        self.total_recs = self.msg[6]
        self.recomm_for = self.msg[7]
        self.existing = self.msg[8]
        self.red_flags = self.msg[9]
        self.payload=self.msg[10]
        self.status=self.msg[11]

    def __repr__(self):
        return "correlation_id ---->{}:Summary(total_recs={}, recomm_for={}, existing={}, red_flags={})". \
            format (self.correlation_id, self.total_recs, self.recomm_for, self.existing, self.red_flags)


    def as_list(self):
        return self.msg

    def get_header(self):
        _header = dict ()
        _header["correlation_id"] = self.correlation_id
        _header["username"] = self.username
        _header["ticket_num"] = self.ticket_no
        _header["type"] = "change_record"
        return _header

    def get_payload(self):
        return self.payload

class GenMessages(object):
    def __init__(self,msg):
#        print("in gen messagess {}".format(msg))
        self.msg=msg
        self.correlation_id=self.msg[2]
        self.username=self.msg[3]
        self.ticket_no=self.msg[4]
        self.type=self.msg[5]
        self.payload=self.msg[6]
        self.status=self.msg[7]

    def __repr__(self):
        return "correlation_id ---->{}:GenMessages(type={}, status={})". \
            format (self.correlation_id, self.type,self.status)


    def as_list(self):
        return self.msg

    def get_header(self):
        _header = dict ()
        _header["correlation_id"] = self.correlation_id
        _header["username"] = self.username
        _header["ticket_num"] = self.ticket_no
        _header["type"] = "change_record"
        return _header

    def get_payload(self):
        return self.payload

#-----------------------------------------------
#python2.7
##########

"""class ApproverApplierMsg(GenMessages):
    def __init__(self,msg):
       super(GenMessages,self).__init__(msg)
       self.count=self.msg[6]
       self.payload = self.msg[7]
       self.status = self.msg[8]"""
       
#python3
########

class ApproverApplierMsg(GenMessages):
    def __init__(self,msg):
       super().__init__(msg)
       self.count=self.msg[6]
       self.payload = self.msg[7]
       self.status = self.msg[8]
    


class MessageInfos(object):
    def __init__(self):
        self.container = []

    def add(self, *args):  # Either this
        self.container.append (Summary (**kwargs))

    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (Summary(row))
        return self.container

    def __getattr__(self, name):
        return getattr (self.container, name)

    def __getitem__(self, item):
        return self.container[item]

    def __len__(self):
        return len (self.container)

class GenMessageInfos(MessageInfos):
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (GenMessages(row))
        return self.container
    
class ApproverApplierInfos(GenMessageInfos):
    @classmethod
    def from_list(cls, rows):  # Or this
        self = cls ()
        for row in rows:
            self.container.append (ApproverApplierMsg(row))
        return self.container










##########################################
class RecommendPolicyNotPresent(object):
  def __init__(self,msg):
        self.msg=msg
        self.db=os.path.join(PROJECT_ROOT,r"utils\cm.db")

        self.gen_table="generate"
        self.summary_table="gen_summary"
        self.new_policy="new_policy"
        self.red_flag="red_flags"

        self.existing_policies='existing_policies'
        self.approver_table='approver'
        self.msg_types=['recomm_for', 'existing', 'red_flags']
        self.available_messages=OrderedDict({"new_policy":None,
                                             "existing_policies":None,
                                              "red_flags":None
                                            })

        self.message_to_be_collected=dict()
        self.all_gen_mesaages=list()
        self.final_message=dict()
        self.gen_msginfocontainer=[]


  def process(self):
      return self.get_new_existing_redflag_msg()
  
  def get_new_existing_redflag_msg(self):
        self.all_gen_messages=self.accumulate_messages()
        msg_status = self.get_message_tobe_collected ()
        if sum([1 for k,v in msg_status.items() if v==True]) == len(self.all_gen_messages):
            import pdb;pdb.set_trace()
            self.gen_msginfocontainer=GenMessageInfos.from_list(self.all_gen_messages)
            self.final_messages=self.get_final_msg(self.gen_msginfocontainer)
            return self.final_messages
        else:
            return False
      


  def get_final_msg(self, gen_msg_contaner):
      #import pdb;pdb.set_trace()
      _final_msg = OrderedDict()
      _final_msg["headers"] = self.msg.get_header()
      _final_msg["payload"]=OrderedDict()
      _final_msg["payload"]["gen_summary"]=self.msg.get_payload()


      for gen_msg in gen_msg_contaner:
          _final_msg["payload"][gen_msg.type] = gen_msg.payload

      return _final_msg

  def get_message_tobe_collected(self):
      ls=[hasattr (self.msg, x) for x in self.msg_types]
      return dict((zip(list(self.available_messages.keys()),ls)))

  def accumulate_messages(self):
      _status=self.get_message_tobe_collected()
      _all_gen_messages=list()
      if all([hasattr(self.msg,x) for x in ['recomm_for','existing','red_flags']]):
              _all_gen_messages=self.get_allgenerate_msgs()
              #import pdb;pdb.set_trace()
              if len(_all_gen_messages) == 3:
                  self.message_to_be_collected={k:True for k,v in self.message_to_be_collected.items()}
                  return _all_gen_messages
              else:
                  logger.error("Not all required messages are available in db ")
                  return False
      else:
          if hasattr(self.msg,'recomm_for'):
              _new_policy=self.get_new_policies()
              if _new_policy:
                  _all_gen_messages.append(_new_policy)
                  self.message_to_be_collected['new_policy']=True
              else:
                  logger.error("recommended policy not available in db")
                  return False
          if hasattr(self.msg,'existing'):
              _existing_policy=self.get_existing_policies()
              if _new_policy:
                  _all_gen_messages.append(_existing_policy)
                  self.message_to_be_collected['existing'] = True
              else:
                  logger.error("existing_policy not available in db")
                  return False
          if hasattr(self.msg,'red_flags'):
              _red_flags=self.get_red_flags()
              if _new_policy:
                  _all_gen_messages.append(_existing_policy)
                  self.message_to_be_collected['red_flags'] = True
              else:
                  logger.error("red flags not available in db")
                  return False
      return _all_gen_messages



  def get_msg_from_datastore(self, tablename,corrid,msg_type=None):
      """ method check if message for the passed corrid in the service datastore table"""

      try:
        if msg_type:
            query_str = "select * from {} where correlation_id=:1 and type=:2".format (tablename)
            binds=(corrid,msg_type)
        else:
            query_str ="select * from {} where correlation_id=:1".format(tablename)
            binds=(corrid,)
        with DataStore (self.db) as dbobj:
            return  dbobj.select_data (query_str, binds)
      except Exception as e:
        logger.exception (e)
        return False


  def get_allgenerate_msgs(self):
        _all_gen_messages=self.get_msg_from_datastore(self.gen_table,self.msg.correlation_id)
        return _all_gen_messages

  def get_new_policies(self):
        return self.get_msg_from_datastore(self.gen_table,self.msg.correlation_id,self.new_policy)

  def get_existing_policies(self):
      return self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id, self.existing_policies)

  def get_red_flags(self):
      return self.get_msg_from_datastore (self.gen_table, self.msg.correlation_id, self.red_flag)

######################################################################################################





class ChangeRecordCreator:
 pass

class Accumulator:
  pass

#######################################################################################################

class RecommendPolicyPresent(RecommendPolicyNotPresent):
    #Todo: Create class which handle if Recommend Policy is there
    def __init__(self,msg):
        #super(RecommendPolicyPresent,self).__init__(msg)
        super().__init__ (msg)

        self.db=os.path.join(PROJECT_ROOT,r"utils/cm.db")

        #tables
        self.gen_table="generate"
        self.summary_table="gen_summary"
        self.approver_table='approver'
        self.applier_table='applier'

        #msg_type
        self.pre_app_matched="pre_approved_matched"
        self.pre_app_not_matched="pre_approved_not_matched"
        self.applier_result="applier_result"
        
        self.app_applier_msg_conatiner=[]
        
        self.final_app_appr_msg=dict()
        
        self.final_msg=dict()


        self.data_to_be_collected={
                            'pre_approved_matched':None,
                            'pre_approved_not_matched':None,
                            'applier_result':None}
        self.approver_applier_msgs=dict()
        self.new_existing_redflag_msgs=dict()

    def get_final_msg(self, gen_msg_contaner):
        import pdb;
        pdb.set_trace ()
        _final_msg = OrderedDict ()
        _final_msg["headers"] = self.msg.get_header ()
        _final_msg["payload"] = OrderedDict ()
        _final_msg["payload"]["gen_summary"] = self.msg.get_payload ()
    
        for gen_msg in gen_msg_contaner:
            _final_msg["payload"][gen_msg.type] = gen_msg.payload
    
        return _final_msg
        
    def process(self):
        _ls=list()
        ret,self.approver_applier_msgs=self.get_approver_applier_msg()
        #import pdb;pdb.set_trace()
        logger.info("get_approver_applier_msg has returend {}".format(self.approver_applier_msgs))
        if ret:
            logger.info("get_approver_applier_msg is succesfull")
            
            logger.info("Creating a container of received msgs")
            for k,v in self.approver_applier_msgs.items():
                 self.app_applier_msg_conatiner.append(ApproverApplierInfos.from_list(v[0]))
            
            logger.info("creating final_applier_approver message")
            self.final_app_appr_msg = self.get_final_msg (self.app_applier_msg_conatiner)
        
        logger.info("collecting new existing and ref flag messages")
        self.new_existing_redflag_msgs = self.get_new_existing_redflag_msg ()
        if  not self.new_existing_redflag_msgs:
                  logger.info("get_new_existing_redflag_msg has  failed returned with{} ".format(self.new_existing_redflag_msgs))
        logger.info("get_new_existing_redflag_msg has returned {}".format(self.new_existing_redflag_msgs))
        
        
        logger.info("collecting 1.new_existing_redflag_msgs and 2.final_app_appr_mesg")
        
        self.final_msg["generator"]=self.new_existing_redflag_msgs
        self.final_msg["applier_approver"]=self.final_app_appr_mesg
        
        return self.final_msg
        
    
    def get_new_existing_redflag_msg(self):
        #import pdb;pdb.set_trace()
        return super().get_new_existing_redflag_msg()
    
    
    def get_approver_applier_msg(self):
        #get approved Matching and Not matching
        #import pdb;pdb.set_trace()
        pa_matching_msg=self.get_pre_approved_matching()

        if pa_matching_msg:
            logger.info("approved matching msg is available proceeding ......")
            self.data_to_be_collected['pre_approved_matched'] = pa_matching_msg
            print(pa_matching_msg)
            ret_pm=self.pre_approved_matched(pa_matching_msg)
            logger.info("pre_approved_matched has returned {}".format(ret_pm))
            if ret_pm:
                return ret_pm,self.data_to_be_collected
        else:
            logger.error("approved_matching is not found in db.....")
            pa_not_matching_msg = self.get_pre_approved_not_matching()
            if pa_not_matching_msg:
                 ret_pnm=self.pre_approved_not_matched(pa_not_matching_msg)
                 if re_pnm:
                      return ret_pnm,self.data_to_be_collected
        logger.error("Neither of the approved or not approved available as of now")
        return False





    def pre_approved_matched(self,pa_matching_msg):
        logger.info("in pre_approved_matched")
        import pdb;pdb.set_trace()

        applier_result_msg = self.get_applier_result()

        pa_not_matching_msg = self.get_pre_approved_not_matching()# pre_approved_not_match


        print(applier_result_msg,pa_not_matching_msg)


        wait_for_pre_app_not_matching, wait_for_applier_result = self.wait_stratery_pre_app_match(pa_matching_msg)

        if wait_for_pre_app_not_matching and wait_for_applier_result:
            if pa_not_matching_msg and applier_result_msg:
                self.data_to_be_collected['pre_approved_not_matched']=pa_not_matching_msg
                self.data_to_be_collected['applier_result']=applier_result_msg
                self.data_to_be_collected['pre_approved_matched']=pa_matching_msg
                return True
            return False

        elif wait_for_applier_result and not wait_for_pre_app_not_matching:
            if applier_result_msg:
                self.data_to_be_collected['applier_result'] = applier_result_msg
                return True
            return False

        elif not wait_for_applier_result and not wait_for_pre_app_not_matching:
            return True

        elif not wait_for_applier_result and wait_for_pre_app_not_matching:
            if pa_not_matching_msg:
                self.data_to_be_collected['pre_approved_not_matched'] = pa_not_matching_msg
                return True
            return False

        else:
            return False



    def pre_approved_not_matched(self,pa_not_matching_msg):


        applier_result_msg = self.get_applier_result(self.msg.correlation_id)
        pa_matching_msg = self.get_pre_approved_matching(self.msg.correlation_id)  # pre_approved_not_match

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




        #wait_for_pre_app_not_matching, wait_for_applier_result = self.wait_stratery_pre_app_match(pa_matching_msg)


    def wait_stratery_pre_app_match(self,_pa_matching_msg):
        wait_for_pre_app_not_matching=None
        wait_for_applier_result=None
        #import pdb;pdb.set_trace()
        if self.msg.recomm_for > _pa_matching_msg[0][6]:
            wait_for_applier_result=True
            wait_for_pre_app_not_matching=True
        #remove the below ------------------
        if self.msg.recomm_for < _pa_matching_msg[0][6]:
            wait_for_applier_result=True
            wait_for_pre_app_not_matching=True
        
        #-------------------------------
        
        
        if self.msg.recomm_for == _pa_matching_msg[0][6]:
            wait_for_applier_result=True
            wait_for_pre_app_not_matching=False

        return wait_for_pre_app_not_matching,wait_for_applier_result

    def wait_stratery_pre_app_not_match(self,_pa_not_matching_msg):
        wait_for_pre_app_matching=None
        wait_for_applier_result=None
        if self.msg.recomm_for == _pa_not_matching_msg[0][6]:
            wait_for_pre_app_matching = False
            wait_for_applier_result = False
        if msg.recomm_for > _pa_not_matching_msg.count:
            wait_for_pre_app_matching = True
            wait_for_applier_result = True
        return wait_for_pre_app_matching,wait_for_applier_result


    def get_msg_from_datastore(self, tablename,msg_type,corrid):
        """ method check if message for the passed corrid in the service datastore table"""

        try:
            query_str = "select * from {} where correlation_id=:1 and type=:2".format (tablename)
            with DataStore (self.db) as dbobj:
                return  dbobj.select_data (query_str, (corrid,msg_type))
        except Exception as e:
            logger.exception (e)
            return False


    def get_pre_approved_matching(self):
          pa_matching_msg=self.get_msg_from_datastore(self.approver_table,self.pre_app_matched,self.msg.correlation_id)
          return pa_matching_msg

    def get_applier_result(self):
        applier_result_msg = self.get_msg_from_datastore(self.applier_table,self.applier_result,self.msg.correlation_id)
        return applier_result_msg

    def get_pre_approved_not_matching(self):
        pa_not_matching_msg = self.get_msg_from_datastore(self.approver_table,self.pre_app_not_matched,self.msg.correlation_id)
        return pa_not_matching_msg


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
        self.msginfocontainer=[]
        self.recommendPolicyPresent=None
        self.recommendPolicyNotPresent=None
        self.changeRecordCreator=ChangeRecordCreator()
        self.db = os.path.join(PROJECT_ROOT,r"utils/cm.db")
        self.gen_table="generate"
        self.summary_table="gen_summary"

    def get_pending_summary_messages(self):
        """   1.Query the table ,get the data
                  ls=query_from_the_table()
              2.create the msginfocontainer by calling
              self.msginfocontainer=MessageInfos.from_list(ls)
              """
        return self.get_msg_from_datastore(self.summary_table,"pending")

    def get_msg_from_datastore(self, tablename,status):
        """ method check if message for the passed corrid in the service datastore table"""

        try:
            query_str = "select * from {} where status=:1".format (tablename)
            print(self.db)
            with DataStore (self.db) as dbobj:
                return  dbobj.select_data (query_str, (status,))
        except Exception as e:
            logger.exception (e)
            return False




    def process_summary_of_message_info(self):
        pass


    def process(self):
        """This method  shold drive the class"""
        # 1. Query the message info table
        # 2.Loop over all the Message

        ret=self.get_pending_summary_messages()
        #import pdb;pdb.set_trace()
        self.msginfocontainer=MessageInfos.from_list(ret)
        #print(self.msginfocontainer)

        for item in self.msginfocontainer:
             #import pdb;pdb.set_trace()
             logger.info("********working on  {}*************".format(item))
             if item.recomm_for:
                   self.recommendPolicyPresent=RecommendPolicyPresent(item)
                   rcp_result=self.recommendPolicyPresent.process()
                   if rcp_result:
                        logger.info("Poller Finished processing  {}".format(item))
                   else:
                       logger.error("Poller failed processing {}".format(item))
             else:
                   self.recommendPolicyNotPresent=RecommendPolicyNotPresent(item)
                   rcnp_result=self.recommendPolicyNotPresent.process()
                   if rcnp_result:
                        logger.info("Poller Finished processing  {}".format(item))
                   else:
                        logger.error("Poller Failed processing {}".format(item))




        """for item in self.msginfocontainer:
            if item.recomm_for:
                result=self.recommendPolicyPresent.process(item)
                if result:
                    change_details=self.create_change_details(messages)  # create the change_details
                    self.changeRecordCreator(change_details)   # Call Change Record Creator,poller done
                else:
                    continue
            else:
                result = self.recommendPolicyNotPresent.process(item)
                if result:
                    change_details = self.create_change_details (messages)
                    self.changeRecordCreator (change_details)
                else:
                    continue"""



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



poll=Poller()
poll.process()

