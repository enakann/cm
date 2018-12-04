from .data_store import DataStore
import logging
import time
import json
import datetime
logger=logging.getLogger("kannan")
import sys
import os

from etc import PROJECT_ROOT


class InvalidMessageReceivedException (Exception):
    pass


class ConsumerDataStoreDriver(object):
    """  Helper class for storing received message form consumer into the service data store """
    
    def __init__(self, msg, table,workflow_monitor=None):
        self.workflow_monitor = workflow_monitor
        self.msg = msg
        #self.connstr = "oracle/oracle@127.0.0.1/xe"    
        self.table = table
        #import pdb;pdb.set_trace()
        self.db = os.path.join(PROJECT_ROOT,"utils/cm.db")



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
            if not self.msg["header"]["type"] in ['change_record','gen_summary', 'new_policy', 'red_flags', 'existing_policies',
                                          'pre_approved_matched', 'pre_approved_not_matched', 'applier_result']:
                raise UnknownMsgType("message type is unkown")

        except Exception as e:
            logger.exception ("Message verification Failed for {} due to {}".format (self.msg,e.args))
            #raise e
            return False
        return True
    
    
    def get_header_payload(self):
        return self.msg["header"], self.msg["payload"]

    def get_msg_type(self):
        return self.msg["header"]["type"]
    
    def get_msg_from_datastore(self, corrid, tablename):
        """ method check if message for the passed corrid in the service datastore table"""
        
        try:
            msg_type=self.get_msg_type()
            query_str = "select * from {} where correlation_id=:1 and type=:2".format (tablename)
            with DataStore (self.db) as dbobj:
                return  dbobj.select_data (query_str, (corrid,msg_type))
        except Exception as e:
            logger.exception (e)
            return False
    
    def store(self):
        if not self.verifymsg:
            return False
        logger.info ("Message verification succesfull")        

        (header, payload) = self.get_header_payload () 
        ret = self.get_msg_from_datastore (header["correlation_id"], self.table)
        """ method check if message for the passed corrid in the service datastore table"""
        
        if not ret:
            try:
                logger.info ("received msg is not already there in validator ,proceding to insert")
                insert_values=self._get_insert_vaues()
                insert_ret =self.insert_items(insert_values)
                if not insert_ret:
                    return False
            except Exception as e:
                logger.exception ("Exception occured while Inserting data into validator for msg {}".format (self.msg))
                raise e
                return False
        else:
            logger.error ("Message already exist in the table")
            # FIXME - may be we have to delete the old message and insert the new one
            return False
        return True

    def _get_insert_vaues(self):
            #import pdb;pdb.set_trace()
            (header, payload) = self.get_header_payload ()
           
            try:
              _insert_values= [None,time.time(),
                                    header["correlation_id"],
                                    header["username"],
                                    header["ticket_num"],
                                    header["type"],
                                    json.dumps (payload),
                                    'pending']
            except Exception as e:
              logger.error("Error in getting insert_values {}".format(e))
            return _insert_values
             


    def insert_items(self, values):
        pholdr = '?'
        pholdrs = ','.join (pholdr for item in values)
        query = "Insert into {} values ({})".format (self.table, pholdrs)
        with DataStore (self.db) as dbobj:
               return dbobj.insert (query, values)



class ConsumerDataStoreDriverForGenSummary(ConsumerDataStoreDriver):
      def __init__(self, msg, table,workflow_monitor=None):
           super(ConsumerDataStoreDriverForGenSummary,self).__init__ (msg,table,workflow_monitor=None)



      def _get_insert_vaues(self):
             header,payload=self.get_header_payload()
             try: 
                _insert_values=[None,datetime.datetime.now(),header["correlation_id"],header["username"],header["ticket_num"],header["type"]]
                _summary=payload["summary"]
                _payload=json.dumps(payload)
             except KeyError as e:
                logger.error(e,exc_info=True)
                raise e
             
             _insert_values= _insert_values+[_summary.get(x,0) for x in ['total_recs','recomm_for','existing','red_flags']]
             _insert_values.append(_payload)
             _insert_values.append('pending')

             return _insert_values


class ConsumerDataStoreDriverForApprover(ConsumerDataStoreDriver):
      def  __init__(self, msg, table,workflow_monitor=None):
           super(ConsumerDataStoreDriverForApprover,self).__init__ (msg,table,workflow_monitor=None)


      def _get_insert_vaues(self):
             header,payload=self.get_header_payload()
             try:
                _insert_values=[None,datetime.datetime.now(),header["correlation_id"],header["username"],header["ticket_num"],header["type"]]
                _count=header["total"]
                _payload=json.dumps(payload)
             except KeyError as e:
                logger.error(e,exc_info=True)
                raise e

             _insert_values.append(_count)
             _insert_values.append(_payload)
             _insert_values.append('pending')

             return _insert_values


class ConsumerDataStoreDriverForApplier(ConsumerDataStoreDriver):
    def __init__(self, msg, table, workflow_monitor=None):
        super (ConsumerDataStoreDriverForApplier, self).__init__ (msg, table, workflow_monitor=None)
    
    
    def _get_insert_vaues(self):
        header, payload = self.get_header_payload ()
        try:
            _insert_values = [None, datetime.datetime.now (), header["correlation_id"], header["username"],
                              header["ticket_num"], header["type"]]
            _summary=payload['summary']
            total_failed = _summary['total_failed']
            total_success = _summary['total_success']
            _payload = json.dumps (payload)
        except KeyError as e:
            logger.error (e, exc_info=True)
            raise e

        _insert_values.append(total_failed)
        _insert_values.append(total_success)
        _insert_values.append (_payload)
        _insert_values.append ('pending')
        
        return _insert_values


class ConsDataStoreDrvrForChangeRecordCreator(ConsumerDataStoreDriver):
    def __init__(self, msg, table, workflow_monitor=None):
        super (ConsumerDataStoreDriverForApplier, self).__init__ (msg, table, workflow_monitor=None)
    

          

    def _get_insert_vaues(self):
        header, payload = self.get_header_payload ()
        try:
            _insert_values = [None, datetime.datetime.now (), header["correlation_id"], header["username"],
                              header["ticket_num"], header["type"]]
            _summary = payload['summary']
            total_recs=_summary['total_recs']
            recomm_for=_summary['recomm_for']
            existing=_summary['existing']
            red_flags=_summary['red_flags']
            pre_approved_matched_count=_summary['pre_approved_matched_count']
            pre_approved_not_matched_count=_summary['pre_approved_not_matched_count']
            applier_success_count = _summary['applier_success_count']
            applier_failed_count = _summary['applier_failed_count']
            _payload = json.dumps (payload)
        except KeyError as e:
            logger.error (e, exc_info=True)
            raise e
        _ls=[total_recs,recomm_for,existing, red_flags,pre_approved_matched_count,pre_approved_not_matched_count,applier_success_count,applier_failed_count]
        _insert_values=_insert_values+_ls
        _insert_values.append (_payload)
        _insert_values.append ('pending')
        
        return _insert_values


      



            
             
             

