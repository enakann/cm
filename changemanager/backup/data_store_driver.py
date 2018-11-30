from .data_store import DataStore
import logging
import time
import json
import datetime
logger=logging.getLogger("kannan")

class InvalidMessageReceivedException (Exception):
    pass


class ConsumerDataStoreDriver(object):
    """  Helper class for storing received message form consumer into the service data store """
    
    def __init__(self, msg, table,workflow_monitor=None):
        self.workflow_monitor = workflow_monitor
        self.msg = msg
        #self.connstr = "oracle/oracle@127.0.0.1/xe"    
        self.table = table
        self.db = "/home/navi/Desktop/changemanager/utils/cm.db" #to be changed to production service datastore
    
    @property
    def verifymsg(self):
        """ verify received message """
        try:
            verify_ret = all ([isinstance (self.msg, dict),
                               "header" in self.msg and len (self.msg["header"]) > 0,
                               "payload" in self.msg and len (self.msg["payload"]) > 0
                               ])
            if not verify_ret:
                raise InvalidMessageReceivedException (self.msg)
            return True
        except Exception as e:
            logger.exception ("Message verification Failed".format (e))
            return False
    
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
                ret = dbobj.select_data (query_str, (corrid,msg_type))
                return ret
        except Exception as e:
            logger.exception (e)
    
    def store(self):
        if not self.verifymsg:
            return False
        logger.info ("Message verification succesfull")
        
        (header, payload) = self.get_header_payload ()
        
        ret = self.get_msg_from_datastore (header["correlation_id"], self.table)
        """ method check if message for the passed corrid in the service datastore table"""
        
        if not ret:
            logger.info ("received msg is not already there in validator ,proceding to insert")
            try:
                with DataStore (self.db) as dbobj:
                    query_str=self.get_queryString()
                    dbobj.insert (query_str, [None,time.time(),
                                                   header["correlation_id"],
                                                   header["username"],
                                                   header["ticket_num"],
                                                   header["type"],
                                                   json.dumps (payload),
                                                   'pending'])
            except Exception as e:
                logger.exception ("Exception occured while Inserting data into validator for msg {}".format (self.msg))
                raise e
                return False
        else:
            logger.error ("Message already exist in the table")
            # FIXME - may be we have to delete the old message and insert the new one
            return False
        return True

    def get_queryString(self):
           return "insert into " + self.table + " values (?,?,?,?,?,?,?,?)"



class ConsumerDataStoreDriverForGenSummary(ConsumerDataStoreDriver):
      def __init__(self, msg, table,workflow_monitor=None):
           super(ConsumerDataStoreDriverForGenSummary,self).__init__ (msg,table,workflow_monitor=None)
      

      def store(self):
        try:
		if not self.verifymsg:
		     return False
		logger.info ("Message verification succesfull")

		(header, payload) = self.get_header_payload ()
               

		ret = self.get_msg_from_datastore (header["correlation_id"], self.table)
		""" method check if message for the passed corrid in the service datastore table"""

		if not ret:
		    logger.info ("received msg is not already there in validator ,proceding to insert")
		    query_str=self.get_queryString()
		    insert_values=self._get_insert_vaues()
		    try:
			with DataStore (self.db) as dbobj:
			   return  dbobj.insert (query_str, insert_values)
		    except Exception as e:
			logger.exception ("Exception occured while Inserting data into validator for msg {}".format (self.msg))
			raise e
			return False
		else:
		    logger.error ("Message already exist in the table")
		    # FIXME - may be we have to delete the old message and insert the new one
		    return False
		return True
        except Exception as e:
             logger.errro(e,exc_info=True)
             raise e 


      def get_queryString(self):
            return "insert into " + self.table + " values (?,?,?,?,?,?,?,?,?,?,?,?)"

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




            
             
             

