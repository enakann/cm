import sqlite3
import os
import logging
import sys

# from utils import export


class NoDataAvailableException (Exception):
    pass


class DBDoesntExistException (Exception):
    pass


logger = logging.getLogger ("kannan")


def logger_with_exception_handling(msg, *exp):
    def decorator(f):
        def inner(*args, **kwargs):
            ret = None
            try:
                logger.info (msg)
                ret = f (*args, **kwargs)
            except exp as e:
                print (e)
                logger.exception (e)
            return ret
        
        return inner
    
    return decorator


# @export
class DataStore:
    """
    Generic Datastore Class for Firms2
    """
    
    def __init__(self, db):
        self.db = db
        self.cur = None
        self.conn = None
    
    def __enter__(self):
        self.connect ()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close ()
    
    @logger_with_exception_handling ("Connecting to db", Exception)
    def connect(self):
        """ connect to db"""
        if not self.db:
            raise DBDoesntExistException (self.db)
        self.conn = sqlite3.connect (self.db)
        self.cur = self.conn.cursor ()
    
    def insert(self, query, data):
        """ insert data into table"""
        
        logger.info ("Insert called :{} with {}".format (query, data))
        try:
            self.cur.execute (query, data)
            self.conn.commit ()
            return True
        except Exception as e:
            logger.error ("Insert of {} failed due to {}".format (data, e),exc_info=True)
            return False
    
    def insert_rows(self, query, data):
        """ inserting multiple rows """
        
        logger.info ("Insert called :{} with {}".format (query, data))
        try:
            self.cur.executemany (query, data)
            self.conn.commit ()
        except Exception as e:
            logger.exception ("Insert of {} failed due to {}".format (data, e))
            return False
        return True
    
    def select_data(self, query, bind=None):
        """ select query  """
        data = list ()
        try:
            if bind:
               self.cur.execute (query, bind)
            else:
               self.cur.execute (query)
            data = self.cur.fetchall ()
            #logger.info ("following items retrieved {}".format (data))
            if not data:
                raise NoDataAvailableException (query, bind, data)
        except Exception as e:
            logger.exception ("Select failed :{}-{} due to {}".format (query, bind, e))
            return False
        return data

    def update(self,query,bind=None):
        logger.info("following query -> {} is called for -->{}".format(query,bind))
        try:
            if bind:
                self.cur.execute (query, bind)
            else:
                self.cur.execute (query)
            self.conn.commit()
        except Exception as e:
            raise e
    
    def delete(self, query, bind):
        logger.info ("delete called :{} with {}".format (query, bind))
        try:
            self.cur.execute (query, bind)
            self.conn.commit ()
        except Exception as e:
            logger.exception ("delete : {}  of {} failed due to {}".format (query, bind, e))
            return False
        return True
    
#    @logger_with_exception_handling ("Closing connection", Exception)
    def close(self):
        self.conn.close ()
    
 #   @logger_with_exception_handling ("creating table", Exception)
"""    def create_table(self,tablename):
        
        self.cur.execute (
             "CREATE TABLE IF NOT EXISTS " +tablename+" (id integer primary key autoincrement,insert_date DATE,correlation_id varchar(10),
                                                         username varchar(10),ticket_no varchar(10),type varchar(10),payload data,status varchar(10))"""


def get_desc():
   con = sqlite3.connect('test.db')
   cursor = con.cursor()
   cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
   print(cursor.fetchall())

def req_sel():
   with DataStore ("test.db") as dbobj:
        ret = dbobj.select_data("select *  from requester_updater ")
        for i in ret:
           print(i)

def val_sel():
   with DataStore ("test.db") as dbobj:
        ret = dbobj.select_data("select *  from validator ")
        print(ret)


def req():
   with DataStore ("test.db") as dbobj:
        ret = dbobj.update("delete  from requester_updater ")
        print(ret)

def val():
     with DataStore ("test.db") as dbobj:
        ret = dbobj.update("delete  from validator ")
        print(ret)




def insert():
   with DataStore ("test.db") as dbobj:
     ret=dbobj.insert ("insert into contacts values(?,?,?,?)", [3, "divi", "nav@gmail.com", 1111])   
     print(ret)


if __name__ == '__main__':
    logger = logging.getLogger(name='mylogger')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
            '[%(asctime)s:%(module)s:%(lineno)s:%(levelname)s] %(message)s'
    )
    streamhandler = logging.StreamHandler(sys.stdout)
    streamhandler.setLevel(logging.WARNING)
    streamhandler.setFormatter(formatter)
    logger.addHandler(streamhandler)
    filehandler = logging.FileHandler('mypython.log')
    filehandler.setLevel(logging.DEBUG)
    filehandler.setFormatter(formatter)
    logger.addHandler(filehandler)
    #query_str='update requester_updater set status="completed" where correlation_id="navi"'
    ls=(None,'1989-12-09',
          '11118',
          'navi',
          'srno1',
       'validator',
       '{"protocol": "tcp", "port": 22, "input-row-id": 1, "source": "10.10.10.1", "destination": "10.172.2.1"}',
       'pending'
       )
    with DataStore ("cm.db") as dbobj:
            #ret = dbobj.update(query_str)
            dbobj.insert("insert into generate values(?,?,?,?,?,?,?,?)", ls)
            



    #req_sel()
    #val_sel()

    
    #req()
    #req_sel()
    #val()
   # with DataStore ("test.db") as dbobj:
   #      ret = dbobj.update("update requester_updater set status='completed' where correlation_id='kamal'")
    

    #req_sel()
    #get_desc()
        # dbobj.create_table()
    
    # d = DataStore("test.db")
    # d.create_table()
    # d.insert("insert into contacts values(?,?,?,?,?)", [3, "navi", "kannan", "nav@gmail.com", 1111])
    # ret = d.select_data("select * from contacts where first_name=:1", ("navi",))
    # print(ret)

    # with DataStore("test.db") as dbobj:
    # ret=dbobj.insert("insert into contacts values(?,?,?,?,?)", [3, "navi", "kannan", "nav@gmail.com", 1111])
    # print(ret)
    #  dbobj.insert ("insert into contacts values(?,?,?,?,?)", [3, "divi", "kannan", "nav@gmail.com", 1111])
    #   print(dbobj.select_data ("select * from contacts where first_name=:1", ("divi",)))
    # ret = dbobj.select_data ("select * from contacts where first_name=:1", ("navi",))
    # dbobj.delete("delete from contacts where first_name=:1",("divi",))
    # ret = dbobj.select_data ("select * from contacts where first_name=:1", ("divi",))
    # print(ret)
    # if ret:
    #    print("data is there")
    # else:
    #    print("data is not there")
        
        
    

