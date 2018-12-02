"""from utils import DataStore
from utils import Logger
logger_obj=Logger("aggregator","log_config.yml")
logger=logger_obj.get_logger()

logger.info("Starting database transactions")

with DataStore ("utils/test.db") as dbobj:
    ret = dbobj.select_data ("select * from contacts where first_name=:1", ("navi",))
    print (ret)"""

from __init__ import PROJECT_ROOT,MSG_TYPE_TABLE_MAPPING,TABLE_DATA_STORE_DRIVER_CLASS

print(MSG_TYPE_TABLE_MAPPING)
print(TABLE_DATA_STORE_DRIVER_CLASS )
