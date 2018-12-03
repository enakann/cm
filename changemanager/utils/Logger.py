import logging
import yaml
import logging.config
#from . import export
import sys
import os
from etc.app_configs import PROJECT_ROOT
    

LOG_PATH=os.path.join(PROJECT_ROOT,"logs")
CONFIG_PATH=os.path.join(PROJECT_ROOT,"etc")

#@export
class Logger:
    def __init__(self, name=None, config_file=None):
        self.name=name
        self.logger=None
        self.config_file=config_file
        self.log_config_path=os.path.join(CONFIG_PATH,self.config_file)
        

    def get_logger(self):
        try:
           with open (self.log_config_path) as f:
               config = yaml.safe_load(f)
               config=self._get_new_config(config)
        except Exception as e:
            raise e
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(self.name)
        return self.logger
    
    def _get_new_config(self,config):
        for k,v in config['handlers'].items():
            if 'filename' in v:
                v['filename']=os.path.join(LOG_PATH,v['filename'])
        #print(config['handlers'])
        return config
        
    


if __name__ == '__main__':
    logger_obj=Logger("kannan","log_config.yml")
    logger=logger_obj.get_logger()
    logger.info("info test")
    logger.debug ("debug test")
    logger.error ("error test")
    logger.exception("exception test")
    

