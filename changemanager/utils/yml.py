import os
import yaml

from etc.app_configs import PROJECT_ROOT
#PROJECT_ROOT="/home/navi/Desktop/changemanager"
CONFIG_PATH=os.path.join(PROJECT_ROOT,"etc")


class YAML:
    def __init__(self,filename=None,name=None):
        self.filenane=filename
        self.name=name
        self.config_path=os.path.join(CONFIG_PATH,self.filenane)
    def get_config(self):
        try:
           with open (self.config_path) as f:
               config = yaml.safe_load(f)
               config=self._get_new_config(config)
        except Exception as e:
            raise e
        return config
        
    
    def _get_new_config(self,config):
        return config[self.name]


if __name__ == '__main__':
    yml=YAML("consumer_config.yml","approver_cm")
    conf=yml.get_config()
    print(conf)

