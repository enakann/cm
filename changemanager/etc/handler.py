import os
import yaml
import logging
import logging.config
def logmaker():
    path = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(path, 'bot.log')
    return logging.FileHandler(path)

def main():
    # The file's path
    path = os.path.dirname(os.path.realpath(__file__))

    # Config file relative to this file
    loggingConf = open('{0}\\log_config.yml'.format(path), 'r')
    logging.config.dictConfig(yaml.load(loggingConf))
    loggingConf.close()
    logger = logging.getLogger('aggregator')
    logger.debug('Hello, world!')

if __name__ == '__main__':
    main()

