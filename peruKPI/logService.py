import logging

def logfunc(f):
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    log_colors_config = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red',
    }
    handler = logging.FileHandler(f)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    chlr = logging.StreamHandler()  # 输出到控制台的handler
    chlr.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(chlr)
    return logger
