import np_session
import np_logging
from np_datajoint import *

logger = np_logging.getLogger()

logger.setLevel('INFO')

# DRPilot('DRpilot_644864_20230131').download()


DRPilot('DRpilot_626791_20220815').upload()
DRPilot('DRpilot_626791_20220816').upload()
DRPilot('DRpilot_626791_20220817').upload()