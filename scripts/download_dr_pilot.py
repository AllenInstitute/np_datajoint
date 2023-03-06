import np_logging
logger = np_logging.getLogger()

import np_datajoint
from np_datajoint.classes import *
from np_datajoint.comparisons import *
from np_datajoint.config import *
from np_datajoint.utils import *

paramset_idx = 1

all_sorted = DRPilot.sorted_sessions(paramset_idx)
for session in all_sorted:
    try:
        session.download(paramset_idx)
    except FileNotFoundError as e:
        logger.exception(e)
        continue