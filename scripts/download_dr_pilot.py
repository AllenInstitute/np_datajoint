import np_logging
logger = np_logging.getLogger()

import np_datajoint

paramset_idx = 1

all_sorted = np_datajoint.DRPilot.sorted_sessions(paramset_idx)
for session in all_sorted:
    try:
        session.download(paramset_idx)
    except FileNotFoundError as e:
        logger.exception(e)
        continue