import pathlib
import logging

import np_logging
import np_datajoint
from np_datajoint.classes import *
from np_datajoint.comparisons import *
from np_datajoint.config import *
from np_datajoint.utils import *

logger = logging.getLogger('root')
logger.handlers = []
logger = np_logging.getLogger()

RUNNING_ON_HPC = True # prevent transfer of job to HPC - won't work with DRPilot class currently

for root in DRPilot.storage_dirs:
    for path in root.glob('DRpilot*'):
        session = DRPilot(path)
        probe_dirs = tuple(session.path.glob('*_probe*'))
        if not probe_dirs:
            print(f'No probe dirs found for {session.path.name}')
            continue

        session.upload(probe_dirs)