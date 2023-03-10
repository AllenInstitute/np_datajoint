import pathlib

import np_logging
import np_datajoint
from np_datajoint.classes import *
from np_datajoint.comparisons import *
from np_datajoint.config import *
from np_datajoint.utils import *

logger = np_logging.getLogger()
logger.setLevel('INFO')
np_logging.getLogger('botocore').setLevel(logging.WARNING)
RUNNING_ON_HPC = True # prevent transfer of job to HPC - won't work with DRPilot class currently

while True:
    with contextlib.suppress(Exception):
        for root in DRPilot.storage_dirs[1:]:
            for path in root.glob('DRpilot*'):
                session = DRPilot(path)
                probe_dirs = tuple(session.path.glob('*_probe*'))
                probes = ''.join(_ for _ in DEFAULT_PROBES for d in probe_dirs if next(d.rglob(f'*.Probe{_}-*'), None))
                
                if not probe_dirs:
                    logger.info(f'No probe dirs found for {session.path.name}')
                    continue
                logger.info(f'Uploading {session.path.name}, probes {probes}...')

                session.upload(probe_dirs, probes)
        logger.info(f'Finished uploading all sessions')
        break