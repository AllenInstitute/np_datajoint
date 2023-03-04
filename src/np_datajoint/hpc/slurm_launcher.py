"Simplified by running this script from the env required for the job - just needs `simple_slurm`."
import argparse
import logging
import os
import pathlib
import sys

import simple_slurm

try:
    import np_logging
except ImportError:
    pass # nice to have but not required

logger = logging.getLogger(__name__)

slurm = simple_slurm.Slurm(
    cpus_per_task=1,
    partition='braintv',
    job_name='dj_upload',
    time='14:00:00',
    # output=f'~/np_datajoint/logs/{simple_slurm.Slurm.JOB_ARRAY_MASTER_ID}_{simple_slurm.Slurm.JOB_ARRAY_ID}.out',
    mem_per_cpu='2gb',
)

# parser = argparse.ArgumentParser()
# parser.add_argument('env', help='path to the environment to run the slurm job in')
# args = vars(parser.parse_args())
# env = args.pop('env', None)
# if sys.argv[1] == '--env':
#     env = sys.argv[2]
#     args = sys.argv[3:]
# else:
#     args = sys.argv[1:]

slurm.sbatch(cmd := f"python {' '.join(sys.argv[1:])}")
logger.info('submitted: slurm.sbatch %s', cmd)
print(cmd)