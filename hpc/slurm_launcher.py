import logging
import os
import pathlib
import sys

import simple_slurm
import np_logging

np_logging.setup()
logger = logging.getLogger(__name__)

env = sys.argv[1]
python_path = f"{pathlib.Path('~').expanduser()}/miniconda3/envs/{env}/bin/python"
logger.debug(python_path)

slurm = simple_slurm.Slurm(
    cpus_per_task=1,
    partition='braintv',
    job_name='dj_upload',
    time='12:00:00',
    mem_per_cpu='2gb',
)

slurm.sbatch(cmd := f"{python_path} {' '.join(sys.argv[2:])}")
logger.debug('submitted: slurm.sbatch %s', cmd)