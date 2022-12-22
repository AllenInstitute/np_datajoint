from __future__ import annotations

import datetime
import functools
import hashlib
import itertools
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from collections import defaultdict
from collections.abc import Iterable
from typing import (Final, Generator, List, Literal, MutableSequence, Optional,
                    Sequence, Set, Tuple)

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import fabric
import IPython
import ipywidgets as ipw
import matplotlib.pyplot as plt
import np_logging
import numpy as np
import pandas as pd
import requests
import seaborn as sns

for log_name in ("Primary", "datajoint"):  # both datajoint packages
    logging.getLogger(log_name).disabled = True
    # logging.getLogger(log_name).propagate = False
    

# config ------------------------------------------------------------------------------
# get zookeeper config via np_logging
zk_config = np_logging.fetch_zk_config("/projects/np_datajoint/defaults/configuration")

# configure datajoint session
dj.config.update(
    zk_config["datajoint"]
)  # dj.config is a custom class behaving as a dict - don't directly assign a dict

S3_SESSION = dj_auth.Session(
    aws_account_id=zk_config["djsciops"]["aws"]["account_id"],
    s3_role=zk_config["djsciops"]["s3"]["role"],
    auth_client_id=zk_config["djsciops"]["djauth"]["client_id"],
    auth_client_secret=zk_config["djsciops"]["djauth"]["client_secret"],
)
S3_BUCKET: str = zk_config["djsciops"]["s3"]["bucket"]

DJ_INBOX: str = zk_config["sorting"][
    "remote_inbox"
]  # f"mindscope_dynamic-routing/inbox"
DJ_OUTBOX: str = zk_config["sorting"][
    "remote_outbox"
]  # f"mindscope_dynamic-routing/inbox"
LOCAL_INBOX = pathlib.Path(zk_config["sorting"]["local_inbox"])

BOTO3_CONFIG: dict = zk_config["djsciops"]["boto3"]

# create virtual datajoint modules for querying tables ---------------------------------- #
DJ_SUBJECT = dj.VirtualModule("subject", "mindscope_dynamic-routing_subject")
DJ_SESSION = dj.VirtualModule("session", "mindscope_dynamic-routing_session")
DJ_EPHYS = dj.VirtualModule("ephys", "mindscope_dynamic-routing_ephys")
DJ_PROBE = dj.VirtualModule("probe", "mindscope_dynamic-routing_probe")

DEFAULT_PROBES = "ABCDEF"

DEFAULT_KS_PARAMS_INDEX: int = zk_config["sorting"][
    "default_kilosort_parameter_set_index"
]  # 1=KS 2.0, 2=KS 2.5
AVAILABLE_PARAMSETS: np.ndarray = DJ_EPHYS.ClusteringParamSet.fetch()
AVAILABLE_PARAMSET_IDX = tuple(s[0] for s in AVAILABLE_PARAMSETS)

RUNNING_ON_HPC: bool = bool(
    re.match("n[0-9]{,}|hpc-login", HOSTNAME := platform.node())
)  # hpc nodes named 'n73'
logging.debug("%s Running on HPC: %s", HOSTNAME, RUNNING_ON_HPC)
RUNNING_ON_ACQ: bool = bool(
    re.match("np[0-9]{1}-acq", os.getenv("aibs_comp_id", '').lower())
)

NPEXP_PATH = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")