from __future__ import annotations
import contextlib

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
from typing import (Any, Literal, Optional,
                    Sequence,)

import np_logging
import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import fabric
import IPython.display
import ipywidgets as ipw
import numpy as np
import pandas as pd
import requests

from np_datajoint import config

logger = np_logging.getLogger(__name__)

class SessionDirNotFoundError(ValueError):
    pass

# general -----------------------------------------------------------------------------#
def checksum(path: pathlib.Path) -> str:
    hasher = hashlib.md5
    blocks_per_chunk = 128    
    multi_part_threshold_gb = 0.2
    if path.stat().st_size < multi_part_threshold_gb * 1024 ** 3:
        return hasher(path.read_bytes()).hexdigest()
    hash = hasher()
    with open(path, 'rb') as f:
        for chunk in iter(
            lambda: f.read(hash.block_size*blocks_per_chunk), b""
        ):
            hash.update(chunk)
    return hash.hexdigest()

def checksums_match(paths: Iterable[pathlib.Path]) -> bool:
    checksums = tuple(checksum(p) for p in paths)
    return all(c == checksums[0] for c in checksums)
    
def copy(src:pathlib.Path, dest:pathlib.Path):
    if not pathlib.Path(dest).parent.exists():
        pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
    attempts = 0
    if dest.exists() and dest.is_symlink():
        dest.unlink()
    while (
        True if not dest.exists() else not checksums_match((src, dest))
    ):  
        if attempts == 2:
            logger.debug(f"Failed to copy {src} to {dest} with checksum-validation after {attempts=}")
            return
        shutil.copy2(src,dest)
        attempts += 1
    logger.debug(f"Copied {src} to {dest} with checksum-validation")

def symlink(src:pathlib.Path, dest:pathlib.Path):
    if 'win' in sys.platform:
        # Remote to remote symlink creation is disabled by default
        subprocess.run('fsutil behavior set SymlinkEvaluation R2R:1')
    if not pathlib.Path(dest).parent.exists():
        pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
    if dest.is_symlink() and dest.resolve() == src.resolve():
        logger.debug(f"Symlink already exists to {src} from {dest}")
        return
    dest.unlink(missing_ok=True)
    with contextlib.suppress(FileExistsError):
        dest.symlink_to(src)
    logger.debug(f"Created symlink to {src} from {dest}")

def dir_size(path: pathlib.Path) -> int:
    """Return the size of a directory in bytes"""
    if not path.is_dir():
        raise ValueError(f"{path} is not a directory")
    dir_size = 0
    dir_size += sum(
        f.stat().st_size
        for f in pathlib.Path(path).rglob("*")
        if pathlib.Path(f).is_file()
    )
    return dir_size

def read_oebin(path: str | pathlib.Path) -> dict[str, Any]:
    return json.loads(pathlib.Path(path).read_bytes())

# local ephys-related (pre-upload) ----------------------------------------------------- #
def is_new_ephys_folder(path: pathlib.Path) -> bool:
    "Look for hallmarks of a v0.6.x Open Ephys recording"
    return bool(
        tuple(path.rglob("Record Node*"))
        and tuple(path.rglob("structure.oebin"))
        and tuple(path.rglob("settings*.xml"))
        and tuple(path.rglob("continuous.dat"))
    )

def is_valid_ephys_folder(path: pathlib.Path) -> bool:
    "Check a single dir of raw data for size, v0.6.x+ Open Ephys for DataJoint."
    if not path.is_dir():
        return False
    if not is_new_ephys_folder(path):
        return False
    if not dir_size(path) > 275 * 1024**3:  # GB
        return False
    return True



def get_raw_ephys_subfolders(path: pathlib.Path) -> list[pathlib.Path]:
    """
    Return a list of raw ephys recording folders, defined as the root that Open Ephys
    records to, e.g. `A:/1233245678_366122_20220618_probeABC`.

    Does not include the path supplied itself - only subfolders
    """

    subfolders = set()

    for f in pathlib.Path(path).rglob("*_probe*"):

        if not f.is_dir():
            continue

        if any(
            k in f.name
            for k in [
                "_sorted",
                "_extracted",
                "pretest",
                "_603810_",
                "_599657_",
                "_598796_",
            ]
        ):
            # skip pretest mice and sorted/extracted folders
            continue

        if not is_new_ephys_folder(f):
            # skip old/non-ephys folders
            continue

        if (size := dir_size(f)) and size < 1024**3 * 50:
            # skip folders that aren't above min size threshold (GB)
            continue

        subfolders.add(f)

    return sorted(list(subfolders), key=lambda s: str(s))


# - If we have probeABC and probeDEF raw data folders, each one has an oebin file:
#     we'll need to merge the oebin files and the data folders to create a single session
#     that can be processed in parallel
def get_single_oebin_path(path: pathlib.Path) -> pathlib.Path:
    """Get the path to a single structure.oebin file in a folder of raw ephys data.

    - There's one structure.oebin per Recording* folder
    - Raw data folders may contain multiple Recording* folders
    - Datajoint expects only one structure.oebin file per Session for sorting
    - If we have multiple Recording* folders, we assume that there's one
        good folder - the largest - plus some small dummy / accidental recordings
    """
    if not path.is_dir():
        raise ValueError(f"{path} is not a directory")

    oebin_paths = list(path.rglob("*structure.oebin"))

    if len(oebin_paths) > 1:
        oebin_parents = [f.parent for f in oebin_paths]
        dir_sizes = [dir_size(f) for f in oebin_parents]
        return oebin_paths[dir_sizes.index(max(dir_sizes))]

    elif len(oebin_paths) == 1:
        return oebin_paths[0]

    else:
        raise FileNotFoundError(f"No structure.oebin found in {path}")

def check_xml_files_match(paths: Sequence[pathlib.Path]) -> None:
    """Check that all xml files are identical, as they should be for
    recordings split across multiple locations e.g. A:/*_probeABC, B:/*_probeDEF
    
    Update: xml files on two nodes can be created at different times, so their date
    fields may differ. Everything else should be identical, so we can just check their size.
    """
    if not all(s == ".xml" for s in [p.suffix for p in paths]):
        raise ValueError("Not all paths are XML files")
    if not all(p.is_file() for p in paths):
        raise FileNotFoundError("Not all paths are files, or they do not exist")
    if not checksums_match(paths):
        sizes = [p.stat().st_size for p in paths]
        if not all(s == sizes[0] for s in sizes):
            raise ValueError("XML files do not match")



def create_merged_oebin_file(
    paths: Sequence[pathlib.Path], probes: Sequence[str] = config.DEFAULT_PROBES
) -> pathlib.Path:
    """Take paths to two or more structure.oebin files and merge them into one.

    For recordings split across multiple locations e.g. A:/*_probeABC, B:/*_probeDEF
    """
    if isinstance(paths, pathlib.Path):
        return paths
    if any(not p.suffix == ".oebin" for p in paths):
        raise ValueError("Not all paths are .oebin files")
    if (
        len(paths) == 1
        and isinstance(paths[0], pathlib.Path)
        and paths[0].suffix == ".oebin"
    ):
        return paths[0]

    # ensure oebin files can be merged - if from the same exp they will have the same settings.xml file
    check_xml_files_match(
        [p / "settings.xml" for p in [o.parent.parent.parent for o in paths]]
    )

    logger.debug(f"Creating merged oebin file with {probes=} from {paths}")
    merged_oebin: dict = {}
    for oebin in sorted(paths):

        with open(oebin, "r") as f:
            oebin_data = json.load(f)

        for key in oebin_data:

            if merged_oebin.get(key, None) == oebin_data[key]:
                continue

            # 'continuous', 'events', 'spikes' are lists, which we want to concatenate across files
            if isinstance(oebin_data[key], list):
                for item in oebin_data[key]:
                    if merged_oebin.get(key, None) and item in merged_oebin[key]:
                        continue
                    # skip probes not specified in input args (ie. not inserted)
                    if "probe" in item.get(
                        "folder_name", ""
                    ):  # one is folder_name:'MessageCenter'
                        if not any(
                            f"probe{letter}" in item["folder_name"] for letter in probes
                        ):
                            continue
                    if merged_oebin.get(key, None) is None:
                        merged_oebin[key] = [item]
                    else:
                        merged_oebin[key].append(item)

    if not merged_oebin:
        raise ValueError("No data found in structure.oebin files")
    merged_oebin_path = pathlib.Path(tempfile.gettempdir()) / "structure.oebin"
    with open(str(merged_oebin_path), "w") as f:
        json.dump(merged_oebin, f, indent=4)

    return merged_oebin_path


# datajoint-related --------------------------------------------------------------------


def wait_on_process(sec=3600, msg="Still processing..."):
    fmt = "%a %H:%M"  # e.g. Mon 12:34
    file = sys.stdout
    time_now = time.strftime(fmt, time.localtime())
    time_next = time.strftime(fmt, time.localtime(time.time() + float(sec)))
    file.write("\n%s: %s\nNext check: %s\r" % (time_now, msg, time_next))
    file.flush()
    time.sleep(sec)


def add_new_ks_paramset(
    params: dict,
    description: str, 
    clustering_method: Literal['kilosort2','kilosort2.5','kilosort3'], 
):

    def dict_to_uuid(key):
        "Given a dictionary `key`, returns a hash string as UUID."
        hash = hashlib.md5()
        for k, v in sorted(key.items()):
            hash.update(str(k).encode())
            hash.update(str(v).encode())
        return uuid.UUID(hex=hash.hexdigest())

    param_dict = {
        "paramset_idx": max(config.AVAILABLE_PARAMSET_IDX)+1,
        "params": params,
        "paramset_desc": description,
        "clustering_method": clustering_method,
        "param_set_hash": dict_to_uuid(
            {**params, "clustering_method": clustering_method}
        ),
    }
    config.DJ_EPHYS.ClusteringParamSet.insert1(param_dict, skip_duplicates=True)


def get_clustering_parameters(
    paramset_idx: int = config.DEFAULT_KS_PARAMS_INDEX,
) -> tuple[str, dict]:
    "Get description and dict of parameters from paramset_idx."
    return (config.DJ_EPHYS.ClusteringParamSet & {"paramset_idx": paramset_idx}).fetch1(
        "params"
    )



def is_hab(session_folder: pathlib.Path) -> Optional[bool]:
    "Return True/False, or None if not enough info to determine"
    for platform_json in session_folder.glob("*_platformD1.json"):
        if "habituation" in platform_json.read_text():
            return True
        return False
    return None

