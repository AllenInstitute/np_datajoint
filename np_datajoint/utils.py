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
from typing import (Generator, List, Literal, MutableSequence, Optional,
                    Sequence, Set, Tuple)

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import fabric
import IPython
import ipywidgets as ipw
import np_logging
import numpy as np
import pandas as pd
import requests

from np_datajoint import classes, config

sorting_status_last_checked: dict[Optional[int], datetime.datetime] = dict()

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
            logging.debug(f"Failed to copy {src} to {dest} with checksum-validation after {attempts=}")
            return
        shutil.copy2(src,dest)
        attempts += 1
    logging.debug(f"Copied {src} to {dest} with checksum-validation")

def symlink(src:pathlib.Path, dest:pathlib.Path):
    if 'win' in sys.platform:
        # Remote to remote symlink creation is disabled by default
        subprocess.run('fsutil behavior set SymlinkEvaluation R2R:1')
    if not pathlib.Path(dest).parent.exists():
        pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        if dest.is_symlink() and dest.resolve() == src.resolve():
            logging.debug(f"Symlink already exists to {src} from {dest}")
            return
        dest.unlink()
    dest.symlink_to(src)
    logging.debug(f"Created symlink to {src} from {dest}")
                
def get_session_folder(path: str | pathlib.Path) -> str | None:
    """Extract [8+digit session ID]_[6-digit mouse ID]_[8-digit date
    str] from a string or path"""
    session_reg_exp = R"[0-9]{8,}_[0-9]{6}_[0-9]{8}"

    session_folders = re.findall(session_reg_exp, str(path))
    if session_folders:
        if not all(s == session_folders[0] for s in session_folders):
            logging.debug(
                f"Mismatch between session folder strings - file may be in the wrong folder: {path}"
            )
        return session_folders[0]
    return None

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


def is_valid_pair_split_ephys_folders(paths: Sequence[pathlib.Path]) -> bool:
    "Check a pair of dirs of raw data for size, matching settings.xml, v0.6.x+ to confirm they're from the same session and meet expected criteria."
    if not paths:
        return False

    if any(not is_valid_ephys_folder(path) for path in paths):
        return False

    check_session_paths_match(paths)
    check_xml_files_match([tuple(path.rglob("settings*.xml"))[0] for path in paths])

    size_difference_threshold_gb = 2
    dir_sizes_gb = tuple(round(dir_size(path) / 1024**3) for path in paths)
    diffs = (abs(dir_sizes_gb[0] - size) for size in dir_sizes_gb)
    if not all(diff <= size_difference_threshold_gb for diff in diffs):
        print(
            f"raw data folders are not within {size_difference_threshold_gb} GB of each other"
        )
        return False

    return True


def get_raw_ephys_subfolders(path: pathlib.Path) -> List[pathlib.Path]:
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

def check_xml_files_match(paths: Sequence[pathlib.Path]):
    """Check that all xml files are identical, as they should be for
    recordings split across multiple locations e.g. A:/*_probeABC, B:/*_probeDEF"""
    if not all(s == ".xml" for s in [p.suffix for p in paths]):
        raise ValueError("Not all paths are XML files")
    if not all(p.is_file() for p in paths):
        raise FileNotFoundError("Not all paths are files, or they do not exist")
    if not checksums_match(paths):
        raise ValueError("XML files do not match")

def check_session_paths_match(paths: Sequence[pathlib.Path]):
    sessions = [get_session_folder(path) for path in paths]
    if any(not s for s in sessions):
        raise ValueError(
            "Paths supplied must be session folders: [8+digit lims session ID]_[6-digit mouse ID]_[6-digit datestr]"
        )
    if not all(s and s == sessions[0] for s in sessions):
        raise ValueError("Paths must all be for the same session")

def get_local_remote_oebin_paths(
    paths: pathlib.Path | Sequence[pathlib.Path],
) -> Tuple[Sequence[pathlib.Path], pathlib.Path]:
    """
    A `structure.oebin` file specifies the relative paths for each probe's files in a
    raw data dir. For split recs, a different oebin file lives in each dir (ABC/DEF).

    To process a new session on DataJoint, a single structure.oebin file needs to be uploaded,
    and its dir on the server specified in the SessionDirectory table / `session_dir` key.
    
    Input one or more paths to raw data folders that exist locally for a single
    session, and this func returns the paths to the `structure.oebin` file for each local folder,
    plus the expected relative path on the remote server for a single combined .oebin file.

    We need to upload the folder containing a `settings.xml` file - two
    folders above the structure.oebin file - but with only the subfolders returned from this function.
    """

    if isinstance(paths, pathlib.Path):
        paths = (paths,)

    if not any(is_new_ephys_folder(path) for path in paths):
        raise ValueError("No new ephys folder found in paths")
    check_session_paths_match(paths)

    local_session_paths: set[pathlib.Path] = set()
    for path in paths:

        ephys_subfolders = get_raw_ephys_subfolders(path)

        if ephys_subfolders and len(paths) == 1:
            # parent folder supplied: we want to upload its subfolders
            local_session_paths.update(e for e in ephys_subfolders)
            break  # we're done anyway, just making this clear

        if ephys_subfolders:
            logging.warning(
                f"Multiple subfolders of raw data found in {path} - expected a single folder."
            )
            local_session_paths.update(e for e in ephys_subfolders)
            continue

        if is_new_ephys_folder(path):
            # single folder supplied: we want to upload this folder
            local_session_paths.add(path)
            continue
    
    if len(local_session_paths) == 1:
        if len(record_nodes := tuple(local_session_paths.pop().glob('Record Node*'))) > 1:
            local_session_paths.update(record_nodes)
            
    local_oebin_paths = sorted(
        list(set(get_single_oebin_path(p) for p in local_session_paths)),
        key=lambda s: str(s),
    )

    local_session_paths_for_upload = [p.parent.parent.parent for p in local_oebin_paths]
    # settings.xml file should be the same for _probeABC and _probeDEF dirs
    if len(local_session_paths_for_upload) > 1:
        check_xml_files_match([p / "settings.xml" for p in local_session_paths_for_upload])

    # and for the server we just want to point to the oebin file from two levels above -
    # shouldn't matter which oebin path we look at here, they should all have the same
    # relative structure
    remote_oebin_path = local_oebin_paths[0].relative_to(
        local_oebin_paths[0].parent.parent.parent
    )

    return local_oebin_paths, remote_oebin_path


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

    logging.debug(f"Creating merged oebin file with {probes=} from {paths}")
    merged_oebin: dict = {}
    for oebin in sorted(paths):

        with open(oebin, "r") as f:
            oebin_data = json.load(f)

        for key in oebin_data:

            if merged_oebin.get(key, None) == oebin_data[key]:
                continue

            # 'continuous', 'events', 'spikes' are lists, which we want to concatenate across files
            if isinstance(oebin_data[key], List):
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
) -> Tuple[str, dict]:
    "Get description and dict of parameters from paramset_idx."
    return (config.DJ_EPHYS.ClusteringParamSet & {"paramset_idx": paramset_idx}).fetch1(
        "params"
    )


def all_sessions() -> dj.Table:
    "Correctly formatted sessions on Datajoint."
    logging.debug("Fetching all correctly-formatted sessions from DataJoint server")
    all_sessions = config.DJ_SESSION.Session.fetch()
    session_str_match_on_datajoint = lambda x: bool(
        get_session_folder(f"{x[1]}_{x[0]}_{x[2].strftime('%Y%m%d')}")
    )
    return (
        config.DJ_SESSION.Session
        & all_sessions[list(map(session_str_match_on_datajoint, all_sessions))]
    )


def get_sorting_status_all_sessions(
    paramset_idx: Optional[int] = config.DEFAULT_KS_PARAMS_INDEX,
) -> dj.Table:
    """Summary of processing for all probes across all sessions, with optional restriction on paramset_idx - modified from Thinh@DJ.
    - `paramset_idx = None` returns all probes
    - `paramset_idx = -1` returns probes with no paramset_idx (ie. haven't started
      processing)

    Table is returned, can be further restricted with queries.
    """

    def paramset_restricted(schema: dj.schemas.Schema) -> dj.schemas.Schema:
        "Restrict table to sessions that used one or more paramset_idx."
        if paramset_idx is None:
            return schema
        if -1 == paramset_idx:
            return schema & {"paramset_idx": None}
        return schema & {"paramset_idx": paramset_idx}

    logging.debug(
        f'Restricting processing status summary to sessions with paramset_idx={paramset_idx if paramset_idx is not None else "all"}'
    )

    @functools.cache
    def _get_sorting_status_all_sessions(paramset_idx) -> dj.Table:
        "Calling all these tables is slow (>10s), so cache the result."
        sorting_status_last_checked[paramset_idx] = datetime.datetime.now()

        session_process_status = all_sessions()

        session_process_status *= config.DJ_SESSION.Session.aggr(
            config.DJ_EPHYS.ProbeInsertion,
            probes="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            config.DJ_EPHYS.EphysRecording,
            ephys="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            config.DJ_EPHYS.LFP, lfp="count(insertion_number)", keep_all_rows=True
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            paramset_restricted(config.DJ_EPHYS.ClusteringTask),
            task="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            paramset_restricted(config.DJ_EPHYS.Clustering),
            clustering="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            paramset_restricted(config.DJ_EPHYS.QualityMetrics),
            metrics="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            paramset_restricted(config.DJ_EPHYS.WaveformSet),
            waveform="count(insertion_number)",
            keep_all_rows=True,
        )
        session_process_status *= config.DJ_SESSION.Session.aggr(
            paramset_restricted(config.DJ_EPHYS.WaveformSet),
            curated="count(insertion_number)",
            pidx_done='GROUP_CONCAT(insertion_number SEPARATOR ", ")',
            keep_all_rows=True,
        )
        return session_process_status.proj(
            ..., all_done="probes > 0 AND waveform = task"
        )

    if sorting_status_last_checked.get(
        paramset_idx, datetime.datetime.now()
    ) < datetime.datetime.now() - datetime.timedelta(hours=1):
        logging.debug("Clearing cache for _get_sorting_status_all_sessions")
        _get_sorting_status_all_sessions.cache_clear()
    return _get_sorting_status_all_sessions(paramset_idx)


def sorting_summary(
    paramset_idx: Optional[int] = config.DEFAULT_KS_PARAMS_INDEX,
) -> pd.DataFrame:
    """Summary of processing for all probes across all sessions, with optional restriction on `paramset_idx` - modified from Thinh@DJ.
    - `paramset_idx = None` returns all probes
    - `paramset_idx = -1` returns probes with no paramset_idx (ie. haven't started
      processing)
    """
    df = pd.DataFrame(get_sorting_status_all_sessions(paramset_idx))
    # make new 'session' column that matches our local session folder names
    session_str_from_datajoint_keys = (
        lambda x: x.session_id.astype(str)
        + "_"
        + x.subject
        + "_"
        + x.session_datetime.dt.strftime("%Y%m%d")
    )
    df = df.assign(session=session_str_from_datajoint_keys)
    # filter for sessions with correctly formatted session/mouse/date keys
    df = df.loc[~(pd.Series(map(get_session_folder, df.session)).isnull())]
    df.set_index("session", inplace=True)
    df.sort_values(by="session", ascending=False, inplace=True)
    # remove columns that were concatenated into the new 'session' column
    df.drop(columns=["session_id", "subject", "session_datetime", "lfp"], inplace=True)
    return df


def sorted_sessions(*args, **kwargs) -> Iterable[classes.DataJointSession]:
    df = sorting_summary(*args, **kwargs)
    yield from (
        classes.DataJointSession(session) for session in df.loc[df["all_done"] == 1].index
    )

def update_metrics_csv_with_missing_columns(path_or_session_folder: str | pathlib.Path, probe_idx: int, paramset_idx: int):
    probe = classes.ProbeDataJoint(session=path_or_session_folder, probe_letter=chr(probe_idx + ord('A')), paramset_idx=paramset_idx)
    if 'quality' in pd.read_csv(probe.metrics_csv).columns:
        logging.debug(f'{probe.metrics_csv} already contains columns added from DataJoint tables')
        return
    path = str(probe.metrics_csv)
    probe.metrics_df.to_csv(path)
    logging.debug(f'updated {probe.metrics_csv} with missing columns from DataJoint tables')

def copy_files_from_raw_to_sorted(path_or_session_folder: str | pathlib.Path, probe_idx: int, paramset_idx: int, make_symlinks=False, original_ap_continuous_dat=True):
    """Copy/rename/modify files to recreate extracted folders from Open Ephys
    pre-v0.6.
    
    Instead of making duplicate copies of files, symlinks can be made wherever possible.
    Lims upload copy utility won't follow symlinks, so the links should be made real
    before upload (possibly by running this again with symlinks disabled).
    
    If we skipped download of large files from DataJoint (default behavior), we have no AP
    `continuous.dat` file available for generating probe noise plots in QC.
    As an alternative, we can use the original, pre-median-subtraction file from the
    raw data folder (always symlinked due to size, and lims upload doesn't apply).  
    """
    self = classes.DataJointSession(path_or_session_folder)
    probe = chr(ord('A')+probe_idx)
    paths = self.get_raw_ephys_paths(probes = probe)
    local_oebin_paths, *_ = get_local_remote_oebin_paths(paths)
    assert len(local_oebin_paths) == 1
        
    raw: pathlib.Path = local_oebin_paths[0].parent
    sorted: pathlib.Path = self.dj_sorted_local_probe_path(probe_idx, paramset_idx)
    
    # Copy small files --------------------------------------------------------------------- #
    src_dest = []
    src_dest.append((
        raw / f"events/Neuropix-PXI-100.Probe{probe}-AP/TTL/states.npy",
        sorted / f"events/Neuropix-PXI-100.0/TTL_1/channel_states.npy",
    ))
    src_dest.append((
        raw / f"events/Neuropix-PXI-100.Probe{probe}-AP/TTL/sample_numbers.npy",
        sorted / f"events/Neuropix-PXI-100.0/TTL_1/event_timestamps.npy",
    ))
    src_dest.append((
        raw / f"events/Neuropix-PXI-100.Probe{probe}-AP/TTL/sample_numbers.npy",
        sorted / f"events/Neuropix-PXI-100.0/TTL_1/sample_numbers.npy",
    ))
    src_dest.append((
        raw / f"events/Neuropix-PXI-100.Probe{probe}-AP/TTL/full_words.npy",
        sorted / f"events/Neuropix-PXI-100.0/TTL_1/full_words.npy",
    ))
    src_dest.append((
        raw / f"continuous/Neuropix-PXI-100.Probe{probe}-AP/timestamps.npy",
        sorted / f"continuous/Neuropix-PXI-100.0/ap_timestamps.npy",
    ))
    src_dest.append((
        raw / f"continuous/Neuropix-PXI-100.Probe{probe}-LFP/continuous.dat",
        sorted / f"continuous/Neuropix-PXI-100.1/continuous.dat",
    ))
    src_dest.append((
        raw / f"continuous/Neuropix-PXI-100.Probe{probe}-LFP/timestamps.npy",
        sorted / f"continuous/Neuropix-PXI-100.1/lfp_timestamps.npy",
    ))
    
    logging.debug(f"{self.session_folder} probe{probe}: copying and renaming selected files from original raw data dir to downloaded sorted data dir")
    for src, dest in src_dest:
        symlink(src, dest) if make_symlinks else copy(src, dest)
                    
    # Fix Open Ephys v0.6.x event timestamps ----------------------------------------------- #
    # see https://gist.github.com/bjhardcastle/e972d59f482a549f312047221cd8eccb
    # check we haven't already applied the operation
    original = raw / f"events/Neuropix-PXI-100.Probe{probe}-AP/TTL/sample_numbers.npy"
    modified = sorted / f"events/Neuropix-PXI-100.0/TTL_1/event_timestamps.npy"
    if not modified.exists() or modified.is_symlink() or checksums_match((original, modified)):
        try:
            modified.unlink()
            modified.touch()
        except OSError:
            pass
        logging.debug(f"{self.session_folder} probe{probe}: adjusting `sample_numbers.npy` from OpenEphys and saving as `event_timestamps.npy`")
    
        src = raw / f"continuous/Neuropix-PXI-100.Probe{probe}-AP/sample_numbers.npy"
        continuous_sample_numbers = np.load(src, mmap_mode='r')
        first_sample = continuous_sample_numbers[0]
        
        event_timestamps = np.load(original.open('rb'))
        event_timestamps -= first_sample
        with modified.open('wb') as f:
            np.save(f, event_timestamps)
    
    # Create symlink to original AP data sans median-subtraction ----------------------------- #
    if original_ap_continuous_dat:
        src_dest = []
        src_dest.append((
            raw / f"continuous/Neuropix-PXI-100.Probe{probe}-AP/continuous.dat",
            sorted / f"continuous/Neuropix-PXI-100.0/continuous.dat",
        ))
        logging.debug(f"{self.session_folder} probe{probe}: copying original AP continuous.dat to downloaded sorted data dir")
        for src, dest in src_dest:
            symlink(src, dest)

def database_diagram() -> IPython.display.SVG:
    diagram = (
        dj.Diagram(config.DJ_SUBJECT.Subject)
        + dj.Diagram(config.DJ_SESSION.Session)
        + dj.Diagram(config.DJ_PROBE)
        + dj.Diagram(config.DJ_EPHYS)
    )
    return diagram.make_svg()


def is_hab(session_folder: pathlib.Path) -> Optional[bool]:
    "Return True/False, or None if not enough info to determine"
    for platform_json in session_folder.glob("*_platformD1.json"):
        if "habituation" in platform_json.read_text():
            return True
        return False
    return None

def session_upload_from_acq_widget() -> ipw.AppLayout:

    sessions = []
    if config.RUNNING_ON_ACQ:
        folders = get_raw_ephys_subfolders(pathlib.Path("A:")) + get_raw_ephys_subfolders(
            pathlib.Path("B:")
        )
        sessions = [
            get_session_folder(folder) 
            for folder in folders 
            if get_session_folder(folder) and is_new_ephys_folder(folder)
        ]
        for session in sessions:
            if sessions.count(session) < 2:
                sessions.remove(session) # folders that aren't split across A:/B:
    
    if not sessions:
        for f in filter(get_session_folder, config.NPEXP_PATH.iterdir()):
            if (
                f.name == get_session_folder(f.name) # excl dir names that have '- copy' appeneded
                and datetime.datetime.strptime(f.name.split('_')[-1], '%Y%m%d') > (datetime.datetime.now() - datetime.timedelta(days=14))
                and not is_hab(f)
            ):
                sessions.append(f.name)

    sessions = sorted(list(set(sessions)))
    probes_available_to_upload = 'ABCDEF'

    out = ipw.Output(layout={"border": "1px solid black"})

    session_dropdown = ipw.Dropdown(
        options=sessions,
        value=None,
        description="session",
        disabled=False,
    )

    upload_button = ipw.ToggleButton(
        description="Upload",
        disabled=True,
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Upload raw data to DataJoint",
        icon="cloud-upload",  # (FontAwesome names without the `fa-` prefix)
    )

    progress_button = ipw.ToggleButton(
        description="Check sorting", 
        disabled=True,
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Check sorting progress on DataJoint",
        icon="hourglass-half",  # (FontAwesome names without the `fa-` prefix)
    )

    surface_image = ipw.Image()
    surface_image.layout.height = '300px'
    surface_image.layout.visibility = 'hidden'
    
    def update_surface_image():
        for img in (config.NPEXP_PATH / session_dropdown.value).glob('*surface-image*'):
            if any(inserted_img in img.name for inserted_img in ('image4', 'image5')):
                break
        else:
            surface_image.layout.visibility = 'hidden'
            return
        with img.open('rb') as f:
            surface_image.value = f.read()
        surface_image.layout.visibility = 'visible'
            
    def handle_dropdown_change(change):
        if get_session_folder(change.new) is not None:
            upload_button.disabled = False
            upload_button.button_style = "warning"
            progress_button.disabled = False
            progress_button.button_style = "info"
            update_surface_image()
    session_dropdown.observe(handle_dropdown_change, names="value")

    def handle_upload_change(change):
        upload_button.disabled = True
        upload_button.button_style = "warning"
        with out:
            logging.info(f"Uploading probes: {probes_from_grid()}")
        session = classes.DataJointSession(session_dropdown.value)
        session.upload(probes=probes_from_grid())

    upload_button.observe(handle_upload_change, names="value")

    def handle_progress_change(change):
        with out:
            logging.info("Fetching summary from DataJoint...")
        progress_button.button_style = ""
        progress_button.disabled = True
        session = classes.DataJointSession(session_dropdown.value)
        try:
            with out:
                IPython.display.display(session.sorting_summary())
        except dj.DataJointError:
            logging.info(
                f"No entry found in DataJoint for session {session_dropdown.value}"
            )

    progress_button.observe(handle_progress_change, names="value")

    buttons = ipw.HBox([upload_button, progress_button])

    probe_select_grid = ipw.GridspecLayout(6, 1, grid_gap="0px")
    for idx, probe_letter in enumerate(probes_available_to_upload):
        probe_select_grid[idx, 0] = ipw.Checkbox(
            value=True,
            description=f"probe{probe_letter}",
            disabled=False,
            indent=True,
        )
    probe_select_and_image = ipw.HBox([surface_image, probe_select_grid])
    def probes_from_grid() -> str:
        probe_letters = ""
        for idx in range(6):
            if probe_select_grid[idx, 0].value == True:
                probe_letters += chr(ord("A") + idx)
        return probe_letters

    app = ipw.TwoByTwoLayout(
        top_right=probe_select_and_image,
        bottom_right=out,
        bottom_left=buttons,
        top_left=session_dropdown,
        width="100%",
        justify_items="center",
        align_items="center",
    )
    return IPython.display.display(app)

def local_dj_probe_pairs() -> Iterable[dict[str, classes.Probe]]:
    for session in sorted_sessions():
        for probe in config.DEFAULT_PROBES:
            try:
                local = classes.ProbeLocal(session, probe)
                dj = classes.ProbeDataJoint(session, probe)
                yield dict(local=local, dj=dj)
            except FileNotFoundError:
                continue
