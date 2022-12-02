import datetime
import functools
import hashlib
import itertools
import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import time
import uuid
from typing import List, Optional, Sequence, Set, Tuple
from collections.abc import Iterable
from typing import Generator

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import np_logging
import pandas as pd
import IPython
import ipywidgets as ipw
import requests

np_logging.setup(
    config = np_logging.fetch_zk_config("/projects/datajoint/defaults/logging"),    
)

# # replace datajoint's logging with ours
# for log_name in ("Primary", 'datajoint'): # all datajoint packages 
#     logging.getLogger(log_name).__dict__ = logging.getLogger().__dict__
    
# config ------------------------------------------------------------------------------
# get zookeeper config via np_logging
zk_config = np_logging.fetch_zk_config("/projects/datajoint/defaults/configuration")

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
DEFAULT_KS_PARAMS_INDEX: int = zk_config["sorting"][
    "default_kilosort_parameter_set_index"
]  # 1=KS 2.0, 2=KS 2.5

# create virtual datajoint modules for querying tables ---------------------------------- #
dj_subject = dj.create_virtual_module("subject", "mindscope_dynamic-routing_subject")
dj_session = dj.create_virtual_module("session", "mindscope_dynamic-routing_session")
dj_ephys = dj.create_virtual_module("ephys", "mindscope_dynamic-routing_ephys")
dj_probe = dj.create_virtual_module("probe", "mindscope_dynamic-routing_probe")

DEFAULT_PROBES = "ABCDEF"


class SessionDirNotFoundError(ValueError):
    pass


class DataJointSession:
    """A class to handle data transfers between local rigs/network shares, and the DataJoint server."""

    def __init__(self, path_or_session_folder: str | pathlib.Path):
        session_folder = get_session_folder(str(path_or_session_folder))
        if session_folder is None:
            raise SessionDirNotFoundError(
                f"Input does not contain a session directory (e.g. 123456789_366122_20220618): {path_or_session_folder}"
            )
        self.session_folder = session_folder
        "[8+digit session ID]_[6-digit mouse ID]_[8-digit date]"
        if any(slash in str(path_or_session_folder) for slash in "\\/"):
            self.path = pathlib.Path(path_or_session_folder)
        else:
            self.path = None
        self.session_id, self.mouse_id, *_ = self.session_folder.split("_")
        self.date = datetime.datetime.strptime(
            self.session_folder.split("_")[2], "%Y%m%d"
        )
        try:
            if self.session_folder != self.session_folder_from_dj:
                raise SessionDirNotFoundError(
                    f"Session folder `{self.session_folder}` does not match components on DataJoint: {self.session_folder_from_dj}"
                )
        except dj.DataJointError:
            pass  # we could add metadata to datajoint here, but better to do that when uploading a folder, so we can verify session_folder string matches an actual folder

    @property
    def session(self):
        "Datajoint session query - can be combined with `fetch` or `fetch1`"
        if not (session := dj_session.Session & {"session_id": self.session_id}):
            raise dj.DataJointError(f"Session {self.session_id} not found in database.")
        return session

    @property
    def session_key(self) -> dict[str, str | int]:
        "subject:`str` and session_id:`int`"
        return self.session.fetch1("KEY")

    @property
    def session_subject(self) -> str:
        return self.session.fetch1("subject")

    @property
    def session_datetime(self) -> datetime.datetime:
        return self.session.fetch1("session_datetime")

    @property
    def session_folder_from_dj(self) -> str:
        return f"{self.session_id}_{self.session_subject}_{self.session_datetime.strftime('%Y%m%d')}"

    @property
    def probe_insertion(self):
        return dj_ephys.ProbeInsertion & self.session_key

    @property
    def clustering_task(self):
        return dj_ephys.ClusteringTask & self.session_key

    @property
    def curated_clustering(self):
        "Don't get subtables from this query - they won't be specific to the session_key"
        return dj_ephys.CuratedClustering & self.session_key

    @property
    def metrics(self):
        "Don't get subtables from this query - they won't be specific to the session_key"
        return dj_ephys.QualityMetrics & self.session_key

    @property
    def sorting_finished(self) -> bool:
        return (
            len(self.clustering_task)
            == len(self.metrics)
            >= len(self.probe_insertion)
            > 0
        )

    @property
    def sorting_started(self) -> bool:
        return len(self.probe_insertion) > 0

    @property
    def remote_session_dir_relative(self) -> str:
        "Relative session_dir on datajoint server with no database prefix."
        return (dj_session.SessionDirectory & self.session_key).fetch1("session_dir")

    @property
    def remote_session_dir_outbox(self) -> str:
        "Root for session sorted data on datajoint server."
        return f"{DJ_OUTBOX}{'/' if not str(DJ_OUTBOX).endswith('/') else '' }{self.session_folder}/"

    @property
    def remote_session_dir_inbox(self) -> str:
        "Root for session uploads on datajoint server."
        return f"{DJ_INBOX}{'/' if not str(DJ_INBOX).endswith('/') else '' }{self.session_folder}/"

    @property
    def acq_paths(self) -> tuple[pathlib.Path,...]:
        paths = []
        for drive, probes in zip("AB", ["_probeABC", "_probeDEF"]):
            path = pathlib.Path(f"{drive}:/{self.session_folder}{probes}")
            if path.is_dir():
                paths.append(path)
        return tuple(paths)

    @functools.cached_property
    def lims_info(self) -> Optional[dict]:
        response = requests.get(f"http://lims2/ecephys_sessions/{self.session_id}.json?")
        if response.status_code != 200:
            return None
        return response.json()
        
    @property
    def lims_path(self) -> Optional[pathlib.Path]:
        return pathlib.Path(self.lims_info.get('storage_directory',None)) if self.lims_info else None

    @property
    def npexp_path(self) -> Optional[pathlib.Path]:
        path = (
            pathlib.Path("//allen/programs/mindscope/workgroups/np-exp")
            / self.session_folder
        )
        return path if path.is_dir() else None

    @property
    def local_download_path(self) -> pathlib.Path:
        return pathlib.Path(LOCAL_INBOX) / self.session_folder

    def downloaded_sorted_probe_paths(
        self, probe_letter: str = None
    ) -> pathlib.Path | Sequence[pathlib.Path]:
        "Paths to all probe data folders downloaded from datajoint with default paramset_idx, or a single folder for a specified probe letter."
        query = {"paramset_idx": DEFAULT_KS_PARAMS_INDEX}
        probes_available = (self.clustering_task & query).fetch("insertion_number")
        probe_idx = ord(probe_letter) - ord("A") if probe_letter else None
        if probe_letter and probe_idx in probes_available:
            query["insertion_number"] = probe_idx
            relative_probe_dir = (self.clustering_task & query).fetch1(
                "clustering_output_dir"
            )
            return pathlib.Path(LOCAL_INBOX) / relative_probe_dir
        elif probe_letter:
            raise FileNotFoundError(
                f"Probe{probe_letter} path not found for session {self.session_folder}"
            )
        elif probe_letter is None:
            relative_probe_dirs = (self.clustering_task & query).fetch(
                "clustering_output_dir"
            )
            return tuple(
                pathlib.Path(LOCAL_INBOX) / probe_dir
                for probe_dir in relative_probe_dirs
            )

    def npexp_sorted_probe_paths(
        self, probe_letter: str = None
    ) -> pathlib.Path | Sequence[pathlib.Path]:
        "Paths to probe data folders sorted locally, with KS pre-2.0, or a single folder for a specified probe."
        path = lambda probe: pathlib.Path(
            Rf"//allen/programs/mindscope/workgroups/np-exp/{self.session_folder}/{self.session_folder}_probe{probe}_sorted/continuous/Neuropix-PXI-100.0"
        )
        if probe_letter is None or probe_letter not in "ABCDEF":
            return tuple(path(probe) for probe in "ABCDEF")
        else:
            return path(probe_letter)

    def add_clustering_task(
        self,
        paramset_idx: int = DEFAULT_KS_PARAMS_INDEX,
        probe_letters: Sequence[str] = "ABCDEF",
    ):
        "For existing entries in dj_ephys.EphysRecording, create a new ClusteringTask with the specified `paramset_idx`"
        if not self.probe_insertion:
            logging.info(
                f"Probe insertions have not been auto-populated for {self.session_folder} - cannot add additional clustering task yet."
            )
            return
            # TODO need an additional check on reqd metadata/oebin file

        for probe_letter in probe_letters:
            probe_idx = ord(probe_letter) - ord("A")

            if (
                not dj_ephys.EphysRecording
                & self.session_key
                & {"insertion_number": probe_idx}
            ):
                if (
                    dj_ephys.ClusteringTask
                    & self.session_key
                    & {"insertion_number": probe_idx}
                ):
                    msg = f"ClusteringTask entry already exists - processing should begin soon, then additional tasks can be added."
                elif self.probe_insertion & {"insertion_number": probe_idx}:
                    msg = f"ProbeInsertion entry already exists - ClusteringTask should be auto-populated soon."
                else:
                    msg = f"ProbeInsertion and ClusteringTask entries don't exist - either metadata/critical files are missing, or processing hasn't started yet."
                logging.info(
                    f"Skipping ClusteringTask entry for {self.session_folder}_probe{probe_letter}: {msg}"
                )
                continue

            insertion_key = {
                "subject": self.mouse_id,
                "session": self.session_id,
                "insertion_number": probe_idx,
            }

            method = (
                (
                    dj_ephys.ClusteringParamSet * dj_ephys.ClusteringMethod
                    & insertion_key
                )
                .fetch("clustering_method")[paramset_idx]
                .replace(".", "-")
            )

            output_dir = f"{self.remote_session_dir_relative}/{method}_{paramset_idx}/probe{probe_letter}_sorted"

            task_key = {
                "subject": self.mouse_id,
                "session_id": self.session_id,
                "insertion_number": probe_idx,
                "paramset_idx": paramset_idx,
                "clustering_output_dir": output_dir,
                "task_mode": "trigger",
            }

            if dj_ephys.ClusteringTask & task_key:
                logging.info(f"Clustering task already exists: {task_key}")
                return
            else:
                dj_ephys.ClusteringTask.insert1(task_key, replace=True)
        
    def get_raw_ephys_paths(self, paths: Optional[Sequence[str | pathlib.Path]] = None) -> tuple[pathlib.Path,...]:
        """Return paths to the session's ephys data.
            The first match is returned from:
            1) paths specified in input arg,
            2) self.path
            3) A:/B: drives if running from an Acq computer
            4) session folder on lims
            5) session folder on npexp (should be careful with older sessions where data
                may have been deleted)
        """
        for path in (paths, self.path, self.acq_paths, self.lims_path, self.npexp_path):
            if not path:
                continue
            matching_session_folders = tuple(itertools.chain(p.glob(f"{self.session_folder}_probe*") for p in path))
            if is_valid_pair_split_ephys_folders(matching_session_folders):
                return matching_session_folders
    
    def upload(
        self,
        probes: Sequence[str] = DEFAULT_PROBES,
        paths: Optional[Sequence[str | pathlib.Path]] = None,
        without_sorting=False,
    ):
        """Upload from rig/network share to DataJoint server.

        Accepts a list of paths to upload, or if None, will try to upload from self.path,
        then A:/B:, then lims, then npexp.
        """
        paths = self.get_raw_ephys_paths(paths)

        local_oebin_paths, remote_oebin_path = get_local_remote_oebin_paths(paths)
        local_session_paths_for_upload = (
            p.parent.parent.parent for p in local_oebin_paths
        )

        if not without_sorting:
            self.create_session_entry(remote_oebin_path)

            # upload merged oebin file first
            # ------------------------------------------------------- #
            temp_merged_oebin_path = create_merged_oebin_file(local_oebin_paths)
            dj_axon.upload_files(
                source=temp_merged_oebin_path,
                destination=f"{self.remote_session_dir_inbox}{remote_oebin_path.parent.as_posix()}/",
                session=S3_SESSION,
                s3_bucket=S3_BUCKET,
            )

        # upload rest of raw data
        # ------------------------------------------------------- #
        logging.getLogger("web").info(
            f"Started uploading raw data {self.session_folder}"
        )
        ignore_regex = ".*\.oebin"
        ignore_regex += "|.*\.".join(
                    [" "]
                    + [
                        f"probe{letter}-.*"
                        for letter in set(DEFAULT_PROBES) - set(probes)
                    ]
                ).strip()
        for local_path in local_session_paths_for_upload:
            dj_axon.upload_files(
                source=local_path,
                destination=self.remote_session_dir_inbox,
                session=S3_SESSION,
                s3_bucket=S3_BUCKET,
                boto3_config=BOTO3_CONFIG,
                ignore_regex=ignore_regex,
            )
        logging.getLogger("web").info(
            f"Finished uploading raw data {self.session_folder}"
        )

    def download(self, wait_on_sorting=False):
        "Download small files from sorting to /workgroups/dynamicrouting/."
        if not self.sorting_finished and not wait_on_sorting:
            logging.info(
                f"Sorting not started or incomplete for {self.session_folder}: skipping download."
            )
            return

        while not self.sorting_finished:
            wait_on_process(
                sec=1800,
                msg=f"Waiting for {self.session_folder} processing to complete to download sorted data...",
            )
        logging.getLogger("web").info(
            f"Downloading sorted data {self.session_folder}"
        )
        dj_axon.download_files(
            source=self.remote_session_dir_outbox,
            destination=f"{self.local_download_path}\\",  # if using linux - this should be fwd slash
            session=S3_SESSION,
            s3_bucket=S3_BUCKET,
            boto3_config=BOTO3_CONFIG,
            ignore_regex=R".*\.dat|.*\.mat|.*\.npy|.*\.json\.*",
        )
        logging.getLogger("web").info(
            f"Finished downloading sorted data {self.local_download_path}"
        )

    def sorting_summary(self):
        df = sorting_summary()
        return df.loc[[self.session_folder]].transpose()

    def create_session_entry(self, remote_oebin_path: pathlib.Path):
        "Insert metadata for session in datajoint tables"

        remote_session_dir_relative = (
            pathlib.Path(self.session_folder) / remote_oebin_path.parent 
        )

        if dj_session.SessionDirectory & {"session_dir": self.session_folder}:
            logging.info(f"Session entry already exists for {self.session_folder}")

        if not dj_subject.Subject & {"subject": self.mouse_id}:
            # insert new subject
            dj_subject.Subject.insert1(
                {
                    "subject": self.mouse_id,
                    "sex": "U",
                    "subject_birth_date": "1900-01-01",
                },
                skip_duplicates=True,
            )

        with dj_session.Session.connection.transaction:
            dj_session.Session.insert1(
                {
                    "subject": self.mouse_id,
                    "session_id": self.session_id,
                    "session_datetime": self.date,
                },
                skip_duplicates=True,
            )
            dj_session.SessionDirectory.insert1(
                {
                    "subject": self.mouse_id,
                    "session_id": self.session_id,
                    "session_dir": remote_session_dir_relative.as_posix() + '/',
                },
                replace=True,
            )


# general ------------------------------------------------------------------------------ #
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

@functools.cache
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
    if not dir_size(path) > 275*1024**3: # GB
        return False
    return True

def is_valid_pair_split_ephys_folders(paths: Sequence[pathlib.Path]) -> bool:
    "Check a pair of dirs of raw data for size, matching settings.xml, v0.6.x+ to confirm they're from the same session and meet expected criteria."
    
    if any(not is_valid_ephys_folder(path) for path in paths):
        return False
    
    check_session_paths_match(paths)
    check_xml_files_match([tuple(path.rglob("settings*.xml"))[0] for path in paths])
    
    size_difference_threshold_gb = 2
    dir_sizes_gb = (
        round(dir_size(path)/ 1024**3)
        for path in paths
        )
    diffs = (abs(dir_sizes_gb[0] - size) for size in dir_sizes_gb)
    if not all(diff <= size_difference_threshold_gb for diff in diffs):
        print(f"raw data folders are not within {size_difference_threshold_gb} GB of each other")
        return False
    
    return True

def get_raw_ephys_subfolders(path: pathlib.Path) -> List[pathlib.Path]:
    """
    Return a list of raw ephys recording folders, defined as the root that Open Ephys
    records to, e.g. `A:/1233245678_366122_20220618_probeABC`.

    Does not include the path supplied itself - only subfolders
    """

    subfolders = set()

    for f in path.rglob("*_probe*"):

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
    - Datajoint expects only one structure.oebing file per Session for sorting
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
    checksums = [hashlib.md5(p.read_bytes()).hexdigest() for p in paths]
    if not all(c == checksums[0] for c in checksums):
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
    """Input one or more paths to raw data folders that exist locally for a single
    session, and get back the path to the `structure.oebin` files for each local folder
    and its expected relative path on the remote server.

    A processing session on DataJoint needs pointing to a single structure.oebin file,
    via the SessionDirectory table and `session_dir` key.

    Locally, we want to upload from the folder containing the settings.xml file - two
    folders above the structure.oebin file - but only the subfolders returned from this function.
    """

    if isinstance(paths, pathlib.Path):
        paths = (paths,)

    if not any(is_new_ephys_folder(path) for path in paths):
        raise ValueError("No new ephys folder found in paths")
    check_session_paths_match(paths)

    local_session_paths: Set[pathlib.Path] = set()
    for path in paths:

        ephys_subfolders = get_raw_ephys_subfolders(path)

        if ephys_subfolders and len(paths) == 1:
            # parent folder supplied: we want to upload its subfolders
            local_session_paths.update(e for e in ephys_subfolders)
            break  # we're done anyway, just making this clear

        if ephys_subfolders:
            logging.warning(f"Multiple subfolders of raw data found in {path} - expected a single folder.")
            local_session_paths.update(e for e in ephys_subfolders)
            continue

        if is_new_ephys_folder(path):
            # single folder supplied: we want to upload this folder
            local_session_paths.add(path)
            continue

    local_oebin_paths = sorted(
        list(set(get_single_oebin_path(p) for p in local_session_paths)),
        key=lambda s: str(s),
    )

    # we don't necessarily want to upload to s3 with the same (excessive) folder structure,
    # and it makes merging the _probeABC and _probeDEF folders easier if we do away with some levels

    # Here we make a new relative path for the server with everything between the
    # session_dir 'root' folder and two levels above the 'structure.oebin' file, which contains the
    # settings.xml file (the same settings.xml for both _probeABC and _probeDEF)
    local_session_paths_for_upload = [p.parent.parent.parent for p in local_oebin_paths]

    check_xml_files_match([p / "settings.xml" for p in local_session_paths_for_upload])

    # and for the server we just want to point to the oebin file from two levels above -
    # shouldn't matter which oebin path we look at here, they should all have the same
    # relative structure
    remote_oebin_path = local_oebin_paths[0].relative_to(
        local_oebin_paths[0].parent.parent.parent
    )

    return local_oebin_paths, remote_oebin_path


def create_merged_oebin_file(paths: Sequence[pathlib.Path], probes:Sequence[str]=DEFAULT_PROBES) -> pathlib.Path:
    """Take paths to two or more structure.oebin files and merge them into one.

    For recordings split across multiple locations e.g. A:/*_probeABC, B:/*_probeDEF
    """
    if isinstance(paths, pathlib.Path):
        return paths
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
    merged_oebin: dict = {}
    for oebin in paths:

        with open(oebin, "r") as f:
            oebin_data = json.load(f)

        if not merged_oebin:
            merged_oebin = oebin_data
            continue

        for key in oebin_data:

            if merged_oebin[key] == oebin_data[key]:
                continue

            # 'continuous', 'events', 'spikes' are lists, which we want to concatenate across files
            if isinstance(oebin_data[key], List):
                for idx, item in enumerate(oebin_data[key]):
                    if merged_oebin[key][idx] == item:
                        continue
                    # skip probes not specified in input args (ie. not inserted)
                    if 'probe' in item.get('folder_name',''): # one is folder_name:'MessageCenter'
                        if not any(f'probe{letter}' in item['folder_name'] for letter in probes):
                            continue
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


def add_new_clustering_parameters(
    clustering_method: str, paramset_desc: str, params: dict, paramset_idx: int
):
    """
    Example:
    add_new_clustering_parameters(
        clustering_method='kilosort2.0',
        paramset_desc='Mindscope parameter set for Kilosort2.0',
        params=KS_PARAMS,
        paramset_idx=3, # 1 and 2 already used
        )
    """

    def dict_to_uuid(key):
        "Given a dictionary `key`, returns a hash string as UUID."
        hashed = hashlib.md5()
        for k, v in sorted(key.items()):
            hashed.update(str(k).encode())
            hashed.update(str(v).encode())
        return uuid.UUID(hex=hashed.hexdigest())

    param_dict = {
        "clustering_method": clustering_method,
        "paramset_idx": paramset_idx,
        "paramset_desc": paramset_desc,
        "params": params,
        "param_set_hash": dict_to_uuid(
            {**params, "clustering_method": clustering_method}
        ),
    }
    dj_ephys.ClusteringParamSet.insert1(param_dict, skip_duplicates=True)


def get_clustering_parameters(paramset_idx: int = DEFAULT_KS_PARAMS_INDEX) -> Tuple[str, dict]:
    "Get description and dict of parameters from paramset_idx."
    return (dj_ephys.ClusteringParamSet & {"paramset_idx": paramset_idx}).fetch1(
        "params"
    )

def all_sessions() -> dj.schemas:
    "Correctly formatted sessions on Datajoint."
    all_sessions = dj_session.Session.fetch()
    session_str_match_on_datajoint = (
        lambda x: bool(get_session_folder(
            f"{x[1]}_{x[0]}_{x[2].strftime('%Y%m%d')}"
    )))
    return dj_session.Session & all_sessions[list(map(session_str_match_on_datajoint, all_sessions))]    

def get_status_all_sessions(paramset_idx:int=DEFAULT_KS_PARAMS_INDEX):
    """Determine which tables have been autopopulated - from Thinh@DJ.
    
    A table indicating the number of entries in each of several database tables,
    starting at the session level.
    
    Can be restricted with an additional query.
    """
    # * not much faster than getting all sessions:
    # def get_status_all_sessions(sessions:str|int|Sequence|None=None):

    # if not sessions:
    #     session_process_status = dj_session.Session
    # elif isinstance(sessions, str|int):
    #     if folder := get_session_folder(str(sessions)):
    #         sessions = folder.split('_')[0]
    #     session_process_status = dj_session.Session & {'session_id': str(sessions)}
    # else:
    #     session_process_status = (
    #         dj_session.Session &
    #         # makes one large string, not working:
    #         ' & '.join(f'session_id={session}' for session in sessions)
    #     )
    session_process_status = all_sessions()

    session_process_status *= dj_session.Session.aggr(
        dj_ephys.ProbeInsertion, probes="count(insertion_number)", keep_all_rows=True
    )
    # session_process_status *= dj_session.Session.aggr(
    #     dj_ephys.ProbeInsertion, pidx="insertion_number", keep_all_rows=True
    # )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.EphysRecording,
        ephys="count(insertion_number)",
        keep_all_rows=True,
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.LFP, lfp="count(insertion_number)", keep_all_rows=True
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.ClusteringTask & {"paramset_idx":paramset_idx},
        task="count(insertion_number)",
        keep_all_rows=True,
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.Clustering  & {"paramset_idx":paramset_idx}, clustering="count(insertion_number)", keep_all_rows=True
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.CuratedClustering & {"paramset_idx":paramset_idx},
        curated="count(insertion_number)",
        pidx='GROUP_CONCAT(insertion_number SEPARATOR ", ")',
        keep_all_rows=True,
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.QualityMetrics  & {"paramset_idx":paramset_idx}, metrics="count(insertion_number)", keep_all_rows=True
    )
    session_process_status *= dj_session.Session.aggr(
        dj_ephys.WaveformSet  & {"paramset_idx":paramset_idx}, waveform="count(insertion_number)", keep_all_rows=True
    )

    return session_process_status.proj(..., all_done="probes > 0 AND waveform = task")

def sorting_summary() -> pd.DataFrame:
    df = pd.DataFrame(get_status_all_sessions())
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


def sorted_sessions() -> Iterable[DataJointSession]:
    df = sorting_summary()
    yield from (
        DataJointSession(session) for session in df.loc[df["all_done"] == 1].index
    )


def database_diagram() -> IPython.display.SVG:
    diagram = (
        dj.Diagram(dj_subject.Subject)
        + dj.Diagram(dj_session.Session)
        + dj.Diagram(dj_probe)
        + dj.Diagram(dj_ephys)
    )
    return diagram.make_svg()


def session_upload_from_acq_widget() -> ipw.AppLayout:

    folders = get_raw_ephys_subfolders(pathlib.Path("A:")) + get_raw_ephys_subfolders(
        pathlib.Path("B:")
    )

    sessions = [
        get_session_folder(folder) for folder in folders if is_new_ephys_folder(folder)
    ]
    for session in sessions:
        if sessions.count(session) < 2:
            sessions.remove(session)
    sessions = sorted(list(set(sessions)))

    probes_to_upload = "ABCDEF"

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

    def handle_dropdown_change(change):
        if get_session_folder(change.new) is not None:
            upload_button.disabled = False
            upload_button.button_style = "warning"
            progress_button.disabled = False
            progress_button.button_style = "info"

    session_dropdown.observe(handle_dropdown_change, names="value")

    def handle_upload_change(change):
        upload_button.disabled = True
        upload_button.button_style = "warning"
        with out:
            logging.info(f"Uploading probes: {probes_from_grid()}")
        session = DataJointSession(session_dropdown.value)
        session.upload(probes=probes_from_grid())

    upload_button.observe(handle_upload_change, names="value")

    def handle_progress_change(change):
        with out:
            logging.info("Fetching summary from DataJoint...")
        progress_button.button_style = ""
        progress_button.disabled = True
        session = DataJointSession(session_dropdown.value)
        try:
            with out:
                IPython.display.display(session.sorting_summary())
        except dj.DataJointError:
            logging.info(f"No entry found in DataJoint for session {session_dropdown.value}")

    progress_button.observe(handle_progress_change, names="value")

    buttons = ipw.HBox([upload_button, progress_button])

    probe_select_grid = ipw.GridspecLayout(6, 1, grid_gap="0px")
    for idx, probe_letter in enumerate(probes_to_upload):
        probe_select_grid[idx, 0] = ipw.Checkbox(
            value=True,
            description=f"probe{probe_letter}",
            disabled=False,
            indent=True,
        )

    def probes_from_grid() -> str:
        probe_letters = ""
        for idx in range(6):
            if probe_select_grid[idx, 0].value == True:
                probe_letters += chr(ord("A") + idx)
        return probe_letters

    app = ipw.TwoByTwoLayout(
        top_right=probe_select_grid,
        bottom_right=out,
        bottom_left=buttons,
        top_left=session_dropdown,
        width="100%",
        justify_items="center",
        align_items="center",
    )
    IPython.display.display(app)


if __name__ == "__main__":
    session = DataJointSession("1222995723_632293_20221101")
    session.metrics
    # session_upload_from_acq_widget()
    # session.upload(without_sorting=False)
