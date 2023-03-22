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
from typing import (Generator, Optional,
                    Sequence, ClassVar)

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

from np_datajoint import config, utils

logger = np_logging.getLogger(__name__)


class SessionDirNotFoundError(ValueError):
    pass

class DataJointSession:
    """A class to handle data transfers between local rigs/network shares, and the
    DataJoint server.
    
    Uses lims session id as Datajoint session id. 
    """
    
    sorting_status_last_checked: ClassVar[dict[Optional[int], datetime.datetime]] = dict()

    def __init__(self, path_or_session_folder: str | pathlib.Path):
        session_folder = self.get_session_folder(str(path_or_session_folder))
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
        logger.debug("%s initialized %s", self.__class__.__name__, self.session_folder)
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.session_folder!r})"
    
    @staticmethod
    def get_session_folder(path: str | pathlib.Path) -> str | None:
        """Extract [8+digit session ID]_[6-digit mouse ID]_[8-digit date
        str] from a string or path"""
        session_reg_exp = R"[0-9]{8,}_[0-9]{6}_[0-9]{8}"

        session_folders = re.findall(session_reg_exp, str(path))
        if session_folders:
            if not all(s == session_folders[0] for s in session_folders):
                logger.debug(
                    f"Mismatch between session folder strings - file may be in the wrong folder: {path}"
                )
            return session_folders[0]
        return None
        
    @classmethod
    def sorted_sessions(cls, *args, **kwargs) -> Iterable[DataJointSession]:
        df = cls.sorting_summary(*args, **kwargs)
        yield from (
            cls(session) for session in df.loc[df["all_done"] == 1].index
        )

    @staticmethod
    def session_str_from_datajoint_keys(session: DataJointSession):
        return (
            session.session_id.astype(str)
            + "_"
            + session.subject
            + "_"
            + session.session_datetime.dt.strftime("%Y%m%d")
        )
    
    @classmethod
    def sorting_summary(
        cls,
        paramset_idx: Optional[int] = config.DEFAULT_KS_PARAMS_INDEX,
    ) -> pd.DataFrame:
        """Summary of processing for all probes across all sessions, with optional restriction on `paramset_idx` - modified from Thinh@DJ.
        - `paramset_idx = None` returns all probes
        - `paramset_idx = -1` returns probes with no paramset_idx (ie. haven't started
        processing)
        """
        df = pd.DataFrame(cls.get_sorting_status_all_sessions(paramset_idx))
        # make new 'session' column that matches our local session folder names
        df = df.assign(session=cls.session_str_from_datajoint_keys)
        # filter for sessions with correctly formatted session/mouse/date keys
        df = df.loc[~(pd.Series(map(cls.get_session_folder, df.session)).isnull())]
        df.set_index("session", inplace=True)
        df.sort_values(by="session", ascending=False, inplace=True)
        # remove columns that were concatenated into the new 'session' column
        df.drop(columns=["session_id", "subject", "session_datetime", "lfp"], inplace=True)
        df.sort_index(inplace=True)
        return df

    @classmethod    
    def all_sessions(cls) -> dj.Table:
        "Correctly formatted sessions on Datajoint."
        logger.debug("Fetching all correctly-formatted sessions from DataJoint server")
        all_sessions = config.DJ_SESSION.Session.fetch()
        session_str_match_on_datajoint = lambda x: bool(
            cls.get_session_folder(f"{x[1]}_{x[0]}_{x[2].strftime('%Y%m%d')}")
        )
        return (
            config.DJ_SESSION.Session
            & all_sessions[list(map(session_str_match_on_datajoint, all_sessions))]
        )

    @property
    def session(self):
        "Datajoint session query - can be combined with `fetch` or `fetch1`"
        if not (session := config.DJ_SESSION.Session & {"session_id": self.session_id}):
            raise dj.DataJointError(f"Session {self.session_id} not found in database.")
        return session

    @property
    def session_key(self) -> dict[str, str | int]:
        "{subject:session_id}"
        return self.session.fetch1("KEY")

    @property
    def session_subject(self) -> str:
        return self.session.fetch1("subject")

    @property
    def session_datetime(self) -> datetime.datetime:
        return self.session.fetch1("session_datetime")

    @property
    def session_folder_from_dj(self) -> str:
        "Remote session dir re-assembled from datajoint table components. Should match our local `session_folder`"
        return f"{self.session_id}_{self.session_subject}_{self.session_datetime.strftime('%Y%m%d')}"

    @property
    def probe_insertion(self):
        return config.DJ_EPHYS.ProbeInsertion & self.session_key

    @property
    def clustering_task(self):
        return config.DJ_EPHYS.ClusteringTask & self.session_key

    @property
    def curated_clustering(self):
        "Don't get subtables from this query - they won't be specific to the session_key"
        return config.DJ_EPHYS.CuratedClustering & self.session_key

    @property
    def metrics(self):
        "Don't get subtables from this query - they won't be specific to the session_key"
        return config.DJ_EPHYS.QualityMetrics & self.session_key
    
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
        return (config.DJ_SESSION.SessionDirectory & self.session_key).fetch1("session_dir")

    @property
    def remote_session_dir_outbox(self) -> str:
        "Root for session sorted data on datajoint server."
        return f"{config.DJ_OUTBOX}{'/' if not str(config.DJ_OUTBOX).endswith('/') else '' }{self.session_folder}/"

    @property
    def remote_session_dir_inbox(self) -> str:
        "Root for session uploads on datajoint server."
        return f"{config.DJ_INBOX}{'/' if not str(config.DJ_INBOX).endswith('/') else '' }{self.session_folder}/"

    def remote_sorted_probe_dir(self, probe_idx: int, paramset_idx: int) -> str:
        return (f"{config.DJ_OUTBOX}{'/' if not str(config.DJ_OUTBOX).endswith('/') else '' }" + 
                f"{(self.clustering_task & {'insertion_number':probe_idx, 'paramset_idx':paramset_idx}).fetch1('clustering_output_dir')}")
    
    @property
    def acq_paths(self) -> tuple[pathlib.Path, ...]:
        paths = []
        for drive, probes in zip("AB", ["_probeABC", "_probeDEF"]):
            path = pathlib.Path(f"{drive}:/{self.session_folder}{probes}")
            if path.is_dir():
                paths.append(path)
        return tuple(paths)

    @functools.cached_property
    def lims_info(self) -> Optional[dict]:
        response = requests.get(
            f"http://lims2/ecephys_sessions/{self.session_id}.json?"
        )
        if response.status_code != 200:
            return None
        return response.json()

    @property
    def lims_path(self) -> Optional[pathlib.Path]:
        if self.lims_info and (path := self.lims_info["storage_directory"]):
            return pathlib.Path('/'+path)
        return None

    @property
    def npexp_path(self) -> Optional[pathlib.Path]:
        path = config.NPEXP_PATH / self.session_folder
        return path if path.is_dir() else None

    @property
    def local_download_path(self) -> pathlib.Path:
        return pathlib.Path(config.LOCAL_INBOX)
    
    def dj_sorted_local_probe_path(self, probe_idx: int, sorted_paramset_idx: int = config.DEFAULT_KS_PARAMS_INDEX):
        return self.local_download_path / f"ks_paramset_idx_{sorted_paramset_idx}" / self.session_folder / f"{self.session_folder}_probe{chr(ord('A') + probe_idx)}_sorted"

    def npexp_sorted_probe_paths(
        self, probe_letter: Optional[str] = None
    ) -> pathlib.Path | Sequence[pathlib.Path]:
        "Paths to probe data folders sorted locally, with KS pre-2.0, or a single folder for a specified probe."
        path = lambda probe: pathlib.Path(
            Rf"//allen/programs/mindscope/workgroups/np-exp/{self.session_folder}/{self.session_folder}_probe{probe}_sorted/continuous/Neuropix-PXI-100.0"
        )
        if probe_letter is None or probe_letter not in config.DEFAULT_PROBES:
            return tuple(path(probe) for probe in config.DEFAULT_PROBES)
        else:
            return path(probe_letter)

    def add_clustering_task(
        self,
        paramset_idx: int = config.DEFAULT_KS_PARAMS_INDEX,
        probe_letters: Sequence[str] = config.DEFAULT_PROBES,
    ) -> None:
        "For existing entries in config.DJ_EPHYS.EphysRecording, create a new ClusteringTask with the specified `paramset_idx`"
        if not self.probe_insertion:
            logger.info(
                f"Probe insertions have not been auto-populated for {self.session_folder} - cannot add additional clustering task yet."
            )
            return
            # TODO need an additional check on reqd metadata/oebin file

        for probe_letter in probe_letters:
            probe_idx = ord(probe_letter) - ord("A")

            if (
                not config.DJ_EPHYS.EphysRecording
                & self.session_key
                & {"insertion_number": probe_idx}
            ):
                if (
                    config.DJ_EPHYS.ClusteringTask
                    & self.session_key
                    & {"insertion_number": probe_idx}
                ):
                    msg = f"ClusteringTask entry already exists - processing should begin soon, then additional tasks can be added."
                elif self.probe_insertion & {"insertion_number": probe_idx}:
                    msg = f"ProbeInsertion entry already exists - ClusteringTask should be auto-populated soon."
                else:
                    msg = f"ProbeInsertion and ClusteringTask entries don't exist - either metadata/critical files are missing, or processing hasn't started yet."
                logger.info(
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
                    config.DJ_EPHYS.ClusteringParamSet * config.DJ_EPHYS.ClusteringMethod
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

            if config.DJ_EPHYS.ClusteringTask & task_key:
                logger.info(f"Clustering task already exists: {task_key}")
                return
            else:
                config.DJ_EPHYS.ClusteringTask.insert1(task_key, skip_duplicates=True)

    def raw_ephys_filter(self, path, probes):
        if match := re.search("(?<=_probe)[A-F]{,}", path.name):
            if any(p in probes for p in match[0]):
                return path
            
    def get_raw_ephys_paths(
        self,
        paths: Optional[Sequence[str | pathlib.Path]] = None,
        probes: str = config.DEFAULT_PROBES,
    ) -> tuple[pathlib.Path, ...]:
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
            if not isinstance(path, Sequence):
                path = (path,)

            matching_session_folders = {
                s for p in path for s in utils.get_raw_ephys_subfolders(p) if self.raw_ephys_filter(s, probes)
            }
            if len(matching_session_folders) == 1 or self.is_valid_pair_split_ephys_folders(
                matching_session_folders
            ):
                logger.debug(matching_session_folders)
                return tuple(matching_session_folders)
        else:
            raise FileNotFoundError(f"No valid ephys raw data folders (v0.6+) found")
        
    @classmethod
    def get_sorting_status_all_sessions(
        cls,
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

        logger.debug(
            f'Restricting processing status summary to sessions with paramset_idx = {paramset_idx if paramset_idx is not None else "all"}'
        )
        @functools.cache
        def _get_sorting_status_all_sessions(paramset_idx) -> dj.Table:
            "Calling all these tables is slow (>10s), so cache the result."
            cls.sorting_status_last_checked[paramset_idx] = datetime.datetime.now()

            session_process_status = cls.all_sessions()

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

        if cls.sorting_status_last_checked.get(
            paramset_idx, datetime.datetime.now()
        ) < datetime.datetime.now() - datetime.timedelta(hours=1):
            logger.debug("Clearing cache for _get_sorting_status_all_sessions")
            _get_sorting_status_all_sessions.cache_clear()
        return _get_sorting_status_all_sessions(paramset_idx)

    @classmethod
    def local_dj_probe_pairs(cls) -> Iterable[dict[str, Probe]]:
        for session in cls.sorted_sessions():
            for probe in config.DEFAULT_PROBES:
                try:
                    local = ProbeLocal(session, probe)
                    dj = ProbeDataJoint(session, probe)
                    yield dict(local=local, dj=dj)
                except FileNotFoundError:
                    continue
    
    @classmethod
    def check_session_paths_match(cls, paths: Sequence[pathlib.Path]) -> None:
        sessions = [cls.get_session_folder(path) for path in paths]
        if any(not s for s in sessions):
            raise ValueError(
                "Paths supplied must be session folders: [8+digit lims session ID]_[6-digit mouse ID]_[6-digit datestr]"
            )
        if not all(s and s == sessions[0] for s in sessions):
            raise ValueError("Paths must all be for the same session")

    @classmethod
    def is_valid_pair_split_ephys_folders(cls, paths: Sequence[pathlib.Path]) -> bool:
        "Check a pair of dirs of raw data for size, matching settings.xml, v0.6.x+ to confirm they're from the same session and meet expected criteria."
        if not paths:
            return False

        if any(not utils.is_valid_ephys_folder(path) for path in paths):
            return False

        cls.check_session_paths_match(paths)
        utils.check_xml_files_match([tuple(path.rglob("settings*.xml"))[0] for path in paths])

        size_difference_threshold_gb = 2
        dir_sizes_gb = tuple(round(utils.dir_size(path) / 1024**3) for path in paths)
        diffs = (abs(dir_sizes_gb[0] - size) for size in dir_sizes_gb)
        if not all(diff <= size_difference_threshold_gb for diff in diffs):
            print(
                f"raw data folders are not within {size_difference_threshold_gb} GB of each other"
            )
            return False

        return True

    def upload_from_hpc(self, paths, probes, without_sorting):
        "Relay job to HPC. See .upload() for input arg details."
        hpc_user = "svc_neuropix"
        logger.info(
            "Connecting to HPC via SSH, logs will continue in /allen/ai/homedirs/%s", hpc_user
        )
        with fabric.Connection(f"{hpc_user}@hpc-login") as ssh:

            # copy up-to-date files to hpc
            src_dir = pathlib.Path(__file__).parent / 'hpc'
            slurm_launcher = "slurm_launcher.py"
            upload_script = "upload_session.py"
            hpc_dir = "np_datajoint/"
            for file in (src_dir / slurm_launcher, src_dir / upload_script):
                ssh.put(file, hpc_dir)

            # launch upload job via slurm
            slurm_env = datajoint_env = "dj"
            cmd = f"conda activate {slurm_env};"
            cmd += f"cd {hpc_dir}; python {slurm_launcher}"# --env {datajoint_env}"
            # addtl args below will be passed to slurm batch job and parsed by upload_script
            cmd += f" {upload_script} --session {self.session_folder} --paths {' '.join(path.as_posix() for path in paths)} --probes {probes}"
            if without_sorting:
                cmd += " --without_sorting"
            ssh.run(cmd)  # submit all cmds in one line to enforce sequence -
            # ensures slurm job doesn't run until conda env is activated
            logger.debug(cmd)

    @classmethod
    def get_local_remote_oebin_paths(
        cls,
        paths: pathlib.Path | Sequence[pathlib.Path],
    ) -> tuple[tuple[pathlib.Path, ...], pathlib.Path]:
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

        if not any(utils.is_new_ephys_folder(path) for path in paths):
            raise ValueError("No new ephys folder found in paths")
        cls.check_session_paths_match(paths)

        local_session_paths: set[pathlib.Path] = set()
        for path in paths:

            ephys_subfolders = utils.get_raw_ephys_subfolders(path)

            if ephys_subfolders and len(paths) == 1:
                # parent folder supplied: we want to upload its subfolders
                local_session_paths.update(e for e in ephys_subfolders)
                break  # we're done anyway, just making this clear

            if ephys_subfolders:
                logger.warning(
                    f"Multiple subfolders of raw data found in {path} - expected a single folder."
                )
                local_session_paths.update(e for e in ephys_subfolders)
                continue

            if utils.is_new_ephys_folder(path):
                # single folder supplied: we want to upload this folder
                local_session_paths.add(path)
                continue
        
        if len(local_session_paths) == 1:
            if len(record_nodes := tuple(next(iter(local_session_paths)).glob('Record Node*'))) > 1:
                local_session_paths.update(record_nodes)
                
        local_oebin_paths = tuple(
            sorted(list(set(utils.get_single_oebin_path(p) for p in local_session_paths)),
            key=lambda s: str(s),
        ))

        local_session_paths_for_upload = [p.parent.parent.parent for p in local_oebin_paths]
        # settings.xml file should be the same for _probeABC and _probeDEF dirs
        if len(local_session_paths_for_upload) > 1:
            utils.check_xml_files_match([p / "settings.xml" for p in local_session_paths_for_upload])

        # and for the server we just want to point to the oebin file from two levels above -
        # shouldn't matter which oebin path we look at here, they should all have the same
        # relative structure
        remote_oebin_path = local_oebin_paths[0].relative_to(
            local_oebin_paths[0].parent.parent.parent
        )

        return local_oebin_paths, remote_oebin_path

    def upload(
        self,
        paths: Optional[Sequence[str | pathlib.Path]] = None,
        probes: Sequence[str] = config.DEFAULT_PROBES,
        without_sorting=False,
    ):
        """Upload from rig/network share to DataJoint server.

        Accepts a list of paths to upload or, if None, will try to upload from self.path,
        then A:/B:, then lims, then npexp.
        """
        paths = self.get_raw_ephys_paths(paths, probes)
        logger.debug(f"Paths to upload: {paths}")

        local_oebin_paths, remote_oebin_path = self.get_local_remote_oebin_paths(paths)
        local_session_paths_for_upload = (
            p.parent.parent.parent for p in local_oebin_paths
        )

        data_on_network = str(paths[0]).startswith("//allen") or str(paths[0]).startswith("/allen")
        if data_on_network and not config.RUNNING_ON_HPC:
            try:
                self.upload_from_hpc(paths, probes, without_sorting)
                return
            except Exception as e:
                logger.exception(
                    "Could not connect to HPC: uploading data on //allen from local machine, which may be unstable"
                )

        if data_on_network or config.RUNNING_ON_HPC:
            config.BOTO3_CONFIG[
                "max_concurrency"
            ] = 100  # transfers over network can crash if set too high
            logger.info(
                "Data on network: limiting max_concurrency to %s",
                config.BOTO3_CONFIG["max_concurrency"],
            )

        if not without_sorting:
            self.create_session_entry(remote_oebin_path)

            # upload merged oebin file
            # ------------------------------------------------------- #
            temp_merged_oebin_path = utils.create_merged_oebin_file(local_oebin_paths, probes)
            dj_axon.upload_files(
                source=temp_merged_oebin_path,
                destination=f"{self.remote_session_dir_inbox}{remote_oebin_path.parent.as_posix()}/",
                session=config.S3_SESSION,
                s3_bucket=config.S3_BUCKET,
                boto3_config=config.BOTO3_CONFIG,
            )

        # upload rest of raw data
        # ------------------------------------------------------- #
        np_logging.web('np_datajoint').info(
            f"Started uploading raw data {self.session_folder}"
        )
        ignore_regex = ".*\.oebin"
        ignore_regex += "|.*\.".join(
            [" "]
            + [f"probe{letter}-.*" for letter in set(config.DEFAULT_PROBES) - set(probes)]
        ).strip()

        for local_path in local_session_paths_for_upload:
            dj_axon.upload_files(
                source=local_path,
                destination=self.remote_session_dir_inbox,
                session=config.S3_SESSION,
                s3_bucket=config.S3_BUCKET,
                boto3_config=config.BOTO3_CONFIG,
                ignore_regex=ignore_regex,
            )
        np_logging.web('np_datajoint').info(
            f"Finished uploading raw data {self.session_folder}"
        )
    
    @functools.cached_property
    def map_probe_idx_to_available_sorted_paramset_idx(self) -> dict[int, tuple[int]]:
        "May return empty tuple if no paramsets processed for probe."
        processed: list[list[int]] = self.curated_clustering.fetch('insertion_number', 'paramset_idx')
        probe_paramset_idx_mapping = defaultdict(tuple)
        for insertion_number in processed[0]:
            probe_paramset_idx_mapping[insertion_number] = tuple(paramset_idx for n, paramset_idx in enumerate(processed[1]) if processed[0][n] == insertion_number)
        return probe_paramset_idx_mapping
    
    def filtered_remote_and_local_sorted_paths(self, probe_idx: int, paramset_idx: int, skip_large_files=True) -> tuple[Sequence[str], Sequence[str]]:
        "Return list of sorted probe files on server and list of corresponding local paths, with mods to match internal sorting pipeline names/folder structure."
        large_file_threshold_gb = 1
        
        remote_sorted_probe_dir: str = self.remote_sorted_probe_dir(probe_idx, paramset_idx)
        local_sorted_probe_dir: pathlib.Path = self.dj_sorted_local_probe_path(probe_idx, paramset_idx)
        
        all_src_files: dict[str, str] = dj_axon.list_files(
            session=config.S3_SESSION,
            s3_bucket=config.S3_BUCKET,
            s3_prefix=remote_sorted_probe_dir,
            include_contents_hash=False,
            as_tree=False,
        )
        
        root = local_sorted_probe_dir
        ap = root / 'continuous/Neuropix-PXI-100.0'
        logs = root / 'logs'
        for folder in (root, ap, logs):
            folder.mkdir(exist_ok=True, parents=True)
            
        src = []
        dest = []
        def append(remote_file: pathlib.Path, remote_size:int, local_dir: pathlib.Path, local_name: str = None):
            dest_path = local_dir / (local_name or remote_file.name)
            if dest_path.exists() and dest_path.stat().st_size == remote_size:
                return
            src.append(remote_file.as_posix())
            dest.append(str(dest_path))
            
        src_paths = (pathlib.Path(src_file['key']) for src_file in all_src_files)
        src_sizes = (int(src_file['_size']) for src_file in all_src_files)
        for path, size in zip(src_paths, src_sizes):
            if skip_large_files and size > large_file_threshold_gb * 1024 ** 3:
                continue
            if '.' in path.stem or '.json.' in path.name:
                # log files with '.' in stem causing a FileNotFoundError on boto3 download
                continue
            if path.stem == 'probe_info':
                append(path, size, root)
            elif path.suffix in ('.json','.txt'):
                append(path, size, logs)
            elif path.stem == 'probe_depth':
                append(path, size, root, f"probe_depth_{chr(probe_idx + ord('A'))}.png")
            elif path.name == 'cluster_group.tsv':
                append(path, size, ap, 'cluster_group.tsv.v2')
            else:
                append(path, size, ap)
        return src, dest
            
    def download(self, paramset_idx: Optional[int] = config.DEFAULT_KS_PARAMS_INDEX, skip_large_files=True):
        "Download files from sorted probe dirs on server to local inbox."
        
        for probe_idx, sorted_paramset_idxs in self.map_probe_idx_to_available_sorted_paramset_idx.items():
            for sorted_paramset_idx in sorted_paramset_idxs:
                if paramset_idx and sorted_paramset_idx != paramset_idx:
                    continue
                logger.info(f"Downloading sorted data for {self.session_folder} probe{chr(ord('A')+probe_idx)}, paramset_idx={sorted_paramset_idx}")
                src_list, dest_list = self.filtered_remote_and_local_sorted_paths(probe_idx, sorted_paramset_idx, skip_large_files)
                trailing_slash = '/' if 'win' not in sys.platform else '\\' # add if dest is dir
                for src, dest in zip(src_list, dest_list):    
                    config.S3_SESSION.s3.Bucket(config.S3_BUCKET).download_file(
                        Key=src,
                        Filename=dest,
                        Config=dj_axon.boto3.s3.transfer.TransferConfig(**config.BOTO3_CONFIG),
                    )
                try:    
                    self.update_metrics_csv_with_missing_columns(probe_idx, sorted_paramset_idx)
                except Exception as exc:
                    logger.exception(exc)
                self.copy_files_from_raw_to_sorted(probe_idx, sorted_paramset_idx, make_symlinks=True, original_ap_continuous_dat=True)
        logger.info(
            f"Finished downloading sorted data for {self.session_folder}"
        )
    
    def copy_files_from_raw_to_sorted(
        self,
        probe_idx: int, 
        paramset_idx: int, 
        make_symlinks=False,
        original_ap_continuous_dat=True,
        path_to_original_session_folder: Optional[pathlib.Path] = None,
        ):
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
        probe = chr(ord('A')+probe_idx)
        paths = self.get_raw_ephys_paths(path_to_original_session_folder, probes = probe)
        local_oebin_paths, *_ = self.get_local_remote_oebin_paths(paths)
        
        for local_oebin_path in local_oebin_paths:
            if continuous := next(local_oebin_path.parent.glob(f'continuous/Neuropix-PXI-*.Probe{probe}-*'), None):
                raw: pathlib.Path = local_oebin_path.parent
                probe_folder: str = '-'.join(continuous.name.split('-')[:-1])
                break
        else:
            raise FileNotFoundError(f"Could not find raw data folder for {self.session_folder} probe{probe} in {local_oebin_paths}")
        sorted: pathlib.Path = self.dj_sorted_local_probe_path(probe_idx, paramset_idx)
        
        # Copy small files --------------------------------------------------------------------- #
        src_dest = []
        src_dest.append((
            raw / f"events/{probe_folder}-AP/TTL/states.npy",
            sorted / f"events/Neuropix-PXI-100.0/TTL_1/channel_states.npy",
        ))
        src_dest.append((
            raw / f"events/{probe_folder}-AP/TTL/sample_numbers.npy",
            sorted / f"events/Neuropix-PXI-100.0/TTL_1/event_timestamps.npy",
        ))
        src_dest.append((
            raw / f"events/{probe_folder}-AP/TTL/sample_numbers.npy",
            sorted / f"events/Neuropix-PXI-100.0/TTL_1/sample_numbers.npy",
        ))
        src_dest.append((
            raw / f"events/{probe_folder}-AP/TTL/full_words.npy",
            sorted / f"events/Neuropix-PXI-100.0/TTL_1/full_words.npy",
        ))
        src_dest.append((
            raw / f"continuous/{probe_folder}-AP/timestamps.npy",
            sorted / f"continuous/Neuropix-PXI-100.0/ap_timestamps.npy",
        ))
        src_dest.append((
            raw / f"continuous/{probe_folder}-LFP/continuous.dat",
            sorted / f"continuous/Neuropix-PXI-100.1/continuous.dat",
        ))
        src_dest.append((
            raw / f"continuous/{probe_folder}-LFP/timestamps.npy",
            sorted / f"continuous/Neuropix-PXI-100.1/lfp_timestamps.npy",
        ))
        
        logging.debug(f"{self.session_folder} probe{probe}: copying and renaming selected files from original raw data dir to downloaded sorted data dir")
        for src, dest in src_dest:
            utils.symlink(src, dest) if make_symlinks else utils.copy(src, dest)
                        
        # Fix Open Ephys v0.6.x event timestamps ----------------------------------------------- #
        # see https://gist.github.com/bjhardcastle/e972d59f482a549f312047221cd8eccb
        # check we haven't already applied the operation
        original = raw / f"events/{probe_folder}-AP/TTL/sample_numbers.npy"
        modified = sorted / f"events/Neuropix-PXI-100.0/TTL_1/event_timestamps.npy"
        if not modified.exists() or modified.is_symlink() or utils.checksums_match((original, modified)):
            try:
                modified.unlink()
                modified.touch()
            except OSError:
                pass
            logging.debug(f"{self.session_folder} probe{probe}: adjusting `sample_numbers.npy` from OpenEphys and saving as `event_timestamps.npy`")
        
            src = raw / f"continuous/{probe_folder}-AP/sample_numbers.npy"
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
                raw / f"continuous/{probe_folder}-AP/continuous.dat",
                sorted / f"continuous/Neuropix-PXI-100.0/continuous.dat",
            ))
            logging.debug(f"{self.session_folder} probe{probe}: copying original AP continuous.dat to downloaded sorted data dir")
            for src, dest in src_dest:
                utils.symlink(src, dest)


    def create_session_entry(self, remote_oebin_path: pathlib.Path):
        "Insert metadata for session in datajoint tables"

        remote_session_dir_relative = (
            pathlib.Path(self.session_folder) / remote_oebin_path.parent
        )

        if config.DJ_SESSION.SessionDirectory & {"session_dir": self.session_folder}:
            logger.info(f"Session entry already exists for {self.session_folder}")

        if not config.DJ_SUBJECT.Subject & {"subject": self.mouse_id}:
            # insert new subject
            config.DJ_SUBJECT.Subject.insert1(
                {
                    "subject": self.mouse_id,
                    "sex": "U",
                    "subject_birth_date": "1900-01-01",
                },
                skip_duplicates=True,
            )

        with config.DJ_SESSION.Session.connection.transaction:
            config.DJ_SESSION.Session.insert1(
                {
                    "subject": self.mouse_id,
                    "session_id": self.session_id,
                    "session_datetime": self.date,
                },
                skip_duplicates=True,
            )
            config.DJ_SESSION.SessionDirectory.insert1(
                {
                    "subject": self.mouse_id,
                    "session_id": self.session_id,
                    "session_dir": remote_session_dir_relative.as_posix() + "/",
                },
                replace=True,
            )

    def update_metrics_csv_with_missing_columns(self, probe_idx: int, paramset_idx: int):
        probe = ProbeDataJoint(self, probe_letter=chr(probe_idx + ord('A')), paramset_idx=paramset_idx)
        if 'quality' in pd.read_csv(probe.metrics_csv).columns:
            logger.debug(f'{probe.metrics_csv} already contains columns added from DataJoint tables')
            return
        path = str(probe.metrics_csv)
        probe.metrics_df.to_csv(path)
        logger.debug(f'updated {probe.metrics_csv} with missing columns from DataJoint tables')

    def plot_driftmap(self, insertion_number, paramset_idx, shank_no=0):
        
        if isinstance(insertion_number, str):
            insertion_number = ord(insertion_number) - ord('A')
        
        units = (config.DJ_EPHYS.CuratedClustering.Unit * config.DJ_PROBE.ElectrodeConfig.Electrode
                & self.probe_insertion & {'insertion_number': insertion_number} & {'paramset_idx': paramset_idx}
                & 'cluster_quality_label = "good"')
        units = (units.proj('spike_times', 'spike_depths')
                * config.DJ_EPHYS.ProbeInsertion.proj()
                * config.DJ_PROBE.ProbeType.Electrode.proj('shank') & {'shank': shank_no})
        
        spike_times, spike_depths = units.fetch('spike_times', 'spike_depths', order_by='unit')
        spike_times = np.hstack(spike_times)
        spike_depths = np.hstack(spike_depths)
        
        # time-depth 2D histogram
        time_bin_count = 1000
        depth_bin_count = 200

        spike_bins = np.linspace(0, spike_times.max(), time_bin_count)
        depth_bins = np.linspace(0, np.nanmax(spike_depths), depth_bin_count)

        spk_count, spk_edges, depth_edges = np.histogram2d(spike_times, spike_depths, bins=[spike_bins, depth_bins])
        spk_rates = spk_count / np.mean(np.diff(spike_bins))
        spk_edges = spk_edges[:-1]
        depth_edges = depth_edges[:-1]
        
        # canvas setup
        fig = plt.figure(figsize=(16, 8))
        grid = plt.GridSpec(12, 12)

        ax_main = plt.subplot(grid[1:, 0:10])
        ax_cbar = plt.subplot(grid[0, 0:10])
        ax_spkcount = plt.subplot(grid[1:, 10:])

        # -- plot main --
        im = ax_main.imshow(spk_rates.T, aspect='auto', cmap='gray_r',
                            extent=[spike_bins[0], spike_bins[-1], depth_bins[-1], depth_bins[0]])
        # cosmetic
        ax_main.invert_yaxis()
        ax_main.set_xlabel('Time (sec)')
        ax_main.set_ylabel('Distance from tip sites (um)')
        ax_main.set_ylim(depth_edges[0], depth_edges[-1])
        ax_main.spines['right'].set_visible(False)
        ax_main.spines['top'].set_visible(False)

        cb = fig.colorbar(im, cax=ax_cbar, orientation='horizontal')
        cb.outline.set_visible(False)
        cb.ax.xaxis.tick_top()
        cb.set_label('Firing rate (Hz)')
        cb.ax.xaxis.set_label_position('top')

        # -- plot spikecount --
        ax_spkcount.plot(spk_count.sum(axis=0) / 10e3, depth_edges, 'k')
        ax_spkcount.set_xlabel('Spike count (x$10^3$)')
        ax_spkcount.set_yticks([])
        ax_spkcount.set_ylim(depth_edges[0], depth_edges[-1])

        ax_spkcount.spines['right'].set_visible(False)
        ax_spkcount.spines['top'].set_visible(False)
        ax_spkcount.spines['bottom'].set_visible(False)
        ax_spkcount.spines['left'].set_visible(False)
        
        fig.suptitle({'insertion_number': insertion_number, 'paramset_idx': paramset_idx, 'shank_no': shank_no})
        
        return fig
        
class Probe:
    
    def __init__(self, session:str|DataJointSession, probe_letter:str, **kwargs):
        if isinstance(session, DataJointSession):
            self.session = session
        else:
            self.session = DataJointSession(session)
        self.probe_letter = probe_letter
        
    @property
    def sorted_data_dir(self) -> pathlib.Path:
        "Local path to sorted data directory"
        raise NotImplementedError
    
    def __repr__(self):
        return f"{self.__class__.__name__}('{self.session.session_folder}', probe_letter='{self.probe_letter}')"
    
    @property
    def probe_idx(self) -> int:
        return ord(self.probe_letter) - ord('A')
    
    @property
    def metrics_csv(self) -> pathlib.Path:
        return self.sorted_data_dir / 'metrics.csv'
    
    @property
    def metrics_df(self) -> pd.DataFrame:
        if not self.metrics_csv.exists():
            raise FileNotFoundError(f"Does not exist: {self.metrics_csv}")
        if not hasattr(self, '_metrics_df'):
            self._metrics_df = pd.read_csv(self.metrics_csv)
            self._metrics_df.set_index('cluster_id', inplace=True) 
        return self._metrics_df
    
    def plot_metric_good_units(
        self, 
        metric:str, 
        ax: Optional[plt.Axes] = None, 
        **kwargs
        ) -> plt.Axes | None:
        if not self.metrics_csv.exists():
            logger.info(f"No metrics.csv file found for {self:!r}")
            return None
        if 'quality' not in self.metrics_df.columns:
            logger.info(f"No quality column in metrics for {self:!r}")
            return None
        if all(self.metrics_df['quality'] == 'noise'):
            logger.info(f"All clusters are noise for {self:!r}")
            return None
        if len(self.metrics_df.loc[self.metrics_df['quality']=='good']) == 1:
            logger.info(f"Only one good cluster for {self:!r}")
            return None
        if ax is None:
            fig, ax = plt.subplots()
        sns.kdeplot(
            self.metrics_df[metric].loc[self.metrics_df['quality'] == 'good'],
            ax=ax,
            **kwargs)
        return ax

    @property
    def qc_units_df(self) -> pd.DataFrame:
        if not hasattr(self, '_qc_units_df'):
            self.get_qc_units()
        return self._qc_units_df
    
    def get_qc_units(self) -> None:
        "From `probeSync.load_spike_info`"
        spike_clusters = np.load(self.sorted_data_dir / 'spike_clusters.npy')
        spike_times = np.load(self.sorted_data_dir / 'spike_times.npy')
        templates = np.load(self.sorted_data_dir / 'templates.npy')
        spike_templates = np.load(self.sorted_data_dir / 'spike_templates.npy')
        channel_positions = np.load(self.sorted_data_dir / 'channel_positions.npy')
        amplitudes = np.load(self.sorted_data_dir / 'amplitudes.npy')
        unit_ids = np.unique(spike_clusters)

        # p_sampleRate = 30_000
        units = {}
        for u in unit_ids:
            ukey = str(u)
            units[ukey] = {}

            unit_idx = np.where(spike_clusters==u)[0]
            unit_sp_times = spike_times[unit_idx] #/p_sampleRate - shift
            
            units[ukey]['times'] = unit_sp_times
            
            #choose 1000 spikes with replacement, then average their templates together
            chosen_spikes = np.random.choice(unit_idx, 1000)
            chosen_templates = spike_templates[chosen_spikes].flatten()
            units[ukey]['template'] = np.mean(templates[chosen_templates], axis=0)
            units[ukey]['peakChan'] = np.unravel_index(np.argmin(units[ukey]['template']), units[ukey]['template'].shape)[1]
            units[ukey]['position'] = channel_positions[units[ukey]['peakChan']]
            units[ukey]['amplitudes'] = amplitudes[unit_idx]
            
        units_df = pd.DataFrame.from_dict(units, orient='index')
        units_df['cluster_id'] = units_df.index.astype(int)
        units_df = units_df.set_index('cluster_id')
        units_df = pd.merge(self.metrics_df, units_df, left_index=True, right_index=True, how='outer')
        units_df['probe'] = self.probe_letter
        units_df['uid'] = units_df['probe'] + units_df.index.astype(str)
        units_df = units_df.set_index('uid')
        units_df = units_df.loc[units_df['quality']=='good']
        self._qc_units_df = units_df
            
class ProbeLocal(Probe):
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.sorted_data_dir.exists():
            raise FileNotFoundError(f"Probe data expected at {self.sorted_data_dir}")
    
    @property
    def sorted_data_dir(self) -> pathlib.Path | Sequence[pathlib.Path]:
        return self.session.npexp_sorted_probe_paths(self.probe_letter)

    @property
    def depth_img(self) -> pathlib.Path:
        img = self.sorted_data_dir.parent.parent / f'probe_depth_{self.probe_letter}.png' 
        return img if img.exists() else self.sorted_data_dir.parent.parent / f'probe_depth.png'

class ProbeDataJoint(Probe):
    
    paramset_idx: int # 1 = KS2.0, 2 = KS2.5
    
    def __init__(self, *args, **kwargs):
        if (
            (paramset_idx := kwargs.pop('paramset_idx', config.DEFAULT_KS_PARAMS_INDEX)) is not None
            and str(paramset_idx).strip().isdigit()
        ):
            self.paramset_idx = int(paramset_idx)
        super().__init__(*args, **kwargs)
        if not self.sorted_data_dir.exists():
            raise FileNotFoundError(f"Probe data expected at {self.sorted_data_dir}")
            
    @property
    def sorted_data_dir(self) -> pathlib.Path:
        return self.session.dj_sorted_local_probe_path(self.probe_idx, self.paramset_idx) / 'continuous/Neuropix-PXI-100.0' 

    @property
    def depth_img(self) -> pathlib.Path:
        img = self.sorted_data_dir.parent.parent / f'probe_depth_{self.probe_letter}.png' 
        return img if img.exists() else self.sorted_data_dir.parent.parent / f'probe_depth.png'
    
    @property
    def metrics_table(self):
        query = {
            'insertion_number': self.probe_idx,
            'paramset_idx': self.paramset_idx,
        }
        query = query | self.session.session_key
        metrics = utils.config.DJ_EPHYS.QualityMetrics.Cluster & query
        # join column from other table with quality label
        metrics *= ( 
            utils.config.DJ_EPHYS.CuratedClustering.Unit & query
        ).proj('cluster_quality_label')
        return metrics
    
    @property
    def metrics_df(self) -> Optional[pd.DataFrame]:
        "CSV from DJ is missing some columns - must be fetched from DJ tables."
        if not self.metrics_csv.exists():
            raise FileNotFoundError(f"Does not exist: {self.metrics_csv}")
        if not hasattr(self, '_metrics_df'):
            # fetch quality column from DJ and rename
            quality = pd.DataFrame(
                self.metrics_table.proj(
                    cluster_id='unit',
                    quality='cluster_quality_label',
                )
            )
            metrics = pd.read_csv(self.metrics_csv)
            #  don't set_index('cluster_id'): we need col 'unnamed: 0' for qc back compat
            self._metrics_df = metrics.join(quality['quality'])
        return self._metrics_df
      

class DRPilot(DataJointSession):
    """A class to handle data transfers between local rigs/network shares, and the
    DataJoint server.
    
    Lims session IDs weren't used for these experiments:
    
    - local session folder is named 'DRpilot_[labtracks mouse ID]_[8-digit date]'

    - DataJoint session ID is [8-digit date]
    """
    
    sorting_status_last_checked: ClassVar[dict[Optional[int], datetime.datetime]] = dict()

    storage_dirs = tuple(
        pathlib.Path(_) for _ in
            (
                '//10.128.50.140/Data2',
                '//allen/programs/mindscope/workgroups/dynamicrouting/PilotEphys/Task 2 pilot',
                '//allen/programs/mindscope/workgroups/np-exp/PilotEphys/Task 2 pilot',
            ))
    
    def __init__(self, session_folder_path: str | pathlib.Path):
        session_folder = self.get_session_folder(session_folder_path)
        "DRpilot_[6-digit mouse ID]_[8-digit date] (used on local filesystem)"
        if session_folder is None:
            raise SessionDirNotFoundError(
                f"Input does not contain a session directory (e.g. DRpilot_366122_20220618): {session_folder_path}"
            )
        self.session_folder = session_folder
        for _ in self.storage_dirs:
            if (path := _ / self.session_folder).exists():
                self.path = path
                break
        _, self.mouse_id, date = self.session_folder.split("_")
        self.date = datetime.datetime.strptime(date, "%Y%m%d").date()
        
        self.session_id = date
        
        logger.debug("%s initialized %s", self.__class__.__name__, self.session_folder)
        
    @staticmethod
    def get_session_folder(path: str | pathlib.Path) -> str | None:
        """Extract [DRpilot_[6-digit mouse ID]_[8-digit date str] from a string or
        path.
        """
        # from filesystem
        session_reg_exp = R"DRpilot_[0-9]{6}_[0-9]{8}"
        session_folders = re.findall(session_reg_exp, str(path))
        if session_folders:
            return session_folders[0] 
        
        # from datajoint (sessionID must be an int)
        session_reg_exp = R"[0-9]{8}_[0-9]{6}_[0-9]{8}"
        session_folders = re.findall(session_reg_exp, str(path))
        if session_folders:
            d1, mouse, d2 = session_folders[0].split("_")
            return f"DRpilot_{mouse}_{d2}" if (d1 == d2) else None
        
    def raw_ephys_filter(self, path, probes):
        if match := re.search("(?<=_probe)[A-F]{,}", path.name):
            if any(p in probes for p in match[0]):
                return path
            
    @property
    def session_folder_from_dj(self) -> str:
        "Remote session dir re-assembled from datajoint table components. Should match our local `session_folder`"
        return f"{self.date:%Y%m%d}_{self.session_subject}_{self.date:%Y%m%d}"
    
    @staticmethod
    def session_str_from_datajoint_keys(session: DRPilot):
        return (
            'DRpilot'
            + "_"
            + session.subject
            + "_"
            + session.session_datetime.dt.strftime("%Y%m%d")
        )

    @property
    def local_download_path(self) -> pathlib.Path:
        return pathlib.Path(config.LOCAL_INBOX) # currently workgroups/dynamicrouting/datajoint
        
    def dj_sorted_local_probe_path(self, probe_idx: int, sorted_paramset_idx: int = config.DEFAULT_KS_PARAMS_INDEX):
        return self.local_download_path / f"ks_paramset_idx_{sorted_paramset_idx}" / self.session_folder / f"{self.session_folder}_probe{chr(ord('A') + probe_idx)}_sorted"

    @property
    def lims_info(self) -> Optional[dict]:
        """Get LIMS info for this session. 
        """
        logger.warning("LIMS info not available: LIMS sessions weren't created for for DRPilot experiments.")

    @classmethod
    def sorted_sessions(cls, *args, **kwargs) -> Iterable[DRPilot]:
        df = cls.sorting_summary(*args, **kwargs)
        with contextlib.suppress(SessionDirNotFoundError): # will raise on non-DRpilot sessions
            yield from (
                cls(session) for session in df.loc[df["all_done"] == 1].index
            )
            
    def copy_files_from_raw_to_sorted(self, *args, **kwargs):
        for _ in self.storage_dirs:
            if (original_path := (_ / self.session_folder)).exists():
                break
        else:
            raise FileNotFoundError(f"Could not find original raw data for {self.session_folder} in any of {self.storage_dirs}")
        super().copy_files_from_raw_to_sorted(*args, **kwargs, path_to_original_session_folder=original_path)

