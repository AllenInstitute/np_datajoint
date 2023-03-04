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
    """A class to handle data transfers between local rigs/network shares, and the DataJoint server."""

    def __init__(self, path_or_session_folder: str | pathlib.Path):
        session_folder = utils.get_session_folder(str(path_or_session_folder))
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
    
    @staticmethod
    def get_session_folder(path: str | pathlib.Path) -> str | None:
        "Extract session folder from a string or path."
        return utils.get_session_folder(path)
    
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
    ):
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
                config.DJ_EPHYS.ClusteringTask.insert1(task_key, replace=True)

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

            def filter(path):
                if match := re.search("(?<=_probe)[A-F]{,}", path.name):
                    if any(p in probes for p in match[0]):
                        return path

            matching_session_folders = {
                s for p in path for s in utils.get_raw_ephys_subfolders(p) if filter(s)
            }
            if len(matching_session_folders) == 1 or utils.is_valid_pair_split_ephys_folders(
                matching_session_folders
            ):
                logger.debug(matching_session_folders)
                return tuple(matching_session_folders)
        else:
            raise FileNotFoundError(f"No valid ephys raw data folders (v0.6+) found")

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

        local_oebin_paths, remote_oebin_path = utils.get_local_remote_oebin_paths(paths)
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
        
        remote_sorted_probe_dir: pathlib.Path = self.remote_sorted_probe_dir(probe_idx, paramset_idx)
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
                logger.debug(f"Downloading sorted data for {self.session_folder} probe{chr(ord('A')+probe_idx)}, paramset_idx={sorted_paramset_idx}")
                src_list, dest_list = self.filtered_remote_and_local_sorted_paths(probe_idx, sorted_paramset_idx, skip_large_files)
                trailing_slash = '/' if 'win' not in sys.platform else '\\' # add if dest is dir
                for src, dest in zip(src_list, dest_list):    
                    config.S3_SESSION.s3.Bucket(config.S3_BUCKET).download_file(
                        Key=src,
                        Filename=dest,
                        Config=dj_axon.boto3.s3.transfer.TransferConfig(**config.BOTO3_CONFIG),
                    )
                try:    
                    utils.update_metrics_csv_with_missing_columns(self.session_folder, probe_idx, sorted_paramset_idx)
                except Exception as exc:
                    logger.exception(exc)
                utils.copy_files_from_raw_to_sorted(self.session_folder, probe_idx, sorted_paramset_idx, make_symlinks=True, original_ap_continuous_dat=True)
        logger.info(
            f"Finished downloading sorted data for {self.session_folder}"
        )

    def sorting_summary(self, *args, **kwargs) -> pd.DataFrame:
        df = utils.sorting_summary(*args, **kwargs)
        return df.loc[[self.session_folder]].transpose()

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
        ax:plt.Axes=None, 
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
    def sorted_data_dir(self) -> pathlib.Path:
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
    """A class to handle data transfers between local rigs/network shares, and the DataJoint server."""

    def __init__(self, session_folder_path: str | pathlib.Path):
        self.path = pathlib.Path(session_folder_path)
        self.session_folder = self.get_session_folder(self.path.name)
        "DRpilot_[6-digit mouse ID]_[8-digit date]"
        if self.session_folder is None:
            raise SessionDirNotFoundError(
                f"Input does not contain a session directory (e.g. DRpilot_366122_20220618): {session_folder_path}"
            )
            
        _, self.mouse_id, date = self.session_folder.split("_")
        self.date = datetime.datetime.strptime(date, "%Y%m%d").date()
        
        self.session_id = date
        
        try:
            if self.session_folder != self.session_folder_from_dj:
                raise SessionDirNotFoundError(
                    f"Session folder `{self.session_folder}` does not match components on DataJoint: {self.session_folder_from_dj}"
                )
        except dj.DataJointError:
            pass  # we could add metadata to datajoint here, but better to do that when uploading a folder, so we can verify session_folder string matches an actual folder
        
        logger.debug("%s initialized %s", self.__class__.__name__, self.session_folder)
        
        utils.get_session_folder = self.get_session_folder
        logger.warning(f'Monkey-patching utils.get_session_folder() to use {__class__}.get_session_folder()')

    @staticmethod
    def get_session_folder(path: str | pathlib.Path) -> str | None:
        """Extract ["DRpilot_[6-digit mouse ID]_[8-digit date str] from a string or path.
        """
        session_reg_exp = R"DRpilot_[0-9]{6}_[0-9]{8}"

        session_folders = re.findall(session_reg_exp, str(path))
        return session_folders[0] if session_folders else None
    
    @property
    def session_folder_from_dj(self) -> str:
        "Remote session dir re-assembled from datajoint table components. Should match our local `session_folder`"
        return f"DRpilot_{self.session_subject}_{self.session_datetime.strftime('%Y%m%d')}"

    @property
    def lims_info(self) -> Optional[dict]:
        """Get LIMS info for this session. 
        """
        logger.warning("LIMS info not available: LIMS sessions weren't created for for DRPilot experiments.")
