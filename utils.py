import datetime
import hashlib
import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import time
import uuid
from typing import List, Sequence, Set, Tuple

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import mpeconfig

# logging.basicConfig(level=logging.DEBUG, filename='upload.log', filemode='w')
os.environ['DJSCIOPS_LOG_LEVEL'] = 'DEBUG'

# get zookeeper config -----------------------------------------------------------------
zk_config =  mpeconfig.source_configuration(
    project_name='datajoint',
    hosts='eng-mindscope:2181',
    rig_id=os.environ.get("aibs_rig_id","BTVTest.1"),
    comp_id=os.environ.get("aibs_comp_id","BTVTest.1-Sync"),
    fetch_logging_config=False,
)
## alternative method to get a specific config by its path
# with mpeconfig.ConfigServer(hosts='eng-mindscope:2181') as zk:
#     zk_config = mpeconfig.fetch_configuration(
#         server=zk,
#         config_path='/projects/datajoint/defaults/configuration',
#         required=True
#     )

# apply zookeeper config to datajoint session
dj.config.update(zk_config['datajoint']) # dj.config is a custom class behaving as a dict - don't directly assign a dict
logger = dj.logger

S3_SESSION = dj_auth.Session(
    aws_account_id=zk_config['djsciops']["aws"]["account_id"],
    s3_role=zk_config['djsciops']["s3"]["role"],
    auth_client_id=zk_config['djsciops']["djauth"]["client_id"],
    auth_client_secret=zk_config['djsciops']["djauth"]["client_secret"],
)
S3_BUCKET = zk_config['djsciops']["s3"]["bucket"]

DJ_INBOX = zk_config['sorting']['remote_inbox'] #f"mindscope_dynamic-routing/inbox"
DJ_OUTBOX = zk_config['sorting']['remote_outbox'] #f"mindscope_dynamic-routing/inbox"
LOCAL_INBOX = pathlib.Path(zk_config['sorting']['local_inbox'])

BOTO3_CONFIG = zk_config['djsciops']['boto3']
KS_PARAMS_INDEX = zk_config['sorting']['default_kilosort_parameter_set_index'] # 1=KS 2.0, 2=KS 2.5

# create virtual datajoin modules for querying tables ---------------------------------- #
dj_subject = dj.create_virtual_module('subject', 'mindscope_dynamic-routing_subject')
dj_session = dj.create_virtual_module('session', 'mindscope_dynamic-routing_session')
dj_ephys = dj.create_virtual_module('ephys', 'mindscope_dynamic-routing_ephys')

# general ------------------------------------------------------------------------------ #
def get_session_folder(path: str|pathlib.Path) -> str|None:
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

def dir_size(path:pathlib.Path) -> int:
    """Return the size of a directory in bytes"""
    if not path.is_dir():
        raise ValueError(f"{path} is not a directory")
    dir_size = 0
    dir_size += sum(f.stat().st_size for f in pathlib.Path(path).rglob('*') if pathlib.Path(f).is_file())
    return dir_size

# local ephys-related (pre-upload) ----------------------------------------------------- #
def is_new_ephys_folder(path: pathlib.Path) -> bool:
    "Look for hallmarks of a v0.6.x Open Ephys recording"
    return bool(
        list(path.rglob("Record Node*"))
        and list(path.rglob("structure.oebin"))
        and list(path.rglob("settings*.xml"))
        and list(path.rglob("continuous.dat"))
    )

def get_raw_ephys_subfolders(path: pathlib.Path) -> List[pathlib.Path]:
    """Return a list of raw ephys recording folders, defined as the root that Open Ephys
    records to, e.g. `A:/1233245678_366122_20220618_probeABC`.
    
    Does not include the path supplied itself - only subfolders
    """
    
    subfolders = set()
    
    for f in path.rglob('*_probe*'):
        
        if not f.is_dir():
            continue
        
        if any(k in f.name for k in ['_sorted','_extracted','pretest','_603810_','_599657_','_598796_']):
            # skip pretest mice and sorted/extracted folders
            continue
        
        if not is_new_ephys_folder(f):
            # skip old/non-ephys folders
            continue
        
        if (size := dir_size(f)) and size < 1024**3 * 50:
            # skip folders that aren't above min size threshold (GB)
            continue
        
        subfolders.add(f)

    return sorted(list(subfolders), key=lambda s:str(s))

    
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
    
    oebin_paths = list(path.rglob('*structure.oebin'))
    
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
    if not all(s == '.xml' for s in [p.suffix for p in paths]):
        raise ValueError("Not all paths are XML files")
    if not all (p.is_file() for p in paths):
        raise FileNotFoundError("Not all paths are files, or they do not exist")
    checksums = [hashlib.md5(p.read_bytes()).hexdigest() for p in paths]
    if not all(c == checksums[0] for c in checksums):
        raise ValueError("XML files do not match")
    
def get_local_remote_oebin_paths(paths: pathlib.Path|Sequence[pathlib.Path]) -> Tuple[Sequence[pathlib.Path], pathlib.Path]:
    """Input one or more paths to raw data folders that exist locally for a single
    session, and get back the path to the `structure.oebin` files for each local folder
    and the expected relative path on the remote server.
    
    A processing session on DataJoint needs pointing to a single structure.oebin file,
    via the SessionDirectory table and `session_dir` key.
    
    Locally, we want to upload from the folder containing the settings.xml file - two
    folders above the structure.oebin file - but only the subfolders returned from this function. 
    """
    
    if isinstance(paths, pathlib.Path):
        paths = (paths,)
    
    def check_session_paths(paths: Sequence[pathlib.Path]):
        sessions = [get_session_folder(path) for path in paths]
        if any(not s for s in sessions):
            raise ValueError("Paths supplied must be session folders: [8+digit lims session ID]_[6-digit mouse ID]_[6-digit datestr]")
        if not all(s and s == sessions[0] for s in sessions):
            raise ValueError("Paths must all be for the same session")
        
    if not any(is_new_ephys_folder(path) for path in paths):
        raise ValueError("No new ephys folder found in paths")
    check_session_paths(paths)
    
    local_session_paths:Set[pathlib.Path] = set()
    for path in paths:
        
        ephys_subfolders =  get_raw_ephys_subfolders(path)
        
        if ephys_subfolders and len(paths) == 1:
            # parent folder supplied: we want to upload its subfolders            
            local_session_paths.update(e for e in ephys_subfolders)
            break # we're done anyway, just making this clear
        
        if ephys_subfolders:
            print(f"multiple paths supplied, with subfolders of raw data: {paths}")
            local_session_paths.update(e for e in ephys_subfolders)
            continue
        
        if is_new_ephys_folder(path):
            # single folder supplied: we want to upload this folder
            local_session_paths.add(path)
            continue
        
    local_oebin_paths = sorted(list(set(get_single_oebin_path(p) for p in local_session_paths)), key=lambda s:str(s))
    
    # we don't necessarily want to upload to s3 with the same (excessive) folder structure,
    # and it makes merging the _probeABC and _probeDEF folders easier if we do away with some levels

    # Here we make a new relative path for the server with everything between the
    # session_dir 'root' folder and two levels above the 'structure.oebin' file, which contains the
    # settings.xml file (the same for _probeABC and _probeDEF)
    local_session_paths_for_upload = [p.parent.parent.parent for p in local_oebin_paths]
    
    check_xml_files_match([p / 'settings.xml' for p in local_session_paths_for_upload])

    # and for the server we just want to point to the oebin file from two levels above -
    # shouldn't matter which oebin path we look at here, they should all have the same
    # relative structure
    remote_oebin_path = local_oebin_paths[0].relative_to(
        local_oebin_paths[0].parent.parent.parent
    )

    return local_oebin_paths, remote_oebin_path


def create_merged_oebin_file(paths: Sequence[pathlib.Path]) -> pathlib.Path:
    """Take paths to two or more structure.oebin files and merge them into one.
    
    For recordings split across multiple locations e.g. A:/*_probeABC, B:/*_probeDEF
    """
    if isinstance(paths, pathlib.Path):
        return paths
    if len(paths) == 1 and isinstance(paths[0], pathlib.Path) and paths[0].suffix == '.oebin':
        return paths[0]
    
    # ensure oebin files can be merged - if from the same exp they will have the same settings.xml file
    check_xml_files_match([p / 'settings.xml' for p in [o.parent.parent.parent for o in paths]])
    merged_oebin: dict = {}
    for oebin in paths:

        with open(oebin, 'r') as f:
            oebin_data = json.load(f)

        if not merged_oebin:
            merged_oebin = oebin_data
            continue

        for key in oebin_data:

            if merged_oebin[key] == oebin_data[key]:
                continue

            # for 'continuous' and 'events' keys, we want to merge the lists
            if isinstance(oebin_data[key], List):
                for idx, item in enumerate(oebin_data[key]):
                    if merged_oebin[key][idx] == item:
                        continue
                    else:
                        merged_oebin[key].append(item)

    if not merged_oebin:
        raise ValueError("No data found in structure.oebin files")
    
    merged_oebin_path = pathlib.Path(tempfile.gettempdir()) / 'structure.oebin' 
    with open(str(merged_oebin_path), 'w') as f:
        json.dump(merged_oebin, f, indent=4)

    return merged_oebin_path

# datajoint-related --------------------------------------------------------------------
class DataJointSession:
    """A class to handle data transfers between local rigs/network shares, and the DataJoint server."""
    class SessionDirNotFoundError(ValueError):
        pass
    
    def __init__(self, path_or_session_folder: str|pathlib.Path):
        session_folder = get_session_folder(str(path_or_session_folder))
        if session_folder is None:            
            raise self.__class__.SessionDirNotFoundError(f"Input does not contain a session directory (e.g. 123456789_366122_20220618): {path_or_session_folder}")
        self.session_folder = session_folder
        "[8+digit session ID]_[6-digit mouse ID]_[8-digit date]"
        if any(slash in str(path_or_session_folder) for slash in '\\/'):
            self.path = pathlib.Path(path_or_session_folder)
        self.session_id, self.mouse_id, *_ = self.session_folder.split('_')
        self.date = datetime.datetime.strptime(self.session_folder.split('_')[2],"%Y%m%d")
    
    @property
    def session_key(self) -> dict[str,str|int]:
        "subject:`str` and session_id:`int`"
        return (dj_session.Session & {'session_id': self.session_id}).fetch1('KEY')
    @property
    def probe_insertion(self):
        return (dj_ephys.ProbeInsertion & self.session_key)
    @property
    def clustering_task(self):
        return (dj_ephys.ClusteringTask & self.session_key)
    @property
    def curated_clustering(self):
        return (dj_ephys.CuratedClustering & self.session_key)
    @property
    def sorting_complete(self) -> bool:
        return (
            len(self.clustering_task) == 
            len(self.curated_clustering) >= 
            len(self.probe_insertion) > 0
            )
        
    @property
    def remote_session_dir_relative(self) -> str:
        "Relative session_dir on datajoint server with no database prefix."
        return (dj_session.SessionDirectory & self.session_key).fetch1('session_dir')
    
    @property
    def remote_session_dir_outbox(self) -> str:
        "Root for session sorted data on datajoint server."
        return DJ_OUTBOX / self.session_folder + '/'
    
    @property
    def remote_session_dir_inbox(self) -> str:
        "Root for session uploads on datajoint server."
        return DJ_INBOX / self.session_folder + '/'
    
    @property
    def acq_paths(self) -> List[pathlib.Path]:
        paths = []
        for drive, probes in zip('AB', ['_probeABC', '_probeDEF']):
            path = pathlib.Path(f"{drive}:{self.session_folder}/{probes}")
            if path.is_dir():
                paths.append(path)
        return paths
    
    @property
    def npexp_path(self) -> pathlib.Path|None:
        path = pathlib.Path("//allen/programs/mindscope/workgroups/np-exp") / self.session_folder
        return path if path.is_dir() else None
    
    @property    
    def local_download_path(self) -> pathlib.Path:
        return pathlib.Path(LOCAL_INBOX) / self.session_folder
    
    def add_clustering_task(self, paramset_idx: int, probe_letters:Sequence[str]='ABCDEF'):
        "For existing entries in dj_ephys.EphysRecording, create a new ClusteringTask with the specified `paramset_idx`"
        for probe_letter in probe_letters:
            probe_idx = ord(probe_letter) - ord('A')
            
            insertion_key = {'subject': self.mouse_id, 'session': self.session_id, 'insertion_number': probe_idx}

            method = (dj_ephys.ClusteringParamSet * dj_ephys.ClusteringMethod &
                    insertion_key).fetch('clustering_method')[paramset_idx].replace(".", "-")
            
            output_dir = f"{self.remote_session_dir_relative}/{method}_{paramset_idx}/probe{probe_letter}_sorted"

            task_key = {
                'subject': self.mouse_id,
                'session_id': self.session_id,
                'insertion_number': probe_idx,
                'paramset_idx': paramset_idx,
                'clustering_output_dir': output_dir,
                'task_mode': 'trigger'
            }

            if dj_ephys.ClusteringTask & task_key:
                logger.info(f'Clustering task already exists: {task_key}')
                return
            else:
                dj_ephys.ClusteringTask.insert1(task_key, replace=True)

        
    def upload(self, paths:Sequence[str|pathlib.Path]=None):
        """Upload from rig/network share to DataJoint server.
        
        Accepts a list of paths to upload, or if None, will try to upload from A:/B:,
        then np-exp, then lims.
        """
        if paths is None:
            if self.path is not None:
                paths = [self.path]
            elif ( # we're currently on a rig Acq computer
                comp := os.environ.get("aibs_comp_id", None)
                and comp in [f'NP.{rig}-Acq' for rig in '012']
                ):
                paths = self.acq_paths
        if paths is None and self.npexp_path:
            paths = [self.npexp_path]
        if paths is None:
            raise ValueError("No paths to upload from")
        
        local_oebin_paths, remote_oebin_path = get_local_remote_oebin_paths(self.path)
        local_session_paths_for_upload = [p.parent.parent.parent for p in local_oebin_paths]

        self.create_session_entry()
        
        # upload merged oebin file first
        # ------------------------------------------------------- #
        # axon upload won't allow us to overwrite an existing file
        temp_merged_oebin_path = create_merged_oebin_file(local_oebin_paths)
        dj_axon.upload_files(
            source=temp_merged_oebin_path,
            destination=f"{self.remote_session_dir_inbox}{remote_oebin_path.as_posix()}/",
            session=S3_SESSION,
            s3_bucket=S3_BUCKET,
            )

        
        # upload rest of raw data
        # ------------------------------------------------------- #
        for local_path in local_session_paths_for_upload:
            dj_axon.upload_files(
                source=local_path,
                destination=self.remote_session_dir_inbox,
                session=S3_SESSION,
                s3_bucket=S3_BUCKET,
                ignore_regex='.*.oebin',
                )

    def download(self, wait_on_sorting=False):
        "Download small files from sorting to /workgroups/dynamicrouting/."
        if not self.sorting_complete and not wait_on_sorting:
            return
        while not self.sorting_complete:
            wait_on_process(sec=1800, msg=f"Waiting for {self.session_folder} processing to complete to download sorted data...")
        dj_axon.download_files(
            source=self.remote_session_dir_inbox,
            destination= f"{self.local_download_path}\\", # if using linux - this should be fwd slash
            session=S3_SESSION,
            s3_bucket=S3_BUCKET,
            ignore_regex=R".*\.dat|.*\.mat|.*\.npy",
        )
        
    def create_session_entry(self):
        "Insert metadata for session in datajoint tables"
        if dj_session.SessionDirectory & {'session_dir': self.session_folder}:
            print(f'Session entry already exists for {self.session_folder}')

        if not dj_subject.Subject & {'subject': self.mouse_id}:
            # insert new subject
            dj_subject.Subject.insert1(
                    {
                    'subject': self.mouse_id,
                    'sex': 'U',
                    'subject_birth_date': "1900-01-01"
                    },
                skip_duplicates=True
            )
        
        with dj_session.Session.connection.transaction:
            dj_session.Session.insert1(
                {
                    'subject': self.mouse_id,
                    'session_id': self.session_id,
                    'session_datetime': self.date,
                },
            skip_duplicates=True,
            )
            dj_session.SessionDirectory.insert1(
                {
                    'subject': self.mouse_id,
                    'session_id': self.session_id,
                    'session_dir': self.remote_session_dir_relative,
                }, 
            replace=True,
            )

def wait_on_process(sec=3600, msg="Still processing..."):
    fmt = "%a %H:%M" # e.g. Mon 12:34
    file = sys.stdout
    time_now = time.strftime(fmt, time.localtime())
    time_next = time.strftime(fmt, time.localtime(time.time() + float(sec)))
    file.write("\n%s: %s\nNext check: %s\r" % (time_now, msg, time_next))
    file.flush()
    time.sleep(sec)

def add_new_clustering_parameters(clustering_method:str, paramset_desc:str, params:dict, paramset_idx:int):
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
        'clustering_method': clustering_method,
        'paramset_idx': paramset_idx,
        'paramset_desc': paramset_desc,
        'params': params,
        'param_set_hash': dict_to_uuid({
            **params, 'clustering_method': clustering_method
        })
    }
    dj_ephys.ClusteringParamSet.insert1(param_dict, skip_duplicates=True)

def get_clustering_parameters(paramset_idx:int=KS_PARAMS_INDEX) -> Tuple[str, dict]:
    "Get description and dict of parameters from paramset_idx."
    return (dj_ephys.ClusteringParamSet & {'paramset_idx': paramset_idx}).fetch1('params')

