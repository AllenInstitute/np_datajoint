import hashlib
import json
import logging
import os
import pathlib
import re
import tempfile
from typing import List, Sequence, Set, Tuple

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings

# import mpeconfig
# configuration =  mpeconfig.source_configuration('/projects/datajoint')

DJ_INBOX = f"mindscope_dynamic-routing/inbox"
KS_PARAMS_INDEX = 1

dj.config['database.host'] = 'rds.datajoint.io'
dj.config['database.user'] = 'bjhardcastle'
dj.config['database.password'] = '@78Yymr4rvDkqc'
dj.config['database.use_tls'] = False

dj_subject = dj.create_virtual_module('subject', 'mindscope_dynamic-routing_subject')
dj_session = dj.create_virtual_module('session', 'mindscope_dynamic-routing_session')
dj_ephys = dj.create_virtual_module('ephys', 'mindscope_dynamic-routing_ephys')

djsc_config = dj_settings.get_config()
djsc_config["djauth"]["client_id"] = "MindscopeDynamicRouting" # put these in a .env file and use os.environ.get('CLIENT_ID')
djsc_config["djauth"]["client_secret"] = os.environ.get('DATAJOINT_CLIENT_SECRET')

s3_session = dj_auth.Session(
    aws_account_id=djsc_config["aws"]["account_id"],
    s3_role=djsc_config["s3"]["role"],
    auth_client_id=djsc_config["djauth"]["client_id"],
    auth_client_secret=djsc_config["djauth"]["client_secret"],
)
s3_bucket = djsc_config["s3"]["bucket"]

# general ------------------------------------------------------------------------------ #
def get_session_folder(path: str|pathlib.Path) -> str|None:
    """Extract [8+digit session ID]_[6-digit mouse ID]_[6-digit date
    str] from a string or path"""
    session_reg_exp = R"[0-9]{8,}_[0-9]{6}_[0-9]{8}"

    session_folders = re.findall(session_reg_exp, str(path))
    if session_folders:
        if not all(s == session_folders[0] for s in session_folders):
            logging.debug(
                f"Mismatch between session folder strings - file may be in the wrong folder: {path}"
            )
        return session_folders[0]
    else:
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
def create_clustering_task_entry(session_dir: str, probe: int, paramset_idx: int = KS_PARAMS_INDEX):
    """
    For existing entries in ephys.EphysRecording,
        create a new ClusteringTask with the specified "paramset_idx"
    Example:
        insertion_key = {'subject': '614608', 'session': 1187940755, 'insertion_number': 0}
        paramset_idx = 1
        create_clustering_task_entry(insertion_key, paramset_idx)
    """
    sessionID, mouseID, dateID, *_ = session_dir.split('_')
    insertion_key = {'subject': mouseID, 'session': sessionID, 'insertion_number': probe}

    method = (ephys.ClusteringParamSet * ephys.ClusteringMethod &
              insertion_key).fetch('clustering_method')[paramset_idx].replace(".", "-")
    output_dir = f"{session_dir}/{method}_{paramset_idx}/probe{chr(ord('A') + insertion_key['insertion_number'])}_sorted"

    task_key = {
        'subject': mouseID,
        'session_id': sessionID,
        'insertion_number': probe,
        'paramset_idx': paramset_idx,
        'clustering_output_dir': output_dir,
        'task_mode': 'trigger'
    }

    if ephys.ClusteringTask & task_key:
        logger.info(f'Clustering task already exists - {task_key}')
        return
    else:
        ephys.ClusteringTask.insert1(task_key, replace=True)


def insert_new_params(clustering_method: str, paramset_desc: str, params: dict, paramset_idx: int):

    def dict_to_uuid(key):
        """
        Given a dictionary `key`, returns a hash string as UUID
        """
        hashed = hashlib.md5()
        for k, v in sorted(key.items()):
            hashed.update(str(k).encode())
            hashed.update(str(v).encode())
        return uuid.UUID(hex=hashed.hexdigest())

    param_dict = {
        'clustering_method': clustering_method,
        'paramset_idx': paramset_idx,
        'paramset_desc': paramset_desc,
        'params': KS_PARAMS,
        'param_set_hash': dict_to_uuid({
            **params, 'clustering_method': clustering_method
        })
    }
    ephys.ClusteringParamSet.insert1(param_dict, skip_duplicates=True)


def create_session_entry(session_dir, remote_session_relative_path):
    """ Insert metadata for session on datajoint server

    Args:
        session_dir (str): local folder name, eg 1187940755_614608_20220629_probeDEF (_probeDEF is optional, has no effect)
        remote_session_relative_path (str): path to structure.oebin file, relative to session_dir's parent 
    """

    remote_session_relative_path = f"{pathlib.Path(remote_session_relative_path).as_posix()}/"

    sessionID, mouseID, dateID, *_ = session_dir.split('_')

    if session.SessionDirectory & {'session_dir': remote_session_relative_path}:
        print(f'Session entry already exists for {remote_session_relative_path}')

    if not subject.Subject & {'subject': mouseID}:
        # insert new subject, default DOB to '1900-01-01' - to be updated by users
        subject.Subject.insert1({
            'subject': mouseID,
            'sex': 'U',
            'subject_birth_date': "1900-01-01"
        },
        skip_duplicates=True)

    KS_PARAMS_INDEX = 1
    # insert_new_params(
    #     clustering_method='kilosort2.0',
    #     paramset_desc='Mindscope parameter set for Kilosort2.0',
    #     params=KS_PARAMS,
    #     paramset_idx=KS_PARAMS_INDEX,
    # )

    for probe_idx in range(6):
        try:
            create_clustering_task_entry(session_dir, probe_idx, KS_PARAMS_INDEX)
        except:
            pass

    with session.Session.connection.transaction:
        session.Session.insert1({
            'subject': mouseID,
            'session_id': sessionID,
            'session_datetime': dateID
        },
                                skip_duplicates=True)
        session.SessionDirectory.insert1(
            {
                'subject': mouseID,
                'session_id': sessionID,
                'session_dir': remote_session_relative_path,
            }, replace=True)


def upload(local_exp_abs_paths: pathlib.Path, remote_session_relative_path: pathlib.Path):
    """ upload contents of all local Record Node 10_/ folders to the same folder on s3 server """

    if isinstance(local_exp_abs_paths, str):
        local_exp_abs_paths = pathlib.Path(local_exp_abs_paths)

    if isinstance(remote_session_relative_path, str):
        remote_session_relative_path = pathlib.Path(remote_session_relative_path)

    if not UPLOAD_BUT_POSTPONE_SORTING:
    # first try to upload merged oebin file (since we can't overwrite files on s3)
        merged_oebin = pathlib.Path(tempfile.gettempdir()) / 'structure.oebin'
        if merged_oebin.exists():
            dj_axon.upload_files(source=merged_oebin,
                                        destination=f"{DJ_INBOX}/{remote_session_relative_path.as_posix()}/",
                                        session=s3_session,
                                        s3_bucket=s3_bucket)

    for local_exp_abs_path in local_exp_abs_paths:
        try:
            dj_axon.upload_files(source=local_exp_abs_path,
                                 destination=f"{DJ_INBOX}/{remote_session_relative_path.parts[0]}/",
                                 session=s3_session,
                                 s3_bucket=s3_bucket,
                                 ignore_regex='.*.oebin')
        except (AssertionError, ValueError):
            print('All files already exist in S3')


def download(session_dir: str, session_relative_path: pathlib.Path, wait_on_sorting=False):
    if isinstance(session_relative_path, str):
        session_relative_path = pathlib.Path(session_relative_path)
    if not (session.SessionDirectory & {'session_dir': session_relative_path.as_posix() + "/"}):
        raise FileNotFoundError(f'No existing session found with session directory: {session_relative_path}')

    destination = LOCAL_INBOX / session_dir
    # destination.mkdir(parents=True, exist_ok=True)
    # #! axon will check if folder or any files exist and abort if they do!
    # possibly want to *delete* the folder if it exists

    session_key = (session.SessionDirectory & {'session_dir': session_relative_path.as_posix() + "/"}).fetch1('KEY')

    # keep trying to download probes every Xseconds until they're available
    def probes_available() -> bool:
        """Check Datajoint for evidence of complete, sorted data"""
        probe_count = len(ephys.ProbeInsertion & session_key)
        clustering_task_count = len(ephys.ClusteringTask & session_key)
        clustering_count = len(ephys.CuratedClustering & session_key)
        #! probe_count is incorrect (len = 0)
        if clustering_task_count == clustering_count >= probe_count > 0:
            return True
        else:
            logger.info(f'{session_dir} - processing not complete: {clustering_task_count=}|{clustering_count=}|{probe_count=}')
            return False

        # return bool(probe_count)

    def wait_on_process(sec=3600, msg="Still processing..."):
        fmt = "%a %H:%M" # e.g. Mon 12:34
        file = sys.stdout
        time_now = time.strftime(fmt, time.localtime())
        time_next = time.strftime(fmt, time.localtime(time.time() + float(sec)))
        file.write("\n%s: %s\nNext check: %s\r" % (time_now, msg, time_next))
        file.flush()
        time.sleep(sec)

    @contextmanager
    def suppress_stdout():
        # from https://thesmithfam.org/blog/2012/10/25/temporarily-suppress-console-output-in-python/#:~:text=Here%E2%80%99s%20a%20handy%20way%20to%20suppress%20stdout%20temporarily,code%20into%20your%20project%3A%20from%20contextlib%20import%20contextmanager
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout
                
    if not probes_available() and not wait_on_sorting:
        return
    
    while not probes_available():
        wait_on_process(sec=1800, msg="Waiting for processing to complete to download sorted data...")

    dj_axon.download_files(
        source=f"mindscope_dynamic-routing/outbox/{session_dir}/",
        destination= f"{destination}/", # if using linux - this should be fwd slash
        session=s3_session,
        s3_bucket=s3_bucket,
        ignore_regex=R".*\.dat|.*\.mat|.*\.npy",
    )

p = pathlib.Path(R"\\allen\programs\mindscope\workgroups\np-exp\1219127153_632295_20221019")
print(get_local_remote_oebin_paths(p))

