import hashlib
import json
import logging
import pathlib
import re
import tempfile
from typing import List, Sequence, Set, Tuple


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

p = pathlib.Path(R"\\allen\programs\mindscope\workgroups\np-exp\1219127153_632295_20221019")
print(get_local_remote_oebin_paths(p))