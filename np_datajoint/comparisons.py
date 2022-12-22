from __future__ import annotations

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
from collections.abc import Iterable
from typing import Generator, List, Optional, Sequence, Set, Tuple

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import IPython
import ipywidgets as ipw
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import PIL
import seaborn as sns

from np_datajoint import classes, config, utils

sns.set_style("whitegrid")
    
LOCAL_QC_ROOT = pathlib.Path("//allen/programs/braintv/workgroups/nc-ophys/corbettb/NP_behavior_pipeline/QC")
DJ_QC_ROOT = pathlib.Path(f"//allen/programs/mindscope/workgroups/dynamicrouting/datajoint/qc")

dj_qc_subdir = lambda paramset_idx: f"ks_paramset_idx_{paramset_idx}"
QC_LOCATIONS = {
    'Local KS 2.0': LOCAL_QC_ROOT,
    'DataJoint KS 2.0': DJ_QC_ROOT / dj_qc_subdir(1),
    'DataJoint KS 2.5': DJ_QC_ROOT / dj_qc_subdir(2),
}
 
def available_qc_sessions() -> tuple[tuple[str, ...], tuple[pathlib.Path, ...]]:
    sessions: set[str] = set()
    paths: set[pathlib.Path] = set()
    for paramset_idx in (1,2):
        sorted_data_repository = DJ_QC_ROOT.with_name('inbox') / dj_qc_subdir(paramset_idx)
        for path in sorted_data_repository.iterdir():
            if session := utils.get_session_folder(path):
                paths.add(path)
                sessions.add(session)
    return tuple(sorted(list(sessions))), tuple(sorted(list(paths)))

def available_qc_img_relpaths(session: pathlib.Path) -> Sequence[pathlib.Path]:
    "Probe-related images from QC run on Datajoint-sorted data"
    root = DJ_QC_ROOT / dj_qc_subdir(paramset_idx=1) / session
    names = ('probe', 'unit', 'receptive', )
    return tuple(p.relative_to(root) for p in root.rglob('*.png') if any(s in str(p).lower() for s in names)) 

def equivalent_qc_imgs(session: str, img_relpath: pathlib.Path) -> dict[str, pathlib.Path]:
    "Versions of the same QC image created from different sorting sessions (local KS 2.0, DataJoint KS 2.0, 2.5)"
    return {label : root / session / img_relpath for label, root in QC_LOCATIONS.items()}
    
def qc_img_compare() -> IPython.display.display:
    
    display_width_pix = 500
    
    sessions, paths = available_qc_sessions()
    
    imgs = tuple(ipw.Image() for _ in QC_LOCATIONS.keys())

    def update_img_displays(session:str, relpath: pathlib.Path):
        img_paths = equivalent_qc_imgs(session, relpath)
        for idx, path in enumerate(img_paths.values()):
            img = imgs[idx]
            if path.exists():
                with path.open('rb') as f:
                    img.value = f.read()
                    width, height = PIL.Image.open(f).size
                r = width/height
                if r < 1:
                    img.layout.height = f'{display_width_pix}px'
                    img.layout.width = f'{int(display_width_pix*r)}px'
                else:
                    img.layout.height = f'{int(display_width_pix/r)}px'
                    img.layout.width = f'{display_width_pix}px'
                img.layout.visibility = 'visible'
                img.path = path
            else:
                img.layout.visibility = 'hidden'
                img.path = None
                
    session_dropdown = ipw.Dropdown(
        options=sessions,
        value='1191631184_615047_20220714',
        description="session",
        disabled=False,
    )
    img_dropdown = ipw.Dropdown(
        value=pathlib.Path('receptive_fields/615047_20220714_C_RFs_by_depth.png'),
        options=available_qc_img_relpaths(session_dropdown.value),
        description="image",
        disabled=False,
    )

    def handle_session_change(change):
        if utils.get_session_folder(change.new):
            img_dropdown.options = available_qc_img_relpaths(session_dropdown.value)
    session_dropdown.observe(handle_session_change, names="value")

    def handle_img_change(change):
        if change.new is not None and img_dropdown.value:
            update_img_displays(session_dropdown.value, img_dropdown.value)
    img_dropdown.observe(handle_img_change, names="value")

    img_link = ipw.HTML(value='Open image: ')
    
    def update_img_link(path: Optional[pathlib.Path]):
        if not path:
            img_link.value = ''
            return
        img_link.value = f'<h>{(path)}</h>'
    
    tabs = ipw.Tab()
    tabs.children = imgs
    for idx, title in enumerate(QC_LOCATIONS.keys()):
        tabs.set_title(idx, title)
    tabs.layout=ipw.Layout(align_content='center',height=f'{display_width_pix}px',width=f'{display_width_pix}px')
    
    def handle_tab_change(change:None):
        update_img_link(imgs[tabs.selected_index].path)
    tabs.observe(handle_tab_change, type='change', names="selected_index")
    
    update_img_displays(session_dropdown.value, img_dropdown.value)
    update_img_link(imgs[tabs.selected_index].path)
    
    return IPython.display.display(
        ipw.VBox([session_dropdown, img_dropdown, tabs, img_link])   
    )
    