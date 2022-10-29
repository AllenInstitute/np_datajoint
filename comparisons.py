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
from collections.abc import Iterable
from typing import Generator

import datajoint as dj
import djsciops.authentication as dj_auth
import djsciops.axon as dj_axon
import djsciops.settings as dj_settings
import mpeconfig
import pandas as pd
import IPython

import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import IPython
import ipywidgets as ipw

from utils import *

sns.set_style("whitegrid")

class Probe:
    def __init__(self, session:str|DataJointSession, probe_letter:str):
        if isinstance(session, DataJointSession):
            self.session = session
        else:
            self.session = DataJointSession(session)
        self.probe_letter = probe_letter
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        
    # def __str__(self):
    #     return f"{self.session.session_folder}_{self.probe_letter}"
    def __repr__(self):
        return f"{self.__class__.__name__}('{self.session.session_folder}', probe='{self.probe_letter}')"
    
    def path(self) -> pathlib.Path:
        raise NotImplementedError
    
    @property
    def metrics(self) -> pathlib.Path:
        return self.path / 'metrics.csv'
    
    @property
    def metrics_df(self) -> pd.DataFrame:
        if not hasattr(self, '_metrics_df'):
            self._metrics_df = pd.read_csv(self.metrics) if self.metrics.exists() else None
        return self._metrics_df
    
    def metrics_plot(self, metric:str, ax:plt.Axes=None, **kwargs) -> plt.Axes:
        if ax is None:
            fig, ax = plt.subplots()
        sns.kdeplot(self.metrics_df[metric], ax=ax, **kwargs)
        return ax
        
class ProbeLocal(Probe):
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        
    @property
    def path(self) -> pathlib.Path:
        return self.session.npexp_sorted_probe_paths(self.probe_letter)

    @property
    def depth_img(self) -> pathlib.Path:
        img = self.path.parent.parent / f'probe_depth_{self.probe_letter}.png' 
        return img if img.exists() else self.path.parent.parent / f'probe_depth.png'
    
class ProbeDataJoint(Probe):
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        
    @property
    def path(self) -> pathlib.Path:
        return self.session.downloaded_sorted_probe_paths(self.probe_letter) 

    @property
    def depth_img(self) -> pathlib.Path:
        return self.path / f'probe_depth.png'
    

def local_dj_probe_pairs() -> Iterable[dict[str,Probe]]:
    for session in sorted_sessions():
        for probe in 'ABCDEF':

            try:
                local = ProbeLocal(session, probe)
                dj = ProbeDataJoint(session, probe)
                yield dict(local=local, dj=dj)
            except FileNotFoundError:
                continue
            