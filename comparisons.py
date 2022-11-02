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
    
    sorted_data_dir:pathlib.Path
    "Local path to sorted data directory;"
    
    def __init__(self, session:str|DataJointSession, probe_letter:str):
        if isinstance(session, DataJointSession):
            self.session = session
        else:
            self.session = DataJointSession(session)
        self.probe_letter = probe_letter
        
    # def __str__(self):
    #     return f"{self.session.session_folder}_{self.probe_letter}"
    def __repr__(self):
        return f"{self.__class__.__name__}('{self.session.session_folder}', probe_letter='{self.probe_letter}')"
    
    @property
    def probe_index(self) -> int:
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
        ) -> plt.Axes:
        if not self.metrics_csv.exists():
            print("No metrics.csv file found")
            return None
        if 'quality' not in self.metrics_df.columns:
            print("No quality column in metrics")
            return None
        if all(self.metrics_df['quality'] == 'noise'):
            print("All clusters are noise")
            return None
        if len(self.metrics_df.loc[self.metrics_df['quality']=='good']) == 1:
            print("Only one good cluster")
            return None
        if ax is None:
            fig, ax = plt.subplots()
        sns.kdeplot(
            self.metrics_df[metric].loc[self.metrics_df['quality'] == 'good'],
            ax=ax,
            **kwargs)
        return ax
        
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
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.sorted_data_dir.exists():
            raise FileNotFoundError(f"Probe data expected at {self.sorted_data_dir}")
      
    @property
    def sorted_data_dir(self) -> pathlib.Path:
        return self.session.downloaded_sorted_probe_paths(self.probe_letter) 

    @property
    def depth_img(self) -> pathlib.Path:
        return self.sorted_data_dir / f'probe_depth.png'

    @property
    def metrics_table(self):
        query = {
            'insertion_number': self.probe_index,
            'paramset_idx': KS_PARAMS_INDEX,
        }
        query = query | self.session.session_key
        metrics = dj_ephys.QualityMetrics.Cluster & query
        # join column from other table with quality label
        metrics *= ( 
            dj_ephys.CuratedClustering.Unit & query
        ).proj('cluster_quality_label')
        return metrics
    
    @property
    def metrics_df(self) -> pd.DataFrame:
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
            quality.set_index('cluster_id', inplace=True)
            metrics = pd.read_csv(self.metrics_csv)
            metrics.set_index('cluster_id', inplace=True)
            self._metrics_df = metrics.join(quality['quality'])
        return self._metrics_df
       
def local_dj_probe_pairs() -> Iterable[dict[str,Probe]]:
    for session in sorted_sessions():
        for probe in 'ABCDEF':

            try:
                local = ProbeLocal(session, probe)
                dj = ProbeDataJoint(session, probe)
                yield dict(local=local, dj=dj)
            except FileNotFoundError:
                continue
            