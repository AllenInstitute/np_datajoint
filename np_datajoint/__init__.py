import np_logging

np_logging.setup(config=np_logging.fetch_zk_config("/projects/np_datajoint/defaults/logging"))

__all__ = ["classes", "utils", "config", "comparisons"]