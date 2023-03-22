import np_session
import np_logging
from np_datajoint import *

logger = np_logging.getLogger()

logger.setLevel('INFO')
np_logging.getLogger('Primary').setLevel('ERROR')
np_logging.getLogger('DataJoint').setLevel('ERROR')


for session, probe in {
    
    '1187940755_614608_20220629': 'C', # not sorted idx=1 yet
    # '1215593634_632296_20221004': 'F',
    '1190892228_615047_20220711': 'B', # not sorted idx=1 yet
    # '1202644967_623784_20220830': 'B',
}.items():
    s = DataJointSession(session)
    # s.upload(probes=[probe])
    s.add_clustering_task(2, probe)
    s.add_clustering_task(1, probe)
    # s.plot_driftmap(probe, paramset_idx=2)
    
# {
#     '1187940755': 2,
#     '1215593634': 5,
#     '1190892228': 1,
#     '1202644967': 1,
# }