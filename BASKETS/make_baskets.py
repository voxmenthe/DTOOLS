import gc
import sys
import time
import pickle

sys.path.append('/home/jovyan/work/a091569/cython_home')
sys.path.append('/home/jovyan/work/a091569/utils_and_queries')

from get_baskets import get_baskets
from get_baskets_cdef import get_baskets_cdef

DATA_PATH = '/home/jovyan/datasets/d_train_20190923_60_0/'
#DATA_PATH = '/home/jovyan/data/yh09262/ur_train_20190131_46/'
#DATA_PATH = '/home/jovyan/data/yh09262/ur_train_20190220_3/'

timestamp = time.ctime()
timestamp = timestamp.replace(" ","_").replace(":","")

combined_dict = {}

viewdict = get_baskets(DATA_PATH + 'ur_production_view.csv',min_n=1)
with open('viewdict__20190923_60.pkl', 'wb') as f:
    pickle.dump(viewdict, f)

for key, value in viewdict.items():
    if key not in combined_dict:
        combined_dict[key] = {}
    combined_dict[key]['view'] = value

del viewdict
gc.collect()

atbdict = get_baskets(DATA_PATH + 'ur_production_atb.csv',min_n=1)
with open('atbdict__20190923_60.pkl', 'wb') as f:
    pickle.dump(atbdict, f)

for key, value in atbdict.items():
    if key not in combined_dict:
        combined_dict[key] = {}
    combined_dict[key]['atb'] = value

del atbdict
gc.collect()

purdict = get_baskets(DATA_PATH + 'ur_production_purchase.csv',min_n=1)
with open('purdict__20190923_60.pkl', 'wb') as f:
    pickle.dump(purdict, f)

for key, value in purdict.items():
    if key not in combined_dict:
        combined_dict[key] = {}
    combined_dict[key]['purchase'] = value

del purdict
gc.collect()

with open('comb_dict_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(combined_dict, f)

print("Saved combined_dict as comb_dict_20190923_60_{}.pkl".format(timestamp))

