import gc
import sys
import time
import pickle

timestamp = time.ctime()
timestamp = timestamp.replace(" ","_").replace(":","")


# sys.path.append('/home/jovyan/work/a091569/cython_home')
# sys.path.append('/home/jovyan/work/a091569/utils_and_queries')

# from get_baskets import get_baskets
# from get_baskets_cdef import get_baskets_cdef

# DATA_PATH = '/home/jovyan/datasets/d_train_20190923_60_0/'
# #DATA_PATH = '/home/jovyan/data/yh09262/ur_train_20190131_46/'
# #DATA_PATH = '/home/jovyan/data/yh09262/ur_train_20190220_3/'

# combined_dict = {}

# viewdict = get_baskets(DATA_PATH + 'ur_production_view.csv',min_n=1)
# with open('viewdict__20190923_60.pkl', 'wb') as f:
#     pickle.dump(viewdict, f)

# for key, value in viewdict.items():
#     if key not in combined_dict:
#         combined_dict[key] = {}
#     combined_dict[key]['view'] = value

# del viewdict
# gc.collect()

# atbdict = get_baskets(DATA_PATH + 'ur_production_atb.csv',min_n=1)
# with open('atbdict__20190923_60.pkl', 'wb') as f:
#     pickle.dump(atbdict, f)

# for key, value in atbdict.items():
#     if key not in combined_dict:
#         combined_dict[key] = {}
#     combined_dict[key]['atb'] = value

# del atbdict
# gc.collect()

# purdict = get_baskets(DATA_PATH + 'ur_production_purchase.csv',min_n=1)
# with open('purdict__20190923_60.pkl', 'wb') as f:
#     pickle.dump(purdict, f)

# for key, value in purdict.items():
#     if key not in combined_dict:
#         combined_dict[key] = {}
#     combined_dict[key]['purchase'] = value

# del purdict
# gc.collect()

# with open('comb_dict_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
#     pickle.dump(combined_dict, f)

# print("Saved combined_dict as comb_dict_20190923_60_{}.pkl".format(timestamp))

# Open the saved dict

print("Starting: {}".format(timestamp))

with open('comb_dict_20190923_60_Mon_Oct__7_030143_2019.pkl', 'rb') as f:
    combined_dict = pickle.load(f)

###########################################################
##################### PROCESS BASKETS #####################
###########################################################

def return_sorted_dicts(dvals):
    viewatbpurds = []
    viewatbds = []
    viewpurds = []
    atbpurds = []
    viewonlyds = []
    atbonlyds = []
    puronlyds = []
    otherds = []
    for basket in dvals:
        keys = basket.keys()
        if set(keys) == set({'view', 'atb', 'purchase'}): viewatbpurds.append(basket)
        elif set(keys) == set({'view', 'atb'}): viewatbds.append(basket)
        elif set(keys) == set({'view', 'purchase'}): viewpurds.append(basket)
        elif set(keys) == set({'atb', 'purchase'}): atbpurds.append(basket)
        elif set(keys) == set({'view'}): viewonlyds.append(basket)
        elif set(keys) == set({'atb'}): atbonlyds.append(basket)
        elif set(keys) == set({'purchase'}): puronlyds.append(basket)
        else: otherds.append(basket)
    return viewatbpurds, viewatbds, viewpurds, atbpurds, viewonlyds, atbonlyds, puronlyds, otherds

viewatbpurds, viewatbds, viewpurds, atbpurds, viewonlyds, atbonlyds, puronlyds, otherds = return_sorted_dicts(combined_dict.values())
print("Done creating baskets")

del combined_dict
gc.collect()

with open('basket_viewatbpurds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(viewatbpurds, f)
with open('basket_viewatbds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(viewatbds, f)
with open('basket_viewpurds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(viewpurds, f)
with open('basket_atbpurds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(atbpurds, f)
with open('basket_viewonlyds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(viewonlyds, f)
with open('basket_atbonlyds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(atbonlyds, f)
with open('basket_puronlyds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(puronlyds, f)
with open('basket_otherds_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(otherds, f)

stamp = time.ctime()
stamp = stamp.replace(" ","_").replace(":","")

print("Saved all baskets: {}".format(stamp))

###################################################################
##################### CREATE SIMPLE SEQUENCES #####################
###################################################################

# TODO: Add data augmentation by shuffling within each respective view, atb and purchase set.

simple_sequences = []
one_sequence = []

for x in viewatbpurds:
    simple_sequences.append(x['view'] + x['atb'] + x['purchase'])
    one_sequence += x['view']
    one_sequence += x['atb']
    one_sequence += x['purchase']

for x in viewatbds:
    simple_sequences.append(x['view'] + x['atb'])
    one_sequence += x['view']
    one_sequence += x['atb']

for x in viewpurds:
    simple_sequences.append(x['view'] + x['purchase'])
    one_sequence += x['view']
    one_sequence += x['purchase']

for x in atbpurds:
    simple_sequences.append(x['atb'] + x['purchase'])
    one_sequence += x['atb']
    one_sequence += x['purchase']

with open('simple_sequences_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(simple_sequences, f)

print("Saved simple sequences")

with open('one_sequence_20190923_60_{}.pkl'.format(timestamp), 'wb') as f:
    pickle.dump(one_sequence, f)

print("Saved one sequence")

del simple_sequences
del one_sequence
gc.collect()

stamp = time.ctime()
stamp = stamp.replace(" ","_").replace(":","")

print("Finish time: {}".format(stamp))

###################################################################
##################### CREATE OTHER SEQUENCES ######################
###################################################################

# TODO