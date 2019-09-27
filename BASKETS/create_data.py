import sys
sys.path.append('/home/jovyan/work/a091569/RECOM_LIBS/recom-evaluator')
sys.path.append('/home/jovyan/work/a091569/RECOM_LIBS/mml-lib')

import gc
import numpy as np
import itertools, logging
np.random.seed(12345)
import warnings
warnings.filterwarnings('ignore')
import os, sys
import pandas as pd
from multiprocessing import  Pool
from gensim.models import Word2Vec
from sklearn.metrics.pairwise import cosine_similarity
import heapq
#from mml_lib.visualization import display_recom_panel, display_product
import recomevaluator.run_all as evaluate_recoms
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt
from mml_lib.datasets import get_datacollection

MIN_VIEW_EVENTS = 2
MIN_ATB_EVENTS = 1
MIN_PURCHASE_EVENTS = 0
DATA_FOLDER = '/home/jovyan/datasets/d_train_20190923_60_0/'

dc = get_datacollection(DATA_FOLDER)(verbose=True)

views = dc.view_events.groupby(['indiv_id'])['product_id'].apply(list)
views = views[views.map(len) > MIN_VIEW_EVENTS]
views = views.to_frame()

atb = dc.atb_events.groupby(['indiv_id'])['product_id'].apply(list)
atb = atb[atb.map(len) > MIN_ATB_EVENTS]
atb = atb.to_frame()

purchase = dc.purchase_events.groupby(['indiv_id'])['product_id'].apply(list)
purchase = purchase[purchase.map(len) > MIN_PURCHASE_EVENTS]
purchase = purchase.to_frame()

view_atb = views.join(atb, on='indiv_id', lsuffix='_views', rsuffix='_atb') 
common_events = view_atb.join(purchase, on='indiv_id', rsuffix = '_purchase')