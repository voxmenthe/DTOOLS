import gc
import sys
import numpy as np
import pandas as pd

sys.path.append('/home/jovyan/work/a091569/cython_home')

# cython utilities
from get_csv_rows import get_csv_rows

DATA_DIR = '/home/jovyan/work/a091569/DTOOLS/BIGQUERY_DATA/'

view_file = 'pros_prod_vw_csv_120_pros_product_view.csv'
atb_file = 'pros_cart_item_addn_csv_120_pros_cart_item_addition.csv'
purchase_file = 'pros_cart_item_pur_csv_120_pros_cart_item_purchase.csv'
attr_file = 'product_attributes_csv_ur_develop_productattribute.csv'

view = pd.read_csv(view_file, sep='\t') 
atb = pd.read_csv(atb_file, sep='\t')
purchase = pd.read_csv(purchase_file, sep='\t')
