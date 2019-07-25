"""
Pulls all the data from MarsDB required for building a dataset for
VML lab ML training.

It consists of the following steps:
- Three queries are read from text files:
    - Image
    - Attributes
    - Text attributes
- Each query is executed on MarsDB
- Results of each query is stored as raw info on disk in a timestamped folder
    e.g. /home/jovyan/vmldata/tmp/20181109/...

"""
import time
import os
import datetime
import pandas as pd

from mars_db_conn import mars_db_conn

#QUERY_IMAGE_PATH = "/home/jovyan/mlt/data/NEW_IDM_PROD_TEXT.sql"
#QUERY_ATTR_PATH  = "/home/jovyan/mlt/data/NEW_IDM_PROD_ATTR.sql"
#QUERY_TEXT_PATH  = "/home/jovyan/mlt/data/NEW_IDM_PROD_TEXT.sql"

QUERY_IMAGE_PATH = "NEW_IDM_IMAGES.sql"
QUERY_ATTR_PATH  = "NEW_IDM_PROD_ATTR.sql"
QUERY_TEXT_PATH  = "NEW_IDM_PROD_TEXT.sql"

OUTPUT_PATH = "/home/jovyan/vmldata/tmp/"
OUTPUT_IMAGE_NAME = "raw_image_01032019.h5"
OUTPUT_ATTR_NAME = "raw_attribute_01032019.h5"
OUTPUT_TEXT_NAME = "raw_text_01032019.h5"

def load_query_from_path(path):
    """Load query as string from a textfile"""
    
    with open(path, 'r') as f:
        
        q = " ".join(map(str.strip, f.readlines()))
        
    return q


def run_query_conn(q, conn):
    """Execute query and return Pandas DataFrame"""

    df = pd.read_sql(q, conn)
    
    return df


def run_query_timed(q, conn):
    """Wrapper around run_query_conn that prints run time"""
    t0 = time.time()
    
    df = run_query_conn(q, conn)
    
    print("It took %s s to get %d records" % (time.time() - t0, df.shape[0]))
    
    return df


def timestamp():
    """Returns current date in YYYYMMMDD format"""
    
    now = datetime.datetime.now()
    
    return datetime.datetime.strftime(now, '%Y%m%d')

if __name__ == "__main__":
    
    # Keep track of overall execution time
    t00 = time.time()
    
    full_output_path = os.path.join(OUTPUT_PATH,
                                    "%s_raw_data" % timestamp())
    
    try:  
        os.mkdir(full_output_path)
    except:
        pass
    
    print("All output will be stored in %s" % full_output_path)
    
    # Define connection to MarsDB
    print("Connecting to MarsDB..."),
    conn = mars_db_conn()
    print("done!")
    
    # Load queries
    q_img = load_query_from_path(QUERY_IMAGE_PATH)
    q_attr = load_query_from_path(QUERY_ATTR_PATH)
    q_text = load_query_from_path(QUERY_TEXT_PATH)
    
    # Execute queries
    print("Start pulling image data...")
    df_img = run_query_timed(q_img, conn)
    df_img.to_hdf(os.path.join(full_output_path, OUTPUT_IMAGE_NAME), key="macys_images")
    
    print("Start pulling attribute data...")
    df_attr = run_query_timed(q_attr, conn)
    df_attr.to_hdf(os.path.join(full_output_path, OUTPUT_ATTR_NAME), key="macys_images")
    
    print("Start pulling attribute data...")
    df_text = run_query_timed(q_text, conn)
    df_text.to_hdf(os.path.join(full_output_path, OUTPUT_TEXT_NAME), key="macys_images")
    
    print("All output stored in %s" % full_output_path)
    print("It took %s s" % (time.time() - t00))