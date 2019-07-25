"""
Defines connection to MarsDB

"""
import os

# The order of import is important here
from ibm_db import connect
import ibm_db_dbi


def mars_db_credentials():
    """Returns user and pwd for MarsDB from environment"""
    
    if 'MARS_DB_USERNAME' not in os.environ or 'MARS_DB_PASSWORD' not in os.environ:
        raise KeyError("Mars Credentials should be available in POD via k8s secrets. Contact support.")
    
    user = os.environ['MARS_DB_USERNAME']
    pwd = os.environ['MARS_DB_PASSWORD']
    
    return user, pwd
    
    
def mars_db_conn():
    """Returns a JDBC connections to MarsDB"""
    
    user, pwd = mars_db_credentials()

    mars_prod = connect('DATABASE=D2CPROD;'
                         'HOSTNAME=11.48.22.142;'  # 127.0.0.1 or localhost works if it's local
                         'PORT=50050;'
                         'PROTOCOL=TCPIP;'
                         'UID=%s;'
                         'PWD=%s;' % (user, pwd), '', '')

    return ibm_db_dbi.Connection(mars_prod)