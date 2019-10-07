from arango import ArangoClient
import gzip
from os.path import join, expanduser
import time
from pprint import pprint

port = 8529
cred_name = 'root'
cred_pass = '123'

gr_name = 'wos_csv'

pub_col_aux = 'publications_aux'
pub_col = 'publications'
cite_col = 'cites'

cnt = 0
batch_size = 100000
n_batches = 0
n_batches_max = 10

# seconds = time.time()