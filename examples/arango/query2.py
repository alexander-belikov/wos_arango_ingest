import numpy as np
from arango import ArangoClient
from wos_db_studies.utils import profile_query
import string
from wos_db_studies.stopwords import _stopwords

test = False
test = True
n_profile = 3
nq = 2
fpath = './../../results/arango'

puncts = list(string.punctuation)

all_stops = puncts + _stopwords

q0 = f"""
FOR doc IN publications _insert_limit 
    FOR word in SPLIT(LOWER(doc.title), ' ') 
        COLLECT uword = word WITH COUNT INTO count
        SORT count DESC
        FILTER uword NOT IN {str(all_stops)}
        RETURN {{uword, count}}
"""

port = 8529
ip_addr = "127.0.0.1"
cred_name = "root"
cred_pass = "123"
hosts = f"http://{ip_addr}:{port}"

client = ArangoClient(hosts=hosts)
sys_db = client.db("_system", username=cred_name, password=cred_pass)


cname = "contributors"

r = sys_db.aql.execute(f"RETURN LENGTH({cname})")
n = list(r)[0]
order_max = int(np.log(n) / np.log(10))

orders = np.arange(1, order_max + 1, 1)
limits = 10 ** orders
if test:
    limits = [100, 1000]
else:
    limits = [int(n) for n in limits] + [None]

print(limits)
for limit in limits:
    if limit:
        q = q0.replace('_insert_limit', f'SORT RAND() LIMIT {limit} ')
    else:
        q = q0.replace("_insert_limit", f"")
    profile_query(q, nq, n_profile, fpath, limit)
