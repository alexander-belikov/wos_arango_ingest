import numpy as np
from arango import ArangoClient
from wos_db_studies.utils import profile_query

test = False
test = True
nq = 4

port = 8529
ip_addr = '127.0.0.1'
cred_name = 'root'
cred_pass = '123'
hosts = f'http://{ip_addr}:{port}'

client = ArangoClient(hosts=hosts)
sys_db = client.db('_system', username=cred_name, password=cred_pass)


cname = 'publications'

r = sys_db.aql.execute(f'RETURN LENGTH({cname})')
n = list(r)[0]
order_max = int(np.log(n)/np.log(10))

q0 = f"""
LET cnts = (
    FOR p IN publications _insert_limit
        LET first = (FOR c IN 1..1 INBOUND p publications_publications_edges RETURN c._id)
        LET second = (FOR c IN 2..2 INBOUND p publications_publications_edges RETURN DISTINCT c._id)
        RETURN {{pub: p._id, fa: LENGTH(first), fb: LENGTH(second)}}
        )
     return cnts"""


n_profile = 3

orders = np.arange(1, order_max, 1)
limits = 10 ** orders
if test:
    limits = [100, 1000]
else:
    limits = [int(n) for n in limits] + [None]

print(limits)
for limit in limits:
    fpath = './../../results/'
    q = q0.replace('_insert_limit', f'LIMIT {limit}')
    print(q)
    profile_query(q, nq, n_profile, fpath, limit)

