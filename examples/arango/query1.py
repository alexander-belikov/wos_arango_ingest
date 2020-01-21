import numpy as np
from arango import ArangoClient
from wos_db_studies.utils import profile_query

test = False
test = True
n_profile = 3
nq = 1
fpath = './../../results/arango'
cyear = 1978

q_aux = """
FOR p IN publications
    LET contrs = (FOR c IN 1..1 INBOUND p contributors_publications_edges RETURN c)
    LET orgs = (FOR org IN 1..1 INBOUND p organizations_publications_edges RETURN org)
    FOR c in contrs
        FOR org in orgs
            INSERT {_from : c._id, _to : org._id, "wosid": p._key, "year": p.year} 
            IN contributors_organizations_edges
"""

port = 8529
ip_addr = '127.0.0.1'
cred_name = 'root'
cred_pass = '123'
hosts = f'http://{ip_addr}:{port}'

client = ArangoClient(hosts=hosts)
sys_db = client.db('_system', username=cred_name, password=cred_pass)

cname = 'media'

r = sys_db.aql.execute(f'RETURN LENGTH({cname})')
n = list(r)[0]
order_max = int(np.log(n)/np.log(10))

q0 = f"""
LET cnts =
    (FOR j IN media _insert_limit
        LET cc = (FOR v IN 1..1 INBOUND j publications_media_edges
            FILTER v.year == {cyear}
            RETURN v)
        RETURN {{journal: j, 'number_pubs': LENGTH(cc)}})
FOR doc in cnts
    SORT doc.number_pubs DESC
    LIMIT 10
RETURN doc"""

orders = np.arange(1, order_max + 1, 1)
limits = 10 ** orders
if test:
    limits = [100, 1000]
else:
    limits = [int(n) for n in limits] + [None]

print(limits)
for limit in limits:
    if limit:
        q = q0.replace('_insert_limit', f'LIMIT {limit}')
    else:
        q = q0.replace('_insert_limit', f'')
    profile_query(q, nq, n_profile, fpath, limit)








