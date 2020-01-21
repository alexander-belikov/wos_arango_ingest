import numpy as np
from arango import ArangoClient
from wos_db_studies.utils import profile_query

test = False
test = True
n_profile = 3
nq = 5
fpath = './../../results/arango'
cyear = 1978
delta_year = 5

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
    FOR j IN media FILTER j.issn == '0014-9446'
    RETURN MERGE({{ja: j.issn}}, {{stats:
    (
        FOR p in 1 INBOUND j publications_media_edges FILTER p.year == {cyear}
            FOR p2 in 1 OUTBOUND p publications_publications_edges
                FILTER p2.year < {cyear} AND p2.year >= {cyear - delta_year}
                FOR j2 in 1 OUTBOUND p2 publications_media_edges
                    COLLECT jbt=j2.issn WITH COUNT INTO size
                    SORT size DESC
        RETURN {{jb: jbt, s: size}}
    )}})
"""

orders = np.arange(1, order_max + 1, 1)
limits = 10 ** orders
if test:
    limits = [100, 1000]
else:
    limits = [int(n) for n in limits] + [None]

limits = [None]

print(limits)
for limit in limits:
    if limit:
        q = q0.replace('_insert_limit', f'LIMIT {limit}')
    else:
        q = q0.replace('_insert_limit', f'')
    profile_query(q, nq, n_profile, fpath, limit)