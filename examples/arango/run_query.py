import numpy as np
from arango import ArangoClient
from wos_db_studies.utils import profile_query
import argparse
from queries import qdict


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser()

parser.add_argument('-t', '--test', default=True,
                    type=str2bool,
                    help='test setting')
parser.add_argument('-n', '--nprofile', default=True,
                    type=str2bool,
                    help='number of times to profile')
parser.add_argument('-v', '--version', default='1',
                    type=str, help='version of query to run')
parser.add_argument('-b', '--batch-size', default=100000,
                    type=int, help='batch size to use for query retrieval')

args = parser.parse_args()
print(args)
test = args.test
n_profile = args.nprofile
nq = args.version
batch_size = args.batch_size

fpath = './../../results/arango'

port = 8529
ip_addr = '127.0.0.1'
cred_name = 'root'
cred_pass = '123'
hosts = f'http://{ip_addr}:{port}'

client = ArangoClient(hosts=hosts)
sys_db = client.db('_system', username=cred_name, password=cred_pass)

current_query = qdict[nq]
sub_keys = [s for s in current_query.keys() if s[0] == '_' and s[1] != '_']

if 'run_q_aux' in current_query and current_query['run_q_aux'] and 'q_aux' in current_query:
    sys_db.aql.execute(current_query['q_aux'])

r = sys_db.aql.execute(f'RETURN LENGTH({current_query["main_collection"]})')
n = list(r)[0]
order_max = int(np.log(n)/np.log(10))

q0 = current_query['q']
for k in sub_keys:
    print(k, current_query[k])
    q0 = q0.replace(k, f'{current_query[k]}')

orders = np.arange(1, order_max + 1, 1)
limits = 10 ** orders
if test:
    limits = [100, 1000]
else:
    limits = [int(n) for n in limits] + [None]

print(limits)
for limit in limits:
    if limit:
        q = q0.replace('__insert_limit', f'LIMIT {2*limit} SORT RAND() LIMIT {limit} ')
    else:
        q = q0.replace('__insert_limit', f'')
    profile_query(q, nq, n_profile, fpath, limit, batch_size=batch_size)
