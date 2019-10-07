from arango import ArangoClient
import gzip
from os.path import join, expanduser
import time
from pprint import pprint

seconds = time.time()
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


client = ArangoClient(protocol='http', host='localhost', port=8529)

sys_db = client.db('_system', username=cred_name, password=cred_pass)

print('databases:')
print(sys_db.databases())

print('collections non system:')
print([c for c in sys_db.collections() if c['name'][0] != '_'])

cnames = [c['name'] for c in sys_db.collections() if c['name'][0] != '_']

for cn in cnames:
    if sys_db.has_collection(cn):
        sys_db.delete_collection(cn)

print('graphs:')
print(sys_db.graphs())

gnames = [g['name'] for g in sys_db.graphs() if gr_name in g['name']]
for gn in gnames:
    if sys_db.has_graph(gn):
        sys_db.delete_graph(gn)

print('graphs:')
print(sys_db.graphs())

if sys_db.has_graph(gr_name):
    sys_db.delete_graph(gr_name)


wos = sys_db.create_graph(gr_name)
print('graphs:')
print(sys_db.graphs())
publications = wos.create_vertex_collection(pub_col_aux)

cite = wos.create_edge_definition(
    edge_collection=cite_col,
    from_vertex_collections=[pub_col_aux],
    to_vertex_collections=[pub_col_aux])

fpath = expanduser('~/data/wos/wos_full/')
ref_fname = 'wos_cut_refs.csv.gz'


# vertex collection and edges collection as general collections

publications_as_gc = sys_db.collection(pub_col_aux)
cite_as_gc = sys_db.collection(cite_col)

# ih = publications_as_gc.add_hash_index(fields=['wid', 'id_source'], unique=True)
# ih = publications_as_gc.add_hash_index(fields=['wid'], unique=False)

reports = []

seconds = time.time()

with gzip.open(join(fpath, ref_fname), 'rt') as f:
    first = f.readline()
    print(first)
    acc = []
    for line in f:
        cnt += 1
        _, x, y = line.rstrip().split(',')
        acc.append((x, y))
        if cnt == batch_size:
            wset = list(set([x for x, _ in acc]))
            uset = list(set([x for _, x in acc]))
            wdicts = [{'wid': x, 'id_source': 'wosid'} for x in wset]
            udicts = [{'wid': x, 'id_source': 'uid'} for x in uset]
            wids_candidates = wdicts + udicts
            print(f'sending vertices; n vertices {len(wids_candidates)}')
            report = publications_as_gc.import_bulk(wids_candidates,
                                                    False)
            reports.append(report)
            seconds2 = time.time()
            n_batches += 1
            print(f'{n_batches*batch_size} iterations {seconds2 - seconds:.1f} sec passed')
            acc = []
            cnt = 0
        if n_batches > n_batches_max:
            break

ih = publications_as_gc.add_hash_index(fields=['wid'], unique=False)


publications2 = wos.create_vertex_collection(pub_col)

query0 = """
    FOR doc IN publications 
        COLLECT wid = doc.wid INTO 
            g = {"_id" : doc._id, "id_source": doc.id_source, "wid": doc.wid}
        LET names = (
            FOR value IN g[*] SORT value.id_source DESC LIMIT 1 
                RETURN {"_id" : value._id, "wid" : value.wid}
                )
        INSERT MERGE(DOCUMENT(names[0]._id), {_key : names[0].wid}) INTO publications2
    """

cursor = sys_db.aql.execute(query0)
print(next(publications2.all()))

seconds = time.time()

with gzip.open(join(fpath, ref_fname), 'rt') as f:
    first = f.readline()
    print(first)
    acc = []
    for line in f:
        cnt += 1
        _, x, y = line.rstrip().split(',')
        acc.append((x, y))
#         source = next(publications2.find({'wid': x}))
#         dest =  next(publications2.find({'wid': y}))
        acc += [{'_from': f'{pub_col}/{x}', '_to': f'{pub_col}/{y}'}]
        if cnt == batch_size:
            report = cite_as_gc.import_bulk(acc, False)
            reports.append(report)
            seconds2 = time.time()
            n_batches += 1
            print(f'{n_batches*batch_size} iterations {seconds2 - seconds:.1f} sec passed')
            acc = []
            cnt = 0
        if n_batches > n_batches_max:
            break