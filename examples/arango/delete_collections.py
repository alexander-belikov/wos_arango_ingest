from arango import ArangoClient
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--collection', default='all',
                    help='test setting')

client = ArangoClient()
sys_db = client.db('_system', username='root', password='123')
args = parser.parse_args()
print(args)
mode = args.collection

if mode == 'all':
    print([c['name'] for c in sys_db.collections() if c['name'][0] != '_'])
    cnames = [c['name'] for c in sys_db.collections() if c['name'][0] != '_']
    for gn in cnames:
        sys_db.delete_collection(gn)
    print([c['name'] for c in sys_db.collections() if c['name'][0] != '_'])

    print('graphs:')
    print([c['name'] for c in sys_db.graphs()])
    gnames = [c['name'] for c in sys_db.graphs()]
    for gn in gnames:
        sys_db.delete_graph(gn)
    print([c['name'] for c in sys_db.graphs()])
else:
    sys_db.delete_collection(mode)