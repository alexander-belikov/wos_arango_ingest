from arango import ArangoClient
import gzip
from os.path import join, expanduser
import time
from pprint import pprint
import json

seconds = time.time()
port = 8529
cred_name = 'root'
cred_pass = '123'

gr_name = 'wos_csv'

pub_col_aux = 'publications_aux'
pub_col = 'publications'
cite_col = 'cites'


def delete_collections(sys_db, cnames=[], gnames=[]):

    print('collections (non system):')
    print([c for c in sys_db.collections() if c['name'][0] != '_'])

    # cnames = [c['name'] for c in sys_db.collections() if c['name'][0] != '_']

    for cn in cnames:
        if sys_db.has_collection(cn):
            sys_db.delete_collection(cn)

    print('collections (after delete operation):')
    print([c for c in sys_db.collections() if c['name'][0] != '_'])

    print('graphs:')
    print(sys_db.graphs())

    # gnames = [g['name'] for g in sys_db.graphs() if gr_name in g['name']]
    for gn in gnames:
        if sys_db.has_graph(gn):
            sys_db.delete_graph(gn)

    print('graphs (after delete operation):')
    print(sys_db.graphs())


def fetch_collection(sys_db, collection_name, erase_existing=False):
    if sys_db.has_collection(collection_name):
        if erase_existing:
            sys_db.delete_collection(collection_name)
        else:
            collection = sys_db.collection(collection_name)
    else:
        collection = sys_db.create_collection(collection_name)
    return collection


def clear_first_level_nones(docs, keys_keep_nones=None):
    docs = [{k: v for k, v in tdict.items() if v or k in keys_keep_nones} for tdict in docs]
    return docs


def upsert_docs_batch(docs, collection_name, match_keys, update_keys=None, filter_uniques=True):
    """

    :param docs: list of dictionaries (json-like, ie keys are strings)
    :param collection_name: collection where to upsert
    :param match_keys: keys on which to look for document
    :param update_keys: keys which to update if doc in the collection, if update_keys='doc', update all
    :param filter_uniques:
    :return:
    """

    if isinstance(docs, list):
        if filter_uniques:
            docs = {json.dumps(d, sort_keys=True) for d in docs}
            docs = [json.loads(t) for t in docs]
        docs = json.dumps(docs)
    upsert_line = ', '.join([f'\"{k}\": doc.{k}' for k in match_keys])
    upsert_line = f'{{{upsert_line}}}'

    if isinstance(update_keys, list):
        update_line = ', '.join([f'\"{k}\": doc.{k}' for k in update_keys])
        update_line = f'{{{update_line}}}'
    elif update_keys == 'doc':
        update_line = 'doc'
    else:
        update_line = '{}'
    q_update = f"""FOR doc in {docs}
                        UPSERT {upsert_line}
                        INSERT doc
                        UPDATE {update_line} in {collection_name}"""
    return q_update


def insert_edges_batch(docs_edges,
                       source_collection_name, target_collection_name,
                       edge_col_name,
                       match_keys_source=('_key', ), match_keys_target=('_key', ),
                       filter_uniques=True):
    """

    :param docs_edges: in format  [{'source': source_doc, 'target': target_doc}]
    :param source_collection_name,
    :param target_collection_name,
    :param edge_col_name:
    :param match_keys_source:
    :param match_keys_target:

    :return:
    """
    example = docs_edges[0]
    if isinstance(docs_edges, list):
        if filter_uniques:
            docs_edges = {json.dumps(d, sort_keys=True) for d in docs_edges}
            docs_edges = [json.loads(t) for t in docs_edges]
        docs_edges = json.dumps(docs_edges)

    if match_keys_source[0] == '_key':
        result_from = f'CONCAT("{target_collection_name}/", edge.source._key)'
        source_filter = ''
    else:
        result_from = 'sources[0]._id'
        filter_source = ' && '.join([f'v.{k} == edge.source.{k}' for k in match_keys_source])
        source_filter = f"""
                            LET sources = (
                                FOR v IN {source_collection_name}
                                  FILTER {filter_source} LIMIT 1
                                  RETURN v)"""

    if match_keys_target[0] == '_key':
        result_to = f'CONCAT("{target_collection_name}/", edge.target._key)'
        target_filter = ''
    else:
        result_to = 'targets[0]._id'
        filter_target = ' && '.join([f'v.{k} == edge.target.{k}' for k in match_keys_target])
        target_filter = f"""
                            LET targets = (
                                FOR v IN {target_collection_name}
                                  FILTER {filter_target} LIMIT 1
                                  RETURN v)"""

    if 'attributes' in example.keys():
        result = f'MERGE({{_from : {result_from}, _to : {result_to}}}, edge.attributes)'
    else:
        result = f'{{_from : {result_from}, _to : {result_to}}}'

    q_update = f"""
        FOR edge in {docs_edges} {source_filter} {target_filter}
            INSERT {result} in {edge_col_name}"""
    return q_update


def import_graph_from_csv(sys_db, file_path, collections_dict, edges, batch_size=100000, n_batches_test=10,
                          header=True, index_col=0, sep=',', extra_attr=None):
    """

    :param file_path:
    :param collections_dict: {i: collection_name}
    :param batch_size:
    :param n_batches_test:
    :param header:
    :param extra_attr: {i: extra_attr_dict}
        e.g. {1: 'wid', 2: 'uid'}
    :return:
    """

    # collections_dict_inv = {v: k for k, v in collections_dict.items()}
    columns_of_interest = sorted(collections_dict.keys())
    collections_of_interest = list(collections_dict.values())
    collection_aux = {c: f'{c}_aux' for c in collections_of_interest}
    cs = {col: fetch_collection(sys_db, col) for col in collections_of_interest}
    cs_aux = {col: fetch_collection(sys_db, col, True) for col in collection_aux}

    with gzip.open(file_path, 'rt') as f:

        cnt = 0
        n_batches = 0
        acc = []
        set_accum = dict()
        collection_accum = dict()
        reports = []

        if header:
            first = f.readline()
            print(first)

        for line in f:
            cnt += 1
            row = line.rstrip().split(sep)
            acc.append(row)

            if cnt == batch_size:
                for ix in columns_of_interest:
                    set_accum[ix] = set((x[ix] for x in acc))

                for ix in columns_of_interest:
                    collection_accum[ix] = [{columns_of_interest[ix]: x, 'extra': extra_attr[ix]}
                                            for x in set_accum[ix]]

                for ix, collection_name in columns_of_interest.items():
                    # print(f'sending vertices; n vertices {len(wids_candidates)}')
                    collection_aux = cs_aux[collection_aux[collection_name]]
                    report = collection_aux.import_bulk(collection_accum[ix],
                                                        False)

                    ih = collection_aux.add_hash_index(fields=[columns_of_interest[ix]], unique=False)
                    query0 = f"""
                        FOR doc IN {collection_aux[collection_name]} 
                            COLLECT wid = doc.wid INTO 
                                g = {{"_id" : doc._id, "extra": doc.id_source, "{collection_name}": doc.wid}}
                            LET names = (
                                FOR value IN g[*] SORT value.id_source DESC LIMIT 1 
                                    RETURN {{"_id" : value._id, "{collection_name}" : value.{collection_name}}}
                                    )
                            INSERT MERGE(DOCUMENT(names[0]._id), {{_key : names[0].{collection_name}}}) 
                                INTO {collection_name}
                        """

                    cursor = sys_db.aql.execute(query0)

                reports.append(report)
                seconds2 = time.time()
                n_batches += 1
                print(f'{n_batches * batch_size} iterations {seconds2 - seconds:.1f} sec passed')
                cnt = 0
                acc = []
                cs_aux = {col: fetch_collection(sys_db, col, True) for col in collection_aux}
            if n_batches > n_batches_test:
                break
