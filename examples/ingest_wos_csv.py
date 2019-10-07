from arango import ArangoClient
from os.path import join, expanduser
import time
import pickle
import gzip
from gc import collect
import json
from pprint import pprint
from wos_arango_ingest.utils import delete_collections, upsert_docs_batch, insert_edges_batch, clear_first_level_nones
from wos_arango_ingest.chunker import Chunker
from arango import ArangoClient
from os.path import join, expanduser
from os import listdir
from os.path import isfile, join
import csv


limit_files = 5
verbose = True
keywords = ['refs', 'publications', 'contributors', 'institutions']

fpath = expanduser('~/data/wos/wos_full/')

files_dict = {}

for keyword in keywords:
    files_dict[keyword] = sorted([f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f])

if limit_files:
    files_dict = {k: v[:limit_files] for k, v in files_dict.items()}

batch_size = 5000000
edges_max = 100000

port = 8529
cred_name = 'root'
cred_pass = '123'
gr_name = 'wos_csv'

pub_col = 'publications'
medium_col = 'media'
lang_col = 'languages'
contributor_col = 'contributors'
organizations_col = 'organizations'

# pub -> pub
cite_col = 'cited'
# pub -> medium
published_in_medium_col = 'published_in_medium'
# pub -> lang
published_in_lang_col = 'published_in_language'
# contributor -> pub
contributed_to_col = 'contributed_to_publication'
# organization -> pub
listed_in_col = 'listed_in_publication'

vertex_cols = [pub_col, medium_col, lang_col, contributor_col, organizations_col]
edge_cols = [cite_col, published_in_lang_col, published_in_medium_col,
             contributed_to_col, listed_in_col]

edges_cols_dict = {
                cite_col: (pub_col, pub_col),
                published_in_medium_col: (pub_col, medium_col),
                published_in_lang_col: (pub_col, lang_col),
                contributed_to_col: (contributor_col, pub_col),
                listed_in_col: (organizations_col, pub_col)
}

index_fields_dict = {
    pub_col: ['wosid'],
    medium_col: ['issn', 'isbn'],
    lang_col: ['language'],
    contributor_col: ['first_name', 'last_name'],
    organizations_col: ['organization', 'country', 'city']
}

client = ArangoClient(protocol='http', host='localhost', port=8529)

sys_db = client.db('_system', username=cred_name, password=cred_pass)

# clean up
delete_collections(sys_db, vertex_cols + edge_cols, [gr_name])

wos = sys_db.create_graph(gr_name)

for c in vertex_cols:
    _ = wos.create_vertex_collection(c)

for edge_col, uvs in edges_cols_dict.items():
    vcol_from, vcol_to = uvs
    published_in_medium = wos.create_edge_definition(
        edge_collection=edge_col,
        from_vertex_collections=[vcol_from],
        to_vertex_collections=[vcol_to])

for v_collection, index_fields in index_fields_dict.items():
    general_collection = sys_db.collection(v_collection)
    ih = general_collection.add_hash_index(fields=index_fields, unique=True)


seconds_start = time.time()

if verbose:
    print('ingesting pubs')

for filename in files_dict['publications']:
    if verbose:
        print(filename)
    chk = Chunker(join(fpath, filename), batch_size, edges_max)
    header = chk.pop_header()
    header = header.split(',')
    header_dict = dict(zip(header, range(len(header))))

    while not chk.done:
        lines = chk.pop()
        if lines:
            lines2 = [next(csv.reader([line.rstrip()], skipinitialspace=True)) for line in lines]
            pubs = [{'wosid': item[header_dict['wos_id']],
                     'accession_no': item[header_dict['accession_no']],
                     'title': item[header_dict['title']],
                     'year': int(item[header_dict['pubyear']]),
                     'month': int(item[header_dict['pubmonth']]),
                     'day': int(item[header_dict['pubday']]),
                     'page_range': item[header_dict['page_range']],
                     'page_count': int(item[header_dict['page_count']])} for item in lines2]

            media = [{
                'issn': item[header_dict['issn']],
                'eissn': item[header_dict['eissn']],
                'isbn': item[header_dict['isbn']],
                'eisbn': item[header_dict['eisbn']],
                'title': item[header_dict['source']]} for item in lines2]

            languages = [{'language': item[header_dict['language']]} for item in lines2]

            pubs2 = clear_first_level_nones(pubs)
            media2 = clear_first_level_nones(media)
            languages2 = clear_first_level_nones(languages)

            seconds0 = time.time()

            query0 = upsert_docs_batch(pubs2, pub_col, ['wosid'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            query0 = upsert_docs_batch(media2, medium_col, ['issn', 'isbn'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            query0 = upsert_docs_batch(languages2, lang_col, ['language'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            seconds2 = time.time()
            if verbose:
                print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

            edges = [{'source': x, 'target': y} for x, y in zip(pubs2, media2)]

            query0 = insert_edges_batch(edges, pub_col, medium_col, published_in_medium_col,
                                        ['wosid'], ['issn', 'isbn'], False)
            cursor = sys_db.aql.execute(query0)

            edges2 = [{'source': x, 'target': y} for x, y in zip(pubs2, languages2) if y['language']]

            query0 = insert_edges_batch(edges2, pub_col, lang_col, published_in_lang_col,
                                        ['wosid'], ['language'], False)
            cursor = sys_db.aql.execute(query0)

            seconds3 = time.time()
            if verbose:
                print(f'ingested {len(edges)} edges; {len(edges2)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

seconds_end = time.time()
print(f'full ingest publications took {(seconds_end - seconds_start) :.1f} sec')

seconds_start = time.time()

for filename in files_dict['contributors']:
    if verbose:
        print(filename)
    chk = Chunker(join(fpath, filename), batch_size, edges_max)
    header = chk.pop_header()
    header = header.split(',')
    header_dict = dict(zip(header, range(len(header))))

    while not chk.done:
        lines = chk.pop()
        if lines:
            lines2 = [next(csv.reader([line.rstrip()], skipinitialspace=True)) for line in lines]
            pubs = [{'wosid': item[header_dict['wos_id']]} for item in lines2]

            contrs = [{
                'first_name': item[header_dict['first_name']],
                'last_name': item[header_dict['last_name']]} for item in lines2]

            pubs2 = clear_first_level_nones(pubs)
            contrs2 = clear_first_level_nones(contrs)

            seconds0 = time.time()

            query0 = upsert_docs_batch(pubs2, pub_col, ['wosid'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            query0 = upsert_docs_batch(contrs2, contributor_col, ['last_name', 'first_name'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            seconds2 = time.time()
            if verbose:
                print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

            edges = [{'source': x, 'target': y, 'attributes': {'position': int(item[header_dict['position']])}}
                     for x, y, item in zip(contrs2, pubs2, lines2)]

            query0 = insert_edges_batch(edges, contributor_col, pub_col, contributed_to_col,
                                        ['last_name', 'first_name'], ['wosid'], False)
            cursor = sys_db.aql.execute(query0)

            seconds3 = time.time()
            if verbose:
                print(f'ingested {len(edges)} edges; {len(edges)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

    seconds_end = time.time()
print(f'full ingest contributors took {(seconds_end - seconds_start) :.1f} sec')

for filename in files_dict['institutions']:
    if verbose:
        print(filename)
    chk = Chunker(join(fpath, filename), batch_size, edges_max)
    header = chk.pop_header()
    header = header.split(',')
    header_dict = dict(zip(header, range(len(header))))

    seconds_start = time.time()

    while not chk.done:
        lines = chk.pop()
        if lines:
            lines2 = [next(csv.reader([line.rstrip()], skipinitialspace=True)) for line in lines]
            pubs = [{'wosid': item[header_dict['wos_id']]} for item in lines2]

            orgs = [{
                'organization': item[header_dict['organization']],
                'country': item[header_dict['country']],
                'city': item[header_dict['city']]
            } for item in lines2]

            pubs2 = clear_first_level_nones(pubs)
            orgs2 = clear_first_level_nones(orgs)

            seconds0 = time.time()

            query0 = upsert_docs_batch(pubs2, pub_col, ['wosid'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            query0 = upsert_docs_batch(orgs2, organizations_col, ['organization', 'country', 'city'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            seconds2 = time.time()
            if verbose:
                print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

            edges = [{'source': x, 'target': y, 'attributes': {'position': item[header_dict['addr_num']]}}
                     for x, y, item in zip(orgs2, pubs2, lines2)]

            query0 = insert_edges_batch(edges, organizations_col, pub_col, listed_in_col,
                                        ['organization', 'country', 'city'], ['wosid'], False)
            cursor = sys_db.aql.execute(query0)

            seconds3 = time.time()
            if verbose:
                print(f'ingested {len(edges)} edges; {len(edges)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

    seconds_end = time.time()

print(f'full ingest organizations took {(seconds_end - seconds_start) :.1f} sec')

for filename in files_dict['refs']:
    if verbose:
        print(filename)
    chk = Chunker(join(fpath, filename), batch_size, edges_max)
    seconds = time.time()
    nodes_time = 0
    edges_time = 0

    seconds_start = time.time()

    while not chk.done:
        lines = chk.pop()
        if lines:
            lines2 = [line.rstrip().split(',') for line in lines]

            seconds0 = time.time()

            pubs_w = [{'wosid': w, 'original': True} for _, w, _ in lines2]
            pubs_u = [{'wosid': u} for _, _, u in lines2]
            pubs_w = clear_first_level_nones(pubs_w)
            pubs_u = clear_first_level_nones(pubs_u)
            pubs = pubs_u + pubs_w

            query0 = upsert_docs_batch(pubs, pub_col, ['wosid'], 'doc', True)
            cursor = sys_db.aql.execute(query0)

            seconds2 = time.time()
            if verbose:
                print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

            edges_ = [{'source': x, 'target': y} for x, y in zip(pubs_w, pubs_u)]

            query0 = insert_edges_batch(edges_, pub_col, pub_col, cite_col, ['wosid'], ['wosid'], False)
            cursor = sys_db.aql.execute(query0)

            seconds3 = time.time()
            if verbose:
                print(f'ingested {len(edges_)} edges; {seconds3 - seconds2:.1f} sec passed')

seconds_end = time.time()
print(f'full ingest refs took {(seconds_end - seconds_start) :.1f} sec')
