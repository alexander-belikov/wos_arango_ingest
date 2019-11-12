import time
import argparse
from os.path import join, expanduser
from os import listdir
from os.path import isfile, join
import csv
from arango import ArangoClient
from wos_db_studies.utils import delete_collections, upsert_docs_batch, insert_edges_batch, clear_first_level_nones
from wos_db_studies.chunker import Chunker


def is_int(x):
    try:
        int(x)
    except:
        return False
    return True


def main(fpath, port=8529, ip_addr='127.0.0.1', cred_name='root', cred_pass='123',
         limit_files=None, max_lines=None, batch_size=50000000,
         modes=('publications', 'contributors', 'institutions', 'refs'),
         clean_start=False,
         verbose=True):

    vcollection = {
        'pub': 'publications',
        'medium': 'media',
        'lang': 'languages',
        'contributor': 'contributors',
        'organization': 'organizations'
    }

    edges_def = [
        (vcollection['pub'], vcollection['pub']),
        (vcollection['pub'],  vcollection['medium']),
        (vcollection['pub'], vcollection['lang']),
        (vcollection['contributor'], vcollection['pub']),
        (vcollection['contributor'], vcollection['organization'])
    ]

    graph_ids = ['pub2pub', 'pub2medium', 'pub2lang', 'contr2pub', 'contr2org']

    graph = {}
    for uv, g in zip(edges_def, graph_ids):
        u, v = uv
        graph_name = f'{u}_{v}_graph'
        ecollection_name = f'{u}_{v}_edges'
        graph[g] = u, v, ecollection_name, graph_name

    index_fields_dict = {
        vcollection['pub']: ['wosid'],
        vcollection['medium']: ['issn', 'isbn', 'title'],
        vcollection['lang']: ['language'],
        vcollection['contributor']: ['first_name', 'last_name'],
        vcollection['organizations']: ['organization', 'country', 'city']
    }

    # # # pub -> pub
    cite_col = 'publications_publications_edges'
    # # # pub -> medium
    published_in_medium_col = 'publications_media_edges'
    # # # pub -> lang
    published_in_lang_col = 'publications_languages_edges'
    # # # contributor -> pub
    contributed_to_col = 'contributors_publications_edges'
    # # # organization -> pub
    listed_in_col = 'organizations_publications_edges'

    # modes2graphs_ = {
    #     'publications': ['publications_media', 'publications_languages'],
    #     'contributors': ['contributors_publications'],
    #     'institutions': ['organizations_publications'],
    #     'refs': ['publications_publications']
    # }
    #
    # modes2graphs = {k: modes2graphs_[k] for k in modes if k in modes2graphs_.keys()}
    #
    # graphs = list(set([x for sublist in modes2graphs.values() for x in sublist]))
    #
    # graphs2triplet = {}
    # vertex_cols = []
    #
    # for g in graphs:
    #     u, v = g.split('_')
    #     graphs2triplet[g] = u, v, g + '_edges'
    #     vertex_cols += [u, v]
    #
    # vertex_cols = list(set(vertex_cols))
    # edge_cols = [x for _, _, x in graphs2triplet.values()]

    files_dict = {}

    for keyword in modes:
        files_dict[keyword] = sorted([f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f])

    if limit_files:
        files_dict = {k: v[:limit_files] for k, v in files_dict.items()}

    hosts = f'http://{ip_addr}:{port}'
    client = ArangoClient(hosts=hosts)

    sys_db = client.db('_system', username=cred_name, password=cred_pass)

    if clean_start:
        delete_collections(sys_db, vertex_cols + edge_cols, graphs)

        for graph_name, triplet in graphs2triplet.items():
            g = sys_db.create_graph(graph_name)
            vcol_from, vcol_to, edge_col = triplet
            if not sys_db.has_collection(vcol_from):
                _ = g.create_vertex_collection(vcol_from)
            if not sys_db.has_collection(vcol_to):
                _ = g.create_vertex_collection(vcol_to)
            _ = g.create_edge_definition(
                edge_collection=edge_col,
                from_vertex_collections=[vcol_from],
                to_vertex_collections=[vcol_to])

        for v_collection, index_fields in index_fields_dict.items():
            general_collection = sys_db.collection(v_collection)
            ih = general_collection.add_hash_index(fields=index_fields, unique=True)

    seconds_start0 = time.time()

    seconds_start = time.time()

    if verbose:
        print('ingesting pubs')
    if 'publications' in files_dict.keys():
        for filename in files_dict['publications']:
            if verbose:
                print(filename)
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
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

                    query0 = upsert_docs_batch(pubs2, vcollection['pub'], ['wosid'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    query0 = upsert_docs_batch(media2, medium_col, ['issn', 'isbn', 'title'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    query0 = upsert_docs_batch(languages2, lang_col, ['language'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    seconds2 = time.time()
                    if verbose:
                        print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

                    edges = [{'source': x, 'target': y} for x, y in zip(pubs2, media2)]

                    query0 = insert_edges_batch(edges, vcollection['pub'], medium_col, published_in_medium_col,
                                                ['wosid'], ['issn', 'isbn', 'title'], False)
                    cursor = sys_db.aql.execute(query0)

                    edges2 = [{'source': x, 'target': y} for x, y in zip(pubs2, languages2) if y['language']]

                    query0 = insert_edges_batch(edges2, vcollection['pub'], lang_col, published_in_lang_col,
                                                ['wosid'], ['language'], False)
                    cursor = sys_db.aql.execute(query0)

                    seconds3 = time.time()
                    if verbose:
                        print(f'ingested {len(edges)} edges; {len(edges2)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

        seconds_end = time.time()
        print(f'full ingest publications took {(seconds_end - seconds_start) :.1f} sec')

    if 'contributors' in files_dict.keys():
        seconds_start = time.time()

        for filename in files_dict['contributors']:
            if verbose:
                print(filename)
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
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

                    query0 = upsert_docs_batch(pubs2, vcollection['pub'], ['wosid'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    query0 = upsert_docs_batch(contrs2, contributor_col, ['last_name', 'first_name'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    seconds2 = time.time()
                    if verbose:
                        print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

                    edges = [{'source': x, 'target': y, 'attributes': {'position': int(item[header_dict['position']])}}
                             for x, y, item in zip(contrs2, pubs2, lines2)]

                    query0 = insert_edges_batch(edges, contributor_col, vcollection['pub'], contributed_to_col,
                                                ['last_name', 'first_name'], ['wosid'], False)
                    cursor = sys_db.aql.execute(query0)

                    seconds3 = time.time()
                    if verbose:
                        print(f'ingested {len(edges)} edges; {len(edges)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

            seconds_end = time.time()
        print(f'full ingest contributors took {(seconds_end - seconds_start) :.1f} sec')

    if 'institutions' in files_dict.keys():
        for filename in files_dict['institutions']:
            if verbose:
                print(filename)
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
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

                    query0 = upsert_docs_batch(pubs2, vcollection['pub'], ['wosid'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    query0 = upsert_docs_batch(orgs2, organizations_col, ['organization', 'country', 'city'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    seconds2 = time.time()
                    if verbose:
                        print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

                    edges = [{'source': x, 'target': y, 'attributes': {'position': item[header_dict['addr_num']]}}
                             for x, y, item in zip(orgs2, pubs2, lines2)]

                    query0 = insert_edges_batch(edges, organizations_col, vcollection['pub'], listed_in_col,
                                                ['organization', 'country', 'city'], ['wosid'], False)
                    cursor = sys_db.aql.execute(query0)

                    seconds3 = time.time()
                    if verbose:
                        print(f'ingested {len(edges)} edges; {len(edges)} edges2 ; {seconds3 - seconds2:.1f} sec passed')

            seconds_end = time.time()

        print(f'full ingest organizations took {(seconds_end - seconds_start) :.1f} sec')

    if 'refs' in files_dict.keys():
        for filename in files_dict['refs']:
            if verbose:
                print(filename)
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
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

                    query0 = upsert_docs_batch(pubs, vcollection['pub'], ['wosid'], 'doc', True)
                    cursor = sys_db.aql.execute(query0)

                    seconds2 = time.time()
                    if verbose:
                        print(f'ingested {len(pubs)} nodes; {seconds2 - seconds0:.1f} sec passed')

                    edges_ = [{'source': x, 'target': y} for x, y in zip(pubs_w, pubs_u)]

                    query0 = insert_edges_batch(edges_, vcollection['pub'], vcollection['pub'],
                                                cite_col, ['wosid'], ['wosid'], False)
                    cursor = sys_db.aql.execute(query0)

                    seconds3 = time.time()
                    if verbose:
                        print(f'ingested {len(edges_)} edges; {seconds3 - seconds2:.1f} sec passed')

        seconds_end = time.time()
        print(f'full ingest refs took {(seconds_end - seconds_start) :.1f} sec')
    seconds_end0 = time.time()
    print(f'full ingest refs took {(seconds_end0 - seconds_start0) :.1f} sec')


if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--datapath',
                            default=expanduser('~/data/wos/wos_full/'),
                            help='Path to data files')

        parser.add_argument('-i', '--id-addr',
                            default='127.0.0.1', type=str,
                            help='port for arangodb connection')

        parser.add_argument('-p', '--port',
                            default=8529, type=int,
                            help='port for arangodb connection')

        parser.add_argument('-l', '--login-name',
                            default='root',
                            help='login name for arangodb connection')

        parser.add_argument('-w', '--login-password',
                            default='123',
                            help='login password for arangodb connection')

        parser.add_argument('-f', '--limit-files',
                            default=None, type=str,
                            help='max files per type to use for ingestion')

        parser.add_argument('-m', '--max-lines',
                            default=None, type=str,
                            help='max lines per file to use for ingestion')

        parser.add_argument('-v', '--verbose',
                            default=False, type=bool,
                            help='verbosity level')

        parser.add_argument('-b', '--batch-size',
                            default=50000000, type=int,
                            help='number of symbols read from (archived) file for a single batch')

        parser.add_argument('-g', '--graph-name',
                            default='wos_big',
                            help='login name for arangodb connection')

        parser.add_argument('--modes',
                            nargs='*',
                            default=['refs', 'publications', 'contributors', 'institutions'])

        parser.add_argument('--clean-start',
                            type=bool,
                            default=False)

        args = parser.parse_args()

        if is_int(args.limit_files):
            limit_files = int(args.limit_files)
        else:
            limit_files = None

        if is_int(args.max_lines):
            max_lines = int(args.max_lines)
        else:
            max_lines = None

        fpath = args.datapath
        port = args.port
        cred_name = args.login_name
        cred_pass = args.login_password
        verbose = args.verbose
        batch_size = args.batch_size
        modes = args.modes
        clean_start = args.clean_start
        id_addr = args.id_addr

        if verbose:
            print(f'max_lines : {max_lines}; limit_files: {limit_files}')
            print(f'modes: {modes}')
            print(f'clean start: {clean_start}')

        main(fpath, port, id_addr, cred_name, cred_pass,
             limit_files, max_lines, batch_size,
             modes,
             clean_start,
             verbose)

