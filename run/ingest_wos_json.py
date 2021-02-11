import time
import argparse
import yaml
from os.path import join, expanduser
from os import listdir
from os.path import isfile, join
import gzip
import json
from wos_db_studies.util.db import (
    delete_collections,
    define_collections,
    upsert_docs_batch,
    insert_edges_batch,
    get_arangodb_client,
    define_extra_edges
)
from wos_db_studies.utils import clear_first_level_nones, update_to_numeric, merge_doc_basis
from wos_db_studies.util.pjson import parse_config
from wos_db_studies.utils_json import process_document_top, parse_edges
import pathos.multiprocessing as mp
from functools import partial
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


def is_int(x):
    try:
        int(x)
    except:
        return False
    return True


def main(
    fpath,
    protocol="http",
    ip_addr="127.0.0.1",
    port=8529,
    database="_system",
    cred_name="root",
    cred_pass="123",
    limit_files=None,
    keyword="DSSHPSH",
    clean_start="all",
    prefix="toy",
    config=None
):
    sys_db = get_arangodb_client(protocol,
                                 ip_addr,
                                 port,
                                 database,
                                 cred_name,
                                 cred_pass)

    vcollections, vmap, graphs, index_fields_dict, eindex = parse_config(config=config_,
                                                                         prefix=prefix)

    edge_des, excl_fields = parse_edges(config["json"], [], defaultdict(list))
    # all_fields_dict = {
    #     k: v["fields"] for k, v in config["vertex_collections"].items()
    # }

    # if clean_start == "all":
    #     delete_collections(sys_db, vcollections + ecollections, actual_graphs)
    # elif clean_start == "edges":
    #     delete_collections(sys_db, ecollections, [])
    delete_collections(sys_db, [], [], delete_all=True)

    define_collections(sys_db, graphs, vmap, index_fields_dict, eindex)

    files = [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]

    for filename in files:
        with gzip.GzipFile(join(fpath, filename), 'rb') as fp:
            fps = fp
            data = json.load(fps)

        seconds0 = time.time()

        # kwargs = {"config": config["json"],
        #           "vertex_config": config["vertex_collections"],
        #           "edge_fields": excl_fields,
        #           "merge_collections": ["publication"]
        #           }
        # func = partial(process_document_top, **kwargs)
        # n_proc = 4
        # with mp.Pool(n_proc) as p:
        #     rtot = p.map(func, data)

        rtot = defaultdict(list)
        for j, item in enumerate(data):
            r0 = process_document_top(item, config["json"],
                                      config["vertex_collections"],
                                      excl_fields, ["publication"]
                                      )
            #     print(j, r0["publication"][0]["_key"])
            for k, v in r0.items():
                rtot[k].extend(v)

        seconds1 = time.time()
        logger.info(
            f"parsed {len(data)} items; {seconds1 - seconds0:.1f} sec"
        )

        kkey_vertex = sorted([k for k in rtot.keys() if isinstance(k, str)])
        kkey_edge = sorted([k for k in rtot.keys() if isinstance(k, tuple)])

        cnt = 0
        for k in kkey_vertex:
            v = rtot[k]
            r = merge_doc_basis(rtot[k], index_fields_dict[k])
            cnt += len(r)
            #         print(k, vmap[k], index_fields_dict[k], len(rtot[k]), len(r))
            query0 = upsert_docs_batch(
                v, vmap[k], index_fields_dict[k], "doc", False
            )
            cursor = sys_db.aql.execute(query0)

        seconds2 = time.time()
        logger.info(
            f"ingested {cnt} vertices; {seconds2 - seconds1:.1f} sec"
        )

        cnt = 0

        for uv in kkey_edge:
            u, v = uv
            cnt += len(rtot[uv])
            query0 = insert_edges_batch(
                rtot[uv],
                vmap[u],
                vmap[v],
                graphs[uv]["edge_name"],
                index_fields_dict[u],
                index_fields_dict[v],
                False,
            )

            cursor = sys_db.aql.execute(query0)

        seconds3 = time.time()
        logger.info(
            f"ingested {cnt} edges; {seconds3 - seconds2:.1f} sec"
        )

        # for filename in files:
        #     seconds_start_file = time.time()
        #     if filename[-2:] == "gz":
        #         open_foo = gzip.GzipFile
        #     elif filename[-3:] == "xml":
        #         open_foo = open
        #     else:
        #         raise ValueError("Unknown file type")
        #     with open_foo(filename, 'rb') as fp:
        #         if pattern:
        #             fps = FPSmart(fp, pattern)
        #         else:
        #             fps = fp
        #         data = json.load(fps)
        #
        #     # parallelize
        #     func = partial(apply_mapper, config["json"])
        #     n_proc = 4
        #     with mp.Pool(n_proc) as p:
        #         fname_merge = p.map(func, data)


    # for cname, fields in numeric_fields_dict.items():
    #     for field in fields:
    #         query0 = update_to_numeric(cname, field)
    #         cursor = sys_db.aql.execute(query0)

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    # for uv, item in graphs.items():
    #     if item["type"] == "indirect":
    #         query0 = define_extra_edges(item)
    #         cursor = sys_db.aql.execute(query0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--datapath", default=expanduser("../data/toy"), help="path to data files"
    )

    parser.add_argument(
        "-i",
        "--id-addr",
        default="127.0.0.1",
        type=str,
        help="port for arangodb connection",
    )

    parser.add_argument(
        "--protocol", default="http", type=str, help="protocol for arangodb connection"
    )

    parser.add_argument(
        "-p", "--port", default=8529, type=int, help="port for arangodb connection"
    )

    parser.add_argument(
        "-l", "--login-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--login-password",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument("--db", default="_system", help="db for arangodb connection")

    parser.add_argument(
        "-f",
        "--limit-files",
        default=None,
        type=str,
        help="max files per type to use for ingestion",
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        default=5000,
        type=int,
        help="number of docs in the batch pushed to db",
    )

    parser.add_argument("--keyword", default="DSSHPSH", help="prefix for files to be processed")

    parser.add_argument("--prefix", default="toy", help="prefix for collection names")

    parser.add_argument(
        "--clean-start",
        type=str,
        default="all",
        help='"all" to wipe all the collections, "edges" to wipe only edges',
    )

    parser.add_argument(
        "--config-path", type=str, default="../conf/wos_json_simple.yaml", help="",
    )

    args = parser.parse_args()

    if is_int(args.limit_files):
        limit_files_ = int(args.limit_files)
    else:
        limit_files_ = None

    batch_size = args.batch_size
    clean_start = args.clean_start

    with open(args.config_path, "r") as f:
        config_ = yaml.load(f, Loader=yaml.FullLoader)

    logger.info(f"limit_files: {limit_files_}")
    logger.info(f"clean start: {clean_start}")

    logging.basicConfig(filename='ingest_json.log', level=logging.INFO)

    main(
        expanduser(args.datapath),
        args.protocol,
        args.id_addr,
        args.port,
        args.db,
        args.login_name,
        args.login_password,
        limit_files_,
        args.keyword,
        clean_start,
        prefix=args.prefix,
        config=config_,
    )
