from arango import ArangoClient
import gzip
from os.path import join, expanduser
import time
import json
from datetime import datetime
seconds = time.time()

gr_name = "wos_csv"

pub_col_aux = "publications_aux"
pub_col = "publications"
cite_col = "cites"


def standardize(k):
    # 1. clean period
    k = k.translate(str.maketrans({'.': ''}))
    # 2. try to split by ", "
    k = k.split(", ")
    if len(k) < 2:
        k = k[0].split(" ")
    else:
        k[1] = k[1].translate(str.maketrans({' ': ''}))
    return ",".join(k)


def parse_date_standard(input_str):
    dt = datetime.strptime(input_str, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day
    return year, month, day


def parse_date_conf(input_str):
    dt = datetime.strptime(input_str, "%Y%m%d")
    year, month, day = dt.year, dt.month, dt.day
    return year, month, day


def parse_conf_date_text(input_str):
    # date_b = 'NOV 03-04, 2008'.split(", ")
    dt = datetime.strptime(input_str, "%Y-%m-%d")

    year = datetime.strptime(dt[-1], "%Y").year
    date_b_ = datetime.strptime(dt[0].split("-")[0], "%b %d")
    month, day = date_b_.month, date_b_.day
    return year, month, day


def try_int(x):
    try:
        x = int(x)
        return x
    except:
        return x


def delete_collections(sys_db, cnames=[], gnames=[]):

    print("collections (non system):")
    print([c for c in sys_db.collections() if c["name"][0] != "_"])

    # cnames = [c['name'] for c in sys_db.collections() if c['name'][0] != '_']

    for cn in cnames:
        if sys_db.has_collection(cn):
            sys_db.delete_collection(cn)

    print("collections (after delete operation):")
    print([c for c in sys_db.collections() if c["name"][0] != "_"])

    print("graphs:")
    print(sys_db.graphs())

    # gnames = [g['name'] for g in sys_db.graphs() if gr_name in g['name']]
    for gn in gnames:
        if sys_db.has_graph(gn):
            sys_db.delete_graph(gn)

    print("graphs (after delete operation):")
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
    docs = [
        {k: v for k, v in tdict.items() if v or k in keys_keep_nones} for tdict in docs
    ]
    return docs


def update_to_numeric(collection_name, field):
    s1 = f"FOR p IN {collection_name} FILTER p.{field} update p with {{"
    s2 = f"{field}: TO_NUMBER(p.{field}) "
    s3 = f"}} in {collection_name}"
    q0 = s1 + s2 + s3
    return q0


def upsert_docs_batch(
    docs, collection_name, match_keys, update_keys=None, filter_uniques=True
):
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
            docs = pick_unique_dict(docs)
        docs = json.dumps(docs)
    upsert_line = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
    upsert_line = f"{{{upsert_line}}}"

    if isinstance(update_keys, list):
        update_line = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
        update_line = f"{{{update_line}}}"
    elif update_keys == "doc":
        update_line = "doc"
    else:
        update_line = "{}"
    q_update = f"""FOR doc in {docs}
                        UPSERT {upsert_line}
                        INSERT doc
                        UPDATE {update_line} in {collection_name}"""
    return q_update


def insert_edges_batch(
    docs_edges,
    source_collection_name,
    target_collection_name,
    edge_col_name,
    match_keys_source=("_key",),
    match_keys_target=("_key",),
    filter_uniques=True,
):
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
            docs_edges = pick_unique_dict(docs_edges)
        docs_edges = json.dumps(docs_edges)

    if match_keys_source[0] == "_key":
        result_from = f'CONCAT("{source_collection_name}/", edge.source._key)'
        source_filter = ""
    else:
        result_from = "sources[0]._id"
        filter_source = " && ".join(
            [f"v.{k} == edge.source.{k}" for k in match_keys_source]
        )
        source_filter = f"""
                            LET sources = (
                                FOR v IN {source_collection_name}
                                  FILTER {filter_source} LIMIT 1
                                  RETURN v)"""

    if match_keys_target[0] == "_key":
        result_to = f'CONCAT("{target_collection_name}/", edge.target._key)'
        target_filter = ""
    else:
        result_to = "targets[0]._id"
        filter_target = " && ".join(
            [f"v.{k} == edge.target.{k}" for k in match_keys_target]
        )
        target_filter = f"""
                            LET targets = (
                                FOR v IN {target_collection_name}
                                  FILTER {filter_target} LIMIT 1
                                  RETURN v)"""

    if "attributes" in example.keys():
        result = f"MERGE({{_from : {result_from}, _to : {result_to}}}, edge.attributes)"
    else:
        result = f"{{_from : {result_from}, _to : {result_to}}}"

    q_update = f"""
        FOR edge in {docs_edges} {source_filter} {target_filter}
            INSERT {result} in {edge_col_name}"""
    return q_update


def basic_query(
    query,
    port=8529,
    ip_addr="127.0.0.1",
    cred_name="root",
    cred_pass="123",
    profile=False,
    batch_size=10000,
    bind_vars=None,
):
    hosts = f"http://{ip_addr}:{port}"
    client = ArangoClient(hosts=hosts)

    sys_db = client.db("_system", username=cred_name, password=cred_pass)
    cursor = sys_db.aql.execute(
        query, profile=profile, stream=True, batch_size=batch_size, bind_vars=bind_vars
    )
    return cursor


def profile_query(query, nq, profile_times, fpath, limit=None, **kwargs):
    limit_str = f"_limit_{limit}" if limit else ""
    if profile_times:
        print(f"starting profiling: {limit}")
        profiling = []
        for n in range(profile_times):
            cursor = basic_query(query, profile=True, **kwargs)
            profiling += [cursor.profile()]
            cursor.close()
        with open(join(fpath, f"query{nq}_profile{limit_str}.json"), "w") as fp:
            json.dump(profiling, fp, indent=4)

    print(f"starting actual query at {limit}")

    cnt = 0
    cursor = basic_query(query, **kwargs)
    chunk = list(cursor.batch())
    with gzip.open(
        join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
        "wt",
        encoding="ascii",
    ) as fp:
        json.dump(chunk, fp, indent=4)

    while cursor.has_more():
        cnt += 1
        with gzip.open(
            join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
            "wt",
            encoding="ascii",
        ) as fp:
            chunk = list(cursor.fetch()["batch"])
            json.dump(chunk, fp, indent=4)


def define_extra_edges(g):
    """
    g create a query from u to v by w : u -> w -> v and add properties of w as properties of the edge

    {
        "source": u,
        "target": v,
        "by": w,
        "edge_name": ecollection_name,
        "edge_weight": item["edge_weight"],
        "type": "indirect"
    }

    :param g:
    :return:
    """
    ucol, vcol, wcol = g["source"], g["target"], g["by"]
    edge_weight = g["edge_weight"]
    s = (
        f"FOR w IN {wcol}"
        f"  LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)"
        f"  LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)"
        f"  FOR u in uset"
        f"      FOR v in vset"
    )
    s_ins_ = ", ".join([f"{v}: w.{k}" for k, v in edge_weight.items()])
    s_ins_ = f"_from: u._id, _to: v._id, {s_ins_}"
    s_ins = f"          INSERT {{{s_ins_}}} "
    s_last = f"IN {ucol}_{vcol}_edges"
    query0 = s + s_ins + s_last
    return query0


def pick_unique_dict(docs):
    docs = {json.dumps(d, sort_keys=True) for d in docs}
    docs = [json.loads(t) for t in docs]
    return docs
