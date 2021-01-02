from itertools import product
from collections import defaultdict, ChainMap
from .utils import pick_unique_dict
import importlib
from pprint import pprint


def apply_mapper(mapper, document, vertex_spec):
    if "how" in mapper:
        mode = mapper["how"]
        vcol = mapper["name"]
        if "__extra" in mapper:
            extra_dict = mapper["__extra"]
        else:
            extra_dict = {}
        # value is a dict
        if mode == "dict":
            if "map" in mapper:
                field_map = mapper["map"]
                mapped_part = {
                            v: document[k]
                            for k, v in field_map.items()
                            if k in document
                        }
            else:
                field_map = dict()
                mapped_part = {}
            # vcol_fields = set(vertex_spec[vcol]["fields"]) - set(field_map.values())
            vcol_fields = vertex_spec[vcol]["fields"]
            def_part =  {k: document[k]
                         for k in vcol_fields
                         if k in document}
            result = {
                        **mapped_part, **def_part,
                        **extra_dict,
                    }
            if "transforms" in vertex_spec[vcol]:
                for transform in vertex_spec[vcol]["transforms"]:
                    if "module" in transform:
                        module = importlib.import_module(transform["module"])
                    elif "class" in transform:
                        module = eval('str')
                    else:
                        raise KeyError("Either module or class keys should be present")
                    fields = transform["fields"]
                    foo = getattr(module, transform["foo"])
                    result = {k: (foo(v) if k in fields else v) for k, v in result.items()}
            return {
                vcol: [result]
            }
        elif mode == "value":
            key = mapper["key"]
            return {vcol: [{**{key: document}}]}
        else:
            raise KeyError("Mapper must have map key if it has vertex key")
    # traverse non terminal nodes
    elif "type" in mapper:
        agg = defaultdict(list)
        if "descend_key" in mapper:
            if mapper["descend_key"] in document:
                document = document[mapper["descend_key"]]
            else:
                return agg
        if mapper["type"] == "list":
            for cdoc in document:
                for m in mapper["maps"]:
                    item = apply_mapper(m, cdoc, vertex_spec)
                    for k, v in item.items():
                        agg[k] += v
            return agg
        elif mapper["type"] == "item":
            for m in mapper["maps"]:
                item = apply_mapper(m, document, vertex_spec)
                for k, v in item.items():
                    agg[k] += v
            if "edges" in mapper:
                # check update
                agg = add_edges(mapper, agg, vertex_spec)
            if "merge" in mapper:
                for item in mapper["merge"]:
                    agg = smart_merge(agg, item["name"],
                                      item["discriminator_key"],
                                      item["discriminator_value"],
                                      )
            return agg
        else:
            raise ValueError(
                "Mapper type key has incorrect value: list or item allowed"
            )
    else:
        raise KeyError("Mapper type has does not have either how or type keys")


def add_edges(mapper, agg, vertex_indices):
    for edge_def in mapper["edges"]:
        # get source and target names
        source, target = (
            edge_def["source"]["name"],
            edge_def["target"]["name"],
        )
        # get source and target edge fields
        source_index, target_index = vertex_indices[source]["index"], vertex_indices[target]["index"]

        # if "fields" in edge_def["source"]:
        #     source_index += edge_def["source"]["fields"]
        # if "fields" in edge_def["target"]:
        #     target_index += edge_def["target"]["fields"]
        # get source and target items
        source_items, target_items = agg[source], agg[target]

        if edge_def["how"] == "all":
            source_items = pick_indexed_items_anchor_logic(source_items,
                                                           source_index,
                                                           edge_def["source"])
            target_items = pick_indexed_items_anchor_logic(target_items,
                                                           target_index,
                                                           edge_def["target"])
            for u, v in product(source_items, target_items):
                weight = dict()
                if "fields" in edge_def["source"]:
                    weight.update({k: u[k] for k in edge_def["source"]["fields"] if k in u})
                if "fields" in edge_def["target"]:
                    weight.update({k: v[k] for k in edge_def["target"]["fields"] if k in v})
                agg[(source, target)] += [
                    (
                        project_dict(u, source_index),
                        project_dict(v, target_index),
                        weight,
                    )
                ]
        if edge_def["how"] == "1-n":
            source_field, target_field = (
                edge_def["source"]["field"],
                edge_def["target"]["field"],
            )
            source_items = pick_indexed_items_anchor_logic(source_items,
                                                           source_index,
                                                           edge_def["source"])
            target_items = pick_indexed_items_anchor_logic(target_items,
                                                           target_index,
                                                           edge_def["target"])
            target_items = [item for item in target_items if target_field in item]
            target_items = dict(
                zip(
                    [item[target_field] for item in target_items],
                    project_dicts(target_items, target_index),
                )
            )
            if "all_value" in edge_def["source"]:
                all_value = edge_def["source"]["all_value"]
            else:
                all_value = None
            for u in source_items:
                pointers = u[source_field]
                up = project_dict(u, source_index)
                weight = dict()
                if all_value is not None and all_value in pointers:
                    agg[(source, target)] += [(up, v, weight) for v in target_items.values()]
                else:
                    agg[(source, target)] += [
                        (up, target_items[p], weight) for p in pointers
                    ]

    return agg


def pick_indexed_items_anchor_logic(items, indices, set_spec, anchor_key="anchor"):
    items_ = [
        item for item in items if any([k in item for k in indices])
    ]

    if anchor_key in set_spec:
        if set_spec[anchor_key]:
            items_ = [
                item for item in items_ if anchor_key in item and item[anchor_key]
            ]
        else:
            items_ = [
                item for item in items_ if anchor_key not in item
            ]
    return items_


def assign_edge_label(edges, label, condition):
    edges_new = [(u, v, label) if condition(u, v) else (u, v, {}) for u, v in edges]
    return edges_new


def clean_arobas(item):
    return {k: v for k, v in item.items() if k[0] != "@"}


def project_dict(item, keys, how="include"):
    if how == "include":
        return {k: v for k, v in item.items() if k in keys}
    elif how == "exclude":
        return {k: v for k, v in item.items() if k not in keys}


def project_dicts(items, keys, how="include"):
    if how == "include":
        return [{k: v for k, v in item.items() if k in keys} for item in items]
    elif how == "exclude":
        return [{k: v for k, v in item.items() if k not in keys} for item in items]


def clean_aux_fields(pack):
    pack_out = {}
    for k, cpack in pack.items():
        if k != "@edges":
            pack_out[k] = [clean_arobas(x) for x in cpack]
        else:
            pack_out[k] = [
                (clean_arobas(x[0]), clean_arobas(x[1]), x[2:]) for x in cpack
            ]
    return pack_out


def parse_edges(croot, edge_acc, mapping_fields):
    """
    extract edge definition and edge fields from definition dict
    :param croot:
    :param edge_acc:
    :param mapping_fields:
    :return:
    """
    # agg = defaultdict(list)
    if isinstance(croot, dict):
        if "maps" in croot:
            for m in croot["maps"]:
                edge_acc_, mapping_fields = parse_edges(m, edge_acc, mapping_fields)
                edge_acc += edge_acc_
        if "edges" in croot:
            edge_acc_ = []
            for evw in croot["edges"]:
                vname, wname = evw["source"]["name"], evw["target"]["name"]
                vname_fields = None
                wname_fields = None
                edge_def = vname, wname, vname_fields, wname_fields
                edge_acc_ += [edge_def]
                if "field" in evw["source"]:
                    mapping_fields[vname] += [evw["source"]["field"]]
                if "field" in evw["target"]:
                    mapping_fields[wname] += [evw["target"]["field"]]
            return edge_acc_ + edge_acc, mapping_fields
        else:
            return [], defaultdict(list)


def merge_documents(docs, key_absent_aux="_key", anchor_key="anchor"):
    mains_, mains, auxs, anchors = [], [], [], []
    # split docs into two groups with and without key_absent_aux
    for item in docs:
        (mains_ if key_absent_aux in item else auxs).append(item)

    for item in mains_:
        (anchors if anchor_key in item and item[anchor_key] else mains).append(item)

    auxs = auxs + anchors
    r = [dict(ChainMap(*auxs))] + mains
    return r

def smart_merge(agg, collection_name, discriminant_key="role", discriminant_value="authour"):
    wos_standard = defaultdict(list)
    without_standard_heap = []
    seed_list = []
    # split group into 3:
    # standard, non_standard and seed_list
    # merge non_standard onto standard (not replacing fields are already standard item)
    for item in agg[collection_name]:
        if discriminant_key in item:
            if "wos_standard" in item and item[discriminant_key] == discriminant_value:
                wos_standard[item["wos_standard"]] += [item]
            else:
                without_standard_heap += [item]
        else:
            seed_list += [item]

    # heuristics
    for item in without_standard_heap:
        if "display_name" in item:
            last_name, first_name = item["display_name"].split(", ")
        elif "last_name" and "first_name" in item:
            last_name, first_name = item["last_name"], item["first_name"]
        else:
            continue
        q = last_name + "," + first_name[0]
        for k in wos_standard:
            if q in k:
                wos_standard[k] += [item]

        for k, v in wos_standard.items():
            seed_list += [dict(ChainMap(*v))]
        agg[collection_name] = seed_list
    return agg

def process_document_top(
    config, doc, vertex_config, edge_fields, merge_collections
):
    acc = apply_mapper(config, doc, vertex_config)

    for k, v in acc.items():
        v = pick_unique_dict(v)
        if not isinstance(k, tuple):
            v = project_dicts(v, edge_fields[k], how="exclude")
        if k in merge_collections:
            v = merge_documents(v)
        acc[k] = v
    return acc
