import yaml
import os
import networkx as nx
from itertools import product
from pprint import pprint

"""
plot db schematics

graphviz attributes 

https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
https://rsms.me/graphviz/
https://graphviz.readthedocs.io/en/stable/examples.html
https://graphviz.org/doc/info/attrs.html

usage: 
    color='red',style='filled', fillcolor='blue',shape='square'

to keep 
level_one = [node1, node2]
sg_one = ag.add_subgraph(level_one, rank='same')
"""

fillcolor_palette = {
    "violet": "#DDD0E5",
    "green": "#BEDFC8",
    "blue": "#B7D1DF",
    "red": "#EBA59E",
}

map_type2shape = {
    "table": "box",
    "vcollection": "ellipse",
    "index": "polygon",
    "field": "octagon",
}
map_type2color = {
    "table": fillcolor_palette["blue"],
    "vcollection": fillcolor_palette["green"],
    "index": "orange",
    "field": fillcolor_palette["red"],
}

edge_status = {"vcollection": "dashed", "table": "solid"}

config_path = "../"
figgpath = "../figs/schema"
fname = os.path.join(config_path, "./conf/wos.yaml")
with open(fname, "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


#######################################################
g = nx.DiGraph()
nodes = []
edges = []
for n in config["table"]:
    k = n["filetype"]
    nodes_table = [(k, {"type": "table"})]
    nodes_collection = [
        (item["type"], {"type": "vcollection"}) for item in n["vertex_collections"]
    ]
    nodes += nodes_table
    nodes += nodes_collection
    edges += [(nt[0], nc[0]) for nt, nc in product(nodes_table, nodes_collection)]

g.add_nodes_from(nodes)
g.add_edges_from(edges)

for n in g.nodes():
    props = g.nodes()[n]
    upd_dict = {
        #                 "fillcolor": ,
        "shape": map_type2shape[props["type"]],
        "color": map_type2color[props["type"]],
        "style": "filled",
    }
    for k, v in upd_dict.items():
        g.nodes[n][k] = v

for e in g.edges(data=True):
    s, t, _ = e
    target_props = g.nodes[s]
    upd_dict = {"style": edge_status[target_props["type"]], "arrowhead": "vee"}
    for k, v in upd_dict.items():
        g.edges[s, t][k] = v

ag = nx.nx_agraph.to_agraph(g)
ag.draw(os.path.join(figgpath, "./tables2vertex.pdf"), "pdf", prog="dot")


g = nx.DiGraph()
nodes = []
edges = []
for n in config["table"]:
    nodes_collection = [
        (item["type"], {"type": "vcollection"}) for item in n["vertex_collections"]
    ]
    nodes += nodes_collection
    edges += [(x, y) for x, y in n["edge_collection"]]

g.add_nodes_from(nodes)
g.add_edges_from(edges)

for n in g.nodes():
    props = g.nodes()[n]
    upd_dict = {
        "shape": map_type2shape[props["type"]],
        "color": map_type2color[props["type"]],
        "style": "filled",
    }
    for k, v in upd_dict.items():
        g.nodes[n][k] = v

for e in g.edges(data=True):
    s, t, _ = e
    target_props = g.nodes[s]
    upd_dict = {"style": edge_status[target_props["type"]], "arrowhead": "vee"}
    for k, v in upd_dict.items():
        g.edges[s, t][k] = v

ag = nx.nx_agraph.to_agraph(g)
ag.draw(os.path.join(figgpath, "./vc2vc.pdf"), "pdf", prog="dot")


### fields/indexes
g = nx.DiGraph()
nodes = []
edges = []
for k, props in config["vertex_collections"].items():
    nodes_collection = [(k, {"type": "vcollection"})]
    nodes_fields = [
        (f"{k}:{item}", {"type": "field", "label": item}) for item in props["fields"]
    ]
    nodes += nodes_collection
    nodes += nodes_fields
    edges += [(x[0], y[0]) for x, y in product(nodes_collection, nodes_fields)]

g.add_nodes_from(nodes)
g.add_edges_from(edges)

for n in g.nodes():
    props = g.nodes()[n]
    upd_dict = props.copy()
    if "type" in upd_dict:
        upd_dict["shape"] = map_type2shape[props["type"]]
        upd_dict["color"] = map_type2color[props["type"]]
    if "label" in upd_dict:
        upd_dict["forcelabel"] = True
    upd_dict["style"] = "filled"

    for k, v in upd_dict.items():
        g.nodes[n][k] = v

for e in g.edges(data=True):
    s, t, _ = e
    target_props = g.nodes[s]
    upd_dict = {"style": "solid", "arrowhead": "vee"}
    for k, v in upd_dict.items():
        g.edges[s, t][k] = v

ag = nx.nx_agraph.to_agraph(g)


for k, props in config["vertex_collections"].items():
    nodes_collection = [(k, {"type": "vcollection"})]
    level_index = [f"{k}:{item}" for item in props["index"]]
    index_subgraph = ag.add_subgraph(level_index, name=f"cluster_{k[:3]}:def")
    index_subgraph.node_attr["style"] = "filled"
    index_subgraph.node_attr["label"] = "definition"

ag.draw(os.path.join(figgpath, "./vc2fields.pdf"), "pdf", prog="dot")


#######################################################
# table -> fields -> collection_fields -> collection map
g = nx.DiGraph()
nodes = []
edges = []

for n in config["table"]:
    k = n["filetype"]
    nodes_table = [(f"table:{k}", {"type": "table", "label": k})]
    vcols = n["vertex_collections"]
    for item in vcols:
        cname = item["type"]
        ref_fields = config["vertex_collections"][cname]["index"]
        if "map_fields" in item:
            cmap = item["map_fields"]
        else:
            cmap = dict()
        fields_collection_complementary = set(ref_fields) - set(cmap.values())
        cmap.update({qq: qq for qq in list(fields_collection_complementary)})

        #         pprint(k)
        #         pprint(fields_collection_complementary)
        #         pprint(cname)
        #         pprint(cmap)

        node_collection = (
            f"collection:{cname}",
            {"type": "vcollection", "label": cname},
        )
        nodes_fields_table = [
            (f"table:field:{kk}", {"type": "field", "label": kk}) for kk in cmap.keys()
        ]
        nodes_fields_collection = [
            (f"collection:field:{kk}", {"type": "field", "label": kk})
            for kk in cmap.values()
        ]
        edges_fields = [
            (f"table:field:{kk}", f"collection:field:{vv}") for kk, vv in cmap.items()
        ]
        edge_table_fields = [(f"table:{k}", q) for q, _ in nodes_fields_table]
        edge_collection_fields = [
            (q, node_collection[0]) for q, _ in nodes_fields_collection
        ]
        nodes += (
            nodes_table
            + [node_collection]
            + nodes_fields_table
            + nodes_fields_collection
        )
        edges += edges_fields + edge_table_fields + edge_collection_fields

g.add_nodes_from(nodes)
g.add_edges_from(edges)

for n in g.nodes():
    props = g.nodes()[n]
    upd_dict = props.copy()
    if "type" in upd_dict:
        upd_dict["shape"] = map_type2shape[props["type"]]
        upd_dict["color"] = map_type2color[props["type"]]
    if "label" in upd_dict:
        upd_dict["forcelabel"] = True
    upd_dict["style"] = "filled"
    for k, v in upd_dict.items():
        g.nodes[n][k] = v

for e in g.edges(data=True):
    s, t, _ = e
    target_props = g.nodes[s]
    upd_dict = {
        #         "style": edge_status[target_props["type"]],
        "arrowhead": "vee"
    }
    for k, v in upd_dict.items():
        g.edges[s, t][k] = v

ag = nx.nx_agraph.to_agraph(g)
ag.draw(os.path.join(figgpath, "./tables2vertex_ext.pdf"), "pdf", prog="dot")
