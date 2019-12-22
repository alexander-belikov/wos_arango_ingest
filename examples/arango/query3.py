from wos_db_studies.utils import profile_query

q_aux = """
FOR p IN toy_publications
    LET contrs = (FOR c IN 1..1 INBOUND p toy_contributors_toy_publications_edges RETURN c)
    LET orgs = (FOR org IN 1..1 INBOUND p toy_organizations_toy_publications_edges RETURN org)
    FOR c in contrs
        FOR org in orgs
            INSERT {_from : c._id, _to : org._id, "wosid": p._key, "year": p.year} 
            IN toy_contributors_toy_organizations_edges
"""

n_profile = 3
nq = 1

q = """
FOR a IN contributors
    LET times = LENGTH(FOR org IN 1..1 OUTBOUND a contributors_organizations_edges 
    RETURN DISTINCT org.country) FILTER times > 2 
    RETURN MERGE(a, {'cnt': times})"""

fpath = './../../results/'


profile_query(q, nq, n_profile, fpath)
