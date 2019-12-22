from wos_db_studies.utils import profile_query

n_profile = 3
nq = 1

q = f"""
LET cnts = (
    FOR p IN publications LIMIT 1000
        LET first = (FOR c IN 1..1 OUTBOUND p publications_publications_edges RETURN c._id)
        LET second = (FOR c IN 2..2 OUTBOUND p publications_publications_edges RETURN DISTINCT c._id)
        RETURN {{pub: p._id, fa: LENGTH(first), fb: LENGTH(second)}}
        )
     return cnts"""

fpath = './../../results/'


profile_query(q, nq, n_profile, fpath)
