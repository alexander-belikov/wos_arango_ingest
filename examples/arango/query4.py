from wos_db_studies.utils import profile_query

n_profile = 3
nq = 4

test = False
test = True

if test:
    limit_str = 'LIMIT 1000'
else:
    limit_str = ''

q = f"""
LET cnts = (
    FOR p IN publications {limit_str}
        LET first = (FOR c IN 1..1 OUTBOUND p publications_publications_edges RETURN c._id)
        LET second = (FOR c IN 2..2 OUTBOUND p publications_publications_edges RETURN DISTINCT c._id)
        RETURN {{pub: p._id, fa: LENGTH(first), fb: LENGTH(second)}}
        )
     return cnts"""

fpath = './../../results/'


profile_query(q, nq, n_profile, fpath)
