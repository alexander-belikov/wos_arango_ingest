from wos_db_studies.utils import profile_query

n_profile = 3
nq = 1

test = False
test = True

if test:
    limit_str = 'LIMIT 1000'
else:
    limit_str = ''

q = f"""
LET cnts =
    (FOR j IN media
        LET cc = (FOR v IN 1..1 INBOUND j publications_media_edges
            FILTER v.year == 1978
            RETURN v)
        RETURN {{journal: j, 'number_pubs': LENGTH(cc)}})
FOR doc in cnts
    SORT doc.number_pubs DESC
    LIMIT 10
RETURN doc"""

fpath = './../../results/'


profile_query(q, nq, n_profile, fpath)
