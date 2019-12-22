from wos_db_studies.utils import profile_query_save_results
import json

n_profile = 3

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

profiling = [profile_query_save_results(q, profile=True).profile() for n in range(n_profile)]
with open('./../../results/query1_profile.json', 'w') as fp:
    json.dump(profiling, fp, indent=4)

qr = profile_query_save_results(q)
with open('./../../results/query1_result.json', 'w') as fp:
    json.dump(qr, fp, indent=4)

