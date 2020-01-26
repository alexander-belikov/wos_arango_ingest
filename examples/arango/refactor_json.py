import gzip
import json
import pandas as pd

with gzip.open('../../results/arango/query1_result_batch_0.json.gz', "rb") as f:
    data = json.loads(f.read(), encoding="utf-8")

issns = [x['journal']['issn'] for x in data]
cnts = [x['number_pubs'] for x in data]
table = zip(issns, cnts)

df = pd.DataFrame(table,
                  columns=['issn', 'counts']).sort_values('counts', ascending=False)

df.to_csv('../../results/arango/journal_count.csv')
