from wos_db_studies.stopwords import stop_words_nltk
import string
puncts = list(string.punctuation)
all_stops = puncts + stop_words_nltk

qdict = {
    '1':
        {
            'description':
            'return most popular journals by number of publications, for year _current_year',
            '_current_year': 1978,
            'main_collection': 'media',
            'q': f"""
                LET cnts =
                    (FOR j IN media __insert_limit
                        LET cc = (FOR v IN 1..1 INBOUND j publications_media_edges
                            FILTER v.year ==  _current_year
                            RETURN v)
                        RETURN {{journal: j, 'number_pubs': LENGTH(cc)}})
                FOR doc in cnts
                    SORT doc.number_pubs DESC
                RETURN doc""",
            'q_aux': """
                FOR p IN publications
                    LET contrs = (FOR c IN 1..1 INBOUND p contributors_publications_edges RETURN c)
                    LET orgs = (FOR org IN 1..1 INBOUND p organizations_publications_edges RETURN org)
                    FOR c in contrs
                        FOR org in orgs
                            INSERT {_from : c._id, _to : org._id, "wosid": p._key, "year": p.year} 
                            IN contributors_organizations_edges"""
        },

    '2':
        {
            'description': 'return 100 most popular words (minus stop words) from titles',
            'main_collection': 'publications',
            'q': f"""
                FOR doc IN publications __insert_limit 
                    FOR word in SPLIT(LOWER(doc.title), ' ') 
                        COLLECT uword = word WITH COUNT INTO count
                        SORT count DESC
                        FILTER uword NOT IN {str(all_stops)}
                        RETURN {{uword, count}}""",
        },

    '3':
        {
            'description': 'return the list of authors who changed their country more than twice. '
                           'NB: make sure contributors_organizations_edges collection is set up',
            'main_collection': 'publications',
            'q': f"""
                FOR a IN contributors __insert_limit
                    LET times = LENGTH(FOR org IN 1..1 OUTBOUND a contributors_organizations_edges 
                    RETURN DISTINCT org.country) FILTER times > 2 
                    RETURN MERGE(a, {{'cnt': times}})""",
        },

    '4':
        {
            'description': 'for publication x compute the ratio of number of second order neighbours to '
                           'first order neighbours in the directed network of citations. '
                           '(neighbours cite x)',
            '_threshold': 1.5,
            'main_collection': 'publications',
            'q': f"""
                FOR p IN publications __insert_limit
                    LET first = (FOR c IN 1 INBOUND p publications_publications_edges RETURN c._id)
                    FILTER LENGTH(first) > 0
                    LET second = (FOR c IN 2 INBOUND p publications_publications_edges RETURN DISTINCT c._id)
                    FILTER LENGTH(second) > _threshold*LENGTH(first)
                    COLLECT fraction = LENGTH(second)/LENGTH(first) INTO gg
                    SORT fraction DESC
                    RETURN {{f: fraction, ids: gg[*].p._key}}""",
        },

    '5':
        {
            'description': 'aux calculation for eigenfactor:'
                           'count the number of times publications from journal j,'
                           ' published during census period (_current_year)'
                           ' cite publications in journal j prime published in target period'
                           ' (_current_year - , _current_year - _delta_year'
                           'NB: take 10 most popular journals from q1.'
                           ' The result is a 10 by 10 matrix.',
            '_current_year': 1978,
            '_delta_year': 5,
            '_issns': '',
            'main_collection': 'publications',
            'q': f"""
                FOR j IN media FILTER j.issn == '0014-9446'
                RETURN MERGE({{ja: j.issn}}, {{stats:
                (
                    FOR p in 1 INBOUND j publications_media_edges FILTER p.year == _current_year
                        FOR p2 in 1 OUTBOUND p publications_publications_edges
                            FILTER p2.year < _current_year AND p2.year >= (_current_year - _delta_year)
                            FOR j2 in 1 OUTBOUND p2 publications_media_edges
                                COLLECT jbt=j2.issn WITH COUNT INTO size
                                SORT size DESC
                    RETURN {{jb: jbt, s: size}}
                )}})""",
        },

    '6':
        {
            'description': 'given a subset of publications, compute the cardinality of the power set,'
                           ' defined as papers cited by p, papers that are cited by papers cited by p etc. '
                           '5 order out',
            '_current_year': 1978,
            'main_collection': 'publications',
            'q': f"""
                FOR p IN publications FILTER p.year ==  _current_year __insert_limit 
                    LET power_set = (FOR c IN 1..5 OUTBOUND p publications_publications_edges RETURN DISTINCT c._id)
                        COLLECT size = LENGTH(power_set) INTO gg
                        SORT size DESC
                        RETURN {{pset5: size, ids: gg[*].p._key}}""",
        },
}
