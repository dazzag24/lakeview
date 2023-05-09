from typing import List

from loader import Inventory


DUPLICATES_STMT = """
SELECT
    e_tag, ARRAY_JOIN(ARRAY_AGG(key), ';') AS files, COUNT(*) AS num_dups
FROM {table_name}
    WHERE dt = '{date}' AND is_latest AND NOT is_delete_marker AND size > 10000
GROUP BY e_tag HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
"""

def duplicates_for(db: Inventory, date: str) -> List[dict]:
    results = db.query(DUPLICATES_STMT, date=date)
    return [{'e_tag': r.get('e_tag'), 'files': r.get('files').split(';'), 'num_dups': int(r.get('num_dups'))} for r in results]