import pandas as pd
import os

CONN_STRING = os.getenv('FLY_PG_PROXY_CONN_STRING').replace("postgres://", "postgresql://")
OUTPUT_FILE_NAME = 'posts.parquet'

# Check for vote conflicts
vote_conflict_post_ids = pd.read_sql('''
SELECT post_id FROM votes
GROUP BY post_id
HAVING COUNT(DISTINCT(vote)) > 1;
''', CONN_STRING)['post_id'].tolist()

if len(vote_conflict_post_ids) > 0:
    raise ValueError(f'Vote conflicts were found for following ids: '
                     '\n'.join(vote_conflict_post_ids)
                     )
else:
    print('No vote conflicts found.')

# Get all posts
pd.read_sql('''
SELECT DISTINCT ON (p.id)
p.id, p.title, p.body, p.submitter, p.utc_time, p.flair, p.hit, v.vote
FROM posts p
LEFT JOIN votes v ON v.post_id = p.id;
''', CONN_STRING).to_parquet(OUTPUT_FILE_NAME, index=False)
print(f'Posts saved to {OUTPUT_FILE_NAME}.')
