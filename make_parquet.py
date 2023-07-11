import sqlite3
import pandas as pd

INPUT_DATABASE_NAME = 'posts.db'
OUTPUT_FILE_NAME = 'posts.parquet'

# Read in data from SQLite database
conn = sqlite3.connect(INPUT_DATABASE_NAME)
posts = pd.read_sql('SELECT * FROM posts', conn)
votes = pd.read_sql('SELECT * FROM votes', conn)
conn.close()

# Check for vote conflicts (i.e. posts where two people voted differently)
vote_conflict_ids = (votes.loc[votes.duplicated(subset=['id'], keep=False)]
                     .groupby('id')
                     .agg({'vote': 'nunique'})
                     .query('vote > 1')
                     .index
                     .to_list()
                     )
if vote_conflict_ids:
    raise ValueError(f'Vote conflicts were found for following ids: '
                     '\n'.join(vote_conflict_ids)
                     )
else:
    print('No vote conflicts found.')

# Merge votes onto post database, and output
posts = (posts.merge(votes, on='id', how='left')
         .query('vote == 1 or vote == 0')
         .drop(columns=['url', 'n', 'username', 'truth'])
         .drop_duplicates(subset=['id'])
         )
posts['hit'] = posts['hit'].astype(bool)
posts['vote'] = posts['vote'].astype(bool)
posts.to_parquet(OUTPUT_FILE_NAME, index=False)
print(f'{len(posts)} posts saved to {OUTPUT_FILE_NAME}.')
