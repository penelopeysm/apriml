import sqlite3
import pandas as pd

conn = sqlite3.connect('posts.db')
posts = pd.read_sql('SELECT * FROM posts', conn)
votes = pd.read_sql('SELECT * FROM votes', conn)
conn.close()

posts = (posts.merge(votes, on='id', how='left', validate='one_to_many')
         .query('vote == 1 or vote == 0')
         .drop(columns=['url', 'n', 'username', 'truth'])
         )
posts['hit'] = posts['hit'].astype(bool)
posts['vote'] = posts['vote'].astype(bool)
posts.to_parquet('posts.parquet', index=False)
