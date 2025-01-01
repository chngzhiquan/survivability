import sqlite3

def copy_from(df, table_name):
    conn = sqlite3.connect('soraka.db')
    df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    conn.commit()



