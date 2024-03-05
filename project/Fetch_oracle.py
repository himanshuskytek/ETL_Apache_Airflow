


import cx_Oracle

# Database connection information
dsn = cx_Oracle.makedsn('reade.forest.usf.edu', 1521, sid='cdb9')
connection = cx_Oracle.connect(user='SQL116', password='SKY@8tek', dsn=dsn)

# SQL query
sql_query = """
    SELECT imdb_movie_id
    FROM sqlmdb.movies
    ORDER BY COALESCE(last_updated, first_created)
    FETCH FIRST 10 ROWS ONLY
"""

# Execute the query
cursor = connection.cursor()
cursor.execute(sql_query)

# Fetch and print the results
print("Movie IDs:")
for row in cursor:
    print(row[0])

# Close cursor and connection
cursor.close()
connection.close()




