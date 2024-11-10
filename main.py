"""
Main cli or app entry point
"""
from mylib.lib import spark_session, extract_data, read_data, sql_query

if __name__ == "__main__":
    session = spark_session("PySpark")
    extract_data()
    data = read_data("data/nba_2015.csv", session)
    sql_query(data, session)
