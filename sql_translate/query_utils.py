from typing import Optional
import pyodbc


def fetch(query: str,
          conn: pyodbc.Connection,
          chunksize: Optional[int] = None):
    """
    Run query and fetch results.

    Args:
        query (str): SQL statement
        conn (pyodbc.Connection): query engine connection object
        chunksize (int): Chunksize in bytes

    Returns:
        results: list of row object or None (if query fails)
    """
    try:
        curr = conn.cursor().execute(query)
    except Exception as e:
        msg = str(e) + '----' + 'The failed query: {query}'.format(query=query)
        raise Exception(msg)
    else:
        if chunksize is None:
            results = curr.fetchall()
        else:
            results = _fetch_many(curr, chunksize)
    finally:
        if conn is None:
            conn.close()
    return results


def run_query(query: str, conn: pyodbc.Connection) -> None:
    """
    Run query without fetching results.

    Args:
        query (str): SQL statement (example: 'select x, y from table_XY')
        conn (pyodbc.Connection): query engine connection object
    """
    try:
        _run_query(query, conn)
    except Exception as e:
        msg = str(e) + '----' + 'The failed query: {query}'.format(query=query)
        raise Exception(msg)


def _fetch_many(curr, chunksize: int):
    """
    Fetch results with chunksize.

    Arguments:
        curr: cursor object
        query (str): SQL statment
        chunksize (int): Chunk size in bytes

    Returns:
        results: list of Row object
    """
    endoftable = False
    results = []
    while not endoftable:
        data_chunk = curr.fetch_many(chunksize)
        endoftable = (len(data_chunk) < chunksize)
        results.extend(data_chunk)
    return results


def _run_query(query: str, conn: pyodbc.Connection) -> None:
    """
    Run query without fetching results.

    Args:
        query (str): SQL statement (example: 'select x, y from table_XY')
        conn (pyodbc.Connection): query engine connection object
    """
    conn.cursor().execute(query)
