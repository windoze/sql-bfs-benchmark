import time
import os

# Benchmark against MSSQL
# db = "mssql"

# Benchmark against MySQL
db = "mysql"

# DB support

def get_conn_mssql():
    import pymssql
    conn = pymssql.connect(
        host=os.environ["MSSQL_HOST"],
        database=os.environ["MSSQL_DB"],
        user=os.environ["MSSQL_USER"],
        password=os.environ["MSSQL_PASS"],
        charset='utf8')
    return conn


def get_conn_mysql():
    import mysql.connector
    conn = mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        database=os.environ["MYSQL_DB"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASS"],
        port=3306,
        ssl_ca="DigiCertGlobalRootCA.crt.pem", 
        ssl_disabled=False)
    return conn


def get_dict_cursor_mssql(conn):
    return conn.cursor(as_dict=True)


def get_dict_cursor_mysql(conn):
    return conn.cursor(dictionary=True)


if db == "mysql":
    get_conn = get_conn_mysql
    get_dict_cursor = get_dict_cursor_mysql
else:
    get_conn = get_conn_mssql
    get_dict_cursor = get_dict_cursor_mssql


def quote(id):
    if isinstance(id, str):
        return f"'{id}'"
    else:
        return ",".join([f"'{i}'" for i in id])


def bfs(conn, cursor, id, conn_type):
    """
    Breadth first traversal
    Starts from `id`, follow edges with `conn_type` only.
    """
    edges = []
    to_ids = [(None, id)]
    # BFS over SQL
    while len(to_ids) != 0:
        to_ids = _bfs_step(cursor, to_ids, conn_type)
        edges.extend(to_ids)
    ids = set([id])
    for r in edges:
        ids.add(r[0])
        ids.add(r[1])
    sql = f"select entity_id, entity_name, entity_type from entities where entity_id in ({quote(ids)})"
    cursor = get_dict_cursor(conn)
    cursor.execute(sql)
    entities = cursor.fetchall()
    return (entities, edges)


def _bfs_step(cursor, ids, conn_type):
    """
    One step of the BFS process
    Returns all edges that connect to node ids the next step
    """
    ids = list([id[1] for id in ids])
    sql = f"""select from_id, to_id, conn_type from entity_dep where conn_type = '{conn_type}' and from_id in ({quote(ids)})"""
    cursor.execute(sql)
    rs = cursor.fetchall()
    from_ids = set([p for p in rs])
    return from_ids


def get_lineage(conn, id):
    """
    Get feature lineage on both upstream and downstream
    Returns [entity_id:entity] map and list of edges have been traversed.
    """
    cursor = conn.cursor()
    upstream_entities, upstream_edges = bfs(conn, cursor, id, "Consumes")
    downstream_entities, downstream_edges = bfs(conn, cursor, id, "Produces")
    entities = {}
    for e in upstream_entities:
        entities[e['entity_id']] = e
    for e in downstream_entities:
        entities[e['entity_id']] = e
    edges = set()
    for e in upstream_edges:
        edges.add(e)
    for e in downstream_edges:
        edges.add(e)
    return entities, edges


conn = get_conn()
cursor = conn.cursor()
sql = "select entity_id from entities where entity_type = 'DerivedFeature'"
cursor.execute(sql)
rs = cursor.fetchall()
total = 0.0
latencies = []
try:
    for n in rs:
        tic = time.perf_counter()
        print(f"Lineage for feature('{n}'): ", get_lineage(conn, n[0]))
        toc = time.perf_counter()

        latencies.append(toc - tic)
        total += toc - tic
except Exception as e:
    print(e)
finally:
    print(
        f"\nTotally {len(latencies)} out of {len(rs)} lineages have been calculated.")
    print(f"Average time: {total/len(latencies):0.4f}")
    latencies.sort()
    print(f"Min time: {latencies[0]:0.4f}")
    print(f"Max time: {latencies[-1]:0.4f}\n")
