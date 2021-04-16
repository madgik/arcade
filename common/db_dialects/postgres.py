import asyncio

async def check_for_params(db_objects, table, attributes):
    cur = db_objects["node"]["async_con"].cursor()
    params = [table] + attributes
    attr = await cur.execute(
        "select column_name from information_schema.columns where table_name = %s and column_name in ("
        + ",".join(["%s" for x in set(attributes)])
        + ");",
        [(*params)],
    )
    res = cur.fetchall()
    if len(res) != len(attributes):
        cur.close()
        if len(res) == 0:
            raise Exception("Requested data does not exist in all local nodes")
        raise Exception(
            "Attributes other than "
            + str(res)
            + " does not exist in all local nodes"
        )
    cur.close()

async def init_remote_connections(db_objects):
    glob = urlparse(db_objects["global"]["dbname"])
    await destroy_remote_connections(db_objects)
    for i, local_node in enumerate(db_objects["local"]):
        loc = urlparse(local_node["dbname"])
        await db_objects["global"]["async_con"].cursor().execute(
            "CREATE SERVER local_"
            + str(i)
            + " FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, dbname %s, port '%s');",
            [loc.hostname, loc.path[1:], loc.port],
        )
        await db_objects["global"]["async_con"].cursor().execute(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER local_"
            + str(i)
            + " OPTIONS (user 'postgres', password 'mypassword');"
        )
        await local_node["async_con"].cursor().execute(
            "CREATE SERVER global FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, dbname %s, port '%s');",
            [glob.hostname, glob.path[1:], glob.port],
        )
        await local_node["async_con"].cursor().execute(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER global OPTIONS (user 'postgres', password 'mypassword');"
        )

async def destroy_remote_connections(db_objects):
    glob = urlparse(db_objects["global"]["dbname"])
    for i, local_node in enumerate(db_objects["local"]):
        loc = urlparse(local_node["dbname"])
        await db_objects["global"]["async_con"].cursor().execute(
            "DROP SERVER if exists local_" + str(i) + " CASCADE;"
        )
        await local_node["async_con"].cursor().execute(
            "DROP SERVER if exists global CASCADE;"
        )

async def broadcast_inparallel(
    local, globalresulttable, globalschema, dbname
):
    await local.cursor().execute(
        "DROP FOREIGN TABLE IF EXISTS %s; CREATE FOREIGN TABLE %s (%s) SERVER global;"
        % (globalresulttable, globalresulttable, globalschema)
    )

async def merge(db_objects, localtable, globaltable, localschema):
    cur = db_objects["node"]["async_con"].cursor()
    query = "DROP VIEW IF EXISTS "+ globaltable +"; CREATE VIEW " + globaltable + " as "
    for i, local_node in enumerate(db_objects["local"]):
        await cur.execute(
            "DROP FOREIGN TABLE IF EXISTS %s_%s; CREATE FOREIGN TABLE %s_%s (%s) SERVER local_%s;"
            % (localtable, i, localtable, i, localschema, i)
        )
        if i < len(db_objects["local"]) - 1:
            query += " select * from " + localtable + "_" + str(i) + " UNION ALL "
        else:
            query += " select * from " + localtable + "_" + str(i) + " ;"
    await cur.execute(query)

async def broadcast(db_objects, globalresulttable, globalschema):
    await asyncio.gather(
        *[
            broadcast_inparallel(
                local_node["async_con"],
                globalresulttable,
                globalschema,
                db_objects["global"]["dbname"],
            )
            for i, local_node in enumerate(db_objects["local"])
        ]
    )

async def transferdirect(node1, localtable, node2, transferschema):
    await node2[2].cursor().execute(
        "CREATE REMOTE TABLE %s (%s) on 'mapi:%s';"
        % (localtable, transferschema, node1[1])
    )

async def clean_tables(
    db_objects, globaltable, localtable, viewlocaltable, globalrestable
):
    await db_objects["global"]["async_con"].cursor().execute(
        "drop view if exists %s;" % globaltable
    )
    await db_objects["global"]["async_con"].cursor().execute(
        "drop table if exists %s;" % globalrestable
    )
    for i, local in enumerate(db_objects["local"]):
        await local["async_con"].cursor().execute(
            "drop view if exists " + viewlocaltable + ";"
        )
        await local["async_con"].cursor().execute(
            "drop foreign table if exists " + globalrestable + ";"
        )
        await local["async_con"].cursor().execute(
            "drop table if exists " + localtable + "_" + str(i) + ";"
        )
        await db_objects["global"]["async_con"].cursor().execute(
            "drop foreign table if exists " + localtable + "_" + str(i) + ";"
        )

