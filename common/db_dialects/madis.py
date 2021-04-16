import asyncio

async def execute_query(db_objects, query, params):
    pass

async def create_table(db_objects, table_name, schema):
    pass

async def check_for_params(db_objects, table, attributes):
   pass

async def init_remote_connections(db_objects):
    await asyncio.sleep(0)


async def broadcast_inparallel(
        local, globalresulttable, globalschema, dbname
):
    await local.cursor().execute(
        "DROP TABLE IF EXISTS  %s; CREATE REMOTE TABLE %s (%s) on 'mapi:%s';"
        % (globalresulttable, globalresulttable, globalschema, dbname)
    )


async def merge(db_objects, localtable, globaltable, localschema):
    cur = db_objects["node"]["async_con"].cursor()

    query = "DROP VIEW IF EXISTS " + globaltable + "; CREATE VIEW " + globaltable + " as "
    for i, local_node in enumerate(db_objects["remote"]):
        await cur.execute(
            "DROP TABLE IF EXISTS %s_%s; CREATE REMOTE TABLE %s_%s (%s) on 'mapi:%s';"
            % (localtable, i, localtable, i, localschema, local_node["dbname"])
        )
        if i < len(db_objects["remote"]) - 1:
            query += " select * from " + localtable + "_" + str(i) + " UNION ALL "
        else:
            query += " select * from " + localtable + "_" + str(i) + " ;"
    await cur.execute(query)


async def merge1(db_objects, localtable, globaltable, localschema):
    cur = db_objects["node"]["async_con"].cursor()
    await cur.execute(
        "DROP TABLE IF EXISTS %s; CREATE MERGE TABLE %s (%s);" % (globaltable, globaltable, localschema))
    for i, local_node in enumerate(db_objects["node"]):
        await cur.execute(
            "DROP TABLE IF EXISTS %s_%s; CREATE REMOTE TABLE %s_%s (%s) on 'mapi:%s';"
            % (localtable, i, localtable, i, localschema, local_node["dbname"])
        )
        await cur.execute(
            "ALTER TABLE %s ADD TABLE %s_%s;" % (globaltable, localtable, i)
        )


async def broadcast(db_objects, globalresulttable, globalschema):
    await broadcast_inparallel(
        db_objects["node"]["async_con"],
        globalresulttable,
        globalschema,
        db_objects["remote"][0]["dbname"],
    )


async def transferdirect(node1, localtable, node2, transferschema):
    await node2[2].cursor().execute(
        "CREATE REMOTE TABLE %s (%s) on 'mapi:%s';"
        % (localtable, transferschema, node1[1])
    )


async def clean_tables(
        db_objects, globaltable, localtable, viewlocaltable, globalrestable
):
    await db_objects["node"]["async_con"].cursor().execute(
        "drop view if exists " + viewlocaltable + ";"
    )
    await db_objects["node"]["async_con"].cursor().execute(
        "drop table if exists " + globalrestable + ";"
    )
    await db_objects["node"]["async_con"].cursor().execute(
        "drop view if exists " + globaltable + ";"
    )
    await db_objects["node"]["async_con"].cursor().execute(
        "drop table if exists " + localtable + ";"
    )
