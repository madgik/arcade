from urllib.parse import urlparse
from common.connections import servers
import importlib
import pkgutil
import algorithms
import settings
DEBUG = settings.DEBUG
from master import worker_tasks


def get_udfs(reload = False):
    modules = []
    for importer, modname, ispkg in pkgutil.iter_modules(algorithms.__path__):
        modules.append(modname)
    # get all statically defined udfs from algorithms package
    mpackage = "algorithms"
    importlib.import_module(mpackage)
    all_udfs = []

    for algorithm in modules:
        try:
            algo = importlib.import_module("." + algorithm, mpackage)
            if reload:
                importlib.reload(algo)
            for udf in algo.udf_list:
                all_udfs.append(udf)
        except:
            pass
    return all_udfs

class Connections:
    def __init__(self, servers_file, runtimes_file = None):
        self.db_objects = {}
        self.db_objects["node"] = {}
        self.db_objects["remote"] = []
        self.udfs_list = []
        self.servers_file = servers_file
        self.runtimes_file = runtimes_file
        self.mservers, self.runtimes = servers.get_servers(servers_file, runtimes_file)
        self.node = urlparse(self.mservers[0])
        if self.node.scheme == "monetdb":
            from dbapis.aiopymonetdb import pool
            from common.db_dialects import monetdb as db
            self.user = "monetdb"
            self.password = "monetdb"
        if self.node.scheme == "postgres":
            from dbapis.aiopg import pool
            from common.db_dialects import postgres as db
            self.user = "postgres"
            self.password = "mypassword"
        if self.node.scheme == "madis":
            from aiomadis import pool
            from dblibs import madis as db
            self.user = ""
            self.password = ""
        self.pool = pool
        self.db = db
        self.worker = worker_tasks.Worker
        
    async def initialize(self):  ### create connection pools
        if self.db_objects["node"] == {}:
            self.db_objects["node"]["pool"] = await self.pool.create_pool(
                host = self.node.hostname,
                port = self.node.port,
                user = self.user,
                password = self.password,
                database = self.node.path[1:],
            )
            self.db_objects["node"]["dbname"] = self.mservers[0]

            for i, db in enumerate(self.mservers[1:]):
                remote_node = {}
                remote_node["dbname"] = db
                self.db_objects["remote"].append(remote_node)

            for i, runtime in enumerate(self.runtimes):
                self.db_objects["remote"][i]['async_con'] = self.worker(runtime)

            con = await self.acquire()
            await self.db.init_remote_connections(con)
            self.udfs_list = get_udfs()
            for udf in self.udfs_list:
                try:
                    await con["node"]["async_con"].cursor().execute(udf)
                except:
                    raise
                # at this time due to minimal error handling and due to testing there may be tables in the DB which
                #  are not dropped and are dependent on some UDFs, so their recreation may fail
                # (You cannot replace a UDF which is in use)
            await self.release(con)

    async def acquire(self):  #### get connection objects
        db_conn = {}
        db_conn["node"] = {}
        db_conn["remote"] = []
        await self._reload()
        conn = await self.db_objects["node"]["pool"].acquire()
        db_conn["node"]["async_con"] = conn
        db_conn["node"]["dbname"] = self.db_objects["node"]["dbname"]
        db_conn["node"]["dblib"] = self.db
        for db_object in self.db_objects["remote"]:
            remote_node = {}
            remote_node["dbname"] = db_object["dbname"]
            try:
                remote_node["async_con"] = db_object["async_con"]
            except KeyError: ## there aren't remote runtimes - current node is local)
                pass
            db_conn["remote"].append(remote_node)
        if DEBUG:
            await self._reload_udfs(db_conn)
        return db_conn

    ### TODO asyncio locks are needed because there may be a reload
    async def release(self, db_conn):  ### release connection objects back to pool
        if db_conn["node"]["dbname"] == self.db_objects["node"]["dbname"]:
            await self.db_objects["node"]["pool"].release(
                db_conn["node"]["async_con"]
            )

    async def clearall(self):
        await self.db_objects["node"]["pool"].clear()

    async def _reload_udfs(self, con):
        udfs_list = get_udfs(True)
        if udfs_list != self.udfs_list:
            for udf in list(set(udfs_list) - set(self.udfs_list)):
                await con["node"]["async_con"].cursor().execute(udf)
        self.udfs_list = udfs_list

    ###### reload federation nodes
    async def _reload(self):
        mservers, runtimes = servers.get_servers(self.servers_file, self.runtimes_file)
        if self.mservers != mservers or self.runtimes != runtimes:
            await self.clearall()  #### re-init all the connections
            self.__init__(self.servers_file, self.runtimes_file)
            await self.initialize()
        return 0
