import algorithms
import json
import asyncio
import time


current_time = lambda: int(round(time.time() * 1000))

class Task:
    def __init__(self, db_objects, table_id, params, transfer_runner, algorithm_name):
        self.localtable = "local" + table_id
        self.globaltable = "global" + table_id
        self.viewlocaltable = "localview" + table_id
        self.globalresulttable = "globalres" + table_id
        self.params = params
        self.attributes = params['attributes']
        self.parameters = params['parameters']
        self.db_objects = db_objects
        self.transfer_runner = transfer_runner
        self.local_schema = None
        self.global_schema = None
        self.iternum = 0
        self.table_id = table_id
        self.algorithm_name = algorithm_name
        self.db = db_objects["node"]["dblib"]

    def create_task_generator(self, algorithm):
        return algorithm(
            self.viewlocaltable,
            self.globaltable,
            self.attributes,
            self.globalresulttable,
            *self.parameters
        )

    # submit step to local executors
    async def _initialize_local(self):
        _local_execute_calls = [
            local["async_con"].submit(self.algorithm_name, self.table_id, self.params, 0, id)
            for id, local in enumerate(self.db_objects["remote"])
        ]
        await asyncio.gather(*_local_execute_calls)


    async def _initialize_global_schema(self):
        query = "drop table if exists %s; create table if not exists %s (%s);" % (
            self.globalresulttable,
            self.globalresulttable,
            self.global_schema,
        )
        await self.db_objects["node"]["async_con"].cursor().execute(query)


    async def init_tables_master(self, local_schema, global_schema):
        self.global_schema = global_schema
        self.local_schema = local_schema
        await self.transfer_runner.initialize_global(local_schema)
        await self._initialize_local()
        await self._initialize_global_schema()

    # parameters binding is an important processing step on the parameters that will be concatenated in an SQL query
    # to avoid SQL injection vulnerabilities. This step is not implemented for postgres yet but only for monetdb
    # so algorithms that contain parameters (other than attribute names) will raise an exception if running with postgres
    def bindparameters(self, parameters):
        boundparam = []
        for i in parameters:
            if isinstance(i, (int, float, complex)):
                boundparam.append(i)
            else:
                boudparam.append(self.db_objects["node"]["async_con"].bind_str(i))
        return boundparam

    async def createlocalviews_master(self):
        t1 = current_time()
        print(self.db_objects["remote"])
        _create_view_calls = [
            local["async_con"].submit(self.algorithm_name, self.table_id, self.params, 0, id)
            for id, local in enumerate(self.db_objects["remote"])
        ]
        await asyncio.gather(*_create_view_calls)
        print("time " + str(current_time() - t1))



    async def task_local_master(self, schema):
        t1 = current_time()
        static_schema = 1
        ### TODO move these parts to scheduler
        if self.local_schema == None or self.local_schema != schema:
            self.local_schema = schema
            static_schema = 0
            await self.transfer_runner.initialize_global(self.local_schema)

        _local_execute_calls = [
            local['async_con'].submit(self.algorithm_name, self.table_id, self.params, static_schema, id)
            for id, local in enumerate(self.db_objects["remote"])
        ]
        await asyncio.gather(*_local_execute_calls)

        print("time " + str(current_time() - t1))

    #### run a task on all local nodes and sets up the transfer of the results to global node



    async def task_global(self, schema, sqlscript):

        t1 = current_time()
        flag_to_run_local_schema = 0
        ### TODO move these parts to scheduler
        if self.global_schema == None or self.global_schema != schema:
            self.global_schema = schema
            flag_to_run_local_schema = 1
            await self._initialize_global_schema()

        if 'iternum' not in schema and 'history' not in schema:
            query = (
                "delete from "
                + self.globalresulttable
                + "; insert into "
                + self.globalresulttable
                + " "
                + sqlscript
                + "; "
                + f" select * from {self.globalresulttable};"
            )
        elif 'iternum' in schema:
            query = (
                    "insert into "
                    + self.globalresulttable
                    + " "
                    + sqlscript
                    + "; "
                    +"delete from "
                    + self.globalresulttable
                    + " where iternum <= "+ str(self.iternum-1)
                    + ";"
                    + f" select * from {self.globalresulttable};"
            )
        else:
            query = (
                    "insert into "
                    + self.globalresulttable
                    + " "
                    + sqlscript
                    + "; "
                    + f"select * from {self.globalresulttable} where iternum = {self.iternum};"
            )

        cur = self.db_objects["node"]["async_con"].cursor()
        result = await cur.execute(query)
        print("iternum: "+str(self.iternum))
        print("time " + str(current_time() - t1))
        self.iternum += 1
        if flag_to_run_local_schema:
            print('fofo')
            _local_execute_calls = [
                local["async_con"].submit(self.algorithm_name, self.table_id, self.params, 0, id)
                for id, local in enumerate(self.db_objects["remote"])
            ]
            await asyncio.gather(*_local_execute_calls)

        return await cur.fetchall()


    async def register_udf(self, udf):
        # this is not implemented
        await self.db_objects["node"]["async_con"].create_function(udf)

    async def clean_up_master(self):
        await self.db.clean_tables(
            self.db_objects, self.globaltable, self.localtable, self.viewlocaltable, self.globalresulttable
        )
        _local_execute_calls = [
            local["async_con"].submit(self.algorithm_name, self.table_id, self.params, 0, id, cleanup = True)
            for id, local in enumerate(self.db_objects["remote"])
        ]
        await asyncio.gather(*_local_execute_calls)

