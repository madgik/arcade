class Scheduler:
    def __init__(self, task_executor, algorithm):
        self.task_executor = task_executor
        # if static schema is False the algorithm defines dynamically the returned schema in each step.
        # otherwise the algorithm sets a static schema for local/global tasks and all the next local global tasks follow
        # the defined static schema.
        self.local_schema = None
        self.global_schema = None
        self.static_schema = False
        # the termination condition is processed either in the dbms or in python algorithm's workflow
        # to check the condition in the dbms an additional attribute named `termination` is required in global schema
        self.termination_in_dbms = False
        ## bind parameters before pushing them to the algorithm - necessary step to avoid sql injections
        self.parameters = task_executor.bindparameters(task_executor.parameters)
        self.task_generator = task_executor.create_task_generator(algorithm)

    async def schedule(self):

        result = None
        await self.task_executor.createlocalviews_master()


        for task in self.task_generator:
            try:
                if 'run_global' in task:
                        result = await  self.run_global(task['run_global'])
                        if not self.termination_in_dbms:
                            task = self.task_generator.send(result)
                        else:
                            if self.termination(result):
                                break

                if 'set_schema' in task:
                    await self.set_schema(task['set_schema'])
                elif 'define_udf' in task:
                    try:
                        await self.define_udf(task['define_udf'])
                    except:
                        raise Exception('''online UDF definition is not implemented yet''')
                elif 'run_local' in task:
                    #if self.states['termination']:
                    await self.run_local(task['run_local'])
                    #else:
                    #    await self.run_local(self.task_generator.send(await self.task_executor.get_global_result()))
            except StopIteration:
                break

        if 'termination' in  self.global_schema  and 'iternum' in  self.global_schema:
            result = [x[2:] for x in result]
        elif 'termination' in  self.global_schema:
            result = [x[1:] for x in result]

        await self.task_executor.clean_up_master()
        return result


    async def set_schema(self, schema):
        self.schema = schema

        self.static_schema = True
        if 'termination' in schema['global']:
            self.termination_in_dbms = True
        await self.task_executor.init_tables_master(schema['local'], schema['global'])


    async def define_udf(self, udf):
        await self.task_executor.register_udf(udf)

    async def run_local(self, step_local):
        if not self.static_schema and 'schema' not in step_local:
            raise Exception('''Schema definition is missing''')

        if self.static_schema:
            self.local_schema = self.schema['local']
        else:
            self.local_schema = step_local['schema']
        await self.task_executor.task_local_master(self.local_schema,)


    async def run_global(self, step_global):
        if not self.static_schema and 'schema' not in step_global:
            raise Exception('''Schema definition is missing''')

        if self.static_schema:
            self.global_schema = self.schema['global']
        else:
            self.global_schema = step_global['schema']
            if 'termination' in self.global_schema:
                self.termination_in_dbms = True
        result = await self.task_executor.task_global(self.global_schema, step_global['sqlscript'])
        return result


    def termination(self, global_result):
        return global_result[len(global_result)-1][0]
