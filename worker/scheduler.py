class Scheduler:
    def __init__(self, task_executor, algorithm, states = None, schema = False, cleanup = False):
        self.task_executor = task_executor
        # if static schema is False the algorithm defines dynamically the returned schema in each step.
        # otherwise the algorithm sets a static schema for local/global tasks and all the next local global tasks follow
        # the defined static schema.
        self.cleanup  = cleanup
        self.static_schema = int(schema)
        self.schema = {}
        self.local_schema = None
        self.global_schema = None
        # the termination condition is processed either in the dbms or in python algorithm's workflow
        # to check the condition in the dbms an additional attribute named `termination` is required in global schema
        self.termination_in_dbms = False
        ## bind parameters before pushing them to the algorithm - necessary step to avoid sql injections
        self.parameters = task_executor.bindparameters(task_executor.parameters)

        if 'task_generator' not in states:
            self.task_generator = task_executor.create_task_generator(algorithm)
            states['task_generator'] = self.task_generator
        else:
            self.task_generator = states['task_generator']
        self.states = states
        if 'termination' not in self.states:
            self.states['termination'] = False

    async def schedule(self):

        if 'views' not in self.states:
            await self.task_executor.createlocalviews()
            self.states['views'] = True
            return 1
        if self.cleanup == 'True':
            await self.task_executor.clean_up()
            return 1

        for task in self.task_generator:
            try:
                if 'run_global' in task:
                    if 'schema' in task['run_global']:
                        await self.run_global(task['run_global'])
                        if self.states['termination']:
                            break
                    if not self.states['termination']:
                        task = self.task_generator.send(await self.task_executor.get_global_result())

                if 'set_schema' in task:
                    await self.set_schema(task['set_schema'])
                    break
                elif 'define_udf' in task:
                    try:
                        await self.define_udf(task['define_udf'])
                    except:
                        raise Exception('''online UDF definition is not implemented yet''')
                    break
                elif 'run_local' in task:
                    #if self.states['termination']:
                    await self.run_local(task['run_local'])
                    break
                    #else:
                    #    await self.run_local(self.task_generator.send(await self.task_executor.get_global_result()))
            except StopIteration:
                break

        return 1

    async def set_schema(self, schema):
        self.schema = schema

        if 'termination' in schema['global']:
            self.termination_in_dbms = True
            self.states['termination'] = True
        else:
            self.termination_in_dbms = False
            self.states['termination'] = False
        await self.task_executor.init_tables(schema['local'], schema['global'])

    async def define_udf(self, udf):
        await self.task_executor.register_udf(udf)

    async def run_local(self, step_local):
        if not self.static_schema and 'schema' not in step_local:
            raise Exception('''Schema definition is missing''')


        if not self.static_schema:
            self.local_schema = step_local['schema']
            await self.task_executor.initialize_local_schema(self.local_schema)

        await self.task_executor.task_local(self.local_schema, self.static_schema, step_local['sqlscript'])

    async def run_global(self, step_global):
        if not self.static_schema and 'schema' not in step_global:
            raise Exception('''Schema definition is missing''')

        if 'termination' in step_global['schema']:
            self.termination_in_dbms = True
            self.states['termination'] = True
        else:
            self.termination_in_dbms = False
            self.states['termination'] = False

            #if 'schema' in step_global: # TODO solve if not self.static_schema bug
        if not self.static_schema:
            result = await self.task_executor.init_global_remote_table(step_global['schema'])
            return result

    def termination(self, global_result):
        return global_result[len(global_result)-1][0]
