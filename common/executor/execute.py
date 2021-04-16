import datetime
import random
from common.data_transfering import transfer
import json
import settings
from master import scheduler as master_scheduler
from worker import scheduler as worker_scheduler
from master import task_executor as master_task_executor
from worker import task_executor as worker_task_executor

DEBUG = settings.DEBUG

def get_uniquetableid():
    return "user{0}".format(
        datetime.datetime.now().microsecond + (random.randrange(1, 100 + 1) * 100000)
    )

async def exec_master(algorithm, algorithm_instance, params, db_objects):
    table_id = get_uniquetableid()
    params = json.loads(params)
    transfer_runner = transfer.Transfer(db_objects, table_id)
    task_executor_instance = master_task_executor.Task(db_objects, table_id, params, transfer_runner, algorithm)
    scheduler_instance = master_scheduler.Scheduler(task_executor_instance, algorithm_instance.algorithm)
    result = await scheduler_instance.schedule()
    return result

async def exec_worker(algorithm, algorithm_instance, params, db_objects, hash_value, schema, node_id, states, cleanup = False):
    table_id = hash_value
    transfer_runner = transfer.Transfer(db_objects, table_id)
    task_executor_instance = worker_task_executor.Task(db_objects, table_id, params, node_id, transfer_runner, algorithm)
    scheduler_instance = worker_scheduler.Scheduler(task_executor_instance, algorithm_instance.algorithm, states, schema,
                                             cleanup)
    result = await scheduler_instance.schedule()
    return result

#async def run(algorithm, algorithm_instance, params, db_objects, hash_value, schema, node_id, states, cleanup = False):
#    if hash_value:
#        table_id = hash_value
#    else:
#        table_id = get_uniquetableid()
#        params = json.loads(params)
#    transfer_runner = transfer.Transfer(db_objects, table_id)
#    task_executor_instance = task_executor.Task(db_objects, table_id, params, node_id, transfer_runner, algorithm)
#    scheduler_instance  = scheduler.Scheduler(task_executor_instance, algorithm_instance.algorithm, states, schema, cleanup)
#    result = await scheduler_instance.schedule()
#    return result
