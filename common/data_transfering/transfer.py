import asyncio

class Transfer:
    def __init__(self, db_objects, table_id):
        self.localtable = "local" + table_id
        self.globaltable = "global" + table_id
        self.globalresulttable = "globalres" + table_id
        self.db_objects = db_objects
        self.db = db_objects["node"]["dblib"]

    async def initialize_local(self, globalschema):
        await self.db.broadcast(self.db_objects, self.globalresulttable, globalschema)

    async def initialize_global(self, localschema):
        await self.db.merge(self.db_objects, self.localtable, self.globaltable, "node_id INT, " + localschema)

    async def merge(self, localschema):
        await self.db.merge(
            db_objects, self.localtable, self.globaltable, localschema
        )

    async def broadcast(self, globalschema):
        await self.db.broadcast(
            self.db_objects, self.globalresulttable, globalschema
        )

    async def transfer(self, node1, node2, transferschema):
        await self.db.transferdirect(node1, self.localtable, node2)


