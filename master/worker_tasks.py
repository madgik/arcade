from tornado import httpclient
import json

class Worker:
    def __init__(self, ip):
        self.ip = ip

    async def submit(self, algorithm_name, table_id, params, schema, node_id, cleanup = False):
        http_client = httpclient.AsyncHTTPClient()

        body = 'params={"algorithm":"'+algorithm_name+'","hash":"'+str(table_id)+'","cleanup":"'+str(cleanup)+'","node_id":'+str(node_id)+',"schema":'+str(schema)+',"params":'+json.dumps(params)+'}'
        print(body)
        response = await http_client.fetch(self.ip, method="POST", body=body)
        if response.body != b'1':
            raise Exception(response.body)




