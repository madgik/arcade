# servers = ["postgres://88.197.53.126:5432/guru99", "postgres://88.197.53.136:5433/template1","postgres://88.197.53.126:5432/template1"]
import json

def get_servers(servers_file, runtimes_file = None):
    servers, runtimes = [], []

    with open(servers_file, 'r') as file:
        servers = json.loads(file.read())
    if runtimes_file:
        with open(runtimes_file, 'r') as file:
            runtimes = json.loads(file.read())

    return servers, runtimes

