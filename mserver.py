import logging
import sys
from common.connections import connections
from common.executor import execute
import tornado.web
from tornado.log import enable_pretty_logging
from tornado.options import define, options
import json
import importlib
import settings

MASTER = True
DEBUG = settings.DEBUG

define("port", default=sys.argv[1], help="run on the given port", type=int)

class AlgorithmException(Exception):
    def __init__(self, message):
        super(AlgorithmException, self).__init__(message)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [(r"/", MainHandler)]
        tornado.web.Application.__init__(self, handlers)

class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, *args):
        tornado.web.RequestHandler.__init__(self, *args)

class MainHandler(BaseHandler):
    # logging stuff..
    enable_pretty_logging()
    logger = logging.getLogger("MainHandler")
    hdlr = logging.FileHandler("./mserver.log", "w+")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
    access_log = logging.getLogger("tornado.access")
    app_log = logging.getLogger("tornado.application")
    gen_log = logging.getLogger("tornado.general")
    access_log.addHandler(hdlr)
    app_log.addHandler(hdlr)
    gen_log.addHandler(hdlr)
    try:
        dbs = connections.Connections(sys.argv[2], sys.argv[3])
    except:
        dbs = connections.Connections(sys.argv[2])

    states = {}

    def get_package(self,algorithm):
        try:
            mpackage = "algorithms"
            importlib.import_module(mpackage)
            algo = importlib.import_module("." + algorithm, mpackage)
            if DEBUG:
                importlib.reload(algo)
        except ModuleNotFoundError:
            raise Exception(f"`{algorithm}` does not exist in the algorithms library")
        return algo

    def put(self):
        ## to do it updates the node status (if it is master or worker)
        ## maybe informs for addition/deletion/edits of other nodes
        global MASTER
        if MASTER == False:
            MASTER = True
        else:
            MASTER = False
        self.finish()


    async def post(self):
        ## get params, algorithm contains the name of the algorithm, params is a valid json file
        await self.dbs.initialize()
        db_objects = await self.dbs.acquire()

        if MASTER:
            algorithm = self.get_argument("algorithm")
            params = self.get_argument("params")
            algorithm_instance = self.get_package(algorithm).Algorithm()
            try:
                result = await execute.exec_master(algorithm, algorithm_instance, params, db_objects)
                self.write("{}".format(result))
            except Exception as e:
                # raise tornado.web.HTTPError(status_code=500,log_message="...the log message??")
                self.logger.debug(
                    "(MadisServer::post) QueryExecutionException: {}".format(str(e))
                )
                # print "QueryExecutionException ->{}".format(str(e))

                await self.dbs.release(db_objects)
                self.write("Error: " + str(e))
                self.finish()
                raise
        else:
            parameters = json.loads(self.get_argument("params"))
            algorithm = parameters["algorithm"]
            params = parameters["params"]
            hash_value = parameters["hash"]
            static_schema = parameters["schema"]
            node_id = parameters["node_id"]
            cleanup = parameters["cleanup"]
            if hash_value in self.states:
                algorithm_instance = self.states[hash_value]['algorithm']
            else:
                algorithm_instance = self.get_package(algorithm).Algorithm()
                self.states[hash_value] = {}
                self.states[hash_value]['algorithm'] = algorithm_instance
            try:
                result = await execute.exec_worker(algorithm, algorithm_instance, params, db_objects, hash_value, static_schema, node_id,
                                                   self.states[hash_value], cleanup)
                self.write("{}".format(result))
            except Exception as e:
                # raise tornado.web.HTTPError(status_code=500,log_message="...the log message??")
                self.logger.debug(
                    "(MadisServer::post) QueryExecutionException: {}".format(str(e))
                )
                # print "QueryExecutionException ->{}".format(str(e))

                await self.dbs.release(db_objects)
                self.write("Error: " + str(e))
                self.finish()
                raise

        await self.dbs.release(db_objects)
        self.logger.debug("(MadisServer::post) str_result-> {}".format(result))
        # self.write("{}".format(result))
        self.finish()



def main(args):
    sockets = tornado.netutil.bind_sockets(options.port)
    server = tornado.httpserver.HTTPServer(Application())
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main(sys.argv)
