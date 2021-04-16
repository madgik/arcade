"""
This is a MonetDB Python API.

To set up a connection use pymonetdb.connect()

"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import pkg_resources

from dbapis.aiopymonetdb.profiler import ProfilerConnection
from dbapis.aiopymonetdb.sql.connections import Connection
from dbapis.aiopymonetdb.sql import *
from dbapis.aiopymonetdb.exceptions import *

try:
    __version__ = pkg_resources.require("pymonetdb")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "1.0rc"

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

__all__ = [
    "sql",
    "mapi_async",
    "exceptions",
    "BINARY",
    "Binary",
    "connect",
    "Connection",
    "DATE",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NUMBER",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "ROWID",
    "STRING",
    "TIME",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]


async def connect(*args, **kwargs):
    con = Connection(*args, **kwargs)
    await con.open()
    return con


def profiler_connection(*args, **kwargs):
    c = ProfilerConnection()
    c.connect(*args, **kwargs)
    return c


connect.__doc__ = Connection.__init__.__doc__
profiler_connection.__doc__ = ProfilerConnection.__init__.__doc__
