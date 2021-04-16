"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import socket
import asyncio
import logging
import struct
from dbapis.aiopymonetdb.sql import monetize
import hashlib

from six import BytesIO, PY3
from typing import Optional

from pymonetdb.exceptions import (
    OperationalError,
    DatabaseError,
    ProgrammingError,
    NotSupportedError,
    IntegrityError,
)
import os

logger = logging.getLogger(__name__)

MAX_PACKAGE_LENGTH = (1024 * 8) - 2

MSG_PROMPT = ""
MSG_MORE = "\1\2\n"
MSG_INFO = "#"
MSG_ERROR = "!"
MSG_Q = "&"
MSG_QTABLE = "&1"
MSG_QUPDATE = "&2"
MSG_QSCHEMA = "&3"
MSG_QTRANS = "&4"
MSG_QPREPARE = "&5"
MSG_QBLOCK = "&6"
MSG_HEADER = "%"
MSG_TUPLE = "["
MSG_TUPLE_NOSLICE = "="
MSG_REDIRECT = "^"
MSG_OK = "=OK"

STATE_INIT = 0
STATE_READY = 1

# MonetDB error codes
errors = {
    "42S02": OperationalError,  # no such table
    "M0M29": IntegrityError,  # INSERT INTO: UNIQUE constraint violated
    "2D000": IntegrityError,  # COMMIT: failed
    "40000": IntegrityError,  # DROP TABLE: FOREIGN KEY constraint violated
}


def handle_error(error):
    """Return exception matching error code.

    args:
        error (str): error string, potentially containing mapi error code

    returns:
        tuple (Exception, formatted error): returns OperationalError if unknown
            error or no error code in string

    """

    if error[:13] == "SQLException:":
        idx = str.index(error, ":", 14)
        error = error[idx + 10 :]
    if len(error) > 5 and error[:5] in errors:
        return errors[error[:5]], error[6:]
    else:
        return OperationalError, error


def encode(s):
    """only encode string for python3"""
    if PY3:
        return s.encode()
    return s


def decode(b):
    """only decode byte for python3"""
    if PY3:
        return b.decode()
    return b


# noinspection PyExceptionInherit
class Connection:
    """
    MAPI (low level MonetDB API) connection
    """

    def __init__(self):
        # self.url = url
        # self.logger = logging.getLogger(self.url)
        # self.parsed_url = urlparse.urlparse(url)

        # self.write_buffer = 'GET %s HTTP/1.0\r\n\r\n' % self.url
        # self.read_buffer = StringIO()
        # self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        # address = (self.parsed_url.netloc, 80)
        # self.logger.debug('connecting to %s', address)
        # self.connect(address)

        self.state = STATE_INIT
        self._result = None
        self.socket = None  # type: Optional[socket.socket]
        self.hostname = ""
        self.port = 0
        self.username = ""
        self.password = ""
        self.database = ""
        self.language = ""
        self.loop = None
        self.reader = None
        self.writer = None
        self.connect_timeout = socket.getdefaulttimeout()

    async def connect(
        self,
        database,
        username,
        password,
        language,
        hostname=None,
        port=None,
        unix_socket=None,
        connect_timeout=-1,
    ):
        """setup connection to MAPI server

        unix_socket is used if hostname is not defined.
        """
        if not unix_socket and os.path.exists("/tmp/.s.monetdb.%i" % port):
            unix_socket = "/tmp/.s.monetdb.%i" % port


        self.unix_socket = unix_socket
        if self.unix_socket == None:
            self.reader, self.writer = await asyncio.open_connection(hostname, port)
        else:
            self.reader, self.writer = await asyncio.open_unix_connection(self.unix_socket)
            # initialize socket
            self.writer.write('0'.encode())
            await self.writer.drain()

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.language = language

        await self._login()

        # self.socket.settimeout(socket.getdefaulttimeout())
        self.state = STATE_READY

    async def _login(self, iteration=0):
        """Reads challenge from line, generate response and check if
        everything is okay"""

        challenge = await self._getblock()

        response = self._challenge_response(challenge)
        await self._putblock(response)
        prompt = await self._getblock()
        prompt = prompt.strip()

        if len(prompt) == 0:
            # Empty response, server is happy
            pass
        elif prompt == MSG_OK:
            pass
        elif prompt.startswith(MSG_INFO):
            logger.info("%s" % prompt[1:])

        elif prompt.startswith(MSG_ERROR):
            logger.error(prompt[1:])
            raise DatabaseError(prompt[1:])

        elif prompt.startswith(MSG_REDIRECT):
            # a redirect can contain multiple redirects, for now we only use
            # the first
            redirect = prompt.split()[0][1:].split(":")
            if redirect[1] == "merovingian":
                logger.debug("restarting authentication")
                if iteration <= 10:
                    await self._login(iteration=iteration + 1)
                else:
                    raise OperationalError(
                        "maximal number of redirects " "reached (10)"
                    )

            elif redirect[1] == "monetdb":
                self.hostname = redirect[2][2:]
                self.port, self.database = redirect[3].split("/")
                self.port = int(self.port)
                logger.info(
                    "redirect to monetdb://%s:%s/%s"
                    % (self.hostname, self.port, self.database)
                )
                self.writer.close()
                await self.writer.wait_closed()
                self.connect(
                    hostname=self.hostname,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                    language=self.language,
                )

            else:
                raise ProgrammingError("unknown redirect: %s" % prompt)

        else:
            raise ProgrammingError("unknown state: %s" % prompt)

    def bind(self, operation, parameters):
        if parameters:
            if isinstance(parameters, dict):
                query = operation % {
                    k: monetize.convert(v) for (k, v) in parameters.items()
                }
            elif type(parameters) == list or type(parameters) == tuple:
                query = operation % tuple(
                    [monetize.convert(item) for item in parameters]
                )
            elif isinstance(parameters, str):
                query = operation % monetize.convert(parameters)
            else:
                msg = "Parameters should be None, dict or list, now it is %s"
        else:
            query = operation
        return query

    def bindsingle(self, parameter):
        return monetize.convert(parameter)

    async def disconnect(self):
        """ disconnect from the monetdb server """
        print(self.hostname)
        self.state = STATE_INIT
        self.writer.close()
        await self.writer.wait_closed()
        logger.info("disconnecting from database")

    async def cmd(self, operation):
        """ put a mapi command on the line"""
        logger.debug("executing command %s" % operation)

        if self.state != STATE_READY:
            raise (ProgrammingError, "Not connected")

        await self._putblock(operation)
        response = await self._getblock()

        if not len(response):
            return ""
        elif response.startswith(MSG_OK):
            return response[3:].strip() or ""
        if response == MSG_MORE:
            # tell server it isn't going to get more
            return self.cmd("")
        # print(operation+": "+response)
        # If we are performing an update test for errors such as a failed
        # transaction.

        # We are splitting the response into lines and checking each one if it
        # starts with MSG_ERROR. If this is the case, find which line records
        # the error and use it to call handle_error.
        if response[:2] == MSG_QUPDATE:
            lines = response.split("\n")
            if any([l.startswith(MSG_ERROR) for l in lines]):
                index = next(i for i, v in enumerate(lines) if v.startswith(MSG_ERROR))
                exception, msg = handle_error(lines[index][1:])
                raise exception('\n'.join(lines))

        if response[0] in [MSG_Q, MSG_HEADER, MSG_TUPLE]:
            return response
        elif response[0] == MSG_ERROR:
            exception, msg = handle_error(response[1:])
            raise exception(msg)
        elif response[0] == MSG_INFO:
            logger.info("%s" % (response[1:]))
        elif self.language == "control" and not self.hostname:
            if response.startswith("OK"):
                return response[2:].strip() or ""
            else:
                return response
        else:
            raise ProgrammingError("unknown state: %s" % response)

    def _challenge_response(self, challenge):
        """ generate a response to a mapi login challenge """
        challenges = challenge.split(":")
        salt, identity, protocol, hashes, endian = challenges[:5]
        password = self.password

        if protocol == "9":
            algo = challenges[5]
            try:
                h = hashlib.new(algo)
                h.update(encode(password))
                password = h.hexdigest()
            except ValueError as e:
                raise NotSupportedError(str(e))
        else:
            raise NotSupportedError("We only speak protocol v9")

        h = hashes.split(",")
        if "SHA1" in h:
            s = hashlib.sha1()
            s.update(password.encode())
            s.update(salt.encode())
            pwhash = "{SHA1}" + s.hexdigest()
        elif "MD5" in h:
            m = hashlib.md5()
            m.update(password.encode())
            m.update(salt.encode())
            pwhash = "{MD5}" + m.hexdigest()
        else:
            raise NotSupportedError(
                "Unsupported hash algorithms required" " for login: %s" % hashes
            )

        return (
            ":".join(["BIG", self.username, pwhash, self.language, self.database]) + ":"
        )

    async def _getblock(self):
        """ read one mapi encoded block """
        if self.language == "control" and not self.hostname:
            return (
                await self._getblock_socket()
            )  # control doesn't do block splitting when using a socket
        else:
            return await self._getblock_inet()

    async def _getblock_inet(self):
        result = BytesIO()
        last = 0
        while not last:
            flag = await self._getbytes(2)
            unpacked = struct.unpack("<H", flag)[0]  # little endian short
            length = unpacked >> 1
            last = unpacked & 1
            result.write(await self._getbytes(length))
        return decode(result.getvalue())

    async def _getblock_socket(self):
        buffer = BytesIO()
        # lock = asyncio.Lock()
        while True:
            #  async with lock:
            x = await self.reader.read(1)
            if len(x):
                buffer.write(x)
            else:
                break
        return decode(buffer.getvalue().strip())

    async def _getbytes(self, bytes_):
        """Read an amount of bytes from the socket"""
        result = BytesIO()
        count = bytes_
        while count > 0:
            #  lock = asyncio.Lock()
            #  async with lock:
            recv = await self.reader.read(count)

            if len(recv) == 0:
                raise OperationalError("Server closed connection")
            count -= len(recv)
            result.write(recv)
        return result.getvalue()

    async def _putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        if self.language == "control" and not self.hostname:
            # lock = asyncio.Lock()
            # async with lock:
            self.writer.write(encode(block))
            return await self.writer.drain()
            # return self.socket.send(encode(block))  # control doesn't do block splitting when using a socket
        else:
            await self._putblock_inet(block)

    async def _putblock_inet(self, block):
        pos = 0
        last = 0
        block = encode(block)
        while not last:
            data = block[pos : pos + MAX_PACKAGE_LENGTH]
            length = len(data)
            if length < MAX_PACKAGE_LENGTH:
                last = 1
            flag = struct.pack("<H", (length << 1) + last)
            # lock = asyncio.Lock()
            # async with lock:
            self.writer.write(flag)
            await self.writer.drain()
            # async with lock:
            self.writer.write(data)
            await self.writer.drain()

            pos += length

    async def __del2__(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    def set_reply_size(self, size):
        # type: (int) -> None
        """
        Set the amount of rows returned by the server.

        args:
            size: The number of rows
        """

        self.cmd("Xreply_size %s" % size)
