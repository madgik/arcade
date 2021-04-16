#!/usr/bin/env python

import os
import sys
from importlib import reload
from builtins import input
import ast
from moz_sql_parser import parse

import re
import json
import math
import random
import readline
from lib import jopts

from os import environ
import urllib.request, urllib.error, urllib.parse


def urlrequestpost(*args):
    domainExtraHeaders = {"Accept-Encoding": "gzip"}
    try:
        req = urllib.request.Request(
            "".join((x for x in args[1:] if x != None)), None, domainExtraHeaders
        )

        datain = args[0]

        dataout = []
        if type(datain) == list:
            for i in range(0, len(datain), 2):
                dataout.append(
                    (datain[i].encode("utf_8"), datain[i + 1].encode("utf_8"))
                )
        else:
            dataout = [
                (x.encode("utf_8"), y.encode("utf_8")) for x, y in list(datain.items())
            ]

        if dataout == []:
            raise functions.OperatorError(
                "urlrequestpost", "A list or dict should be provided"
            )

        hreq = urllib.request.urlopen(
            req, urllib.parse.urlencode(dataout).encode("utf-8")
        )

        if [
            1
            for x, y in list(hreq.headers.items())
            if x.lower() in ("content-encoding", "content-type")
            and y.lower().find("gzip") != -1
        ]:
            hreq = gzip.GzipFile(fileobj=hreq)

        return str(hreq.read(), "utf-8", errors="replace")

    except urllib.error.HTTPError as e:
        if args[1] == None:
            return None
        else:
            raise e


class MyCompleter(object):  # Custom completer
    def __init__(self, options):
        self.options = sorted(options)

    def complete(self, text, state):
        if state == 0:  # on first trigger, build possible matches
            if not text:
                self.matches = self.options[:]
            else:
                self.matches = [s for s in self.options if s and s.startswith(text)]

        # return match indexed by state
        try:
            return self.matches[state]
        except IndexError:
            return None

    def display_matches(self, substitution, matches, longest_match_length):
        line_buffer = readline.get_line_buffer()
        columns = environ.get("COLUMNS", 80)

        print()

        tpl = "{:<" + str(int(max(map(len, matches)) * 1.2)) + "}"

        buffer = ""
        for match in matches:
            match = tpl.format(match[len(substitution) :])
            if len(buffer + match) > columns:
                print(buffer)
                buffer = ""
            buffer += match

        if buffer:
            print(buffer)

        print("> ", end="")
        print(line_buffer, end="")
        sys.stdout.flush()


pipedinput = not sys.stdin.isatty()
errorexit = True
nobuf = False


if pipedinput:
    # If we get piped input use dummy readline
    readline = lambda x: x
    readline.remove_history_item = lambda x: x
    readline.read_history_file = lambda x: x
    readline.write_history_file = lambda x: x
    readline.set_completer = lambda x: x
    readline.add_history = lambda x: x
    readline.parse_and_bind = lambda x: x
    readline.set_completer_delims = lambda x: x
else:
    # Workaround for absence of a real readline module in win32
    import lib.reimport

    if sys.platform == "win32":
        import pyreadline as readline
    else:
        import readline

import datetime
import time
import locale
import os

from lib.dsv import writer
import csv

try:
    if pipedinput:
        raise Exception("go to except")

    import lib.colorama as colorama
    from colorama import Fore, Back, Style

    colnums = True
except:
    colorama = lambda x: x

    def dummyfunction():
        pass

    colorama.deinit = colorama.init = dummyfunction
    Fore = Style = Back = dummyfunction
    Fore.RED = Style.BRIGHT = Style.RESET_ALL = ""
    colnums = False
    pass

DELIM = Fore.RED + Style.BRIGHT + "|" + Style.RESET_ALL


class mtermoutput(csv.Dialect):
    def __init__(self):
        self.delimiter = "|"
        if not allquote:
            self.quotechar = "|"
            self.quoting = csv.QUOTE_MINIMAL
        else:
            self.quotechar = '"'
            self.quoting = csv.QUOTE_NONNUMERIC
        self.escapechar = "\\"
        self.lineterminator = "\n"


def raw_input_no_history(*args):
    global pipedinput

    if pipedinput:
        try:
            input2 = input()
        except EOFError:
            connection.close()
            exit(0)
        return input

    try:
        input2 = input(*args)
    except:
        return None
    if input2 != "":
        try:
            readline.remove_history_item(readline.get_current_history_length() - 1)
        except:
            pass
    return input2


def sizeof_fmt(num, use_kibibyte=False):
    base, infix = [(1000.0, ""), (1024.0, "i")][use_kibibyte]
    for x in ["", "K%s" % infix, "M%s" % infix, "G%s" % infix]:
        if num < base and num > -base:
            if x == "":
                return str(int(num))
            else:
                return "%3.1f%s" % (num, x)
        num /= base
    return "%3.1f %s" % (num, "T%sB" % infix)


def mcomplete(textin, state):
    global number_of_kb_exceptions
    number_of_kb_exceptions = 0

    def normalizename(col):
        if re.match(r"\.*[\w_$\d.]+\s*$", col, re.UNICODE):
            return col
        else:
            return "`" + col.lower() + "`"

    def numberedlist(c):
        maxcolchars = len(str(len(c) + 1))
        formatstring = "{:" + ">" + str(maxcolchars) + "}"
        o = []
        for num in range(len(c)):
            o.append(formatstring.format(num + 1) + "|" + c[num])
        return o

    text = textin

    # Complete \t to tabs
    if text[-2:] == "\\t":
        if state == 0:
            return text[:-2] + "\t"
        else:
            return

    prefix = ""

    localtables = []
    completions = []

    linebuffer = readline.get_line_buffer()

    beforecompl = linebuffer[0 : readline.get_begidx()]

    # Only complete '.xxx' completions when space chars exist before completion
    if re.match(r"\s*$", beforecompl):
        completions += dotcompletions
    # If at the start of the line, show all tables
    if beforecompl == "" and text == "":
        localtables = alltables[:]

        # Check if all tables start with the same character
        if localtables != []:
            prefcharset = set((x[0] for x in localtables))
            if len(prefcharset) == 1:
                localtables += [" "]
        completions = localtables
    # If completion starts at a string boundary, complete from local dir
    elif beforecompl != "" and beforecompl[-1] in ("'", '"'):
        completions = os.listdir(os.getcwd())
        hits = [x for x in completions if x[: len(text)] == str(text)]
        if state < len(hits):
            return hits[state]
        else:
            return
    # Detect if in simplified 'from' or .schema
    elif re.search(
        r"(?i)(from\s(?:\s*[\w\d._$]+(?:\s*,\s*))*(?:\s*[\w\d._$]+)?$)|(^\s*\.schema)|(^\s*\.t)|(^\s*\.tables)",
        beforecompl,
        re.DOTALL | re.UNICODE,
    ):
        localtables = alltablescompl[:]
        completions = localtables
    else:
        localtables = alltablescompl[:]
        completions += lastcols + colscompl
        completions += sqlandmtermstatements + allfuncs + localtables

    hits = [
        x.lower() for x in completions if x.lower()[: len(text)] == str(text.lower())
    ]

    update_cols_from_tables_in_text(linebuffer)

    if hits == [] and text.find(".") != -1 and re.match(r"[\w\d._$]+", text):
        tablename = re.match(r"(.+)\.", text).groups()[0].lower()
        update_cols_for_table(tablename)
        hits = [
            x.lower() for x in colscompl if x.lower()[: len(text)] == str(text.lower())
        ]

    # If completing something that looks like a table, complete only from cols
    if hits == [] and text[-2:] != "..":
        prepost = re.match(r"(.+\.)([^.]*)$", text)
        if prepost:
            prefix, text = prepost.groups()
            hits = [
                x.lower()
                for x in lastcols + [y for y in colscompl if y.find(".") == -1]
                if x.lower()[: len(text)] == str(text.lower())
            ]
            # Complete table.number
            if len(hits) == 0 and text.isdigit():
                cols = get_table_cols(prefix[:-1])
                colnum = int(text)
                if 0 < colnum <= len(cols):
                    hits = [cols[colnum - 1]]
                elif colnum == 0:
                    hits = numberedlist(cols)
                    if state < len(hits):
                        return hits[state]
                    else:
                        return

    try:
        # Complete from colnums
        icol = int(text)
        if len(hits) == 0 and text.isdigit() and icol >= 0:
            # Show all tables when completing 0
            if icol == 0 and newcols != []:
                if len(newcols) == 1:
                    if state > 0:
                        return
                    return prefix + normalizename(newcols[0])
                hits = numberedlist(newcols)
                if state < len(hits):
                    return hits[state]
                else:
                    return
            # Complete from last seen when completing for other number
            if icol <= len(lastcols) and lastcols != [] and state < 1:
                return prefix + normalizename(lastcols[icol - 1])
    except:
        pass

    if state < len(hits):
        sqlstatem = set(sqlandmtermstatements)
        altset = set(localtables)

        if hits[state] == "..":
            if text == ".." and lastcols != []:
                return prefix + ", ".join([normalizename(x) for x in lastcols]) + " "
            else:
                return prefix + hits[state]
        if hits[state] in sqlstatem:
            return prefix + hits[state]
        if hits[state] in colscompl:
            if text[-2:] == "..":
                tname = text[:-2]
                try:
                    cols = get_table_cols(tname)
                    return prefix + ", ".join(cols) + " "
                except:
                    pass
        if hits[state] in altset:
            if text in altset:
                update_cols_for_table(hits[state])
            return prefix + hits[state]
        else:
            return prefix + normalizename(hits[state])
    else:
        return


def buildrawprinter(separator):
    return writer(sys.stdout, dialect=mtermoutput(), delimiter=str(separator))


def printrow(row):
    global rawprinter, colnums
    # print("lala",connection.openiters)
    if not colnums:
        rawprinter.writerow(row)
        return
    rowlen = len(row)
    i1 = 1
    for d in row:
        if rowlen > 3:
            if i1 == 1:
                sys.stdout.write(
                    Fore.RED + Style.BRIGHT + "[" + "1" + "|" + Style.RESET_ALL
                )
            else:
                sys.stdout.write(
                    Fore.RED + "[" + str(i1) + Style.BRIGHT + "|" + Style.RESET_ALL
                )
        else:
            if i1 != 1:
                sys.stdout.write(Fore.RED + Style.BRIGHT + "|" + Style.RESET_ALL)
        if type(d) in (int, float, int):
            d = str(d)
        elif d is None:
            d = Style.BRIGHT + "null" + Style.RESET_ALL
        try:
            sys.stdout.write(d)
        except KeyboardInterrupt:
            raise
        except:
            sys.stdout.write(d)

        i1 += 1
    sys.stdout.write("\n")


def printterm(*args, **kwargs):
    global pipedinput

    msg = ",".join([str(x) for x in args])

    if not pipedinput:
        print(msg)


def exitwitherror(*args):
    msg = ",".join([str(x) for x in args])

    if errorexit:
        print()
        sys.exit(msg)
    else:
        print((json.dumps({"error": msg}, separators=(",", ":"), ensure_ascii=False)))
        print()
        sys.stdout.flush()


VERSION = "1.0"
mtermdetails = "Monetdb-federate - version " + VERSION
intromessage = """Enter ".help" for instructions
Enter SQL statements terminated with a ";" """

helpmessage = """.functions             Lists all functions
.help                  Show this message (also accepts '.h' )
.help FUNCTION         Show FUNCTION's help page
.quit                  Exit this program
.schema TABLE          Show the CREATE statements
.quote                 Toggle between normal quoting mode and quoting all mode
.beep                  Make a sound when a query finishes executing
.tables                List names of tables (you can also use ".t" or double TAB)
.t TABLE               Browse table
.explain               Explain query plan
.colnums               Toggle showing column numbers
.separator SEP         Change separator to SEP. For tabs use 'tsv' or '\\t' as SEP
                       Separator is used only when NOT using colnums
.vacuum                Vacuum DB using a temp file in current path
.queryplan query       Displays the queryplan of the query

Use: FILE or CLIPBOARD function for importing data
     OUTPUT or CLIPOUT function for exporting data"""

if "HOME" not in os.environ:  # Windows systems
    if "HOMEDRIVE" in os.environ and "HOMEPATH" in os.environ:
        os.environ["HOME"] = os.path.join(
            os.environ["HOMEDRIVE"], os.environ["HOMEPATH"]
        )
    else:
        os.environ["HOME"] = "C:\\"

histfile = os.path.join(os.environ["HOME"], ".mfederate")


automatic_reload = False
if not pipedinput:
    try:
        readline.read_history_file(histfile)
    except IOError:
        pass
    import atexit

    atexit.register(readline.write_history_file, histfile)
    automatic_reload = True
    readline.set_completer(mcomplete)
    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims(" \t\n`!@#$^&*()=+[{]}|;:'\",<>?")


separator = "|"
allquote = False
beeping = False
db = ""
language, output_encoding = locale.getdefaultlocale()


if output_encoding == None:
    output_encoding = "UTF8"


rawprinter = buildrawprinter(separator)


sqlandmtermstatements = [
    "select ",
    "create ",
    "where ",
    "table ",
    "group by ",
    "drop ",
    "order by ",
    "index ",
    "from ",
    "alter ",
    "limit ",
    "delete ",
    "..",
    "attach database '",
    "detach database ",
    "distinct",
    "exists ",
]
dotcompletions = [
    ".help ",
    ".colnums",
    ".schema ",
    ".functions ",
    ".tables",
    ".explain ",
    ".vacuum",
    ".queryplan ",
]

# Intro Message
if not pipedinput:
    print(mtermdetails)
    print("running on Python: " + ".".join([str(x) for x in sys.version_info[0:3]]))
    print(intromessage)

number_of_kb_exceptions = 0

while True:

    statement = raw_input_no_history("mfederate> ")
    if statement == None:
        number_of_kb_exceptions += 1
        print()
        if number_of_kb_exceptions < 2:
            continue
        else:
            readline.write_history_file(histfile)
            break

    # Skip comments
    statement = str(statement)
    if statement.startswith("--"):
        continue

    number_of_kb_exceptions = 0

    # scan for commands
    iscommand = re.match(
        "\s*\.(?P<command>\w+)\s*(?P<argument>([\w\.]*))(?P<rest>.*)$", statement
    )
    validcommand = False
    queryplan = False

    if iscommand:
        validcommand = True
        command = iscommand.group("command")
        argument = iscommand.group("argument")
        rest = iscommand.group("rest")
        origstatement = statement
        statement = None

        if "select".startswith(command):

            update_tablelist()
            argument = argument.rstrip("; ")
            if not argument:
                for i in sorted(alltables):
                    printterm(i)
            else:
                statement = "select * from " + argument + ";"

        elif "quit".startswith(command):
            exit(0)

        elif command == "autoreload":
            automatic_reload = automatic_reload ^ True
            printterm("Automatic reload is now: " + str(automatic_reload))

        else:
            validcommand = False
            printterm("""unknown command. Enter ".help" for help""")

        if validcommand:
            histstatement = "." + command + " " + argument + rest
            try:
                readline.add_history(histstatement)
            except:
                pass

    if statement:

        histstatement = statement

        number_of_kb_exceptions = 0
        if not statement:
            printterm()
            continue
        try:
            if not validcommand:
                readline.add_history(histstatement)
        except:
            pass

        before = datetime.datetime.now()
        try:
            colorama.init()
            rownum = 0

            if not pipedinput:
                try:
                    st = parse(statement)
                except:
                    print("SQLError: not a valid federated sql statement")
                    continue
                try:
                    algo_params = st["select"]["value"]
                    mkeys = algo_params.keys()
                    for i in mkeys:
                        algorithm = i
                    all_params = st["select"]["value"][algorithm]
                    attr = []
                    parameters = []
                    for par in all_params:
                        if isinstance(par, (int, float, complex)):
                            parameters.append(par)
                        elif isinstance(par, str):
                            attr.append(par)
                        elif isinstance(par, dict):
                            parameters.append(par["literal"])

                    if type(all_params) == str:
                        attr = [all_params]
                    filterexists = 0
                    filters = []
                    table = st["from"]
                    try:
                        filt = st["where"]
                    except:
                        filterexists = -1

                    disjunctive = 0
                    if filterexists == 0:
                        options = {
                            "eq": "=",
                            "neq": "<>",
                            "gte": ">=",
                            "gt": ">",
                            "lt": "<",
                            "lte": "<=",
                        }
                        for i in filt.keys():
                            if i not in ["and", "or"]:
                                filters.append(
                                    [st["where"][i][0], options[i], st["where"][i][1]]
                                )
                            elif i == "and":
                                for j in filt["and"]:
                                    for k in j:
                                        filters.append([j[k][0], options[k], j[k][1]])

                            elif i == "or":
                                disjunctive = 1
                                for num, j in enumerate(filt[i]):
                                    for k in j.keys():
                                        if k not in ["and", "or"]:
                                            filters.append(
                                                [
                                                    [
                                                        filt[i][num][k][0],
                                                        options[k],
                                                        filt[i][num][k][1],
                                                    ]
                                                ]
                                            )
                                        elif k == "and":
                                            filtpart = []
                                            for ma in filt[i][num]["and"]:
                                                for kk in ma:
                                                    filtpart.append(
                                                        [
                                                            ma[kk][0],
                                                            options[kk],
                                                            ma[kk][1],
                                                        ]
                                                    )
                                            filters.append(filtpart)
                    if disjunctive:
                        filters = json.dumps(filters)
                    else:
                        filters = "[" + json.dumps(filters) + "]"
                except Exception as err:
                    print(err)
                    continue
                try:
                    res = urlrequestpost(
                        [
                            u"algorithm",
                            algorithm,
                            u"params",
                            u'{"table":"'
                            + table
                            + '", "attributes":'
                            + json.dumps(attr)
                            + ',"parameters":'
                            + json.dumps(parameters)
                            + ',"filters":'
                            + filters
                            + "}",
                        ],
                        sys.argv[1],
                    )
                except Exception as err:
                    print(err)
                    continue
                try:
                    for row in ast.literal_eval(res):
                        printrow(row)
                except:
                    printrow([res])
            else:
                print(
                    (
                        json.dumps(
                            {"schema": desc}, separators=(",", ":"), ensure_ascii=False
                        )
                    )
                )
                for row in cexec:
                    print((json.dumps(row, separators=(",", ":"), ensure_ascii=False)))
                    if nobuf:
                        sys.stdout.flush()
                print()
                sys.stdout.flush()

            after = datetime.datetime.now()
            tmdiff = after - before

            if not pipedinput:
                if rownum == 0:
                    printterm(
                        "Query executed in %s min. %s sec %s msec."
                        % (
                            (
                                int(tmdiff.days) * 24 * 60 + (int(tmdiff.seconds) / 60),
                                (int(tmdiff.seconds) % 60),
                                (int(tmdiff.microseconds) / 1000),
                            )
                        )
                    )
                else:
                    print("Query executed and displayed %s" % (rownum), end=" ")
                    if rownum == 1:
                        print("row", end=" ")
                    else:
                        print("rows", end=" ")
                    print(
                        "in %s min. %s sec %s msec."
                        % (
                            (
                                int(tmdiff.days) * 24 * 60 + (int(tmdiff.seconds) / 60),
                                (int(tmdiff.seconds) % 60),
                                (int(tmdiff.microseconds) / 1000),
                            )
                        )
                    )
            if beeping:
                printterm("\a\a")

            colscompl = []
            updated_tables = set()

        except:
            raise

        finally:
            colorama.deinit()
            try:
                cursor.close()
            except:
                # print "Not proper clean-up"
                pass
