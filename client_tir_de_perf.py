import sys
import random
from dasbus.connection import SessionMessageBus
from dasbus.identifier import DBusServiceIdentifier
import argparse
import threading
import psycopg
import time
import datetime


def start_dbus_con():
    try:
        # Define the message bus.
        SESSION_BUS = SessionMessageBus()

        # Define services and objects.
        BUS = DBusServiceIdentifier(
            namespace=("io", "sylink", "sqlite"),
            message_bus=SESSION_BUS
        )
        # Create a proxy of the object /org/example/HelloWorld
        # provided by the service org.example.HelloWorld
        proxy = BUS.get_proxy()
        return proxy
    except Exception as e:
        if debug:
            print(e)
        return False


def make_query(proxy, db, query):
    try:
        # Call the DBus method Hello and print the return value.
        res = proxy.send_query({'db': db, 'queries': query})
        if res:
            return res
        else:
            return False

    except Exception as e:
        if debug:
            print(e)
        return False


def handler():
    conn = start_dbus_con()
    dbs = ['/home/r0x00/Data/Projects/sqlite_manager/Tests/test_db1.db',
           '/home/r0x00/Data/Projects/sqlite_manager/Tests/test_db2.db',
           '/home/r0x00/Data/Projects/sqlite_manager/Tests/test_db3.db',
           '/home/r0x00/Data/Projects/sqlite_manager/Tests/test_db4.db',
           '/home/r0x00/Data/Projects/sqlite_manager/Tests/test_db5.db']

    while True:
        try:
            # time.sleep(1)
            queries = ['SELECT MAX( id ) + 1 from firewall_rules',
                       'SELECT name, version from default_category',
                       'SELECT id, action, keyword, category from disabled_rules',
                       f'SELECT id FROM category_rules WHERE id = {random.randint(20000000, 20015111)}'
                       ]
            random_db = dbs[random.randint(0, len(dbs) - 1)]
            random_query = queries[random.randint(0, len(queries) - 1)]
            # pgconn = psycopg.connect('postgresql://snort:snortpass@10.77.7.11:5432/sqlite_manager')
            # pgconn = ""

            start = datetime.datetime.now().timestamp()
            res = make_query(conn, random_db, random_query)
            if res['status']:
                print(random_db, random_query)

                with psycopg.connect('postgresql://snort:snortpass@10.77.7.11:5432/sqlite_manager') as pgconn:
                    cur = pgconn.cursor()
                    cur.execute("""
                        INSERT INTO exec (exectimestamp, duration, request)
                        VALUES (%s, %s, %s);
                        """, (time.time(), (datetime.datetime.now().timestamp() - start), random_query))

        except KeyboardInterrupt:
            return
        except Exception:
            pass


if __name__ == "__main__":
    debug = True
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument(
                '--debug',
                action='store_true',
                help='Print debug info'
            )
        parser.add_argument(
            '-db',
            '--database',
            help='Set DB to use',
            required=True
        )
        parser.add_argument(
            '-q',
            '--query',
            help='Set query to send',
            required=True
        )

        args = parser.parse_args()
        if args.debug:
            debug = True
        if args.q and args.db:
            conn = start_dbus_con()
            res = make_query(conn, args.q, args.db)
            print(res)
    else:
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=handler, name=str(i)))
        for t in threads:
            t.start()
        print(threading.enumerate())
