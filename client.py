import sys

from dasbus.connection import SessionMessageBus
from dasbus.identifier import DBusServiceIdentifier
import argparse


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
        conn = start_dbus_con()
        import random
        while True:
            res = make_query(conn, '/home/r0x00/test_dbus.db', 'SELECT * FROM demo')
            print(random.randint(0, 124124124), res)
            res = make_query(conn, '/home/r0x00/test_dbus.dbz', 'SELECT * FROM demo')
            print(random.randint(0, 124124124), res)
            exit()
