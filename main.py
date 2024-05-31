import _queue
import traceback
import cProfile

from gi.repository import GLib
import sqlite3
from time import sleep, time
import threading
import queue
import os.path
import random
import dasbus.typing
from dasbus.loop import EventLoop
from dasbus.connection import SessionMessageBus
from dasbus.server.interface import dbus_interface


# def profile(func):
#     # def wrapper(*args, **kwargs):
#     #     datafn = f"profiles/{func.__name__}.profile"  # Name the data file sensibly
#     #     prof = cProfile.Profile()
#     #     retval = prof.runcall(func, *args, **kwargs)
#     #     prof.dump_stats(datafn)
#     #     return retval
#
#     def wrap(*args, **kwargs):
#         started_at = time()
#         result = func(*args, **kwargs)
#         logging.warning(time() - started_at)
#         return result
#
#     return wrap


def connect_db(database):
    err_msg = ''
    for _ in range(5):
        try:
            conn = sqlite3.connect(database)
            return conn, conn.cursor()
        except Exception as e:
            err_msg = e
            sleep(.1)
    print(err_msg)
    return False, False


def make_db_request(conn, cur, rq):
    try:
        res = ''
        cur.execute(rq)
        if rq.lower().startswith('select'):
            res = cur.fetchall()
        return {"status": True, "msg": '', "data": str(res)}

    except Exception as error:
        return {"status": False, "msg": error.__str__(), "data": ''}
    finally:
        conn.commit()


class BusObj(object):
    __dbus_xml__ = """
        <node>
            <interface name="io.sylink.sqlite">
                <method name="send_query">
                    <arg direction="in" name="data" type="a{ss}" />
                    <arg direction="out" name="return" type="a{ss}" />
                </method>
            </interface>
        </node>
        """

    def send_query(self, data: dasbus.typing.Dict) -> dasbus.typing.Dict:  # {'db': 'WiFi', 'queries': 'Manager'}
        try:
            if type(data) is dict:
                start = time()
                if len(data) == 2:
                    database, query = (i for i in data.values())
                    return_dict = ''
                    if type(query) is str:
                        qid = random.randint(0, 9999999999)
                        if debug:
                            print('NEW QUERY :', {'ID': qid, 'DB': database, 'QUERY': query})
                        waiting_queue.put({'id': qid, 'db': database, 'q': query}, block=False, timeout=None)
                        while True:
                            if (time() - start) > 30:
                                raise Exception('Timeout')
                            else:
                                if qid in result_arr:
                                    return_dict = result_arr[qid]
                                    if debug:
                                        print('RESULT ID', qid, ':', return_dict)
                                    break
                            sleep(.0001)
                        return_dict['status'] = str(return_dict['status'])
                        return return_dict
                        #return {'db': 'WiFi', 'queries': 'Manager'}
                    else:
                        raise Exception('Incorrect query format')
            else:
                raise Exception("Incorrect datatype for data")
        except Exception as error:
            print(traceback.print_exc())
            return {"status": 'False', "msg": error.__str__(), "data": ''}


def conn_db(infos):
    try:
        if os.path.exists(infos['db']):
            conn, cur = connect_db(infos['db'])
            if conn:
                start = time()
                while (time() - start) < 30:
                    # print((time() - start))
                    query_queue = infos['q']
                    try:
                        query = query_queue.get(block=True, timeout=.0001)
                    except _queue.Empty:
                        continue

                    if query:
                        res = make_db_request(conn, cur, query)
                        if res:
                            infos['return_q'].put(res)
                            query_queue.task_done()
                            start = time()
                cur.close()
                conn.close()
            else:
                raise Exception('Cannot connect to DB')
        else:
            raise Exception('Database does not exist')

    except Exception as error:
        print(traceback.print_exc())
        infos['return_q'].put({"status": False, "msg": error.__str__(), "data": ''})


class ThreadList:
    def __init__(self):
        self.thread_list = dict()
        self.lock = False
        self.changes = random.randint(1, 9999999)

    def get_list(self):
        while self.lock:
            sleep(.0001)
        return self.thread_list

    def get_changes(self, nb):
        if nb != self.changes:
            return self.changes
        else:
            return False

    def set_list(self, new_list):
        auto_unlock = True
        if self.lock:
            auto_unlock = False
        else:
            self.lock = True

        self.thread_list = new_list
        self.changes = random.randint(0, 9999999)

        if auto_unlock:
            self.lock = False

    def append_list(self, item):
        auto_unlock = True
        if self.lock:
            auto_unlock = False
        else:
            self.lock = True

        for db in item:
            if db not in self.thread_list:
                self.thread_list[db] = item[db]
        self.changes = random.randint(0, 9999999)

        if auto_unlock:
            self.lock = False

    def set_lock(self):
        self.lock = True

    def unset_lock(self):
        self.lock = False


def handler_db_conn(thread_list_obj):
    change = thread_list_obj.get_changes(0)
    while True:
        try:
            new_change = thread_list_obj.get_changes(change)
            if new_change:
                change = new_change
                threads = thread_list_obj.get_list()
                # print('Number of conn thread running:', len(threads))
                if threads:
                    thread_list_obj.set_lock()
                    keys_to_remove = []
                    for entry in threads:
                        if not threads[entry]['t'].is_alive():
                            keys_to_remove.append(entry)

                    for key in keys_to_remove:
                        threads[key]['t'].join()
                        threads[key]['q'].join()
                        threads.pop(key)
                    thread_list_obj.set_list(threads)
                    thread_list_obj.unset_lock()
            sleep(5)
        except Exception:
            print(traceback.print_exc())


def handler(w_queue, thread_list_obj):
    try:
        thread_dict = dict()  # db: {'q': queue, 't': thread, 'db': db}
        return_queue = queue.Queue()
        return_data = ''

        change = thread_list_obj.get_changes(0)
        while True:
            # print('&&é"&é"&é"')
            if thread_list_obj.get_changes(change):
                thread_dict = thread_list_obj.get_list()

            try:
                data = w_queue.get(block=True, timeout=1)
            except _queue.Empty:
                continue
            if data:
                req_id = data['id']
                db = data['db']
                query = data['q']
                if db not in thread_dict:
                    thread_dict[db] = {'q': queue.Queue(), 't': threading.Thread(), 'db': db, 'return_q': return_queue}
                    thread_dict[db]['t'] = threading.Thread(target=conn_db, args=(thread_dict[db],))
                    thread_dict[db]['t'].start()

                    thread_list_obj.append_list({db: thread_dict[db]})

                    thread_dict[db]['q'].put(query, block=False, timeout=False)
                    return_data = return_queue.get()
                    return_queue.task_done()

                else:
                    thread_dict[db]['q'].put(query, block=False, timeout=False)
                    return_data = return_queue.get()
                    return_queue.task_done()

                result_arr[req_id] = return_data
                return_data = ''

    except Exception as error:
        print(traceback.print_exc())
        if req_id:
            result_arr[req_id] = {"status": False, "msg": error.__str__(), "data": ''}


if __name__ == "__main__":
    debug = False

    waiting_queue = queue.Queue()
    result_arr = dict()
    thread_list = ThreadList()

    handler_conn_db_thread = threading.Thread(target=handler_db_conn, args=(thread_list,))
    handler_conn_db_thread.start()

    handler_thread = threading.Thread(target=handler, args=(waiting_queue, thread_list))
    handler_thread.start()

    mainloop = EventLoop()
    bus = SessionMessageBus()
    bus.publish_object("/io/sylink/sqlite", BusObj())
    bus.register_service("io.sylink.sqlite")

    mainloop.run()
