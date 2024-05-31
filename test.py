a = {"a": 1, "b": 'ABC'}

b, c = (i for i in a.values())

# print(b, c)

import queue

e = queue.Queue()
f = queue.Queue()
print(1)
e.put(2)
f.put(2)

print(e.get())
print(f.get())

# import hashlib
# with open('Fonctionement.drawio', 'br') as file:
#     print(len(hashlib.md5(file.read()).hexdigest()))

import datetime
print(datetime.datetime.now().timestamp())
