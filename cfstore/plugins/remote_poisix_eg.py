
from cfstore.interface import CollectionDB
from cfstore.plugins.posix import RemotePosix
import os, time
import contextlib


testdb = 'sqlite:///jasmin-test.db'
with contextlib.suppress(FileNotFoundError):
    os.remove(testdb)

db = CollectionDB()
db.init(testdb)

x = RemotePosix(db, 'jasmin')
x.configure('xfer1', 'lawrence')



tick = time.time()
x.add_collection('hiresgw/xjanp','xjanp','test stuff')
tock = time.time()-tick
print(f"Entire process took {tock:.2f}s")