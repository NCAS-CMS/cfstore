# not a unit-test because we don't want it to run automatically
from cfstore.plugins.et_main import et_main
from cfstore.interface import CollectionDB

if __name__ == '__main__':
    db = CollectionDB()
    db.init('sqlite:///hiresgw.db')
    et_main(db, 'init', 'hiresgw')
