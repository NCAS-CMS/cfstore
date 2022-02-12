import unittest
import lorem
from sqlalchemy import and_

from cfstore.db import Variable, CoreDB, CellMethod


CELL_TEST_DATA = {
     'eg1': "area: time: mean lat: lon: mean",
     'eg2': "time: mean lat: lon: mean"
}

CELL_QUERIES = [('area', 'mean'),]

class TestDB(unittest.TestCase):
    """ 
    Test basic DB implementation.
    """

    def setUp(self):
        """ Instantiate a variable"""
        self.db = CoreDB()
        self.db.init('sqlite://')


    def test_add_simple_attribute(self):
        """ 
        test adding simple attributes to a variable 
        """
        t = Variable(long_name='Air Temperature')
        t['units'] = 'K'
        u = Variable(long_name='meridional wind')
        u['units'] = 'm/s'
        self.db.session.add(t)
        self.db.session.add(u)
        self.db.session.commit()

        q = self.db.session.query(Variable).filter(Variable.long_name=='Air Temperature').all()
        for v in q:
            assert v['units'] == 'K'
        q = self.db.session.query(Variable).filter(
            Variable.with_other_attributes("units", 'm/s')).all()

        for w in q:
            print(w)
            assert w.long_name == 'meridional wind'


    def test_add_cell_method(self):

        t1 = Variable(long_name='fred')
        t1.cell_methods = CELL_TEST_DATA['eg2']
        t2 = Variable(long_name='joe')
        t2.cell_methods = CELL_TEST_DATA['eg1']
        self.db.session.add(t1)
        self.db.session.add(t2)
        self.db.session.commit()

        for a, m in CELL_QUERIES:

            # FIXME: we probably want to make sure adding these things is unique so we can pull all variables with same cell methods
            # i.e it should not be possible to pull out more than one q here ... so we would want to use .one()
            q = self.db.session.query(CellMethod).filter(and_(CellMethod.axis == a, CellMethod.method == m)).all()
            v0  = [str(v) for v in q[0].used_in]
            assert "joe" in v0
            

if __name__=="__main__":
    unittest.main()

