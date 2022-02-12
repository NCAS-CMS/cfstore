import unittest
import lorem

from cfstore.db import Variable, CoreDB

TEST_DATA1 = {
  'units': 'm',
  'history': lorem.paragraph(),
  'missing_value':  1.00000002004088e+20,
  'forcing_index': 1, 
}

CELL_TEST_DATA = {
     'cell_methods': "area: time: mean lat: lon: mean",
}


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
        u = Variable(long_name='merdional wind')
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
        

if __name__=="__main__":
    unittest.main()

