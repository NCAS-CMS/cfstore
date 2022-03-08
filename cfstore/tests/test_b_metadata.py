import unittest
import lorem
from sqlalchemy import and_
import os, pathlib

from cfstore.db import Variable, CoreDB, CellMethod
from cfstore.interface import CollectionDB

CELL_TEST_DATA = {
     'eg1': "area: time: mean lat: lon: mean",
     'eg2': "time: mean lat: lon: mean"
}

CELL_QUERIES = [('area', 'mean'),]

class TestDBsimple(unittest.TestCase):
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
            assert w.long_name == 'meridional wind'

    def test_size(self):
        size = 100*36000*36000 # proxy for 1km 100 level global grid
        t = Variable(long_name='Air Temperature', cfdm_size=size)
        t['big number'] = size
        self.db.session.add(t)
        self.db.session.commit()
        q = self.db.session.query(Variable).filter(Variable.cfdm_size==size).all()
        tdm = q[0]
        self.assertEqual(tdm.cfdm_size, size)
        self.assertEqual(tdm['big number'], size)


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

class TestDBreal(unittest.TestCase):
    
    def setUp(self):
        self.db = CollectionDB()
        self.db.init('sqlite://')
        DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))/'data'
        if not DIR.exists:
            raise FileNotFoundError('No data directory for testing')
        if len(list(DIR.glob('*.nc'))) == 0:
            raise FileNotFoundError('No NetCDF files found for testing')
        self.DIR = DIR

    def test_parse_real_files(self):
        """ 
        If there is a test_data directory available, attempt to parse any netcdf files
        found there.
        """
        for f in self.DIR.glob('*.nc'):
            self.db.add_variables_from_file(f)
    
    def test_variable_queries(self):
        """ Not really implemented yet"""
        for f in self.DIR.glob('*.nc'):
            self.db.add_variables_from_file(f)
        vars = self.db.session.query(Variable).all()
        #for v in vars:
        #    print(v, v.long_name, v.cfdm_size, v.cfdm_domain, [(k,v[k]) for k in v.other_attributes])
        self.assertEqual(len(vars),3)
        # watch out, capitalisation matters in the keys
        results = self.db.retrieve_variable(standard_name='air_temperature', Conventions='CF-1.9')
        #results = self.db.retrieve_variable(standard_name='air_temperature', cfdm_size=130200)
        self.assertEqual(len(results),1)
              
            

if __name__=="__main__":
    unittest.main()

