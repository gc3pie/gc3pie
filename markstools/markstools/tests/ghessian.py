import unittest
import time

import markstools
from markstools.io.gamess import ReadGamessInp, WriteGamessInp
from markstools.calculators.gamess.calculator import GamessGridCalc

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        '''Do something before each test function is run'''
        pass
    
    def tearDown(self):
        '''Do something after each test function is run'''
        pass

    def test_water_hessian(self):
        f_inp = 'markstools/examples/water_UHF_gradient.inp'
        db = utils.Mydb('mark',options.db_name,options.db_url).cdb()

        myfile = open(f_inp, 'rb')
        reader = ReadGamessInp(myfile)
        myfile.close()
        params = reader.params
        atoms = reader.atoms
        
        ghessian = GHessian()
        gamess_calc = GamessGridCalc(db)
        ghessian.initialize(db, gamess_calc, atoms, params)
        
        ghessian.run()
        while ghessian.status not in State.terminal:
            time.sleep(10)
            ghessian.run()
        
        #run a normall gamess hessian
        params.set_group_param('$CONTRL', 'RUNTYP', 'HESSIAN')
        #then run the normal gamess job
        
        self.assertEqual(self.seq, range(10))

    def test_choice(self):
        element = random.choice(self.seq)
        self.assertTrue(element in self.seq)

    def test_sample(self):
        self.assertRaises(ValueError, random.sample, self.seq, 20)
        for element in random.sample(self.seq, 5):
            self.assertTrue(element in self.seq)

if __name__ == '__main__':
    #unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSequenceFunctions)
    unittest.TextTestRunner(verbosity=2).run(suite)
