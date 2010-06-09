import StringIO as StringIO
import os

import markstools
from markstools.calculators.gamess import *

from gorg.model.gridjob import JobInterface
from gorg.gridjobscheduler import STATES as RUN_STATES
from gorg.model.gridtask import TaskInterface
from parser import ParseGamessDat, ParseGamessOut
from result import GamessResult 
from ase.atoms import Atoms 
from markstools.lib.exceptions import *

class GamessParams:
    '''Holds the GAMESS run parameters'''
    groups = dict()
    job_user_data_dict = dict()
    title = None
    r_orbitals = None
    r_hessian = None
    
    def get_group(self, group_key):
        return self.groups[group_key]

    def set_group_param(self, group_key, param_key, value):
        if not group_key in self.groups:
            self.groups[group_key] = dict()
        group_dict=self.groups[group_key]
        group_dict[param_key]=value
    
    def get_group_param(self, group_key, param_key):
        group_dict=self.groups[group_key]
        return group_dict[param_key]

class GamessAtoms(Atoms):
    # Shoenflies space group
    symmetry = None

    def copy(self):
        import copy
        new_atoms = GamessAtoms(Atoms.copy(self))
        new_atoms.symmetry= copy.deepcopy(self.symmetry)
        return new_atoms

class GamessGridCalc(CalculatorBase):
    """Calculator class that interfaces with GAMESS-US using
    a database. The jobs in the database are executed on a grid.
    """
    def __init__(self, mydb):
        super(GamessGridCalc, self).__init__()
        self.db = mydb
        self.parsed_dat = ParseGamessDat()
        self.parsed_out = ParseGamessOut()
    
    def generate(self, atoms, params, a_task, application_to_run, selected_resource,  cores, memory, walltime):
        """We run GAMESS in here."""
        from markstools.io.gamess import WriteGamessInp
        #Generate the input file
        f_inp = StringIO.StringIO()
        f_inp.name = params.title + '.inp'
        writer = WriteGamessInp()
        writer.write(f_inp, atoms, params)
        f_inp.seek(0)
        a_job = JobInterface(self.db).create(params.title,  self.__class__.__name__, [f_inp], application_to_run, 
                           selected_resource,  cores, memory, walltime)
        f_inp.close()
        # Convert the python dictionary to one that uses the couchdb schema
        a_job.user_data_dict = params.job_user_data_dict
        a_task.add_child(a_job)
        a_task.store()
        markstools.log.info('Generated Job %s and it was added to Task %s'%(a_job.id, a_task.id))
        return a_job
    
    def get_file(self, a_job, type):
        return a_job.get_attachment(type)

    def calculate(self, a_thing):
        """We run GAMESS in here."""
        if isinstance(a_thing, TaskInterface):
            job_list = tuple(a_thing.children)
        elif isinstance(a_thing, JobInterface):
            job_list = tuple([a_thing])
        elif isinstance(a_thing, tuple) or isinstance(a_thing, list):
            job_list = a_thing
        else:
            assert False, 'Can not handle objects of type: %s'%(type(a_thing))
        for a_job in job_list:
            if a_job.status == RUN_STATES.HOLD:
                a_job.status = RUN_STATES.READY
                markstools.log.info('Job %s was in state %s and is now in state %s'%(a_job.id, RUN_STATES.HOLD, RUN_STATES.READY))
                a_job.store()
        return job_list

    def parse(self, a_job, force_a_reparse=False):
        """Rather than running GAMESS, we just read
        and parse the results already in the database."""
        from markstools.io.gamess import ReadGamessInp
        parser = None
        if not force_a_reparse:            
            parser = a_job.parser 
            if parser != self.__class__.__name__:
                raise MyTypeError('Can not reparse, parsed results are from %s, expecting %s'%(parser, self.__class__.__name_))
            a_result = a_job.parsed
            if a_result:
                if not isinstance(a_result, GamessResult):
                    raise MyTypeError('Unpickled parse results are of type %s, expecting %s'%(type(a_result)), GamessResult)
                markstools.log.info('Using previously parsed results for Job %s'%(a_job.id))
        if a_result is None:
            markstools.log.info('Starting to parse Job %s results'%(a_job.id))
            markstools.log.debug('Starting to parse Run %s results'%(a_job.run.id))
            try:
                f_inp = a_job.get_attachment('inp')
                reader = ReadGamessInp(f_inp)
                f_dat = a_job.get_attachment('dat')
                self.parsed_dat.parse_file(f_dat)
                f_stdout = a_job.get_attachment('stdout')
                self.parsed_out.parse_file(f_stdout)
            finally:
                f_inp.close()
                f_dat.close()
                f_stdout.close()
            a_result = GamessResult(reader.atoms, reader.params, self.parsed_dat.group, self.parsed_out.group)
            self._save_parsed_result(a_job, a_result)
        return a_result

    def _save_parsed_result(self, a_job, a_result):
        queryable = a_result._get_queryable()
        a_job.result_data_dict.update(queryable)
        a_job.parsed = a_result
        a_job.store()

class GamessLocalCalc(CalculatorBase):
    EXT_INPUT_FILE = 'inp'
    EXT_STDOUT_FILE = 'stdout'
    EXT_STDERR_FILE = 'stderr'
    EXT_DAT_FILE = 'dat'
    EXT_LIST = (EXT_INPUT_FILE, EXT_STDOUT_FILE, EXT_STDERR_FILE, EXT_DAT_FILE)
    
    CMD_GAMESS = 'rungms'
    
    def __init__(self, author, f_dir='/tmp'):
        super(GamessGridCalc, self).__init__(author)
        self.f_dir = f_dir
    
    def preexisting_result(self, f_name):
        """Rather than running GAMESS, we just read
        and parse the results already on the file system."""
        from markstools.io.gamess import ReadGamessInp
        f_dict = self.get_files(f_name)
        new_reader = ReadGamessInp(f_dict['inp'])
        result = GamessResult(new_reader.atoms, new_reader.params, self)        
        result.a_job = f_name
        return result
        
    def get_file(self, f_name, type):
        assert type in EXT_LIST, 'Invalid extension type: %s.'%(type)
        return open('%s/%s.%s'%(self.f_dir, f_name, type))
#        f_dict = dict()
#        f_dict[self.EXT_INPUT_FILE] = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_INPUT_FILE))
#        f_dict[self.EXT_STDOUT_FILE] = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_STDOUT_FILE))
#        f_dict[self.EXT_STDERR_FILE] = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_STDERR_FILE))
#        f_dict[self.EXT_DAT_FILE] = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_DAT_FILE))
     
    def generate(self, atoms, params):
        writer = WriteGamessInp()
        f_name = GamessLocalCalc.generate_new_docid()
        f_inp = open('%s/%s.%s'%(self.f_dir, f_name, self.EXT_INPUT_FILE), 'w')
        writer.write(f_inp, atoms, params)
        f_inp.close()
        return f_inp.name
        
    def calculate(self, f_name):
        import os
        from markstools.io.gamess import WriteGamessInp
        result = GamessResult(atoms, params, self)
        cmd = 'cd %s; %s %s.%s 1> %s.%s 2> %s.%s'%(self.f_dir, self.CMD_GAMESS, 
                                          f_name, self.EXT_INPUT_FILE, f_name, self.EXT_STDOUT_FILE, 
                                          f_name, self.EXT_STDERR_FILE)
        return_code = os.system(cmd)
        assert return_code == 0, 'Error runing GAMESS. Check %s/%s.%s'%(self.f_dir, f_name, self.EXT_STDOUT_FILE)
        result.a_job = f_name
        return result
     
    def wait(self, job_id, status='DONE', timeout=60, check_freq=10):
        """We block on the calculate method, so we know the job is done."""
        return True
    
    def save_queryable(self, job_id, key, value):
        print 'key %s'%key
        print value
        pass
    
    @staticmethod
    def generate_new_docid():
        from uuid import uuid4
        return uuid4().hex
