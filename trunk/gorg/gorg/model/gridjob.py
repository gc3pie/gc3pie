from couchdb.mapping import *
from baserole import BaseroleModel, BaseroleInterface
from couchdb import client as client
from datetime import datetime
from gorg.lib.utils import generate_new_docid, generate_temp_dir, write_to_file

from gorg.lib import state

from gorg.lib.exceptions import *
import os
import gorg
from gc3utils import Application,  Job
import time

STATE_HOLD = state.State.create('HOLD', 'HOLD desc')

class GridjobModel(BaseroleModel):
    SUB_TYPE = 'GridjobModel'
    VIEW_PREFIX = 'GridjobModel'
    sub_type = TextField(default=SUB_TYPE)    
    parser_name = TextField()
    
    def __init__(self, *args):
        super(GridjobModel, self).__init__(*args)
        self._run_id = None
    
    def commit(self, db):
        self.store(db)
    
    def refresh(self, db):
        self = GridjobModel.load_job(db, self.id)
        return self
    
    @ViewField.define('GridjobModel')    
    def view_all(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridjobModel':
                    yield doc['_id'],doc

    @ViewField.define('GridjobModel')    
    def view_author(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridjobModel':
                    yield doc['author'],doc

    @ViewField.define('GridjobModel')    
    def view_children(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridjobModel':
                    if doc['children']:
                        for job_id in doc['children']:
                            yield job_id, doc
                    else:
                        yield [],doc

    @ViewField.define('GridjobModel', include_docs=True)    
    def view_author_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                for job_id in doc['owned_by']:
                    yield (doc['author'], doc['raw_status']), {'_id':job_id}
    

    @ViewField.define('GridjobModel', include_docs=True)   
    def view_by_task_author_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridtaskModel':
                    yield doc['author'], {'_id':doc['children']}

    @staticmethod
    def load_job(db, job_id):
        a_job = GridjobModel.load(db, job_id)
        view = GridrunModel.view_job(db, key=job_id)
        if len(view) == 0:
            DocumentError('Job %s does not have a run associated with it.'%(a_job.id))
        if len(view) > 1:
            DocumentError('Job %s has more than one run associated with it.'%(a_job.id))
        a_job._run_id = view.rows[0].id
        return a_job
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)

class JobInterface(BaseroleInterface):
    
    def create(self, title,  parser_name, files_to_run, application_tag='gamess', 
                        requested_resource='ocikbpra',  requested_cores=2, requested_memory=1, requested_walltime=-1):
        self.controlled = GridjobModel().create(self.db.username, title)
        gorg.log.debug('Job %s has been created'%(self.id))        
        a_run = GridrunModel()
        a_run = a_run.create( self.db, files_to_run, self.controlled, 
                                            application_tag, requested_resource, 
                                            requested_cores, requested_memory, 
                                            requested_walltime)
        self.controlled._run_id = a_run.id
        self.parser = parser_name
        self.controlled.commit(self.db)
        return self    
    
    def load(self, id):
        self.controlled=GridjobModel.load_job(self.db, id)
        return self
    
    def add_parent(self, parent):
        parent.add_child(self)
    
    def task():
        def fget(self):
            from gridtask import GridtaskModel, TaskInterface
            self.controlled.refresh(self.db)
            view = GridtaskModel.view_children(self.db)
            task_id = view[self.controlled.id].rows[0].id
            a_task=TaskInterface(self.db).load(task_id)
            return a_task
        return locals()
    task = property(**task())

    def parents():            
        def fget(self):
            job_list = list()
            view = GridjobModel.view_children(self.db)
            for a_parent in view[self.controlled.id]:
                job_list.append(a_parent)
            return tuple(job_list)
        return locals()
    parents = property(**parents())
    
    def run():
        def fget(self):
            return GridrunModel.load(self.db, self.controlled._run_id)
        return locals()
    run = property(**run())

    def status():
        def fget(self):
            a_run = self.run
            return a_run.status
        def fset(self, status):
            a_run = self.run
            a_run.status = status
            a_run.commit(self.db)
        return locals()
    status = property(**status())

    def wait(self, timeout=60, check_freq=10):
        from time import sleep
        if timeout == 'INFINITE':
            timeout = sys.maxint
        if check_freq > timeout:
            check_freq = timeout
        starting_time = time.time()
        while True:
            if starting_time + timeout < time.time() or self.status.terminal:
                break
            else:
                time.sleep(check_freq)
        if self.terminal:
            # We did not timeout 
            return True
        else:
            # Timed out
            return False

    def attachments():
        def fget(self):
            f_dict = super(JobInterface, self).attachments
            f_dict.update(self.run.attachments_to_files(self.db))
            return f_dict
        return locals()
    attachments = property(**attachments())

    def run_id():        
        def fget(self):
            return self.controlled._run_id
        return locals()
    run_id = property(**run_id())
    
    def application():        
        def fget(self):
            return self.run.raw_application
        return locals()
    application = property(**application())
    
    def job():        
        def fget(self):
            return self.controlled
        def fset(self, a_job):
            self.controlled = a_job
        return locals()
    job = property(**job())

    def parser():        
        def fget(self):
            return self.controlled.parser_name
        def fset(self, parser_name):
            self.controlled.parser_name = parser_name
            self.controlled.commit(self.db)
        return locals()
    parser = property(**parser())

    def parsed():
        def fget(self):
            import cPickle as pickle
            f_parsed = self.get_attachment('parsed')
            if f_parsed:
                parsed = pickle.load(f_parsed)
                f_parsed.close()
                return parsed
        def fset(self, parsed):
            import cPickle as pickle
            import cStringIO as StringIO
            pkl = StringIO.StringIO(pickle.dumps(parsed))
            self.put_attachment(pkl,'parsed')
        return locals()
    parsed = property(**parsed())


def _reduce_author_status(keys, values, rereduce):
    return sum(values)
        
class GridrunModel(Document):
    VIEW_PREFIX = 'GridrunModel'
    SUB_TYPE = 'GridrunModel'
    
    # Attributes to store in the database
    author = TextField()
    dat = DateTimeField(default=datetime.today())
    base_type = TextField(default='GridrunModel')
    sub_type = TextField(default='GridrunModel')

    owned_by = ListField(TextField())
    # This holds the files we wish to run as well as their hashes
    files_to_run = DictField()
    
    raw_status = DictField(default = STATE_HOLD)
    raw_application = DictField()
    raw_job = DictField()
    gsub_message = TextField()
    
    def __init__(self, *args):
        super(GridrunModel, self).__init__(*args)
        self.subtype = self.SUB_TYPE
        self._hold_file_pointers = list()
    
    def get_jobs(self, db):
        job_list = list()
        for a_job_id in self.owned_by:
            job_list.append(GridjobModel.load_job(db, a_job_id))
        return tuple(job_list)
    
    def get_tasks(self, db):
        task_list = list()
        job_list = self.get_jobs(db)
        for a_job in job_list:
            task_list.append(a_job.get_task(db))
        return tuple(task_list)

    def create(self, db, files_to_run, a_job, application_tag, 
                        requested_resource,  requested_cores, 
                        requested_memory, requested_walltime):       
        if not isinstance(files_to_run, list) and not isinstance(files_to_run, tuple):
            files_to_run = [files_to_run]
        # Generate the input file hashes
        hash_dict = dict()
        for a_file in files_to_run:
            base_name = os.path.basename(a_file.name)
            self.files_to_run[base_name] = GridrunModel.md5_for_file(a_file)
        # We now need to build a new run record
        self.author = a_job.author
        self.owned_by.append(a_job.id)
        self.application = Application.Application(application_tag = application_tag,
                                                                       requested_resource = requested_resource,  
                                                                       requested_memory = requested_memory, 
                                                                       requested_cores = requested_cores, 
                                                                       requested_walltime = requested_walltime, 
                                                                       job_local_dir = '/tmp', 
                                                                       inputs = [], 
                                                                       application_arguments = None)
        self.id = generate_new_docid()
        self = self._commit_new(db, a_job, files_to_run)
        return self
    

    def _commit_new(self, db, a_job, files_to_run):
        if len(files_to_run) == 0:
            raise DocumentError('No files associated with run %s'%self.id)
        # The run has never been store in the database
        # Can we use a run that is already in the database?
        a_run_already_in_db = self._check_for_previous_run(db)
        if a_run_already_in_db:
            self = a_run_already_in_db
            if a_job.id not in self.owned_by:
                self.owned_by.append(a_job.id)
                self.store(db)
        else:
            # We need to attach the input files to the run,
            # to do that we have to first store the run in the db
            self.store(db)
            for a_file in files_to_run:
                base_name = os.path.basename(a_file.name)
                self.put_attachment(db, a_file, base_name)
            self = self.refresh(db)
        return self

    def _check_for_previous_run(self, db):
        from gorg.gridjobscheduler import STATE_COMPLETED
        a_view = GridrunModel.view_hash(db, key=self.files_to_run.values())
        result = None
        for a_run in a_view:
            if a_run.status == STATE_COMPLETED:
                result = a_run
        return result
    
    def status():        
        def fget(self):
            return state.State(**self.raw_status)
        def fset(self, state):
            self.raw_status = state
        return locals()
    status = property(**status())

    def application():        
        def fget(self):
            if not isinstance(self.raw_application, Application.Application):
                self.raw_application = Application.Application(self.raw_application)
            return self.raw_application
        def fset(self, application):
            self.raw_application = application
        return locals()
    application = property(**application())
    
    def job():        
        def fget(self):
            if not isinstance(self.raw_job, Job.Job):
                job = Job.Job()
                job.update(self.raw_job)
                self.raw_job = job
            return self.raw_job
        def fset(self, job):
            self.raw_job = job
        return locals()
    job = property(**job())

    def commit(self, db):
        self.store(db)
    
    def refresh(self, db):
        self = GridrunModel.load(db, self.id)
        return self
        
    def get_attachment(self, db, filename, when_not_found=None):
        return db.get_attachment(self, filename, when_not_found)
    
    def put_attachment(self, db, content, filename, content_type='text/plain'):
        content.seek(0)
        db.put_attachment(self, content, filename, content_type)
        return self.refresh(db)
            
    def delete_attachment(self, db, filename):
        return db.delete_attachment(self, filename)
    
    def attachments_to_files(self, db, f_names=[]):
        '''We often want to save all of the attachments on the local computer.'''
        tempdir = generate_temp_dir(self.id)
        f_attachments = dict()
        if not f_names and '_attachments' in self:
            f_names = self['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self.get_attachment(db, attachment)
            f_attachments[attachment] = write_to_file(tempdir, attachment, attached_data)
        return f_attachments

    @ViewField.define('GridrunModel')
    def view_all(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield doc['_id'], doc

    @ViewField.define('GridrunModel')
    def view_author(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield doc['author'], doc

    @ViewField.define('GridrunModel', wrapper=GridjobModel, include_docs=True)
    def view_job(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                for owner in doc['owned_by']:
                    yield owner, {'_id':owner}

    @ViewField.define('GridrunModel', wrapper=state.State)
    def view_job_status(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                for owner in doc['owned_by']:
                    yield owner, doc['raw_status']
    
    @ViewField.define('GridrunModel')
    def view_status(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':                
                yield str(doc['raw_status']), doc

    @ViewField.define('GridrunModel')
    def view_hash(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield doc['files_to_run'].values(), doc
    
    @ViewField.define('GridrunModel', reduce_fun=_reduce_author_status, wrapper=None, group=True)
    def view_author_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield (doc['author'], doc['raw_status']), 1
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)
        
    @staticmethod
    def md5_for_file(f, block_size=2**20):
        """This function takes a file like object and feeds it to
        the md5 hash function a block_size at a time. That
        allows large files to be hashed without requiring the entire 
        file to be in memory."""
        import re
        import hashlib
        # Remove all the white space from a string
        #re.sub(r'\s', '', myString)
        reg_remove_white = re.compile(r'\s')
        md5 = hashlib.sha224()
        while True:
            data = f.read(block_size)
            data = reg_remove_white.sub('', data)
            if not data:
                break
            md5.update(data)
        f.seek(0)
        #TODO: When mike runs this, it doesn't work
        #return u'%s'%(generate_new_docid())
        return u'%s'%md5.digest()
