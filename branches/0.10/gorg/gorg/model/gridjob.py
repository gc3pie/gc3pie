from couchdb.mapping import *
from baserole import *
from couchdb import client as client
from datetime import datetime
from gorg.lib.utils import generate_temp_dir, write_to_file

from gorg.lib.exceptions import *
import os
import gorg
from gc3utils import Application,  Job
import time

from gorg.lib import state
from gorg.gridscheduler import STATES


class GridjobModel(BasenodeModel):
    SUB_TYPE = 'GridjobModel'
    VIEW_PREFIX = 'GridjobModel'
    sub_type = TextField(default=SUB_TYPE)    
    parser = TextField()
    run_id = TextField()
    

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
                    for job_id in doc['children_ids']:
                        yield job_id, doc

#    @ViewField.define('GridjobModel')    
#    def view_children(doc, ):
#        if 'base_type' in doc:
#            if doc['base_type'] == 'BaseroleModel':
#                if doc['sub_type'] == 'GridjobModel':
#                    if doc['children']:
#                        for job_id in doc['children']:
#                            yield job_id, doc
#                    else:
#                        yield [],doc

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
                    yield doc['author'], {'_id':doc['children_ids']}
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)

    def create(self, title,  parser_name, files_to_run, application_tag='gamess', 
                        requested_resource='ocikbpra',  requested_cores=2, requested_memory=1, requested_walltime=None):
        self.author = self.db.username
        self.title = title
        a_run = GridrunModel(self.db).create(files_to_run, self, 
                                                                    application_tag, requested_resource, 
                                                                    requested_cores, requested_memory, 
                                                                    requested_walltime)
        self.run_id = a_run.id
        self.parser = parser_name
        self.store()
        gorg.log.debug('Job %s has been created'%(self.id))        
        return self 
#TODO: When this is called it always returns a new run obj.
    @property
    def run(self):
        return GridrunModel().load(self.db, self.run_id)

    def load(self, db=None, id=None):
        self = super(GridjobModel, self).load(db, id)
        view = GridrunModel.view_job(self.db, key = self.id)
        if len(view) == 0:
            DocumentError('Job %s does not have a run associated with it.'%(id))
        if len(view) > 1:
            DocumentError('Job %s has more than one run associated with it.'%(id))
        return self
    
    def task():
        def fget(self):
            from gridtask import GridtaskModel
            a_task = None
            view = GridtaskModel.view_children(self.db, key = self.id)
            if len(view) != 0:
                task_id = view[self.id].rows[0].id
                a_task = GridtaskModel().load(self.db, task_id)
            return a_task
        return locals()
    task = property(**task())

    def status():
        """Here we need to check to see what kind of status we have. If more than one job is pointing to the same
        run, then changing its status might mess up the other jobs pointing to it."""
        def fget(self):
            return self.run.status
        def fset(self, status):
            temp = self.run
            temp.status = status
            temp.store()
        return locals()
    status = property(**status())

    def attachments():
        def fget(self):
            f_dict = super(GridjobModel, self).attachments
            f_dict.update(self.run.attachments)
            return f_dict
        return locals()
    attachments = property(**attachments())
    
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
            self.status = STATES.COMPLETED
            self.put_attachment(pkl,'parsed')
        return locals()
    parsed = property(**parsed())

def _reduce_author_status(keys, values, rereduce):
    return sum(values)
        
class GridrunModel(BaseroleModel):
    VIEW_PREFIX = 'GridrunModel'
    SUB_TYPE = 'GridrunModel'
    
    # Attributes to store in the database
    sub_type = TextField(default=SUB_TYPE)
    # This holds the files we wish to run as well as their hashes
    files_to_run = DictField()
    
    raw_status = DictField(default = STATES.HOLD)
    raw_application = DictField()
    raw_job = DictField()
    gsub_message = TextField()
    
    def __init__(self, *args):
        super(GridrunModel, self).__init__(*args)

    def application():        
        def fget(self):
            if not isinstance(self.raw_application, Application.Application) and self.raw_application:
                self.raw_application = Application.Application(self.raw_application)
            return self.raw_application
        def fset(self, application):
            self.raw_application = application
        return locals()
    application = property(**application())
    
    def job():        
        def fget(self):
            if not isinstance(self.raw_job, Job.Job) and self.raw_job:
                job = Job.Job(self.raw_job)
                self.raw_job = job
            return self.raw_job
        def fset(self, job):
            self.raw_job = job
        return locals()
    job = property(**job())

    @ViewField.define('GridrunModel')
    def view_all(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':
                    yield doc['_id'], doc

    @ViewField.define('GridrunModel')
    def view_author(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':
                    yield doc['author'], doc

    @ViewField.define('GridrunModel', wrapper=GridjobModel, include_docs=True)
    def view_job(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':
                    for owner in doc['children_ids']:
                        yield owner, {'_id':owner}

    @ViewField.define('GridrunModel')
    def view_children(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':
                    for job_id in doc['children_ids']:
                        yield job_id, doc

    @ViewField.define('GridrunModel', wrapper=state.State)
    def view_job_status(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':
                    for owner in doc['children_ids']:
                        yield owner, doc['raw_status']
        
    @ViewField.define('GridrunModel')
    def view_status(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':      
                if doc['sub_type'] == 'GridrunModel':          
                    yield doc['raw_status'], doc

    @ViewField.define('GridrunModel')
    def view_hash(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':          
                    yield doc['files_to_run'].values(), doc
    
    @ViewField.define('GridrunModel', reduce_fun=_reduce_author_status, wrapper=None, group=True)
    def view_author_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                if doc['sub_type'] == 'GridrunModel':          
                    yield (doc['author'], doc['raw_status']), 1
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)

    def create(self, files_to_run, a_job, application_tag, 
                        requested_resource,  requested_cores, 
                        requested_memory, requested_walltime):       
        # Generate the input file hashes
        for a_file in files_to_run:
            base_name = os.path.basename(a_file.name)
            self.files_to_run[base_name] = self.md5_for_file(a_file)
        # We now need to build a new run record
        self.author = self.db.username
        self.add_child(a_job)
        self.application = Application.Application(application_tag = application_tag,
                                                                       requested_resource = requested_resource,  
                                                                       requested_memory = requested_memory, 
                                                                       requested_cores = requested_cores, 
                                                                       requested_walltime = requested_walltime, 
                                                                       job_local_dir = '/tmp', 
                                                                       inputs = [], 
                                                                       application_arguments = None)
        self = self._commit_new(a_job, files_to_run)
        return self

    def _commit_new(self, a_job, files_to_run):
        if len(files_to_run) == 0:
            raise DocumentError('No files associated with run %s'%self.id)
        # The run has never been store in the database
        # Can we use a run that is already in the database?
        a_run_already_in_db = self._check_for_previous_run()
        if a_run_already_in_db:
            self.add_child(a_job)
            self.store()
        else:
            # We need to attach the input files to the run,
            # to do that we have to first store the run in the db
            self.store()
            for a_file in files_to_run:
                base_name = os.path.basename(a_file.name)
                self.put_attachment(a_file, base_name)
            self.load(self.db)
        return self

    def _check_for_previous_run(self):
        a_view = GridrunModel.view_hash(self.db, key=self.files_to_run.values())
        result = None
        for a_run in a_view:
            if a_run.status == STATES.COMPLETED:
                result = a_run
        return result
    
    def task():
        def fget(self):
            task_list = list()
            job_list = self.children
            for a_job in job_list:
                task_list.append(a_job.task)
            return tuple(task_list)
        return locals()
    task = property(**task())
    
    def status():
        def fget(self):
            return state.State(**self.raw_status)
        def fset(self, status):
            if isinstance(status, tuple):
                value = status[0]
                key = status[1]
            else:
                value = status
                key = 'I have no key'            

            if self.status.locked is not None:
                if self.status.locked == key:
                    self.raw_status = value
                else:
                    raise DocumentError('Run %s is locked, and you provided the wrong key.'%(self.id))
            else:
                self.raw_status = value
        return locals()
    status = property(**status())

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
        return u'%s'%md5.hexdigest()
