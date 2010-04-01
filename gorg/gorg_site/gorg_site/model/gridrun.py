from couchdb import schema as sch
from couchdb import client as client

import os
import time
import copy

map_func_all = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            yield doc['_id'], doc
    '''
map_func_author = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            yield doc['author'], doc
    '''

map_func_by_job = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            for owner in doc['owned_by']:
                yield owner, doc
    '''
map_func_by_job_status = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            for owner in doc['owned_by']:
                yield owner, doc['status']
    '''

map_func_by_status = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            yield doc['status'], doc
    '''

map_func_hash = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            yield doc['files_to_run'].values(), doc
    '''

map_func_author_status = '''
def mapfun(doc):
    if 'base_type' in doc:
        if doc['base_type'] == 'GridrunModel':
            yield (doc['author'], doc['status']), 1
    '''
reduce_func_author_status ='''
def reducefun(keys, values, rereduce):
    return sum(values)
    '''

class GridrunModel(sch.Document):
    POSSIBLE_STATUS = dict(HOLD='HOLD', READY='READY', WAITING='WAITING',RUNNING='RUNNING', 
                                           RETRIEVING='RETRIEVING',FINISHED='FINISHED', DONE='DONE',
                                            ERROR='ERROR')
    VIEW_PREFIX = 'GridrunModel'
    SUB_TYPE = 'GridrunModel'
    
    # Attributes to store in the database
    author = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    base_type = sch.TextField(default='GridrunModel')
    sub_type = sch.TextField(default='GridrunModel')

    owned_by = sch.ListField(sch.TextField())
    # This holds the files we wish to run as well as their hashes
    files_to_run = sch.DictField()
    
    status = sch.TextField(default = 'HOLD')
    run_params = sch.DictField()
    gsub_message = sch.TextField()
    gsub_unique_token = sch.TextField()
    
    def __init__(self, *args):
        super(GridrunModel, self).__init__(*args)
        self.subtype = self.SUB_TYPE
        self._hold_file_pointers = list()
        
    def __setattr__(self, name, value):
        if name == 'status':
            assert value in self.POSSIBLE_STATUS.values(), 'Invalid status. \
            Only the following are valid, %s'%(' ,'.join(self.POSSIBLE_STATUS.values()))
        super(GridrunModel, self).__setattr__(name, value)
    
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

    def create(self, db, files_to_run, a_job, application_to_run='gamess', selected_resource='ocikbpra',  cores=2, memory=1, walltime=-1):       
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
        self.run_params=dict(application_to_run=application_to_run, \
                              selected_resource=selected_resource,  cores=cores, memory=memory, walltime=walltime)
        self.id = GridrunModel.generate_new_docid()
        self = self._commit_new(db, a_job, files_to_run)
        return self
    
    def _commit_new(self, db, a_job, files_to_run):
        assert len(files_to_run) > 0,  'No files associated with run %s'%self.id
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
        a_view = GridrunModel.view_by_hash(db, key=self.files_to_run.values())
        if len(a_view) == 0:
            return None
        for a_run in a_view:
            if a_run.status == 'DONE':
                return a_run
        return None
    
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
    
    def attachments_to_files(self, db, f_names=None):
        '''We often want to save all of the attachments on the local computer.'''
        import tempfile
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        f_attachments = dict()
        if f_names is None:
            f_names = self['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self.get_attachment(db, attachment)
            myfile = open( '%s.%s'%(temp_file.name, attachment), 'wb')
            myfile.write(attached_data)
            myfile.close()
            f_attachments[attachment]=open(myfile.name, 'rb') 
        return f_attachments
    
    @staticmethod
    def view_by_job(db, **options):
        return GridrunModel.my_view(db, 'by_job', **options)
    
    @staticmethod
    def view_by_hash(db, **options):
        return GridrunModel.my_view(db, 'by_hash', **options)
    
    @staticmethod
    def view_by_status(db, **options):
            return GridrunModel.my_view(db, 'by_status', **options)
    
    @staticmethod
    def view_all(db, **options):
        return GridrunModel.my_view(db, 'all', **options)
    
    @staticmethod
    def view_author_status(db, **options):
        options['group']=True
        return GridrunModel.my_view(db,'by_author_status', **options)
    
    @staticmethod
    def view_by_job_status(db, **options):
        return GridrunModel.my_view(db,'by_job_status', **options)

    @classmethod
    def my_view(cls, db, viewname, **options):
        from couchdb.design import ViewDefinition
        viewnames = cls.sync_views(db, only_names=True)
        assert viewname in viewnames, 'View not in view name list.'
        a_view = super(cls, cls).view(db, '%s/%s'%(cls.VIEW_PREFIX, viewname), **options)
        #a_view=.view(db, 'all/%s'%viewname, **options)
        return a_view
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        if only_names:
            viewnames=('all', 'by_author', 'by_author_status', 'by_hash', 'by_job', 'by_status', 'by_job_status')
            return viewnames
        else:
            all = ViewDefinition(cls.VIEW_PREFIX, 'all', map_func_all, wrapper=cls, language='python')
            by_author = ViewDefinition(cls.VIEW_PREFIX, 'by_author', map_func_author, wrapper=cls, language='python')
            by_author_status = ViewDefinition(cls.VIEW_PREFIX, 'by_author_status', map_func_author_status, wrapper=cls, \
                                                  reduce_fun=reduce_func_author_status, language='python')
            by_hash = ViewDefinition(cls.VIEW_PREFIX, 'by_hash', map_func_hash, wrapper=cls, language='python')
            by_job = ViewDefinition(cls.VIEW_PREFIX, 'by_job', map_func_by_job, \
                                             wrapper=cls, language='python') 
            by_status = ViewDefinition(cls.VIEW_PREFIX, 'by_status', map_func_by_status, \
                                             wrapper=cls, language='python') 
            by_job_status = ViewDefinition(cls.VIEW_PREFIX, 'by_job_status', map_func_by_job_status, \
                                             language='python') 

            views=[all, by_author, by_author_status, by_hash, by_job, by_status, by_job_status]
            ViewDefinition.sync_many( db,  views)
        return views
    
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
        md5 = hashlib.md5()
        while True:
            data = f.read(block_size)
            data = reg_remove_white.sub('', data)
            if not data:
                break
            md5.update(data)
        f.seek(0)
        return u'%s'%md5.digest()
    
    @staticmethod
    def generate_new_docid():
        from uuid import uuid4
        return uuid4().hex
