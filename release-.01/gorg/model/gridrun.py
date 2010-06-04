import gorg
from couchdb.mapping import *
from couchdb import client as client
from gorg.model.gridjob import GridjobModel

import os
from datetime import datetime
import copy
import tempfile
from gorg.lib.utils import generate_new_docid, generate_tempfile_prefix

import gc3utils

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
    
    status = NumberField(default = States.HOLD)
    run_params = DictField()
    gjob = DictField()
    gsub_message = TextField()
    
    def __init__(self, *args):
        super(GridrunModel, self).__init__(*args)
        self.subtype = self.SUB_TYPE
        self._hold_file_pointers = list()
        
#    def __setattr__(self, name, value):
#        if name == 'status':
#            assert value in PossibleStates.values(), 'Invalid status. \
#            Only the following are valid, %s'%(' ,'.join(PossibleStates.values()))
#        super(GridrunModel, self).__setattr__(name, value)
    
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

    def create(self, db, files_to_run, a_job, application_tag='gamess', requested_resource='ocikbpra',  requested_memory=2, requested_cores=1, requested_walltime=-1):       
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
        self.run_params= gc3utils.Application.Application(application_tag,
                                                                                       requested_resource,  
                                                                                       requested_memory, 
                                                                                       requested_cores, 
                                                                                       requested_walltime, 
                                                                                       job_local_dir='/tmp', 
                                                                                       input_file_name=None)
        self.id = generate_new_docid()
        self = self._commit_new(db, a_job, files_to_run)
        log.debug('Run %s has been created'%(self.id))
        return self
    
    def _commit_new(self, db, a_job, files_to_run):
        if len(files_to_run) > 0:
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
        a_view = GridrunModel.view_by_hash(db, key=self.files_to_run.values())
        result = None
        for a_run in a_view:
            if a_run.status == States.COMPLETED:
                result = a_run
                log.debug('Input file matches run %s that was already in the database'%(self.id))
        return result
    
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
        tempdir,  prefix = generate_tempfile_prefix()
        f_attachments = dict()
        if not f_names and '_attachments' in self:
            f_names = self['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self.get_attachment(db, attachment)
            try:
                myfile = open( '%s/%s_%s'%(tempdir, prefix, attachment), 'wb')
                myfile.write(attached_data)
                f_attachments[attachment]=open(myfile.name, 'rb') 
            except IOError:
                myfile.close()
                raise
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

    @ViewField.define('GridrunModel', wrapper=GridjobModel)
    def view_job(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                for owner in doc['owned_by']:
                    yield owner, {'_id':owner}

#    @ViewField.define('GridrunModel', wrapper=GridjobModel)
#    def view_job_status(doc):    
#        if 'base_type' in doc:
#            if doc['base_type'] == 'GridrunModel':
#                for owner in doc['owned_by']:
#                    yield doc['status'], {'_id':owner}

    @ViewField.define('GridrunModel', wrapper=None)
    def view_job_status(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                for owner in doc['owned_by']:
                    yield owner, doc['status']
    
    @ViewField.define('GridrunModel')
    def view_hash(doc):    
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield doc['files_to_run'].values(), doc
    
    @staticmethod
    def _reduce_author_status(keys, values, rereduce):
        return sum(values)
    
    @ViewField.define('GridrunModel', reduce_fun=_reduce_author_status, wrapper=None,  defaults={'group':True})
    def view_author_status(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'GridrunModel':
                yield (doc['author'], doc['status']), 1
    
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
        md5 = hashlib.md5()
        while True:
            data = f.read(block_size)
            data = reg_remove_white.sub('', data)
            if not data:
                break
            md5.update(data)
        f.seek(0)
        #TODO: When mike runs this, it doesn't work
        return u'%s'%(generate_new_docid())
        #return u'%s'%md5.digest()
    
