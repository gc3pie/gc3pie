from couchdb.mapping import *
from couchdb import client as client
from datetime import datetime
from gorg.lib.utils import generate_new_docid, generate_temp_dir, write_to_file
import logging
'''When you need to query for a key like this ('sad','saq') do this:
self.view(db,'who_task_owns',startkey=[self.id],endkey=[self.id,{}])
when you want to match a key ('sad','sad') do this:
a_job.view(db,'by_task_ownership',keys=[[a_task.id,a_task.id]])
'''
#GridrunModel.my_view(db, 'by_job_and_status', startkey=[job_id],endkey=[job_id,status])

_log = logging.getLogger('gorg')

class BaseroleModel(Document):
    VIEW_PREFIX = 'BaseroleModel'
    
    author = TextField()
    title = TextField()
    dat = DateTimeField(default=datetime.today())
    base_type = TextField(default='BaseroleModel')
    sub_type = TextField()
    user_data_dict = DictField()
    result_data_dict = DictField()
    
    children = ListField(TextField())
    
    # This works like a dictionary, but allows you to go a_job.test.application_to_run='a app' as well as a_job.test['application_to_run']='a app'
    #test=DictField(Schema.build(application_to_run=TextField(default='gamess')))
    
    def create(self, author, title):
        self.id = generate_new_docid()
        self.author = author
        self.title = title
        return self

    def commit(self, db):
        raise NotImplementedError('Must implement a commit(self, db) method')
    
    @ViewField.define('BaseroleModel')
    def view_all(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                yield doc['_id'], doc
    
    @ViewField.define('BaseroleModel')
    def view_author(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                yield doc['author'], doc
    
    @ViewField.define('BaseroleModel')
    def view_title(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                yield doc['title'], doc

    @ViewField.define('BaseroleModel')
    def view_children(doc):
        if 'base_type' in doc:
            if doc['base_type'] == 'BaseroleModel':
                    for job_id in doc['children']:
                        yield job_id, doc
    
    def __copy__(self):
        import copy
        '''We want to make a copy of this job, so we can use the same setting to 
        create another job.'''
        new_record=copy.copy(self)
        del new_record['_id']
        del new_record['_rev']
        return new_record
    
    @classmethod
    def sync_views(cls, db,  only_names=False):
        from couchdb.design import ViewDefinition
        definition_list = list()
        for key, value in cls.__dict__.items():
            if isinstance(value, ViewField):
                definition_list.append(eval('cls.%s'%(key)))
        ViewDefinition.sync_many( db,  definition_list)

class ObservableDict( object ):
    def __init__( self, interface, a_dict ):
        self.interface=interface
        self.a_dict = a_dict
    def __setitem__( self, key, value ):
        self.a_dict[key]=value
        self.interface.controlled.user_data_dict[key]=value
        self.interface.controlled.commit(self.interface.db)
    def __repr__(self):
        return self.a_dict.__repr__()
    def __str__(self):
        return self.a_dict.__str__()
    def __delitem__(self, key):
        result = self.a_dict.__delitem__(key)
        self.interface.controlled.commit(self.interface.db)
        return result
    def __getitem__(self, key):
        return self.a_dict.__getitem__(key)
    def update(self, a_dict):
        result = self.a_dict.update(a_dict)
        self.interface.controlled.commit(self.interface.db)
        return result

class BaseroleInterface(object):
    def __init__(self, db):
        self.db = db
        self.controlled = None
    
    def create(*args, **kwargs):
        raise NotImplementedError('Must implement a create(*args, **kwargs) method')
    
    def __repr__(self):
        return self.controlled.__repr__()
    def __str__(self):
        return self.controlled.__str__()
    
    def add_child(self, child):
        from gridjob import GridjobModel
        child_job = child.controlled
        assert isinstance(child_job, GridjobModel),'Only jobs can be chilren.'
        self.controlled.refresh(self.db)
        if child_job.id not in self.controlled.children:
            self.controlled.children.append(child_job.id)
        self.controlled.commit(self.db)

    def children():            
        def fget(self):
            from gridjob import JobInterface
            self.controlled.refresh(self.db)
            job_list=list()
            for job_id in self.controlled.children:
                a_job = JobInterface(self.db).load(job_id)
                job_list.append(a_job)
            return tuple(job_list)
        return locals()
    children = property(**children())

    def user_data_dict():        
#TODO: Not sure how to handle the dictionaries. When a user changes the dict
# we will only know to update the database if we put something in the __setitem__
# dict function. But that might get wierd if the user uses the dict for something else,
# and forgets that everything that goes into the dict is sent to the database.
        def fget(self):
            self.controlled.refresh(self.db)
            obs_dict = ObservableDict(self, self.controlled.user_data_dict)
            return obs_dict
        def fset(self, user_dict):
            self.controlled.user_data_dict = user_dict
            self.controlled.commit(self.db)
        return locals()
    user_data_dict = property(**user_data_dict())
    
    def result_data_dict():        
        def fget(self):
            self.controlled.refresh(self.db)
            obs_dict = ObservableDict(self, self.controlled.result_data_dict)
            return obs_dict
        def fset(self, result_dict):
            self.controlled.result_data_dict = result_dict
            self.controlled.commit(self.db)
        return locals()
    result_data_dict = property(**result_data_dict())

    def id():        
        def fget(self):
            return self.controlled.id
        return locals()
    id = property(**id())
    
    def title():        
        def fget(self):
            return self.controlled.title
        def fset(self, title):
            self.controlled.title = title
            self.controlled.commit(db)
        return locals()
    title = property(**title())
    
    def author():
        def fget(self):
            return self.controlled.author
        def fset(self, author):
            self.controlled.author = author
            self.controlled.commit(db)
        return locals()
    author= property(**author())
    
    def dat():
        def fget(self):
            return self.controlled.dat
        def fset(self, dat):
            self.controlled.dat = dat
            self.controlled.commit(db)
        return locals()
    dat= property(**dat())
    
    def get_attachment(self, ext):
        import os
        f_dict = dict()
        for key in self.attachments:
           if key.rfind(ext) >= 0:
                f_dict[key] = self.attachments[key]
        if len(f_dict) != 1:
            raise ViewWarning('More than one file matches your attachment request.')
        if len(f_dict) == 1:
            return f_dict.values()[0]
        elif len(f_dict) > 1:
            return f_dict

    def attachments():
        def fget(self):
            return self._attachments_to_files()
        return locals()
    attachments = property(**attachments())
    
    def _get_attachment(self, filename, when_not_found=None):
        return self.db.get_attachment(self.controlled, filename, when_not_found)
    
    def put_attachment(self, content, filename, content_type='text/plain'):
        content.seek(0)
        self.db.put_attachment(self.controlled, content, filename, content_type)
        self.controlled = self.controlled.refresh(self.db)

    def delete_attachment(self, filename):
        return self.db.delete_attachment(self.controlled, filename)

    def _attachments_to_files(self, f_names=[]):
        '''We often want to save all of the attachments on the local computer.'''
        tempdir = generate_temp_dir(self.id)
        f_attachments = dict()
        if not f_names and '_attachments' in self.controlled:
            f_names = self.controlled['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self.get_attachment(self.db, attachment)
            f_attachments[attachment] = write_to_file(tempdir, attachment, attached_data)
        return f_attachments
