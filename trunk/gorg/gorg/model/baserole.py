import gorg
from couchdb.mapping import *
from couchdb import client as client
from datetime import datetime
from gorg.lib.utils import generate_new_docid, generate_temp_dir, write_to_file
import gorg
from gorg.lib.exceptions import *
import inspect

'''When you need to query for a key like this ('sad','saq') do this:
self.view(db,'who_task_owns',startkey=[self.id],endkey=[self.id,{}])
when you want to match a key ('sad','sad') do this:
a_job.view(db,'by_task_ownership',keys=[[a_task.id,a_task.id]])
'''
#GridrunModel.my_view(db, 'by_job_and_status', startkey=[job_id],endkey=[job_id,status])

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

class ObjectWrapper(object):
    """ObjectWrapper class redirects unhandled calls to wrapped object.
 
    Intended as an alternative when inheritance from the wrapped object is not
    possible.  This is the case for some python objects implemented in c.
 
    """
    def __init__(self, obj):
        self.wrap(obj)
    
    def wrap(self, obj):
        """Set the wrapped object."""
        super(ObjectWrapper, self).__setattr__('_obj', obj)

        # __methods__ and __members__ are deprecated but still used by
        # dir so we need to set them correctly here
        methods = []
        for nameValue in inspect.getmembers(obj, inspect.ismethod):
            methods.append(nameValue[0])
        super(ObjectWrapper, self).__setattr__('__methods__', methods)
 
        def isnotmethod(object_):
            return not inspect.ismethod(object_)
        members = []
        for nameValue in inspect.getmembers(obj, isnotmethod):
            members.append(nameValue[0])        
        super(ObjectWrapper, self).__setattr__('__members__', members)
    
    def __getattr__(self, name):
        """Redirect unhandled get attribute to self._obj."""
        if not hasattr(self._obj, name):
            raise AttributeError, ("'%s' has no attribute %s" %
                                   (self.__class__.__name__, name))
        else:
            return getattr(self._obj, name)
 
    def __setattr__(self, name, value):
        """Redirect set attribute to self._obj if necessary."""
        # note that we don't want to call hasattr(self, name) or dir(self)
        # we need to check if it is actually an attr on self directly
        selfHasAttr = True
        try:
            super(ObjectWrapper, self).__getattribute__(name)
        except AttributeError:
            selfHasAttr = False
 
        if (name == "_obj" or not hasattr(self, "_obj") or
            not hasattr(self._obj, name) or selfHasAttr):
            return super(ObjectWrapper, self).__setattr__(name, value)
        else:
            return setattr(self._obj, name, value)

class BaseInterface(ObjectWrapper):
    def __init__(self, db):
        super(BaseInterface, self).__init__(object)
        self.db = db

    def create(*args, **kwargs):
        raise NotImplementedError('Must implement a create(*args, **kwargs) method')

    def load(self, id=None):
        raise NotImplementedError('Must implement a load method')
    
    def store(self):
        self._obj.store(self.db)
    
    def __repr__(self):
        return self._obj.__repr__()
    
    def __str__(self):
        return self._obj.__str__()

    def get_attachment(self, ext):
        import os
        f_dict = dict()
        for key in self.attachments:
           if key.rfind(ext) >= 0:
                f_dict[key] = self.attachments[key]
        if len(f_dict) > 1:
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
        return self.db.get_attachment(self.id, filename, when_not_found)
    
    def put_attachment(self, content, filename, content_type='text/plain'):
        content.seek(0)
        self.db.put_attachment(self._obj, content, filename, content_type)
        self.load()

    def delete_attachment(self, filename):
        return self.db.delete_attachment(self.id, filename)

    def _attachments_to_files(self, f_names=[]):
        '''We often want to save all of the attachments on the local computer.'''
        tempdir = generate_temp_dir(self.id)
        f_attachments = dict()
        if not f_names and '_attachments' in self._obj:
            f_names = self._obj['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self._get_attachment(attachment)
            f_attachments[attachment] = write_to_file(tempdir, attachment, attached_data)
        return f_attachments
    
    def wait(self, timeout=60):
        import time
        check_freq = 10
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
        if self.status.terminal:
            # We did not timeout 
            return True
        else:
            # Timed out
            return False
    
class BaseGraphInterface(BaseInterface):
        
    def add_child(self, child):
        from gridjob import JobInterface
        assert isinstance(child, JobInterface),'Only jobs can be chilren.'
        self.load()
        if child.id not in self.children:
            self._obj.children.append(child.id)
        self.store()

    def children():            
        def fget(self):
            from gridjob import JobInterface
            self.load()
            job_list=list()
            for job_id in self._obj.children:
                a_job = JobInterface(self.db).load(job_id)
                job_list.append(a_job)
            return tuple(job_list)
        return locals()
    children = property(**children())
