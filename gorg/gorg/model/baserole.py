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
    
    children_ids = ListField(TextField())
    # This works like a dictionary, but allows you to go a_job.test.application_to_run='a app' as well as a_job.test['application_to_run']='a app'
    #test=DictField(Schema.build(application_to_run=TextField(default='gamess')))
    
    def __init__(self, *args):
        self.db = None
        args = list(args)
        for arg in args:
            if isinstance(arg,  client.Database):
                self.db=arg
                args.remove(arg)
        super(BaseroleModel, self).__init__(*args)
        self.id = generate_new_docid()

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
                for job_id in doc['children_ids']:
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
    
    def store(self):
        super(BaseroleModel, self).store(self.db)
    
    def load(self, db=None, id=None):
        if id is None:
            id = self.id
        if db is None:
            db = self.db
        self = super(BaseroleModel, self).load(db, id)
        self.db = db
        return self
        
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
        self.db.put_attachment(self, content, filename, content_type)
        return self.load()

    def delete_attachment(self, filename):
        return self.db.delete_attachment(self.id, filename)

    def _attachments_to_files(self, f_names=[]):
        '''We often want to save all of the attachments on the local computer.'''
        tempdir = generate_temp_dir(self.id)
        f_attachments = dict()
        if not f_names and '_attachments' in self:
            f_names = self['_attachments']
        # Loop through each attachment and save it
        for attachment in f_names:
            attached_data = self._get_attachment(attachment)
            f_attachments[attachment] = write_to_file(tempdir, attachment, attached_data)
        return f_attachments
    
    def wait(self, wait_for_state = 'terminal', timeout=60):
        import time
        check_freq = 10
        if timeout == 'INFINITE':
            timeout = sys.maxint
        if check_freq > timeout:
            check_freq = timeout
        starting_time = time.time()
        while starting_time + timeout > time.time():
            if wait_for_state == 'terminal':
                if self.status.terminal:
                    break
            else:
                if self.status == wait_for_state:
                    break
            time.sleep(check_freq)
        if wait_for_state == 'terminal':
            if self.status.terminal:
                return True
        else:
            if self.status == wait_for_state:
                return True
        # Timed out
        return False
    
    def add_child(self, child):
        if child.id not in self.children_ids:
            self.children_ids.append(child.id)

    def children():
        def fget(self):
            from gridjob import GridjobModel
            from gridtask import GridtaskModel
            ret = list()
            for child_id in self.children_ids:
                wrapped = self._get_interface_from_id(child_id)
                ret.append(wrapped)
            return tuple(ret)
        return locals()
    children = property(**children())
    
    def _get_interface_from_id(self, id):
        from gridjob import GridjobModel
        from gridtask import GridtaskModel
        doc = self.db.get(id)
        if doc['sub_type'] == 'GridjobModel':
            new = GridjobModel().wrap(doc)
            new.db = self.db
        else:
            new = GridtaskModel().wrap(doc)
            new.db = self.db
        return new
    
class BasenodeModel(BaseroleModel):
    def add_parent(self, parent):
        parent.add_child(self)
        
    def parents():            
        def fget(self):
            from gridjob import GridjobModel
            from gridtask import GridtaskModel
            tasks = list()
            jobs = list()
            view = GridtaskModel.view_children(self.db)
            for doc in view[self.id]:
                new = GridtaskModel().wrap(doc)
                new.db = self.db
                tasks.append(new)
            view = GridjobModel.view_children(self.db)
            for doc in view[self.id]:
                new = GridtaskModel().wrap(doc)
                new.db = self.db
                jobs.append(new)
            return tuple(tasks + jobs)
        return locals()
    parents = property(**parents())
