from couchdb import schema as sch
from couchdb import client as client

import time

map_func_all = '''
    def mapfun(doc):
        if doc['type'] == 'GridtaskModel':
            yield None, doc
    '''
map_func_status = '''
    def mapfun(doc):
        if doc['type'] == 'GridtaskModel':
            yield doc['status'], doc
    '''
map_func_author = '''
    def mapfun(doc):
        if doc['type'] == 'GridtaskModel':
            yield doc['author'], doc
    '''
map_func_title = '''
    def mapfun(doc):
        if doc['type'] == 'GridtaskModel':
            yield doc['title'], doc
    '''

class GridtaskModel(sch.Document):       
    POSSIBLE_STATUS = ('FINISHED',  'SUBMITTED', 'ERROR', 'RUNNING', 'RETRIEVING')
    author = sch.TextField()
    title = sch.TextField()
    dat = sch.DateTimeField(default=time.gmtime())
    type = sch.TextField(default='GridtaskModel')
    # Type defined by the user
    defined_type = sch.TextField(default='GridtaskModel')
    # Each new entry is added as a parent.
    # A parent may or may not have childern
    # A parent without childern has an empty list
    job_relations = sch.DictField()
    status = sch.TextField(status = sch.TextField(default = 'SUBMITTED'))

    def add_job(self, job_id, my_parent=None):
        if not id in self.job_relations:
            self.job_relations[job_id]=list()
        for a_parent in my_parent:
            self.job_relations[a_parent].append(job_id)

    def __setattr__(self, name, value):
        if name == 'status':
            assert value in self.POSSIBLE_STATUS, 'Invalid status. \
            Only the following are valid, %s'%(' ,'.join(POSSIBLE_STATUS))
        super(GridjobModel, self).__setattr__(name, value)
    
    @staticmethod
    def run_view(db, viewname):
        from couchdb.design import ViewDefinition
        viewnames = GridtaskModel.couchdb_views(only_names=True)
        assert viewname in viewnames
        a_view=GridtaskModel.view(db, 'all/%s'%viewname)
        return a_view
    
    @staticmethod
    def couchdb_views(only_names=False):
        from couchdb.design import ViewDefinition
        if only_names:
            viewnames=('all', 'by_author', 'by_status', 'by_title')
            return viewnames
        else:
            all = ViewDefinition('all', 'all', map_func_all, language='python')
            by_author = ViewDefinition('all', 'by_author', map_func_author, language='python')
            by_status = ViewDefinition('all', 'by_status', map_func_status, language='python')
            by_title = ViewDefinition('all', 'by_title', map_func_title, language='python')
            views=[all, by_author, by_status, by_title]
        return views
