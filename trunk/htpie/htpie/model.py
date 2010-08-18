from mongokit import *
from pymongo.objectid import ObjectId
import datetime
import uuid
import time
import sys
import os

from htpie.lib import utils
from htpie.lib.exceptions import *

import numpy as np
import copy
import cPickle as pickle

MONGO_DB = "name"
MONGO_IP = "0.0.0.0"
MONGO_PORT = 27017

con = Connection(host=MONGO_IP, port=MONGO_PORT)

class MongoBase(Document):
    collection_name = 'MongoBase'
    
    structure = {'_lock':unicode,
                         'create_d': datetime.datetime, 
                         '_type':unicode, 
    }
    
    use_autorefs = True
    use_dot_notation=True
    #atomic_save = True
    
    default_values = {
        'create_d': datetime.datetime.now(),
        '_lock':u'', 
    }
    
    indexes = [
    {
        'fields': [('create_d', INDEX_ASCENDING)],
        'unique': False,
    },
    ]
    
    required_fields = ['_type']
    
    def __init__(self, *args, **kwargs):
        super(MongoBase, self).__init__(*args, **kwargs)
        self._l_lock = u'%s'%(uuid.uuid1())
    
    @property
    def id(self):
        if hasattr(self, '_id'):
            return str(self._id)
        else:
            return None
    
    @classmethod
    def doc(cls):
        cls_name = cls.__name__
        return eval('cls.collection().%s'%(cls_name))
    
    @classmethod
    def new(cls):
        cls_name = cls.__name__
        return eval('cls.collection().%s()'%(cls_name))
    
    @classmethod
    def collection(cls):
        return con[MONGO_DB][cls.collection_name]
    
    @classmethod
    def create(cls):
        obj = cls.new()
        obj.save()
        return obj
    
    def save(self):
        if self.authorize():
            super(MongoBase, self).save()
    
    def acquire(self):
        if self.authorize():
            self._lock =  self._l_lock
            self.save()
        self._lock =  self._l_lock
        self.save()
    
    def release(self):
        if self.authorize():
            self._lock= u''
            self.save()
    
    def authorize(self, timeout=10):
        poll_interval = 1
        if poll_interval > timeout:
            poll_interval = timeout
        done = False
        starting_time = time.time()
        while starting_time + timeout > time.time():
            if self._lock ==  self._l_lock or not self._lock:
                done = True
                break
            time.sleep(poll_interval)
        if not done:
            raise AuthorizationException( 'Mongodb record %s authorization timedout after %d seconds'%(self.id, timeout))
        return done

class MongoAttachObj(MongoBase):
    collection_name = 'MongoAttach'
    structure = {}
    
    gridfs = {'containers':['attach']}
    
    default_values = {
        '_type':u'MongoAttach', 
    }
    
    def _put_attach(self, name, obj, container='attach'):
        try:
            a_file = self.fs.__dict__[container].open(name, 'w')
            if isinstance(obj, np.ndarray):
                np.save(a_file, obj)
            else:
                #Must be protocol 2 for the .readline = .read to work
                pickle.dump(obj, a_file, 2)
        finally:
            a_file.close()
    
    def _get_attach(self, name, container = 'attach'):
        obj = None
        if name in self.fs.__dict__[container].list():
            try:
                a_file = self.fs.__dict__[container].open(name, 'r')
                tester = a_file.read(6)
                a_file.seek(0)
                if tester == '\x93NUMPY':
                    #Numpy loads a martix as an ndarray
                    obj = np.load(a_file)
                else:
                    #Currently there is no readline on gridfs, therefore
                    #we fake it here
                    a_file.readline = a_file.read
                    obj = pickle.load(a_file)
            finally:
                a_file.close()
        return obj
    
class MongoMatrix(MongoAttachObj):
    def __init__(self, *args, **kwargs):
        super(MongoMatrix, self).__init__(*args, **kwargs)
        self._l_matrix = None
    
    def save(self):
        if not hasattr(self, '_id'):
            super(MongoMatrix, self).save()
            self.matrix = self._l_matrix
            self._l_matrix = None
        else:
            super(MongoMatrix, self).save()
    
    def matrix():
        def fget(self):
            if not hasattr(self, '_id'):
                return self._l_matrix
            else:
                np_matrix = self._get_attach('matrix')
            return np_matrix
        def fset(self, np_matrix):
            if not hasattr(self, '_id'):
                self._l_matrix = copy.deepcopy(np_matrix)
            else:
                self._put_attach('matrix', np_matrix)
        return locals()
    matrix = property(**matrix())

class MongoPickle(MongoAttachObj):
    
    def __init__(self, *args, **kwargs):
        super(MongoPickle, self).__init__(*args, **kwargs)
        self._l_pickled = None
    
    def save(self):
        if not hasattr(self, '_id'):
            super(MongoPickle, self).save()
            self.pickle = self._l_pickled
            self._l_pickled = None
        else:
            super(MongoPickle, self).save()
    
    def pickle():
        def fget(self):
            if not hasattr(self, '_id'):
                return self._l_pickle
            else:
                obj = self._get_attach('pickle')
            return obj
        def fset(self, obj):
            if not hasattr(self, '_id'):
                self._l_pickle = copy.deepcopy(obj)
            else:
                self._put_attach('pickle', obj)
        return locals()
    pickle = property(**pickle())

class Task(MongoBase):
    collection_name = 'Task'
    
    structure = {
        'name':unicode, 
        'state': unicode,
        'transition': unicode,
        'app_tag': unicode, 
        'children_dbref': [DBRef], 
        'last_exec_d': datetime.datetime, 
        'result': MongoBase,
    }
    
    gridfs = {
        'files':[],
        'containers': ['input', 'output'],
    }
    
    default_values = {
        '_type':u'Task',
    }
    
    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        self._l_children = list()
    
    def add_child(self, obj):
        dbref = DBRef(obj.collection_name, obj._id)
        if dbref not in self.children_dbref:
            self.children_dbref.append(dbref)
            self._l_children.append(obj)
            self.save()
    
    def add_parent(self, obj):
        #cls_name =  self.__class__.__name__
        #doc = eval('con[MONGO_DB][self.collection_name].%s'%(cls_name))
        #parent = doc.one({'_id':objid._id})
        obj.add_child(self)
    
    @property
    def children(self):
        cls_name =  self.__class__.__name__
        for dbref in self.children_dbref:            
            found = False
            for got in self._l_children:
                if dbref.id == got._id:
                    found = True
                    break
            if not found:
                doc = Task.load(dbref.id)
                self._l_children.append(doc)
        return tuple(self._l_children)
    
    @staticmethod
    def load(id):
        collection = Task.collection_name
        col = eval('con[MONGO_DB][\'%s\']'%(collection))
        raw = col.one({'_id':ObjectId(id)})
        doc = eval('con[MONGO_DB][\'%s\'].%s'%(collection, raw['_type']))
        assert not isinstance(doc,Collection),  'Doc can not be a collection. Did you import everything?'
        doc = doc.one({'_id':raw['_id']})
        return doc
    
    def retry(self):
        raise NotImplementedError('Retry method not implemented for %s'%(cls.__class__.__name__))
    
    def kill(self):
        raise NotImplementedError('Kill method not implemented for %s'%(cls.__class__.__name__))

    def save(self):
        super(Task, self).save()        
        to_save = self.span()
        for child in to_save:
            if self is not child:
                #print 'I saved %s'%(child.id)
                super(Task, child).save()
    
    def span(self):
        return self._follow_tree(self, set())
    
    @staticmethod
    def _follow_tree(starting_node, visited):
        if starting_node not in visited:
            visited.add(starting_node)
            for child in starting_node._l_children:
                #print 'I visited %s'%(child.id)
                child._follow_tree(child, visited)
        return visited
    
    def attach_file(self, f_container, container):
        try:
            to_read = utils.verify_file_container(f_container)
            to_write = self.fs.__dict__[container].open(os.path.basename(to_read.name), 'w')
            _chunk_trans(to_write, to_read)
        finally:
            to_write.close()
            to_read.close()
        self.save()
    
    def _open(self, container, mode='r'):
        f_container = list()
        for f_name in map(str,self.fs.__dict__[container].list()):
            a_file = self.fs.__dict__[container].open(f_name, mode)
            #FIXME: gridfs name bugTrying to set the name attr doesn't work
            #a_file.name = '%s'%(a_file.metadata['name'])
            f_container.append(a_file)
        return f_container
    
    def open(self, container):
        #FIXME: gridfs doesn't suppore name, therefore we need to return a local
        #file copy.
        return self.mk_local_copy(container)
    
    def mk_local_copy(self, container):
        f_list = list()
        try:
            f_container = self._open(container, 'r')
            for to_read in f_container:
                try:
                    rootdir = utils.generate_temp_dir(self.id, container)
                    #FIXME: gridfs name bug
                    name = to_read.metadata['name']
                    to_write = open( '%s/%s'%(rootdir,name), 'w')
                    _chunk_trans(to_write, to_read)
                    f_list.append(open(to_write.name, 'r'))
                finally:
                    to_write.close()
        finally:
            [f.close() for f in f_container]
        return f_list    

con.register([Task, MongoBase, MongoMatrix, MongoAttachObj])

class CustomArray(CustomType):
    """ SET custom type to handle python set() type """
    init_type = None
    mongo_type = list
    python_type = np.ndarray
    
    def __init__(self, structure_type=None):
        super(CustomArray, self).__init__()
        self._structure_type = structure_type
    
    def to_bson(self, value):
        if value is not None:
            return list([unicode(v) for v in value])
    
    def to_python(self, value):
        if value is not None:
            return np.array([float(v) for v in value])
    
    def validate(self, value, path):
        if value is not None and self._structure_type is not None:
            for val in value:
                if not isinstance(val, self._structure_type):
                    raise ValueError('%s must be an instance of %s not %s' %
                      (path, self._structure_type.__name__, type(val).__name__))

def _chunk_trans(to_write, to_read):
    chunk = 50000
    line = to_read.read(chunk)
    while line:
        to_write.write(line)
        line = to_read.read(chunk)

if __name__ == '__main__':
#    Task.structure['state']=[Task]
#        use_autorefs = True
    task = Task.create()
    task.name = u'1111111'
    node2 = Task.create()
    node2.name = u'2222222'
    node3 = Task.create()
    node3.name = u'3333333'
    task.add_child(node2)
    task._l_children=list()
    task.children[0].add_child(node3)
    task.save()
    big = open('/home/mmonroe/Desktop/both.pdb', 'r')
    task.attach_file(big, 'input')
    big.close()
    big = open('/home/mmonroe/Desktop/both.pdb', 'r')
    task.attach_file(big, 'output')
    me=task.attachment
    print task.id
    print me.mk_local_copy()

#task.add_child(node2)
#node2.add_child(node3)
#node3.add_child(node2)
#node3.add_child(task)
#task.children
#task.save()
#task.children[0].children[0].name=u'ccccccccccccccccccccccc'
#task.save()
#node3.reload()
#node3.name == u'ccccccccccccccccccccccc'
#
#con[MONGO_DB][Task.collection_name].save(task)
#
#task.create(u'a_name')
#me=con[MONGO_DB][Task.collection_name].Task.one({'_id'  : ObjectId('4c4eb20c49e41b6b81000030')})
#me.children
