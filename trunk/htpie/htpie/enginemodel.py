import mongoengine
import pymongo

import datetime
import uuid
import time
import sys
import os
import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *

import numpy as np
import copy
import cPickle as pickle

MONGO_DB = "engine"
MONGO_IP = "130.60.40.14"
MONGO_PORT = 27017

db = mongoengine.connect(MONGO_DB, host=MONGO_IP, port=MONGO_PORT)

class MongoBase(mongoengine.Document):
    meta = {'collection':'MongoBase'}
    
    create_d = mongoengine.DateTimeField(default=datetime.datetime.now())
    
    def __init__(self, *args, **kwargs):
        super(MongoBase, self).__init__(*args, **kwargs)

    @classmethod
    def create(cls):
        obj = cls()
        obj.save()
        return obj

class NumpyArrayField(mongoengine.base.BaseField):
    """A numpy array storage field.
    """
    
    def __init__(self, **kwargs):
        super(NumpyArrayField, self).__init__(**kwargs)
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        
        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        if grid_file:
            return np.load(grid_file)
        return mongoengine.fields.GridFSProxy()
    
    def __set__(self, instance, value):
        if isinstance(value, np.ndarray):                
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except:
                    pass
                # Create a new file with the new data
                np.save(grid_file, value)
                grid_file.close()
            else:
                # Create a new proxy object as we don't already have one
                instance._data[self.name] = mongoengine.fields.GridFSProxy()
                np.save(instance._data[self.name], value)
                instance._data[self.name].close()
        else:
            ValidationException('NumpyArrayField can only store numpy arrays and matrices')
    
    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, mongoengine.fields.GridFSProxy) and value.grid_id is not None:
            return value.grid_id
        return None
    
    def to_python(self, value):
        if value is not None:
            return mongoengine.fields.GridFSProxy(value)
    
    def validate(self, value):
        if value is not None:
            assert not isinstance(value, np.ndmatrix)
            assert isinstance(value, np.ndarray)

from mongoengine.fields import *
import mongoengine
import pymongo
import numpy as np
from htpie.lib.exceptions import *

import pickle

class PickleProxy(mongoengine.fields.GridFSProxy):
    """Proxy object to handle writing and reading of files to and from GridFS
    """
    
    def __init__(self, grid_id=None):
        super(PickleProxy, self).__init__(grid_id)
        if not self.grid_id:
            self.new_file()
    
    def pickle():
        def fget(self):
            _temp = self.read()
            return pickle.loads(_temp)
        def fset(self, value):
            self.delete()
            self.new_file()
            pickle.dump(value, self.newfile, pickle.HIGHEST_PROTOCOL)
            self.close()
        return locals()
    pickle = property(**pickle())


class PickleField(mongoengine.base.BaseField):
    """A Pickle field storage field.
    """
    
    def __init__(self, **kwargs):
        super(PickleField, self).__init__(**kwargs)
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        
        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        if grid_file:
            return grid_file
        return PickleProxy()
    
    def __set__(self, instance, value):
        if isinstance(value, file) or isinstance(value, str):
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except:
                    pass
                # Create a new file with the new data
                grid_file.put(value)
            else:
                # Create a new proxy object as we don't already have one
                instance._data[self.name] = PickleProxy()
                instance._data[self.name].put(value)
        else:
            instance._data[self.name] = value
    
    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, PickleProxy) and value.grid_id is not None:
            return value.grid_id
        return None
    
    def to_python(self, value):
        if value is not None:
            return PickleProxy(value)
    
    def validate(self, value):
        return
        if value.grid_id is not None:
            assert isinstance(value, PickleProxy)
            assert isinstance(value.grid_id, pymongo.objectid.ObjectId)








class PickleField(mongoengine.base.BaseField):
    """A python pickle storage field.
    """
    
    def __init__(self, **kwargs):
        super(PickleField, self).__init__(**kwargs)
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        
        # Check if a file already exists for this model
        grid_file = instance._data.get(self.name)
        if grid_file:
            return pickle.load(grid_file)
        return mongoengine.fields.GridFSProxy()
    
    def __set__(self, instance, value):
        if isinstance(value, np.ndarray):                
            # using "FileField() = file/string" notation
            grid_file = instance._data.get(self.name)
            # If a file already exists, delete it
            if grid_file:
                try:
                    grid_file.delete()
                except:
                    pass
                # Create a new file with the new data
                pickle.dump(value, grid_file, pickle.HIGHEST_PROTOCOL)
                grid_file.close()
            else:
                # Create a new proxy object as we don't already have one
                instance._data[self.name] = mongoengine.fields.GridFSProxy()
                pickle.dump(value, instance._data[self.name], pickle.HIGHEST_PROTOCOL)
                instance._data[self.name].close()
        else:
            ValidationException('NumpyArrayField can only store numpy arrays and matrices')
    
    def to_mongo(self, value):
        # Store the GridFS file id in MongoDB
        if isinstance(value, mongoengine.fields.GridFSProxy) and value.grid_id is not None:
            return value.grid_id
        return None
    
    def to_python(self, value):
        if value is not None:
            return mongoengine.fields.GridFSProxy(value)
    
    def validate(self, value):
        if value is not None:
            pass

    

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
        '_lock':unicode,
    }
    
    gridfs = {
        'files':[],
        'containers': ['input', 'output'],
    }
    
    default_values = {
        '_type':u'Task',
        '_lock':u'',
        'last_exec_d':datetime.datetime.now(), 
    }
    
    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        self._l_children = list()
        #We set the lock id to the connection id
        self._l_lock = u'%s'%(db.command( "whatsmyuri" ) [u'you'])
    
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
        col = eval('db[\'%s\']'%(collection))
        raw = col.one({'_id':ObjectId(id)})
        doc = eval('db[\'%s\'].%s'%(collection, raw['_type']))
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
    
    def save(self):
        if self.authorize():
            super(Task, self).save()
    
    def acquire(self, timeout=0):
        if self.authorize(timeout):
            #Update the lock and make sure that it was updated.
            #We could check the last_error to make sure it worked, but 
            #we hack it this way instead.
            self.collection.update({'_id':self._id, '_lock':u''}, {'$set':{'_lock':self._l_lock}}, safe=True)
            #We want to make sure that we have the lock
            self.reload()
            self.authorize()
        #htpie.log.debug('Acquire %s'%(self._lock))
    
    def release(self):
        if self.authorize():
            self['_lock'] = u''
            self.save()
        #htpie.log.debug('Released %s to %s'%(self._l_lock,self._lock))
    
    def authorize(self, timeout=0):
        #htpie.log.debug('Authorize %s to local %s'%(self._lock, self._l_lock))
        poll_interval = 1
        done = False
        starting_time = time.time()
        if self._lock ==  self._l_lock or not self._lock:
            #We have the lock, so we are good
            done = True
        while starting_time + timeout - poll_interval > time.time():
            time.sleep(poll_interval)
            #We only want to query for the lock.
            self._lock = self.collection.find_one({'_id':self._id}, ['_lock'])['_lock']
            if self._lock ==  self._l_lock or not self._lock:
                #We have the lock, so we are good
                done = True
                break
        if not done:
            raise AuthorizationException( 'Mongodb record %s authorization timedout after %d seconds'%(self.id, timeout))
        return done

    @classmethod
    def implicit_release(cls):
        '''If a thread crashs, it will not unlock the document. We unlock it here.'''
        def get_con_ids():
            #Get all of the connection ids to the database
            con_ids = db.eval('db.$cmd.sys.inprog.findOne( {$all : 1 } )')
            valid_locks = [id[u'client'] for id in con_ids[u'inprog']]
            valid_locks.append(u'')
            return valid_locks
        to_unlock = cls.collection().find({'_lock': {'$nin':get_con_ids()}}, ['_id'])
        obj_ids=[v['_id'] for v in to_unlock]
        output = ['%s'%(v) for v in obj_ids]
        if output:
            htpie.log.debug('These tasks will be implicitly released : %s'%(output))
            cls.collection().update({'_lock':{'$nin':get_con_ids()}}, {'$set':{'_lock':u''}}, safe=True, multi=True)
    
    def done(self):
        import htpie.statemachine
        if self.transition in htpie.statemachine.Transitions.terminal():
            return True
        else:
            return False
    
    def successful(self):
        import htpie.statemachine
        if self.done():
            if self.state == htpie.statemachine.States.COMPLETE and \
                self.transition == htpie.statemachine.Transitions.COMPLETE:
                return True
        return False

con.register([Task, MongoBase, MongoMatrix, MongoPickle, MongoAttachObj])

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
