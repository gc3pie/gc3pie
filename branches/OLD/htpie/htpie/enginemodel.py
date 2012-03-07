import mongoengine
from mongoengine.fields import *
from mongoengine.fields import GridFSProxy
from mongoengine.base import BaseField
from mongoengine.document import *

from htpie import states

import pymongo

import datetime
import time
import os
import htpie

from htpie.lib import utils
from htpie.lib.exceptions import *
from htpie import status

import cPickle as pickle


from htpie import gcmd
_config = gcmd.read_config()

db = mongoengine.connect(_config.database_name, host=_config.database_uri, port=_config.database_port)

# Old way to get your uri, better to just as kthe socket you are using!!!
#_session_lock = u'%s'%(db.command( "whatsmyuri" ) [u'you'])

class MyGridFSProxy(GridFSProxy):
    def read(self,size=-1):
        try:
            return self.get().read(size)
        except:
            return None

class MongoBase(Document):
    meta = {'collection':'MongoBase'}
    
    create_d = DateTimeField(default=datetime.datetime.now())
    
    def __init__(self, **kwargs):
        super(MongoBase, self).__init__(**kwargs)
    
    @property
    def cls_name(self):
        return self.__class__.__name__
    
    @classmethod
    def create(cls):
        obj = cls()
        obj.save()
        return obj

class PickleProxy(MyGridFSProxy):
    """Proxy object to handle writing and reading of files to and from GridFS
    """
    
    def __init__(self, grid_id=None):
        super(PickleProxy, self).__init__(grid_id)
    
    def pickle():
        def fget(self):
            if self.grid_id:
                _temp = self.read()
                return pickle.loads(_temp)
            return None
        def fset(self, value):
            if self.grid_id:
                self.delete()
            #FIXME: If you the program is killed here, the gridfsproxy has been deleted,
            # and you still haven't created the new one. That means you lose the data!!!!
            self.new_file()
            pickle.dump(value, self.newfile, pickle.HIGHEST_PROTOCOL)
            self.close()
        return locals()
    pickle = property(**pickle())

class PickleField(BaseField):
    """A Pickle field storage field.
    """
    
    def __init__(self, **kwargs):
        super(PickleField, self).__init__(**kwargs)
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        
        # Check if a file already exists for thismodel.EmbeddedDocumen model
        grid_file = instance._data.get(self.name)
        if grid_file:
            return grid_file
        return PickleProxy()
    #FIXME: This should only except strings, as pickles are always strings
    def __set__(self, instance, value):
        #htpie.log.debug('value: %s'%(type(value)))
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

class TaskHistory(EmbeddedDocument):
    last_update_d = DateTimeField(default=datetime.datetime.now())
    start_state = StringField()
    end_state = StringField()
    reason = StringField()
    count = IntField(default=1)

    @classmethod
    def create(cls, start_state, end_state, reason):
        obj = cls()
        obj.start_state = start_state
        obj.end_state = end_state
        obj.reason = reason
        return obj
    
    def inc(self):
        self.time = datetime.datetime.now()
        self.count += 1
    
    def __repr__(self):
        output = '< History %s %s %s at %s >'%(self.start_state,  self.end_state,  self.reason.__name__, hex(id(self)))
        return output
    
    def __eq__(self, other):
        if isinstance(other, History):
            if self.start_state == other.start_state and \
                self.end_state == other.end_state and \
                self.reason == other.reason:
                return True
            else:
                return False
        else:
            return False

class Files(EmbeddedDocument):
    inputs = ListField(FileField())
    outputs = ListField(FileField())

class Task(MongoBase):
    meta = {'collection':'Task'}
    
    name = StringField()
    _state = StringField()
    _status = StringField(default=status.Status.PAUSED)
    app_tag = StringField()
    last_exec_d = DateTimeField(default=datetime.datetime.now())
    result = GenericReferenceField()
    _lock = StringField(default=u'')
    files = EmbeddedDocumentField(Files)
    history = ListField(EmbeddedDocumentField(TaskHistory))
    
    meta = {
        'indexes': [ ('_status', '_lock'), ('id', '_lock')]
    }

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)
        # Pymongo creates a pool of connections to the 
        # database, everything goes over one ip, but different ports.
        # Here we ask the socket we are using what port and ip it is on.
        # Remember that two different threads might use the same port!
        _session_lock = db.connection._Connection__pool.socket().getsockname()
        _session_lock = u'%s:%d'%(_session_lock[0], _session_lock[1])
        # We set the local lock value used for a particular multiprocess
        # thread. Because pymongo uses pooling, we can not just use
        # the ip:port number. Two threads may use the same port!
        pid = os.getpid()
        self._l_lock = u'%s__%d'%(_session_lock, pid)
        self._track_history = False

    @classmethod
    def create(cls,*args, **kwargs):
        obj = cls()
        obj.files = Files()
        if 'track_history' in kwargs:
            obj._track_history = kwargs['track_history']
        #obj.save()
        return obj
    
    def setstate(self, new_state, reason=None, exec_state=True):
        if self._track_history:
            his = History(self._state, new_state, reason)
            if not self.history:
                self.history.append(his)
            else:
                if self.history[-1] == his:
                    self.history[-1].inc()
                else:
                    self.history.append(his)
        self._state = new_state
        if exec_state:
            self.status = status.Status.PAUSED
    
    @property
    def state(self):        
        return self._state

    def retry(self):
        if self.status in status.Status.terminal():
            try:
                self.acquire(120)
            except:
                raise
            else:
                self.status = status.Status.PAUSED
                self.release()
    
    def done(self):
        if self.status in status.Status.terminal():
            return True

    def status():
        def fget(self):
            return self._status
        def fset(self, status):
            self._status = status
            self.save()
        return locals()
    status = property(**status())
    
    def display_fsm(self):
        fsm = self.cls_fsm()
        return fsm.states.display(self.cls_name)

    def attach_file(self, f_container, container):
        try:
            to_read = utils.verify_file_container(f_container)
            f = MyGridFSProxy()
            f.put(to_read, filename=os.path.basename(to_read.name))
        finally:
            to_read.close()
        self.files[container].append(f)
        self.save()
    
    def open(self, container):
        #FIXME: gridfs doesn't suport '.name', therefore we need to return a local
        #file copy.
        return self.mk_local_copy(container)
    
    def mk_local_copy(self, container):
        f_list = list()
        for f_proxy in self.files[container]:
            try:
                rootdir = utils.generate_temp_dir(str(self.id), container)
                to_read = f_proxy.get()
                to_write = open( '%s/%s'%(rootdir,to_read.name), 'w')
                _chunk_trans(to_write, to_read)
                f_list.append(open(to_write.name, 'r'))
            finally:
                to_write.close()
        return f_list
    
    def save(self):
        if self.authorize():
            super(Task, self).save()
    
    def acquire(self, timeout=0):
        if self.authorize(timeout):
            # The lock should be free
            # We verify that it went through, so no need to do a safe update here
            Task.objects(id=self.id, _lock=u'').update_one(set___lock = self._l_lock)
            #self.objects._collection.update({'_id':self.id, '_lock':u''}, {'$set':{'_lock':self._l_lock}}, safe=True)
            #We want to make sure that we have the lock
            self._verify()
        #htpie.log.debug('Acquire %s'%(self._lock))
    
    def _verify(self):
        #Get the latest from the database
        self.reload()
        if self._lock ==  self._l_lock:
            #We have the lock, so we are good
            return True
        raise AuthorizationException( 'Mongodb record %s _verify failed'%(self.id))
    
    def release(self):
        if self.authorize():
            self._lock = u''
            self.save()
        #htpie.log.debug('Released %s to %s'%(self._l_lock,self._lock))
    
    def authorize(self, timeout=0):
        #htpie.log.debug('Authorize %s to local %s'%(self._lock, self._l_lock))
        poll_interval = 1
        done = False
        starting_time = time.time()
        if self._lock ==  self._l_lock or not self._lock:
            # Lock checks out
            done = True
        else:
            # Better wait a while to see if the lock becomes free
            while starting_time + timeout - poll_interval > time.time():
                time.sleep(poll_interval)
                #We only want to query for the lock.
                self._lock = Task.objects(id=self.id).only('_lock')[0]._lock
                if self._lock ==  self._l_lock or not self._lock:
                    # Lock checks out
                    done = True
                    break
        if not done:
            raise AuthorizationException( 'Mongodb record %s authorization timedout after %d seconds'%(self.id, timeout))
        return done
    
    @classmethod
    def implicit_release(cls):
        '''If a thread crashs, it will not unlock the document. We unlock it here.'''
        def get_con_ids():
            import re
            #FIXME: Not sure it this returns all port numbers connected, or just the main port number.
            # If it is only the main port number, we may unlock tasks that shold really be locked!
            #Get all of the connection ids to the database
            con_ids = db.eval('db.$cmd.sys.inprog.findOne( {$all : 1 } )')
            valid_locks = [id[u'client'] for id in con_ids[u'inprog']]
            # We want to match only the first part of the lock, as
            # the second part is the PID. Since only one
            # taskscheduler will be running on the same machine we kill
            # all locks from that machine. If multiple taskschedulers
            # are on the same machine, we would need to check all the pids first on that machine.
            # Otherwise we might unlock a task associated with another
            # taskscheduler. We check only the first part of the _lock
            # by using re.
            valid_locks =  [re.compile(u'^%s'%i) for i in valid_locks]
            valid_locks.append(u'')
            return valid_locks
        to_unlock= cls.objects(_lock__nin=get_con_ids()).only('id')
        #to_unlock = cls.objects._collection.find({'_lock': {'$nin':get_con_ids()}}, ['_id'])
        obj_ids=[v['id'] for v in to_unlock]
        output = ['%s'%(v) for v in obj_ids]
        if output:
            htpie.log.debug('These tasks will be implicitly released : %s'%(output))
            cls.objects(_lock__nin=get_con_ids()).update(set___lock = u'', safe_update=True)
            #val = cls.objects._collection.update({'_lock':{'$nin':get_con_ids()}}, {'$set':{'_lock':u''}}, safe=True, multi=True)
            #When we do a update we get this back, {u'updatedExisting': True, u'ok': 1.0, u'err': None, u'n': 1L}
            #Lets check for an error
            #assert not val['err'], 'implicit_release errored'

def _chunk_trans(to_write, to_read):
    chunk = 50000
    line = to_read.read(chunk)
    while line:
        to_write.write(line)
        line = to_read.read(chunk)

if __name__ == '__main__':
    task = Task.create()
    task.name = u'1111111'
    node2 = Task.create()
    node2.name = u'2222222'
    node3 = Task.create()
    node3.name = u'3333333'
    task.save()
    big = open('/home/mmonroe/restore/ocikbs11/mmonroe/Desktop/ethben/both.pdb', 'r')
    task.attach_file(big, 'inputs')
    big.close()
    big = open('/home/mmonroe/restore/ocikbs11/mmonroe/Desktop/ethben/both.pdb', 'r')
    task.attach_file(big, 'outputs')
    me = task.mk_local_copy('inputs')
    task.acquire()
    task.release()


