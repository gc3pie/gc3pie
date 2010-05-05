class tuct(dict):
    '''A tuctionary, or tuct, is the combination of a tuple with
    a dictionary. A tuct has named items, but they cannot be
    deleted or rebound, nor new can be added. New key/value
    paris can be added like this:
    
    tuct_store=tuct(test=1)
    tuct_store['test']=90 # will not work
    tuct_store.update(test=90) # updates test key to 90
    tuct_store.setdefualt(newkey,100) # creates newkey and sets it to 100
    tuct_store.update(anotherkey=90) # creates anotherkey and sets it to 90
    '''
    
    def __setitem__(self, key, value):
        print 'This is a tuct, not a dictionary.'

    def __hash__(self):
        items = self.items()
        res = hash(items[0])
        for item in items[1:]:
            res ^= hash(item)
        return res
        
class MyUtilities(object):
    import itertools

    @staticmethod
    def striplist(l):
        '''String the white space from a list that contains strings
        '''
        return([x.strip() for x in l])
    
    @staticmethod
    def strip_tuple(l):
        '''String the white space from a tuple that contains strings
        '''
        return(tuple([x.strip() for x in l]))
    
    @staticmethod
    def search_list_for_substring(l, substring):
        """Search a list to see if any of the list's strings contain the substring."""
        for i in range(0, len(l)): 
            if -1 != l[i].find(substring) :
                found = i
                break
        return found

    @staticmethod
    def split_seq(iterable, size):
        """ Split a interable into chunks of the given size
            tuple(split_seq([1,2,3,4], size=2)
                            returns ([1,2],[3,4])
            
        """
        import itertools
        it = iter(iterable)
        item = list(itertools.islice(it, size))
        while item:
            yield item
            item = list(itertools.islice(it, size))
    
    @staticmethod
    def flatten(l, ltypes=(list, tuple)):
        '''Remove any nesting from a tuple or list
        [[1,2],[2,3]], becomes [1,2,2,3]
        '''
        ltype = type(l)
        l = list(l)
        i = 0
        while i < len(l):
            while isinstance(l[i], ltypes):
                if not l[i]:
                    l.pop(i)
                    i -= 1
                    break
                else:
                    l[i:i + 1] = l[i]
            i += 1
        return ltype(l)

    @staticmethod
    def sortNumericalStr(alist):
        '''Sort string containing numbers and chars in numerical order
        Some times you want to sort a string that is a mix of numbers and
        characters. ['hess1','hess10','hess8'] becomes ['hess1','hess8','hess10']
        '''    
        # inspired by Alex Martelli
        # http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52234
        indices = map(MyUtilities._generate_index, alist)
        decorated = zip(indices, alist)
        decorated.sort()
        return [ item for index, item in decorated ]
    @staticmethod
    def _generate_index(str):
        """
        Splits a string into alpha and numeric elements, which
        is used as an index for sorting"
        """
        #
        # the index is built progressively
        # using the _append function
        #
        index = []
        def _append(fragment, alist=index):
            if fragment.isdigit(): fragment = int(fragment)
            alist.append(fragment)
        # initialize loop
        prev_isdigit = str[0].isdigit()
        current_fragment = ''
        # group a string into digit and non-digit parts
        for char in str:
            curr_isdigit = char.isdigit()
            if curr_isdigit == prev_isdigit:
                current_fragment += char
            else:
                _append(current_fragment)
                current_fragment = char
                prev_isdigit = curr_isdigit
        _append(current_fragment)    
        return tuple(index)

class Monitor(object):
    '''Monitors the object to see if has been changed.
    
    Each time is_changed(object) it compares it to the last object
    that is_changed was passed. If different, returns false,
    otherwise returns true.        
    '''
    from cPickle import dumps
    _cm_last_dump = None
    def is_changed(self, obj):
        prev_dump = self._cm_last_dump
        self._cm_last_dump = None
        cur_dump = self.dumps(obj, -1)
        self._cm_last_dump = cur_dump
        return ( ( prev_dump is not None ) and ( prev_dump != cur_dump ) )
