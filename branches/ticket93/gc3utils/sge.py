import re

import gc3utils


try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        """
        A backport of `defaultdict` to Python 2.4
        See http://docs.python.org/library/collections.html
        """
        def __new__(cls, default_factory=None):
            return dict.__new__(cls)
        def __init__(self, default_factory):
            self.default_factory = default_factory
        def __missing__(self, key):
            try:
                return self.default_factory()
            except:
                raise KeyError("Key '%s' not in dictionary" % key)
        def __getitem__(self, key):
            if not dict.__contains__(self, key):
                dict.__setitem__(self, key, self.__missing__(key))
            return dict.__getitem__(self, key)


from utils import to_bytes
def _int_floor(s):
    return int(float(s))

# `_convert` is a `dict` instance, mapping key names to functions
# that parse a value from a string into a Python native type.
_convert = {
    'slots':int,
    'slots_used':int,
    'slots_resv':int,
    'slots_total':int,
    'load_avg':float,
    'load_short':float,
    'load_medium':float,
    'load_long':float,
    'np_load_avg':float,
    'np_load_short':float,
    'np_load_medium':float,
    'np_load_long':float,
    'num_proc':_int_floor, # SGE considers `num_proc` a floating-point value...
    'swap_free':to_bytes,
    'swap_total':to_bytes,
    'swap_used':to_bytes,
    'mem_free':to_bytes,
    'mem_used':to_bytes,
    'mem_total':to_bytes,
    'virtual_free':to_bytes,
    'virtual_used':to_bytes,
    'virtual_total':to_bytes,
}
def _parse_value(key, value):
    try:
        return _convert[key](value)
    except:
        return value


def parse_qstat_f(qstat_output):
    """
    Parse SGE's ``qstat -F`` output (as contained in string `qstat_output`)
    and return a `dict` instance, mapping each queue name to its attributes.
    """
    # a job report line starts with a numeric job ID
    _job_line_re = re.compile(r'^[0-9]+ \s+', re.X)
    # queue report header line starts with queuename@hostname 
    _queue_header_re = re.compile(r'^([a-z0-9\._-]+)@([a-z0-9\.-]+) \s+ ([BIPCTN]+) \s+ ([0-9]+)/([0-9]+)/([0-9]+)', 
                                  re.I|re.X)
    # property lines always have the form 'xx:propname=value'
    _property_line_re = re.compile(r'^[a-z]{2}:([a-z_]+)=(.+)', re.I|re.X)
    def dzdict():
        def zdict():
            return defaultdict(lambda: 0)
        return defaultdict(zdict)
    result = defaultdict(dzdict)
    qname = None
    for line in qstat_output.split('\n'):
        # strip leading and trailing whitespace
        line = line.strip()
        # is this a queue header?
        match = _queue_header_re.match(line)
        if match:
            qname, hostname, kind, slots_used, slots_resv, slots_total = match.groups()
            if 'B' not in kind:
                continue # ignore non-batch queues
            # key names are taken from 'qstat -xml' output
            result[qname][hostname]['slots_used'] = _parse_value('slots_used', slots_used)
            result[qname][hostname]['slots_resv'] = _parse_value('slots_resv', slots_resv)
            result[qname][hostname]['slots_total'] = _parse_value('slots_total', slots_total)
        # is this a property line?
        match = _property_line_re.match(line)
        if match:
            key, value = match.groups()
            result[qname][hostname][key] = _parse_value(key, value)
    return result


def compute_nr_of_slots(qstat_output):
    """
    Compute the number of total, free, and used/reserved slots from
    the output of SGE's ``qstat -F``.

    Return a dictionary instance, mapping each host name into a
    dictionary instance, mapping the strings ``total``, ``available``,
    and ``unavailable`` to (respectively) the the total number of
    slots on the host, the number of free slots on the host, and the
    number of used+reserved slots on the host.

    Cluster-wide totals are associated with key ``global``.

    **Note:** The 'available slots' computation carried out by this
    function is unreliable: there is indeed no notion of a 'global' or
    even 'per-host' number of 'free' slots in SGE.  Slot numbers can
    be computed per-queue, but a host can belong in different queues
    at the same time; therefore the number of 'free' slots available
    to a job actually depends on the queue it is submitted to.  Since
    SGE does not force users to submit explicitly to a queue, rather
    encourages use of a sort of 'implicit' routing queue, there is no
    way to compute the number of free slots, as this entirely depends
    on how local policies will map a job to the available queues.
    """
    qstat = parse_qstat_f(qstat_output)
    def zero_initializer():
        return 0
    def dict_with_zero_initializer():
        return defaultdict(zero_initializer)
    result = defaultdict(dict_with_zero_initializer)
    for q in qstat.iterkeys():
        for host in qstat[q].iterkeys():
            r = result[host]
            s = qstat[q][host]
            r['total'] = max(s['slots_total'], r['total'])
            r['unavailable'] = max(s['slots_used'] + s['slots_resv'], 
                                   r['unavailable'])
    # compute available slots by subtracting the number of used+reserved from the total
    g = result['global']
    for host in result.iterkeys():
        r = result[host]
        r['available'] = r['total'] - r['unavailable']
        # update cluster-wide ('global') totals
        g['total'] += r['total']
        g['unavailable'] += r['unavailable']
        g['available'] += r['available']
    return result


def parse_qhost_f(qhost_output):
    """
    Parse SGE's ``qhost -F`` output (as contained in string `qhost_output`)
    and return a `dict` instance, mapping each host name to its attributes.
    """
    result = defaultdict(dict)
    n = 0
    for line in qhost_output.split('\n'):
        # skip header lines
        n += 1
        if n < 3:
            continue
        # property lines start with TAB
        if line.startswith(' '):
            key, value = line.split('=')
            ignored, key = key.split(':')
            result[hostname][key] = _parse_value(key, value)
        # host lines begin at column 0
        else:
            hostname = line.split(' ')[0]
    return result
    
    
def count_jobs(qstat_output, whoami):
    """
    Parse SGE's ``qstat`` output (as contained in string `qstat_output`)
    and return a quadruple `(R, Q, r, q)` where:
      * `R` is the total number of running jobs in the SGE cell (from any user);
      * `Q` is the total number of queued jobs in the SGE cell (from any user);
      * `r` is the number of running jobs submitted by user `whoami`;
      * `q` is the number of queued jobs submitted by user `whoami`
    """
    total_running = 0
    total_queued = 0
    own_running = 0
    own_queued = 0
    n = 0
    for line in qstat_output.split('\n'):
        # skip header lines
        n += 1
        if n < 3:
            continue
        # remove leading and trailing whitespace
        line = line.strip()
        jid, prio, name, user, state, rest = re.split(r'\s+', line, 5)
        # skip in error/hold/suspended/deleted state
        if (('E' in state) or ('h' in state) or ('T' in state) 
            or ('s' in state) or ('S' in state) or ('d' in state)):
            continue
        if 'q' in state:
            total_queued += 1
            if user == whoami:
                own_queued += 1
        if 'r' in state:
            total_running += 1
            if user == whoami:
                own_running += 1
    return (total_running, total_queued, own_running, own_queued)
        
