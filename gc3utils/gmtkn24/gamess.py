#! /usr/bin/env python
#
"""
Classes for manipulating GAMESS Input and Output files.
"""
__docformat__ = 'reStructuredText'
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'

import sys
import re
_whitespace_re = re.compile(r'\s+', re.M)


import iterators


class WordIterator(iterators.BufferingIterator):
    """
    Iterate over a file or text word by word.
    """
    def __init__(self, lines, cleanup=None):
        """
        Create and return a `WordIterator` instance returning the
        given `lines` (which must be itself iterable) word by word.

        Convenient wrappers for iterating over words contained
        in a string or text file are given in `words_from_file`
        and `words_from_text` functions.
        """
        self._lines = lines
        if cleanup:
            self.cleanup = cleanup
        BufferingIterator.__init__()

    def refill(self):
        """
        Refill the buffer with next batch of words.
        """
        try:
            return [ w for w 
                     in _whitespace_re.split(self._lines.next())
                     if (w != '') ]
        except StopIteration:
            self.cleanup()
            raise

    def next_line():
        """
        Return the entire next input line, without breaking it
        into words.

        Raises `StopIteration` if there are no more input lines.
        """
        try:
            return self._lines.next()
        except StopIteration:
            self.cleanup()
            raise


def words_from_file(filename):
    """
    Return an iterator over the words contained into the text file
    `filename`.
    """
    contents = open(filename, 'r')
    def close(self):
        contents.close()
    it = WordIterator(contents.xreadlines(),
                      cleanup=close)
    return it


def words_from_string(text):
    """
    Return an iterator over the words contained in string `text`.
    """
    return WordIterator(text.split('\n'))


class Struct(dict):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.  
    Example::

      >>> x = Struct()
      >>> x.a = 1
      >>> x.has_key('a')
      True
      >>> x['a']
      1
      >>> x['b'] = 2
      >>> hasattr(x, 'b')
      True
      >>> x.b
      2

    An optional transformation function can be applied to the
    attribute/key names; for instance this is how the case-insensitive
    `UpcaseStruct` class (which see) is implemented:

      | >>> x = Struct(transform = lambda name: name.upper())
      | >>> x.a = 1
      | >>> x['A']
      | 1
    """
    def __init__(self, transform=None, **kw):
        if transform is not None:
            self.__dict__['_transform'] = transform
        else:
            def same(x):
                return x
            self.__dict__['_transform'] = same
        for k,v in kw.items():
             self[k] = v
    def __getattr__(self, key):
        return self[self.__dict__['_transform'](key)]
    def __getitem__(self, key):
        return dict.__getitem__(self, self.__dict__['_transform'](key))
    def __setattr__(self, key, val):
        self[self.__dict__['_transform'](key)] = val
    def __setitem__(self, key, val):
        dict.__setitem__(self, self.__dict__['_transform'](key), val)
    def has_key(self, key):
        return dict.has_key(self, self.__dict__['_transform'](key))


class UpcaseStruct(Struct):
    """
    A `dict`-like object, whose keys can be accessed with the usual
    '[...]' lookup syntax, or with the '.' get attribute syntax.  
    Example::

      >>> x = UpcaseStruct()
      >>> x.a = 1
      >>> x.has_key('a')
      True
      >>> x['a']
      1
      >>> x['b'] = 2
      >>> hasattr(x, 'b')
      True
      >>> x.b
      2

    Case of attribute/key names (with either syntax) is not
    significant: all attribute/key names will be canonicalized and
    stored as uppercase::

      >>> hasattr(x, 'B')
      True
      >>> x.has_key('B')
      True
      >>> x.B
      2
      >>> x.b is x.B
      True

    In particular, all attribute/key names will show as
    uppercase when printed::

      >>> x
      {'A': 1, 'B': 2}
    """
    __slots__ = [ ]
    def __init__(self, **kw):
        Struct.__init__(self, transform = lambda key: key.upper(), **kw)


class Atom(object):
    """
    Simple structure describing an atom in a molecule:
    a name, plus the three cartesian coordinates.
    """
    def __init__(self, name, x, y, z):
        self.name = Atom._DATA[name].name
        self.symbol = Atom._DATA[name].symbol
        self.nuclear_charge = Atom._DATA[name].nuclear_charge
        self.x = x
        self.y = y
        self.z = z

    # see: http://www.chem.qmul.ac.uk/iupac/AtWt/index.html
    _DATA = UpcaseStruct(
        H = Struct(name="Hydrogen", symbol="H", nuclear_charge=1.0, weight=1.00794),
        He = Struct(name="Helium", symbol="He", nuclear_charge=2.0, weight=4.002602),
        Li = Struct(name="Lithium", symbol="Li", nuclear_charge=3.0, weight=6.941),
        Be = Struct(name="Beryllium", symbol="Be", nuclear_charge=4.0, weight=9.012182),
        B = Struct(name="Boron", symbol="B", nuclear_charge=5.0, weight=10.811),
        C = Struct(name="Carbon", symbol="C", nuclear_charge=6.0, weight=12.0107),
        N = Struct(name="Nitrogen", symbol="N", nuclear_charge=7.0, weight=14.0067),
        O = Struct(name="Oxygen", symbol="O", nuclear_charge=8.0, weight=15.9994),
        F = Struct(name="Fluorine", symbol="F", nuclear_charge=9.0, weight=18.9984032),
        Ne = Struct(name="Neon", symbol="Ne", nuclear_charge=10.0, weight=20.1797),
        Na = Struct(name="Sodium", symbol="Na", nuclear_charge=11.0, weight=22.98976928),
        Mg = Struct(name="Magnesium", symbol="Mg", nuclear_charge=12.0, weight=24.3050),
        Al = Struct(name="Aluminium", symbol="Al", nuclear_charge=13.0, weight=26.9815386),
        Si = Struct(name="Silicon", symbol="Si", nuclear_charge=14.0, weight=28.0855),
        P = Struct(name="Phosphorus", symbol="P", nuclear_charge=15.0, weight=30.973762),
        S = Struct(name="Sulfur", symbol="S", nuclear_charge=16.0, weight=32.065),
        Cl = Struct(name="Chlorine", symbol="Cl", nuclear_charge=17.0, weight=35.453),
        Ar = Struct(name="Argon", symbol="Ar", nuclear_charge=18.0, weight=39.948),
        K = Struct(name="Potassium", symbol="K", nuclear_charge=19.0, weight=39.0983),
        Ca = Struct(name="Calcium", symbol="Ca", nuclear_charge=20.0, weight=40.078),
        Sc = Struct(name="Scandium", symbol="Sc", nuclear_charge=21.0, weight=44.955912),
        Ti = Struct(name="Titanium", symbol="Ti", nuclear_charge=22.0, weight=47.867),
        V = Struct(name="Vanadium", symbol="V", nuclear_charge=23.0, weight=50.9415),
        Cr = Struct(name="Chromium", symbol="Cr", nuclear_charge=24.0, weight=51.9961),
        Mn = Struct(name="Manganese", symbol="Mn", nuclear_charge=25.0, weight=54.938045),
        Fe = Struct(name="Iron", symbol="Fe", nuclear_charge=26.0, weight=55.845),
        Co = Struct(name="Cobalt", symbol="Co", nuclear_charge=27.0, weight=58.933195),
        Ni = Struct(name="Nickel", symbol="Ni", nuclear_charge=28.0, weight=58.6934),
        Cu = Struct(name="Copper", symbol="Cu", nuclear_charge=29.0, weight=63.546),
        Zn = Struct(name="Zinc", symbol="Zn", nuclear_charge=30.0, weight=65.38),
        Ga = Struct(name="Gallium", symbol="Ga", nuclear_charge=31.0, weight=69.723),
        Ge = Struct(name="Germanium", symbol="Ge", nuclear_charge=32.0, weight=72.64),
        As = Struct(name="Arsenic", symbol="As", nuclear_charge=33.0, weight=74.92160),
        Se = Struct(name="Selenium", symbol="Se", nuclear_charge=34.0, weight=78.96),
        Br = Struct(name="Bromine", symbol="Br", nuclear_charge=35.0, weight=79.904),
        Kr = Struct(name="Krypton", symbol="Kr", nuclear_charge=36.0, weight=83.798),
        Rb = Struct(name="Rubidium", symbol="Rb", nuclear_charge=37.0, weight=85.4678),
        Sr = Struct(name="Strontium", symbol="Sr", nuclear_charge=38.0, weight=87.62),
        Y = Struct(name="Yttrium", symbol="Y", nuclear_charge=39.0, weight=88.90585),
        Zr = Struct(name="Zirconium", symbol="Zr", nuclear_charge=40.0, weight=91.224),
        Nb = Struct(name="Niobium", symbol="Nb", nuclear_charge=41.0, weight=92.90638),
        Mo = Struct(name="Molybdenum", symbol="Mo", nuclear_charge=42.0, weight=95.96),
        Tc = Struct(name="Technetium", symbol="Tc", nuclear_charge=43.0, weight=98),
        Ru = Struct(name="Ruthenium", symbol="Ru", nuclear_charge=44.0, weight=101.07),
        Rh = Struct(name="Rhodium", symbol="Rh", nuclear_charge=45.0, weight=102.90550),
        Pd = Struct(name="Palladium", symbol="Pd", nuclear_charge=46.0, weight=106.42),
        Ag = Struct(name="Silver", symbol="Ag", nuclear_charge=47.0, weight=107.8682),
        Cd = Struct(name="Cadmium", symbol="Cd", nuclear_charge=48.0, weight=112.411),
        In = Struct(name="Indium", symbol="In", nuclear_charge=49.0, weight=114.818),
        Sn = Struct(name="Tin", symbol="Sn", nuclear_charge=50.0, weight=118.710),
        Sb = Struct(name="Antimony", symbol="Sb", nuclear_charge=51.0, weight=121.760),
        Te = Struct(name="Tellurium", symbol="Te", nuclear_charge=52.0, weight=127.60),
        I = Struct(name="Iodine", symbol="I", nuclear_charge=53.0, weight=126.90447),
        Xe = Struct(name="Xenon", symbol="Xe", nuclear_charge=54.0, weight=131.293),
        Cs = Struct(name="Caesium", symbol="Cs", nuclear_charge=55.0, weight=132.9054519),
        Ba = Struct(name="Barium", symbol="Ba", nuclear_charge=56.0, weight=137.327),
        La = Struct(name="Lanthanum", symbol="La", nuclear_charge=57.0, weight=138.90547),
        Ce = Struct(name="Cerium", symbol="Ce", nuclear_charge=58.0, weight=140.116),
        Pr = Struct(name="Praseodymium", symbol="Pr", nuclear_charge=59.0, weight=140.90765),
        Nd = Struct(name="Neodymium", symbol="Nd", nuclear_charge=60.0, weight=144.242),
        Pm = Struct(name="Promethium", symbol="Pm", nuclear_charge=61.0, weight=145),
        Sm = Struct(name="Samarium", symbol="Sm", nuclear_charge=62.0, weight=150.36),
        Eu = Struct(name="Europium", symbol="Eu", nuclear_charge=63.0, weight=151.964),
        Gd = Struct(name="Gadolinium", symbol="Gd", nuclear_charge=64.0, weight=157.25),
        Tb = Struct(name="Terbium", symbol="Tb", nuclear_charge=65.0, weight=158.92535),
        Dy = Struct(name="Dysprosium", symbol="Dy", nuclear_charge=66.0, weight=162.500),
        Ho = Struct(name="Holmium", symbol="Ho", nuclear_charge=67.0, weight=164.93032),
        Er = Struct(name="Erbium", symbol="Er", nuclear_charge=68.0, weight=167.259),
        Tm = Struct(name="Thulium", symbol="Tm", nuclear_charge=69.0, weight=168.93421),
        Yb = Struct(name="Ytterbium", symbol="Yb", nuclear_charge=70.0, weight=173.054),
        Lu = Struct(name="Lutetium", symbol="Lu", nuclear_charge=71.0, weight=174.9668),
        Hf = Struct(name="Hafnium", symbol="Hf", nuclear_charge=72.0, weight=178.49),
        Ta = Struct(name="Tantalum", symbol="Ta", nuclear_charge=73.0, weight=180.94788),
        W = Struct(name="Tungsten", symbol="W", nuclear_charge=74.0, weight=183.84),
        Re = Struct(name="Rhenium", symbol="Re", nuclear_charge=75.0, weight=186.207),
        Os = Struct(name="Osmium", symbol="Os", nuclear_charge=76.0, weight=190.23),
        Ir = Struct(name="Iridium", symbol="Ir", nuclear_charge=77.0, weight=192.217),
        Pt = Struct(name="Platinum", symbol="Pt", nuclear_charge=78.0, weight=195.084),
        Au = Struct(name="Gold", symbol="Au", nuclear_charge=79.0, weight=196.966569),
        Hg = Struct(name="Mercury", symbol="Hg", nuclear_charge=80.0, weight=200.59),
        Tl = Struct(name="Thallium", symbol="Tl", nuclear_charge=81.0, weight=204.3833),
        Pb = Struct(name="Lead", symbol="Pb", nuclear_charge=82.0, weight=207.2),
        Bi = Struct(name="Bismuth", symbol="Bi", nuclear_charge=83.0, weight=208.98040),
        Po = Struct(name="Polonium", symbol="Po", nuclear_charge=84.0, weight=209),
        At = Struct(name="Astatine", symbol="At", nuclear_charge=85.0, weight=210),
        Rn = Struct(name="Radon", symbol="Rn", nuclear_charge=86.0, weight=222),
        Fr = Struct(name="Francium", symbol="Fr", nuclear_charge=87.0, weight=223),
        Ra = Struct(name="Radium", symbol="Ra", nuclear_charge=88.0, weight=226),
        Ac = Struct(name="Actinium", symbol="Ac", nuclear_charge=89.0, weight=227),
        Th = Struct(name="Thorium", symbol="Th", nuclear_charge=90.0, weight=232.03806),
        Pa = Struct(name="Protactinium", symbol="Pa", nuclear_charge=91.0, weight=231.03588),
        U = Struct(name="Uranium", symbol="U", nuclear_charge=92.0, weight=238.02891),
        Np = Struct(name="Neptunium", symbol="Np", nuclear_charge=93.0, weight=237),
        Pu = Struct(name="Plutonium", symbol="Pu", nuclear_charge=94.0, weight=244),
        Am = Struct(name="Americium", symbol="Am", nuclear_charge=95.0, weight=243),
        Cm = Struct(name="Curium", symbol="Cm", nuclear_charge=96.0, weight=247),
        Bk = Struct(name="Berkelium", symbol="Bk", nuclear_charge=97.0, weight=247),
        Cf = Struct(name="Californium", symbol="Cf", nuclear_charge=98.0, weight=251),
        Es = Struct(name="Einsteinium", symbol="Es", nuclear_charge=99.0, weight=252),
        Fm = Struct(name="Fermium", symbol="Fm", nuclear_charge=100.0, weight=257),
        Md = Struct(name="Mendelevium", symbol="Md", nuclear_charge=101.0, weight=258),
        No = Struct(name="Nobelium", symbol="No", nuclear_charge=102.0, weight=259),
        Lr = Struct(name="Lawrencium", symbol="Lr", nuclear_charge=103.0, weight=262),
        Rf = Struct(name="Rutherfordium", symbol="Rf", nuclear_charge=104.0, weight=265),
        Db = Struct(name="Dubnium", symbol="Db", nuclear_charge=105.0, weight=268),
        Sg = Struct(name="Seaborgium", symbol="Sg", nuclear_charge=106.0, weight=271),
        Bh = Struct(name="Bohrium", symbol="Bh", nuclear_charge=107.0, weight=272),
        Hs = Struct(name="Hassium", symbol="Hs", nuclear_charge=108.0, weight=270),
        Mt = Struct(name="Meitnerium", symbol="Mt", nuclear_charge=109.0, weight=276),
        Ds = Struct(name="Darmstadtium", symbol="Ds", nuclear_charge=110.0, weight=281),
        Rg = Struct(name="Roentgenium", symbol="Rg", nuclear_charge=111.0, weight=280),
        Cn = Struct(name="Copernicium", symbol="Cn", nuclear_charge=112.0, weight=285),
        Uut = Struct(name="Ununtrium", symbol="Uut", nuclear_charge=113.0, weight=284),
        Uuq = Struct(name="Ununquadium", symbol="Uuq", nuclear_charge=114.0, weight=289),
        Uup = Struct(name="Ununpentium", symbol="Uup", nuclear_charge=115.0, weight=288),
        Uuh = Struct(name="Ununhexium", symbol="Uuh", nuclear_charge=116.0, weight=293),
        Uuo = Struct(name="Ununoctium", symbol="Uuo", nuclear_charge=118.0, weight=294),
        )


class GamessInpStruct(Struct):
    """
    Base class for representing GAMESS '.inp' files.
    
    An instance acts as a Python `dict` instance, mapping GAMESS
    '.inp' section names to a dictionary of key=value pairs.
    """
    __slots__ = [ ]

    @staticmethod
    def _remove_dollar_and_upcase(s):
        if s.startswith('$'):
            return s[1:].upper()
        else:
            return s.upper()

    def __init__(self):
        Struct.__init__(self, transform = _remove_dollar_and_upcase)

    def write(self, dest):
        """
        Dump the contents of this object as a GAMESS '.inp' file text
        into `dest`.  Argument `dest` can be a file-like object, in
        which case text is written using its `write()` method, or a
        file name, which is opened for writing.
        """
        if hasattr(dest, write):
            output = dest
        else:
            # presume `dest` is a file name
            output = open(dest, 'w')
        # output sections, except for $DATA, which requires special treatment
        for section in ['CONTRL', 'DFT', 'BASIS', 'SYSTEM']:
            try:
                # output each section on one line
                output.write(' $%s %s $END\n' 
                             % (section, str.join(' ',
                                                  [ '%s=%s' for k,v in self[section].items() ])))
            except KeyError:
                # ignore missing sections
                pass
        # output DATA section
        output.write(' $DATA\n')
        output.write('%s\n' % self.name)
        output.write('%s\n' % self.DATA.symmetry)
        for atom in self.DATA.atoms:
            output.write('%s\t%s\t%s\t%s\t%s\n' %
                         (atom.name, atom.nuclear_charge, atom.x, atom.y, atom.z))
        output.write(' $END\n')


def read_turbomole_coords(input):
    """
    Import a TURBOMOLE format file.

    Argument `input` can be either a file-like object, providing a
    `read` method that is used to read its full contents, or a
    string, containing the path name of a file to read.
    """
    if hasattr(input, 'read'):
        lines = iter(input.read().split('\n'))
    else:
        f = open(input, 'rU')
        lines = iter(f.read().split('\n'))
        f.close()
    result = [ ]
    try:
        while True:
            line = lines.next().strip()
            # skip empty lines
            if line == '':
                continue
            if line.lower().startswith('$coord'):
                while True:
                    coord_line = lines.next().strip()
                    if coord_line.lower().startswith('$end'):
                        break
                    x, y, z, symbol = _whitespace_re.split(coord_line)
                    result.append(Atom(symbol, float(x), float(y), float(z)))
    except StopIteration:
        pass
    return result
                

def turbomol_to_gamess(input_file_name, output_file_name, template_file_name):
    """
    Read TURBOMOL coordinates and output a GAMESS file using the
    specified template.
    """
    atoms = read_turbomole_coords(input_file_name)
    # construct GAMESS $DATA section
    data = str.join('\n',
                    [ ('%-10s %3.1f  %+2.8f  %+2.8f  %+2.8f' 
                       % (a.name, a.nuclear_charge, a.x, a.y, a.z))
                      for a in atoms ])
    # get GAMESS '.inp' template
    f = open(template_file_name, 'rU')
    template = f.read()
    f.close()
    # write output
    f = open(output_file_name, 'w')
    f.write(template % { 'NAME':input_file_name, 'ATOMS':data, })
    f.close()


## main: run tests

if "__main__" == __name__:
    import doctest
    doctest.testmod(name="gamess",
                    optionflags=doctest.NORMALIZE_WHITESPACE)
