#!/usr/bin/env python
"""
#Import data from the online GMTKN24 database
(http://toc.uni-muenster.de/GMTKN/GMTKNmain.html).
"""
__author__ = 'Riccardo Murri <riccardo.murri@uzh.ch>'
__changelog__ = '''
'''
__docformat__ = 'reStructuredText'


from __future__ import absolute_import, print_function
import itertools
import logging
import os
import os.path
import re
import sys
import tempfile
import time

from optparse import OptionParser

# for the `Gmtkn24` class
from mechanize import Browser, HTTPError
from BeautifulSoup import BeautifulSoup
from zipfile import ZipFile


PROG = os.path.splitext(os.path.basename(sys.argv[0]))[0]

## parse HTML tables

class HtmlTable(object):
    def __init__(self, html):
        """
        Initialize the `HtmlTable` object from HTML source.
        The `html` argument can be either the text source of
        an HTML fragment, or a BeautifulSource `Tag` object.
        """
        self._headers, self._rows = self.extract_html_table(html)

    def rows_as_dict(self):
        """
        Iterate over table rows, representing each row as a dictionary
        mapping header names into corresponding cell values.
        """
        for row in self._rows:
            yield dict([ (th[0], row[n])
                         for n,th in enumerate(self._headers) ])

    @staticmethod
    def extract_html_table(html):
        """
        Return the table in `html` text as a list of rows, each row being
        a tuple of the values found in table cells.
        """
        if not isinstance(html, BeautifulSoup):
            html = BeautifulSoup(html)
        table = html.find('table')
        # extract headers
        headers = [ ]
        for th in table.findAll('th'):
            text = str.join('', th.find(text=True)).strip()
            if th.has_key('colspan'):
                span = int(th['colspan'])
            else:
                span = 1
            headers.append((text, span))
        # extract rows and group cells according to the TH spans
        spans = [ s for (t,s) in headers ]
        rows = [ list(grouped(row, spans)) 
                      for row in HtmlTable.extract_html_table_rows(table) ]
        # all done
        return (headers, rows)

    @staticmethod
    def extract_html_table_rows(table):
        """
        Return list of rows (each row being a `tuple`) extracted 
        from HTML `Tag` element `table`.
        """
        return [ tuple([ str.join('', td.findAll(text=True)).strip()
                         for td in tr.findAll('td') ])
                 for tr in table.findAll('tr') ]

def grouped(iterable, pattern, container=tuple):
    """
    Iterate over elements in `iterable`, grouping them into 
    batches: the `n`-th batch has length given by the `n`-th
    item of `pattern`.  Each batch is cast into an object 
    of type `container`.

    Examples::

      >>> l = [0,1,2,3,4,5]
      >>> list(grouped(l, [1,2,3]))
      [0, (1, 2), (3, 4, 5)]
    """
    iterable = iter(iterable) # need a real iterator for tuples and lists
    for l in pattern:
        yield container(itertools.islice(iterable, l))


## interact with the online DB

class Gmtkn24(object):
    """
    Interact with the online web pages of GMTKN24.
    """
    BASE_URL = 'http://toc.uni-muenster.de/GMTKN/GMTKN24/'
    def __init__(self):
        # initialization
        self._browser = Browser()
        self._browser.set_handle_robots(False)
        self._subsets = self._list_subsets()

    _subset_link_re = re.compile("The (.+) subset")
    #_subset_link_re = re.compile("here")
    def _list_subsets(self):
        """Return dictionary mapping GMTKN24 subset names to download URLs."""
        html = BeautifulSoup(self._browser.open(Gmtkn24.BASE_URL + 'GMTKN24main.html'))
        links = html.findAll(name="a")
        result = { }
        for a in links:
	     if a.string is not None:
                match = Gmtkn24._subset_link_re.match(a.string)
                if match is not None:
                    print a
		    # if a subset has several names, add all of them
                    for name in match.group(1).split(' '):
                        if name == 'and':
                            continue
                        result[name] = Gmtkn24.BASE_URL + a['href']
        print result
	#result = ['google.com', 'cnn.com']
	return result

    def list(self):
        """Return dictionary mapping GMTKN24 subset names to download URLs."""
        return self._subsets

    def get_geometries(self, subset, output_dir='geometries'):
        """
        Download geometry files for the specified GMTKN24 subset,
        and save them into the 'geometries/' subdirectory of the
        current working directory.

        Return list of extracted molecules/filenames.
        """
        subset_url = self._subsets[subset]
        page = self._browser.open(subset_url)
        # must download the zip to a local file -- zipfiles are not stream-friendly ...
        geometries_url = self._browser.click_link(text_regex=re.compile("^Geometries"))
        (filename, headers) = self._browser.retrieve(geometries_url)
        logger.info("%s geometries downloaded into file '%s'", subset, filename)
        geometries_zip = ZipFile(filename, 'r')
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        molecules = self.get_molecule_names(subset)
        extracted = list()
        names = geometries_zip.namelist()
        for name in names:
            basename = os.path.basename(name)
            if basename not in molecules and basename != 'README':
                continue
            # zipfile's `extract` method preserves full pathname, 
            # so let's get the data from the archive and write
            # it in the file WE want...
            content = geometries_zip.read(name)
            output_path = os.path.join(output_dir, basename)
            output = open(output_path, 'w')
            output.write(content)
            output.close()
            if not ('README' == basename):
                extracted.append(basename)
            logger.info("Extracted '%s' into '%s'", basename, output_path)
        geometries_zip.close()
        return extracted

    def get_reference_data(self, subset):
        """
        Iterate over stoichiometry reference data in a given GMTKN24
        subset.  Each returned value is a pair `(r, d)`, where `r` is
        a dictionary mapping compound names (string) to their
        stoichiometric coefficient (integer), and `d` is a (float)
        number representing the total energy.
        """
        subset_url = self._subsets[subset]
        subset_page = self._browser.open(subset_url)
        if subset in ['BH76', 'BH76RC']:
            # special case
            self._browser.follow_link(text=("Go to the %s subset" % subset))
        refdata_page = self._browser.follow_link(text="Reference data")
        table = HtmlTable(refdata_page.read())
        for row in table.rows_as_dict():
            if subset == 'W4-08woMR':
                # The 16 entries marked with an asterisk (*) are not
                # part of the W4-08woMR subset.
                if row['#'] and row['#'][0].endswith('*'):
                    continue
            reactants = row['Systems']
            if len(reactants) == 0:
                continue # ignore null rows
            qtys = row['Stoichiometry']
            refdata = float(row['Ref.'][0])
            reaction = { }
            for n,sy in enumerate(reactants):
                if qtys[n] == '':
                    continue # skip null fields
                reaction[sy] = int(qtys[n])
            yield (reaction, refdata)

    def get_molecule_names(self, subset):
        """Return set of molecule names belonging in the specified subset."""
        # The only generic way to list molecule names seems to be:
        # take the systems names from the ref.data table.
        molecules = set()
        for reaction,data in self.get_reference_data(subset):
            for molecule in reaction:
                molecules.add(molecule)
        return molecules


## TURBOMOL to GAMESS conversion

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
    def __init__(self, transform=None, **extra_args):
        if transform is not None:
            self.__dict__['_transform'] = transform
        else:
            def same(x):
                return x
            self.__dict__['_transform'] = same
        for k,v in extra_args.items():
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
    def __init__(self, **extra_args):
        Struct.__init__(self, transform = lambda key: key.upper(), **extra_args)


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

_whitespace_re = re.compile(r'\s+', re.X)

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
                

def turbomol_to_gamess(input_file_name, output_file_name, template_file_name, **extra_args):
    """
    Read TURBOMOL coordinates and output a GAMESS file using the
    specified template.
    """
    atoms = read_turbomole_coords(input_file_name)
    # construct GAMESS $DATA section
    data = str.join('\n',
                    [ ('%-10s %3.1f  %+2.8f  %+2.8f  %+2.8f' 
                       % (a.symbol, a.nuclear_charge, a.x, a.y, a.z))
                      for a in atoms ])
    # get GAMESS '.inp' template
    f = open(template_file_name, 'rU')
    template = f.read()
    f.close()
    # write output
    f = open(output_file_name, 'w')
    extra_args['NAME'] = os.path.basename(input_file_name)
    extra_args['ATOMS'] = data
    f.write(template % extra_args)
    f.close()


## parse command-line options

cmdline = OptionParser(PROG + " [options] ACTION [SUBSET]",
                       description="""
Imports geometries and reference data from the GMTKN24 database into
GAMESS `.inp` format.  The actual behavior of this script depends on
the ACTION pargument.

If ACTION is "list", a list of all available subsets is printed on the
standard output.

If ACTION is "import", the specified subset is imported into the
current directory.  If no SUBSET is specified, *all* available subsets
are converted.
""")
cmdline.add_option("-d", "--destination-dir", dest="destdir", metavar="DIR",
                   default=os.path.join(os.getcwd(), 'wiki'),
                   help="Import data into the specified directory."
                   " Default: %default")
cmdline.add_option("-t", "--template", dest="template", metavar="TEMPLATE",
                   default="etc/gmtkn24.inp.template",
                   help="Use the specified template file for constructing a GAMESS '.inp'"
                   " input file from GMTKN24 molecule geometries.  (Default: '%default')"
                   )
cmdline.add_option("-v", "--verbose", dest="verbose", action="count", default=0, 
                   help="Verbosely report about program operation and progress."
                   )
(options, args) = cmdline.parse_args()


## main

logging.basicConfig(level=max(1, logging.ERROR - 10 * options.verbose),
                    format='%(name)s: %(message)s')
logger = logging.getLogger(PROG)


def get_required_argument(args, argno=1, argname='SUBSET'):
    try:
        return args[argno]
    except:
        logger.critical("Missing required argument %s."
                        " Aborting now; type '%s --help' to get usage help.", 
                        argname, PROG)
        raise
def get_optional_argument(args, argno=1, argname="SUBSET", default=None):
    try:
        return args[argno]
    except:
        logger.info("Missing argument %s, using default '%s'", argname, default)
        return default


if "__main__" == __name__:
    if len(args) < 1:
        logger.critical("%s: Need at least the ACTION argument."
                        " Type '%s --help' to get usage help."
                        % (PROG, PROG))
        sys.exit(1)

    try:
        if 'import' == args[0]:
            gmtkn24 = Gmtkn24()
            subset = get_optional_argument(args, 1, "SUBSET", None)
            subset_urls = gmtkn24.list()
            if subset:
                subsets = [ subset ]
            else:
                subsets = [ name for name in subset_urls.keys() ]
            tempdir = tempfile.mkdtemp(prefix=PROG)
            destdir = options.destdir
            if not os.path.isabs(destdir):
                destdir = os.path.join(os.getcwd(), destdir)
            template = options.template
            for subset in subsets:
                url = subset_urls[subset]
                if not os.path.exists(os.path.join(destdir, subset)):
                    os.mkdir(os.path.join(destdir, subset))
                ## download geometries and convert to GAMESS .inp format
                logger.info("Downloading %s geometries into '%s' ...", subset, destdir)
                molecules = gmtkn24.get_geometries(subset, tempdir)
                # load GAMESS parameters SCFTYP, ICHARG, MULT with default values
                params = { }
                for molecule in molecules:
                    params[molecule] = { 'SCFTYP':'RHF', 'ICHARG':0, 'MULT':1 }
                readme = os.path.join(tempdir, 'README')
                if os.path.exists(readme):
                    charge_or_unpaired_re = re.compile(r'(?P<molecule>[a-z0-9+_,-]+) \s+'
                                                       r'\((?P<charge_or_unpaired>[+-]?[0-9]+)\)', 
                                                       re.I|re.X)
                    readme_file = open(readme, 'r')
                    for line in readme_file:
                        match = charge_or_unpaired_re.match(line)
                        if match:
                            molecule = match.group('molecule')
                            if molecule not in molecules:
                                logger.warning("Ignoring molecule '%s': mentioned by README file,"
                                               " but not listed in %s stoichiometry data",
                                               molecule, subset)
                                continue
                            charge_or_unpaired = match.group('charge_or_unpaired')
                            if charge_or_unpaired.startswith('+') or charge_or_unpaired.startswith('-'):
                                # signed value, must be charge
                                charge = int(charge_or_unpaired)
                                params[molecule]['ICHARG'] = charge
                                params[molecule]['SCFTYP'] = 'UHF'
                                logger.info("Will use ICHARG=%s for molecule %s", charge, molecule)
                            else:
                                # unsigned value, must be unpaired electron no.
                                mult = 1 + int(charge_or_unpaired)
                                params[molecule]['MULT'] = mult
                                if mult != 1:
                                    params[molecule]['SCFTYP'] = 'UHF'
                                logger.info("Will use MULT=%s for molecule %s", mult, molecule)
                    readme_file.close()
                else:
                    logger.info("No README file accompanies geometries; will use default: SCFTYP=RHF, ICHARG=0 and MULT=1")
                for molecule in molecules:
                    destfile = os.path.join(destdir, subset, molecule + '.inp')
                    logger.info("Generating GAMESS input file '%s' from geometry '%s' and template '%s'...",
                                destfile, molecule, template)
                    turbomol_to_gamess(tempdir + '/' + molecule, destfile, template, 
                                       **params[molecule])
                ## write out reference data in `.csv` format
                refdata = list(gmtkn24.get_reference_data(subset))
                refdata_csv = os.path.join(destdir, subset, 'refdata.csv')
                output = open(refdata_csv, 'w')
                maxwidth = max([ len(reaction) for reaction,energy in refdata ])
                output.write("#;" + 
                             "Systems" + (';' * maxwidth) +
                             "Stoichiometry" + (';' * maxwidth) +
                             "Ref.\n")
                for seqno, data in enumerate(refdata):
                    reaction, energy = data
                    width = len(reaction)
                    systems = str.join(";", reaction.keys() + ([" "] * (maxwidth - width)))
                    stoichiometry = str.join(";", [ str(coeff) for coeff in reaction.values() ]
                                             + ([" "] * (maxwidth - width)))
                    output.write("%d;%s;%s;%.2f\n" % (seqno, systems, stoichiometry, energy))
                output.close()
                ## finally, write subset description page
                output = open(os.path.join(destdir, subset, 'index.mdwn'), 'w')
                output.write("""
[[!tag subset %(database)s %(subset)s]]

Reference data
--------------

For each reaction, the relevant systems' names, the stoichiometry and the reference value are given.
The systems' names refer to the geometry files (see section "GAMESS input" below).
Negative values in the stoichiometry columns refer to reactants, positive values to products. 

<!-- DO NO CHANGE THE "table" LINE BELOW: it generates the table of ref.data
     If you want to edit/replace the reference data, 
     use the `Attachments` button and replace the `refdata.csv` file -->
[[!table class="refdata" delimiter=";" file="subsets/%(subset)s/refdata.csv"]]


Reference GAMESS input
----------------------
<!-- DO NO CHANGE THE "map" LINE BELOW: it generates the list of GAMESS input files.
     If you want to edit/replace the `.inp` files, use the `Attachments` button. -->
[[!map pages="glob(subsets/%(subset)s/*.inp)"]]


--------------------------------
These data have initially been imported from the online
[%(database)s database](%(database_url)s)
""" % { 'database':'GMTKN24',
        'database_url':url,
        'subset':subset, 
        # XXX: URLs need to be relative to the Wiki top-level?
        'refdata_csv':os.path.join(subset, os.path.basename(refdata_csv))
        })
                output.close()
                                 

        elif 'list' == args[0]:
            subset = get_optional_argument(args, 1, "SUBSET")
            if subset:
                print ("Molecules in the %s subset:" % subset)
                for name in Gmtkn24().get_molecule_names(subset):
                    print ("  %s" % name)
            else:
                print ("Available subsets of GMTKN24:")
                ls = Gmtkn24().list()
                for name, url in ls.items():
                    print ("  %s --> %s" % (name, url))
            
        elif 'refdata' == args[0]:
            subset = get_required_argument(args, 1, "SUBSET")
            for r,d in Gmtkn24().get_reference_data(subset):
                print ("%s = %.3f" 
                       % (str.join(' + ', 
                                   [ ("%d*%s" % (qty, sy)) for sy,qty in r.items() ]), 
                          d))

        elif 'doctest' == args[0]:
            import doctest
            doctest.testmod(name="gmtkn24",
                            optionflags=doctest.NORMALIZE_WHITESPACE)

        else:
            logger.critical("Unknown ACTION word '%s'."
                            " Type '%s --help' to get usage help."
                            % (args[0], PROG))
        sys.exit(1)

    except HTTPError, x:
        logger.critical("HTTP error %d requesting page: %s" % (x.code, x.msg))
        sys.exit(1)

