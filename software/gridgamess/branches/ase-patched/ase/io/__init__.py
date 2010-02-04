import sys
import os
from os.path import basename
from tarfile import is_tarfile
from zipfile import is_zipfile
from ase.atoms import Atoms
from ase.units import Bohr


def read(filename, index=-1, format=None):
    """Read Atoms object(s) from file.

    filename: str
        Name of the file to read from.
    index: int or slice
        If the file contains several configurations, the last configuration
        will be returned by default.  Use index=n to get configuration
        number n (counting from zero).
    format: str
        Used to specify the file-format.  If not given, the
        file-format will be guessed by the *filetype* function. If
        it's 'babel', will try to use the OpenBabel library.

    Known formats:

    =========================  ===========
    format                     short name
    =========================  ===========
    GPAW restart-file          gpw
    Dacapo netCDF output file  dacapo
    Old ASE netCDF trajectory  nc
    Virtual Nano Lab file      vnl
    ASE pickle trajectory      traj
    GPAW text output           gpaw-text
    CUBE file                  cube
    XCrySDen Structure File    xsf  
    Dacapo text output         dacapo-text
    XYZ-file                   xyz
    VASP POSCAR/CONTCAR file   vasp
    Protein Data Bank          pdb
    FHI-aims geometry file     aims
    FHI-aims output file       aims_out
    VTK XML Image Data         vti
    VTK XML Structured Grid    vts
    VTK XML Unstructured Grid  vtu
    TURBOMOLE coord file       (filename==='coord')
    exciting input             exi
    AtomEye configuration      cfg
    WIEN2k structure file      struct
    =========================  ===========

    """
    if isinstance(filename, str):
        p = filename.rfind('@')
        if p != -1:
            try:
                index = filename[p + 1:]
            except ValueError:
                pass
            else:
                filename = filename[:p]

    if isinstance(index, str):
        index = string2index(index)

    if format is None:
        format = filetype(filename)

    if format.startswith('gpw'):
        import gpaw
        r = gpaw.io.open(filename, 'r')
        positions = r.get('CartesianPositions') * Bohr
        numbers = r.get('AtomicNumbers')
        cell = r.get('UnitCell') * Bohr
        pbc = r.get('BoundaryConditions')
        tags = r.get('Tags')
        magmoms = r.get('MagneticMoments')

        atoms = Atoms(positions=positions,
                      numbers=numbers,
                      cell=cell,
                      pbc=pbc)
        if tags.any():
            atoms.set_tags(tags)
        if magmoms.any():
            atoms.set_initial_magnetic_moments(magmoms)

        return atoms

    if format == 'exi':
        from ase.io.exciting import read_exciting
        return read_exciting(filename, index)

    if format == 'xyz':
        from ase.io.xyz import read_xyz
        return read_xyz(filename, index)

    if format == 'traj':
        from ase.io.trajectory import read_trajectory
        return read_trajectory(filename, index)

    if format == 'cube':
        from ase.io.cube import read_cube
        return read_cube(filename, index)

    if format == 'nc':
        from ase.io.netcdf import read_netcdf
        return read_netcdf(filename, index)

    if format == 'gpaw-text':
        from ase.io.gpawtext import read_gpaw_text
        return read_gpaw_text(filename, index)

    if format == 'dacapo-text':
        from ase.io.dacapo import read_dacapo_text
        return read_dacapo_text(filename)

    if format == 'dacapo':
        from ase.io.dacapo import read_dacapo
        return read_dacapo(filename)
    
    if format == 'xsf':
        from ase.io.xsf import read_xsf
        return read_xsf(filename, index)

    if format == 'vasp':
        from ase.io.vasp import read_vasp
        return read_vasp(filename)
    
    if format == 'mol':
        from ase.io.mol import read_mol
        return read_mol(filename)

    if format == 'pdb':
        from ase.io.pdb import read_pdb
        return read_pdb(filename)

    if format == 'cif':
        from ase.io.cif import read_cif
        return read_cif(filename)

    if format == 'struct':
        from ase.io.struct import read_struct
        return read_struct(filename)

    if format == 'babel':
        from ase.io.babel import read_babel
        return read_babel(filename, index=index)

    if format == 'vti':
        from ase.io.vtkxml import read_vti
        return read_vti(filename)

    if format == 'vts':
        from ase.io.vtkxml import read_vts
        return read_vts(filename)

    if format == 'vtu':
        from ase.io.vtkxml import read_vtu
        return read_vtu(filename)

    if format == 'aims':
        from ase.io.aims import read_aims
        return read_aims(filename)
    
    if format == 'aims_out':
        from ase.io.aims import read_aims_output
        return read_aims_output(filename)

    if format == 'iwm':
        from ase.io.iwm import read_iwm
        return read_iwm(filename)

    if format == 'Cmdft':
        from ase.io.cmdft import read_I_info
        return read_I_info(filename)

    if format == 'tmol':
        from ase.io.turbomole import read_turbomole
        return read_turbomole(filename)

    if format == 'cfg':
        from ase.io.cfg import read_cfg
        return read_cfg(filename)

    raise RuntimeError('That can *not* happen!')


def write(filename, images, format=None, **kwargs):
    """Write Atoms object(s) to file.

    filename: str
        Name of the file to write to.
    images: Atoms object or list of Atoms objects
        A single Atoms object or a list of Atoms objects.
    format: str
        Used to specify the file-format.  If not given, the
        file-format will be taken from suffix of the filename. If
        given as 'babel', will try to use the OpenBabel library.

    The accepted output formats:
  
    =========================  ===========
    format                     short name
    =========================  ===========
    ASE pickle trajectory      traj
    CUBE file                  cube
    XYZ-file                   xyz
    VASP POSCAR/CONTCAR file   vasp
    Protein Data Bank          pdb
    XCrySDen Structure File    xsf
    FHI-aims geometry file     aims
    gOpenMol .plt file         plt  
    Python script              py
    Encapsulated Postscript    eps
    Portable Network Graphics  png
    Persistance of Vision      pov
    VTK XML Image Data         vti
    VTK XML Structured Grid    vts
    VTK XML Unstructured Grid  vtu
    TURBOMOLE coord file       tmol
    exciting                   exi
    AtomEye configuration      cfg
    WIEN2k structure file      struct
    =========================  ===========
  
    The use of additional keywords is format specific.
  
    The ``cube`` and ``plt`` formats accept (plt requires it) a ``data``
    keyword, which can be used to write a 3D array to the file along
    with the nuclei coordinates.
  
    The ``vti``, ``vts`` and ``vtu`` formats are all specifically directed
    for use with MayaVi, and the latter is designated for visualization of
    the atoms whereas the two others are intended for volume data. Further,
    it should be noted that the ``vti`` format is intended for orthogonal
    unit cells as only the grid-spacing is stored, whereas the ``vts`` format
    additionally stores the coordinates of each grid point, thus making it
    useful for volume date in more general unit cells.

    The ``eps``, ``png``, and ``pov`` formats are all graphics formats,
    and accept the additional keywords:

    rotation: str (default '')
      The rotation angles, e.g. '45x,70y,90z'.

    show_unit_cell: int (default 0)
      Can be 0, 1, 2 to either not show, show, or show all of the unit cell.

    radii: array or float (default 1.0)
      An array of same length as the list of atoms indicating the sphere radii.
      A single float specifies a uniform scaling of the default covalent radii.

    bbox: array (default None)
      XXX

    colors: array (default None)
      An array of same length as the list of atoms, indicating the rgb color
      code for each atom. Default is the jmol_colors of ase/data/colors.

    scale: int (default 20)
      Number of pixels per Angstrom.
      
    For the ``pov`` graphics format, ``scale`` should not be specified.
    The elements of the color array can additionally be strings, or 4
    and 5 vectors for named colors, rgb + filter, and rgb + filter + transmit
    specification. This format accepts the additional keywords:

    ``run_povray``, ``display``, ``pause``, ``transparent``,
    ``canvas_width``, ``canvas_height``, ``camera_dist``,
    ``image_plane``, ``camera_type``, ``point_lights``,
    ``area_light``, ``background``, ``textures``, ``celllinewidth``,
    ``bondlinewidth``, ``bondatoms``
    """
    
    if format is None:
        if filename == '-':
            format = 'xyz'
            filename = sys.stdout
        elif 'POSCAR' in filename or 'CONTCAR' in filename:
            format = 'vasp'
        else:
            suffix = filename.split('.')[-1]
            format = {}.get(suffix, suffix) # XXX this does not make sense
            # Maybe like this:
##             format = {'traj': 'trajectory',
##                       'nc': 'netcdf',
##                       'exi': 'exciting',
##                       'in': 'aims',
##                       'tmol': 'turbomole',
##                       }.get(suffix, suffix)

    if format == 'exi':
        from ase.io.exciting import write_exciting
        write_exciting(filename, images)
        return


    if format == 'xyz':
        from ase.io.xyz import write_xyz
        write_xyz(filename, images)
        return
    elif format == 'in':
        format = 'aims'
    elif format == 'tmol':
        from ase.io.turbomole import write_turbomole
        write_turbomole(filename, images)
        return

    format = {'traj': 'trajectory', 'nc': 'netcdf'}.get(format, format)
    name = 'write_' + format

    if format in ['vti', 'vts', 'vtu']:
        format = 'vtkxml'

    if format is None:
        format = filetype(filename)

    try:
        write = getattr(__import__('ase.io.%s' % format, {}, {}, [name]), name)
    except ImportError:
        raise TypeError('Unknown format: "%s".' % format)
    
    write(filename, images, **kwargs)


def string2index(string):
    if ':' not in string:
        return int(string)
    i = []
    for s in string.split(':'):
        if s == '':
            i.append(None)
        else:
            i.append(int(s))
    i += (3 - len(i)) * [None]
    return slice(*i)


def filetype(filename):
    """Try to guess the type of the file."""
    fileobj = open(filename)
    s3 = fileobj.read(3)
    if len(s3) == 0:
        raise IOError('Empty file: ' + filename)
    
    if is_tarfile(filename):
        return 'gpw'

    if s3 == 'CDF':
        from ase.io.pupynere import NetCDFFile
        nc = NetCDFFile(filename)
        if 'number_of_dynamic_atoms' in nc.dimensions:
            return 'dacapo'

        history = nc.history
        if history == 'GPAW restart file':
            return 'gpw-nc'
        if history == 'ASE trajectory':
            return 'nc'
        if history == 'Dacapo':
            return 'dacapo'
        raise IOError('Unknown netCDF file!')

    
    if is_zipfile(filename):
        return 'vnl'

    fileobj.seek(0)
    lines = fileobj.readlines(1000)

    if lines[0].startswith('PickleTrajectory'):
        return 'traj'

    if lines[1].startswith('OUTER LOOP:') or filename.lower().endswith('.cube'):
        return 'cube'
    
    if '  ___ ___ ___ _ _ _  \n' in lines:
        return 'gpaw-text'

    if (' &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n'
        in lines[:90]):
        return 'dacapo-text'

    for word in ['ANIMSTEPS', 'CRYSTAL', 'SLAB', 'POLYMER', 'MOLECULE']:
        if lines[0].startswith(word):
            return 'xsf'

    filename_v = basename(filename) 
    if 'POSCAR' in filename_v or 'CONTCAR' in filename_v:
        return 'vasp'    

    if filename.lower().endswith('.exi'):
        return 'exi'
    
    if filename.lower().endswith('.mol'):
        return 'mol'

    if filename.lower().endswith('.pdb'):
        return 'pdb'

    if filename.lower().endswith('.cif'):
        return 'cif'
   
    if filename.lower().endswith('.struct'):
        return 'struct'

    if filename.lower().endswith('.in'):
        return 'aims'

    if filename.lower().endswith('.out'):
        return 'aims_out'

    if filename.lower().endswith('.cfg'):
        return 'cfg'
        
    if os.path.split(filename)[1] == 'atoms.dat':
        return 'iwm'

    if filename.endswith('I_info'):
        return 'Cmdft'

    if lines[0].startswith('$coord'):
        return 'tmol'
    if s3 == '<?x':
        from ase.io.vtkxml import probe_vtkxml
        xmltype = probe_vtkxml(filename)
        if xmltype == 'ImageData':
            return 'vti'
        elif xmltype == 'StructuredGrid':
            return 'vts'
        elif xmltype == 'UnstructuredGrid':
            return 'vtu'
        elif xmltype is not None:
            raise IOError('Unknown VTK XML file!')

    return 'xyz'
