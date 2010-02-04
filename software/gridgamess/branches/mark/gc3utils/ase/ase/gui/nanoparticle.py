# encoding: utf-8
"""nanoparticle.py - Window for setting up crystalline nanoparticles.
"""

import gtk
from ase.gui.widgets import pack, cancel_apply_ok, oops
from ase.gui.setupwindow import SetupWindow
from ase.gui.pybutton import PyButton
import ase
import numpy as np
# Delayed imports:
# ase.cluster.data

introtext = """\
You specify the size of the particle by specifying the number of atomic layers
in the different low-index crystal directions.  Often, the number of layers is
specified for a family of directions, but they can be given individually.

When the particle is created, the actual numbers of layers are printed, they
may be less than specified if a surface is cut of by other surfaces."""

py_template = """
from ase.cluster.cluster import Cluster
import ase

layers = %(layers)s
atoms = Cluster(symbol='%(element)s', layers=layers, latticeconstant=%(a).5f,
                symmetry='%(structure)s')

# OPTIONAL: Cast to ase.Atoms object, discarding extra information:
# atoms = ase.Atoms(atoms)
"""

class SetupNanoparticle(SetupWindow):
    "Window for setting up a nanoparticle."
    families = {'fcc': [(0,0,1), (0,1,1), (1,1,1)]}
    defaults = {'fcc': [6, 9, 5]}
    
    def __init__(self, gui):
        SetupWindow.__init__(self)
        self.set_title("Nanoparticle")
        self.atoms = None
        import ase.cluster.data
        self.data_module = ase.cluster.data
        import ase.cluster.cluster
        self.Cluster = ase.cluster.cluster.Cluster
        self.no_update = True
        
        vbox = gtk.VBox()

        # Intoductory text
        self.packtext(vbox, introtext)
           
        # Choose the element
        label = gtk.Label("Element: ")
        label.set_alignment(0.0, 0.2)
        element = gtk.Entry(max=3)
        self.element = element
        lattice_button = gtk.Button("Get structure")
        lattice_button.connect('clicked', self.get_structure)
        self.elementinfo = gtk.Label(" ")
        pack(vbox, [label, element, self.elementinfo, lattice_button], end=True)
        self.element.connect('activate', self.update)
        self.legal_element = False

        # The structure and lattice constant
        label = gtk.Label("Structure: ")
        self.structure = gtk.combo_box_new_text()
        self.allowed_structures = ('fcc',)
        for struct in self.allowed_structures:
            self.structure.append_text(struct)
        self.structure.set_active(0)
        self.structure.connect('changed', self.update)
        
        label2 = gtk.Label("   Lattice constant: ")
        self.lattice_const = gtk.Adjustment(3.0, 0.0, 1000.0, 0.01)
        lattice_box = gtk.SpinButton(self.lattice_const, 10.0, 3)
        lattice_box.numeric = True
        pack(vbox, [label, self.structure, label2, lattice_box])
        self.lattice_const.connect('value-changed', self.update)
        pack(vbox, gtk.Label(""))

        # The number of layers
        pack(vbox, [gtk.Label("Number of layers:")])
        self.layerbox = gtk.VBox()
        pack(vbox, self.layerbox)
        self.make_layer_gui()

        # Information
        label1 = gtk.Label("Number of atoms: ")
        self.natoms_label = gtk.Label("-")
        label2 = gtk.Label("   Avg. diameter: ")
        self.dia1_label = gtk.Label("-")
        label3 = gtk.Label("   Volume-based diameter: ")
        self.dia2_label = gtk.Label("-")
        pack(vbox, [label1, self.natoms_label, label2, self.dia1_label,
                    label3, self.dia2_label])
        pack(vbox, gtk.Label(""))
        
        # Buttons
        self.pybut = PyButton("Creating a nanoparticle.")
        self.pybut.connect('clicked', self.makeatoms)
        buts = cancel_apply_ok(cancel=lambda widget: self.destroy(),
                               apply=self.apply,
                               ok=self.ok)
        pack(vbox, [self.pybut, buts], end=True, bottom=True)
        self.auto = gtk.CheckButton("Automatic Apply")
        fr = gtk.Frame()
        fr.add(self.auto)
        fr.show_all()
        pack(vbox, [fr], end=True, bottom=True)
        
        # Finalize setup
        self.add(vbox)
        vbox.show()
        self.show()
        self.gui = gui
        self.no_update = False
        
    def update(self, *args):
        if self.no_update:
            return
        self.update_element()
        if self.auto.get_active():
            self.makeatoms()
            if self.atoms is not None:
                self.gui.new_atoms(self.atoms)
        else:
            self.clearatoms()
        self.makeinfo()

    def get_structure(self, *args):
        if not self.update_element():
            oops("Invalid element.")
            return
        z = ase.atomic_numbers[self.legal_element]
        ref = ase.data.reference_states[z]
        if ref is None:
            structure = None
        else:
            structure = ref['symmetry'].lower()
                
        if ref is None or not structure in self.allowed_structures:
            oops("Unsupported or unknown structure",
                 "Element = %s,  structure = %s" % (self.legal_element,
                                                    structure))
            return
        for i, s in enumerate(self.allowed_structures):
            if structure == s:
                self.structure.set_active(i)
        a = ref['a']
        self.lattice_const.set_value(a)

    def make_layer_gui(self):
        "Make the part of the gui specifying the layers of the particle"
        # Clear the box
        children = self.layerbox.get_children()
        for c in children:
            self.layerbox.remove(c)
        del children

        # Get the crystal structure
        struct = self.structure.get_active_text()
        # Get the surfaces in the order the ase.cluster module expects
        surfaces = self.data_module.lattice[struct]['surface_names']
        # Get the surface families
        families = self.families[struct]
        defaults = self.defaults[struct]
        
        # Empty array for the gtk.Adjustments for the layer numbers
        self.layers = [None] * len(surfaces)
        self.layer_lbl = [None] * len(surfaces)
        self.layer_spin = [None] * len(surfaces)
        self.layer_owner = [None] * len(surfaces)
        self.layer_label = [None] * len(surfaces)
        self.famlayers = [None] * len(families)
        self.infamily = [None] * len(families)
        self.family_label = [None] * len(families)
        
        # Now, make a box for each family of surfaces
        frames = []
        for i in range(len(families)):
            family = families[i]
            default = defaults[i]
            frames.append(self.make_layer_family(i, family, surfaces, default))
        for a in self.layers:
            assert a is not None

        pack(self.layerbox, frames)
        self.layerbox.show_all()

    def make_layer_family(self, n, family, surfaces, default=1):
        """Make a frame box for a single family of surfaces.

        The layout is a frame containing a table.  For example

        {0,0,1}, SPIN, EMPTY, EMPTY
        -- empty line --
        (0,0,1), SPIN, Label(actual), Checkbox
        ...
        """
        tbl = gtk.Table(2, 4)
        lbl = gtk.Label("{%i,%i,%i}: " % family)
        lbl.set_alignment(1, 0.5)
        tbl.attach(lbl, 0, 1, 0, 1)
        famlayers = gtk.Adjustment(default, 1, 100, 1)
        tbl.attach(gtk.SpinButton(famlayers, 0, 0),
                   2, 3, 0, 1)
        tbl.attach(gtk.Label(" "), 0, 1, 1, 2)
        assert self.famlayers[n] is None
        self.famlayers[n] = famlayers
        self.infamily[n] = []
        self.family_label[n] = gtk.Label("")
        tbl.attach(self.family_label[n], 1, 2, 0, 1)
        row = 2
        myspin = []
        for i, s in enumerate(surfaces):
            s2 = [abs(x) for x in s]
            s2.sort()
            if tuple(s2) == family:
                self.infamily[n].append(i)
                tbl.resize(row+1, 4)
                lbl = gtk.Label("(%i,%i,%i): " % s)
                lbl.set_alignment(1, 0.5)
                tbl.attach(lbl, 0, 1, row, row+1)
                label = gtk.Label("    ")
                tbl.attach(label, 1, 2, row, row+1)
                self.layer_label[i] = label
                lay = gtk.Adjustment(default, 0, 100, 1)
                lay.connect('value-changed', self.update)
                spin = gtk.SpinButton(lay, 0, 0)
                spin.set_sensitive(False)
                tbl.attach(spin, 2, 3, row, row+1)
                assert self.layers[i] is None
                self.layers[i] = lay
                self.layer_lbl[i] = lbl
                self.layer_spin[i] = spin
                self.layer_owner[i] = n
                myspin.append(spin)
                chkbut = gtk.CheckButton()
                tbl.attach(chkbut, 3, 4, row, row+1)
                chkbut.connect("toggled", self.toggle_surface, i)
                row += 1
        famlayers.connect('value-changed', self.changed_family_layers, myspin)
        vbox = gtk.VBox()
        vbox.pack_start(tbl, False, False, 0)
        fr = gtk.Frame()
        fr.add(vbox)
        fr.show_all()
        return fr

    def toggle_surface(self, widget, number):
        "Toggle whether a layer in a family can be specified."
        active = widget.get_active()
        self.layer_spin[number].set_sensitive(active)
        if not active:
            self.layers[number].value = \
                self.famlayers[self.layer_owner[number]].value
        
    def changed_family_layers(self, widget, myspin):
        "Change the number of layers in inactive members of a family."
        self.no_update = True
        x = widget.value
        for s in myspin:
            if s.state == gtk.STATE_INSENSITIVE:
                adj = s.get_adjustment()
                if adj.value != x:
                    adj.value = x
        self.no_update = False
        self.update()
                    
    def makeatoms(self, *args):
        "Make the atoms according to the current specification."
        if not self.update_element():
            self.clearatoms()
            self.makeinfo()
            return False
        assert self.legal_element is not None
        layers = [int(x.value) for x in self.layers]
        struct = self.structure.get_active_text()
        lc = self.lattice_const.value
        self.atoms = self.Cluster(self.legal_element, layers=layers,
                                  latticeconstant=lc, symmetry=struct)
        self.pybut.python = py_template % {'element': self.legal_element,
                                     'layers': str(layers),
                                     'structure': struct,
                                     'a': lc}
        self.makeinfo()

    def clearatoms(self):
        self.atoms = None
        self.pybut.python = None

    def makeinfo(self):
        "Fill in information field about the atoms."
        if self.atoms is None:
            self.natoms_label.set_label("-")
            self.dia1_label.set_label("-")
            self.dia2_label.set_label("-")
            for label in self.layer_label+self.family_label:
                label.set_text("    ")
        else:
            self.natoms_label.set_label(str(len(self.atoms)))
            self.dia1_label.set_label("%.1f Å" % (self.atoms.get_diameter(),))
            s = self.structure.get_active_text()
            a = self.lattice_const.value
            if s == 'fcc':
                at_vol = a**3 / 4
            else:
                raise RuntimeError("Unknown structure: "+s)
            dia = 2 * (3 * len(self.atoms) * at_vol / (4 * np.pi))**(1.0/3.0)
            self.dia2_label.set_label("%.1f Å" % (dia,))
            actual = self.atoms.get_layers()
            for i, label in enumerate(self.layer_label):
                label.set_text("%2i " % (actual[i],))
            for i, label in enumerate(self.family_label):
                relevant = actual[self.infamily[i]]
                if relevant.min() == relevant.max():
                    label.set_text("%2i " % (relevant[0]))
                else:
                    label.set_text("-- ")
            
    def apply(self, *args):
        self.makeatoms()
        if self.atoms is not None:
            self.gui.new_atoms(self.atoms)
            return True
        else:
            oops("No valid atoms.",
                 "You have not (yet) specified a consistent set of parameters.")
            return False

    def ok(self, *args):
        if self.apply():
            self.destroy()
            
