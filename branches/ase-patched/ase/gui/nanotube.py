# encoding: utf-8
"""nanotube.py - Window for setting up Carbon nanotubes and similar tubes.
"""

import gtk
from ase.gui.widgets import pack, cancel_apply_ok, oops
from ase.gui.setupwindow import SetupWindow
from ase.gui.pybutton import PyButton
from ase.structure import nanotube
import ase
import numpy as np

introtext = """\
Set up a Carbon nanotube by specifying the (n,m) roll-up vector.
Please note that m <= n.

Nanotubes of other elements can be made by specifying the element
and bond length.\
"""

py_template = """
from ase.structure import nanotube

atoms = nanotube(%(n)i, %(m)i, length=%(length)i, bond=%(bl).3f, symbol=%(symb)s)
"""


class SetupNanotube(SetupWindow):
    "Window for setting up a (Carbon) nanotube."
    def __init__(self, gui):
        SetupWindow.__init__(self)
        self.set_title("Nanotube")
        vbox = gtk.VBox()

        # Intoductory text
        self.packtext(vbox, introtext)
           
        # Choose the element and bond length
        label1 = gtk.Label("Element: ")
        #label.set_alignment(0.0, 0.2)
        self.element = gtk.Entry(max=3)
        self.element.set_text("C")
        self.element.connect('activate', self.update_element)
        self.bondlength = gtk.Adjustment(1.42, 0.0, 1000.0, 0.01)
        label2 = gtk.Label("  Bond length: ")
        label3 = gtk.Label("Å")
        bond_box = gtk.SpinButton(self.bondlength, 10.0, 3)
        pack(vbox, [label1, self.element, label2, bond_box, label3])
        self.elementinfo = gtk.Label("")
        self.elementinfo.modify_fg(gtk.STATE_NORMAL,
                                   gtk.gdk.color_parse('#FF0000'))
        pack(vbox, [self.elementinfo])
        pack(vbox, gtk.Label(""))

        # Choose the structure.
        pack(vbox, [gtk.Label("Select roll-up vector (n,m) and tube length:")])
        label1 = gtk.Label("n: ")
        label2 = gtk.Label("  m: ")
        self.n = gtk.Adjustment(6, 1, 100, 1)
        self.m = gtk.Adjustment(0, 0, 100, 1)
        spinn = gtk.SpinButton(self.n, 0, 0)
        spinm = gtk.SpinButton(self.m, 0, 0)
        label3 = gtk.Label("  Length: ")
        self.length = gtk.Adjustment(1, 1, 100, 1)
        spinl = gtk.SpinButton(self.length, 0, 0)
        pack(vbox, [label1, spinn, label2, spinm, label3, spinl])
        self.err = gtk.Label("")
        self.err.modify_fg(gtk.STATE_NORMAL, gtk.gdk.color_parse('#FF0000'))
        pack(vbox, [self.err])
        pack(vbox, gtk.Label(""))
        self.n.connect('value-changed', self.update_n)
        self.m.connect('value-changed', self.update_m)

        # Buttons
        self.pybut = PyButton("Creating a nanoparticle.")
        self.pybut.connect('clicked', self.makeatoms)
        buts = cancel_apply_ok(cancel=lambda widget: self.destroy(),
                               apply=self.apply,
                               ok=self.ok)
        pack(vbox, [self.pybut, buts], end=True, bottom=True)

        # Finalize setup
        self.add(vbox)
        vbox.show()
        self.show()
        self.gui = gui

    def update_n(self, *args):
        if self.m.value > self.n.value:
            self.m.value = self.n.value
            self.err.set_text("m decreased! (m may not be larger than n.)")
        else:
            self.err.set_text("")
            
    def update_m(self, *args):
        if self.m.value > self.n.value:
            self.n.value = self.m.value
            self.err.set_text("n increased! (m may not be larger than n.)")
        else:
            self.err.set_text("")

    def update_element(self, *args):
        "Called when a new element may have been entered."
        # Assumes the element widget is self.element and that a label
        # for errors is self.elementinfo.  The chemical symbol is
        # placed in self.legalelement - or None if the element is
        # invalid.
        elem = self.element.get_text()
        if not elem:
            self.invalid_element("  No element specified!")
            return False
        try:
            z = int(elem)
        except ValueError:
            # Probably a symbol
            try:
                z = ase.data.atomic_numbers[elem]
            except KeyError:
                self.invalid_element()
                return False
        try:
            symb = ase.data.chemical_symbols[z]
        except KeyError:
            self.invalid_element()
            return False
        self.elementinfo.set_text("")
        self.legal_element = symb
        return True
        
    def makeatoms(self, *args):
        self.update_element()
        if self.legal_element is None:
            self.atoms = None
            self.pybut.python = None
        else:
            n = int(self.n.value)
            m = int(self.m.value)
            symb = self.legal_element
            length = int(self.length.value)
            bl = self.bondlength.value
            self.atoms = nanotube(n, m, length=length, bond=bl, symbol=symb)
            self.pybut.python = py_template % {'n': n, 'm':m, 'length':length,
                                               'symb':symb, 'bl':bl}
        

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
            

