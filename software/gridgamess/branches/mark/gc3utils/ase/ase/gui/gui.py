
# husk:
# Exit*2?  remove pylab.show()
# close button
# DFT
# ADOS
# grey-out stuff after one second: vmd, rasmol, ...
# Show with ...
# rasmol: set same rotation as ag
# Graphs: save, Python, 3D
# start from python (interactive mode?)
# ascii-art option (colored)
# option -o (output) and -f (force overwrite)
# surfacebuilder
# screen-dump
# icon
# ag-community-server
# translate option: record all translations, and check for missing translations.

import os
import sys
import weakref
import numpy as np

import gtk
from ase.gui.view import View
from ase.gui.status import Status
from ase.gui.widgets import pack, help, Help, oops
from ase.gui.languages import translate as _
from ase.gui.settings import Settings
from ase.gui.surfaceslab import SetupSurfaceSlab
from ase.gui.nanoparticle import SetupNanoparticle


ui_info = """\
<ui>
  <menubar name='MenuBar'>
    <menu action='FileMenu'>
      <menuitem action='Open'/>
      <menuitem action='New'/>
      <menuitem action='Save'/>
      <separator/>
      <menuitem action='Quit'/>
    </menu>
    <menu action='EditMenu'>
      <menuitem action='SelectAll'/>
      <menuitem action='Invert'/>
      <menuitem action='SelectConstrained'/>
      <separator/>
      <menuitem action='First'/>
      <menuitem action='Previous'/>
      <menuitem action='Next'/>
      <menuitem action='Last'/>
    </menu>
    <menu action='ViewMenu'>
      <menuitem action='ShowUnitCell'/>
      <menuitem action='ShowAxes'/>
      <menuitem action='ShowBonds'/>
      <separator/>
      <menuitem action='Repeat'/>
      <menuitem action='Rotate'/>
      <menuitem action='Focus'/>
      <menuitem action='ZoomIn'/>
      <menuitem action='ZoomOut'/>
      <menuitem action='Settings'/>
      <menuitem action='VMD'/>
      <menuitem action='RasMol'/>
      <menuitem action='XMakeMol'/>
    </menu>
    <menu action='ToolsMenu'>
      <menuitem action='Graphs'/>
      <menuitem action='Movie'/>
      <menuitem action='Modify'/>
      <menuitem action='Constraints'/>
      <menuitem action='NEB'/>
      <menuitem action='BulkModulus'/>
    </menu>
    <menu action='SetupMenu'>
      <menuitem action='Surface'/>
      <menuitem action='Nanoparticle'/>
    </menu>
    <menu action='HelpMenu'>
      <menuitem action='About'/>
      <menuitem action='Webpage'/>
      <menuitem action='Debug'/>
    </menu>
  </menubar>
</ui>"""

class GUI(View, Status):
    def __init__(self, images, rotations='', show_unit_cell=True,
                 show_bonds=False):
        self.images = images
        self.window = gtk.Window(gtk.WINDOW_TOPLEVEL)
        #self.window.set_icon(gtk.gdk.pixbuf_new_from_file('guiase.png'))
        self.window.set_position(gtk.WIN_POS_CENTER)
        #self.window.connect("destroy", lambda w: gtk.main_quit())
        self.window.connect('delete_event', self.exit)
        vbox = gtk.VBox()
        self.window.add(vbox)
        if gtk.pygtk_version < (2, 12):
            self.set_tip = gtk.Tooltips().set_tip

        actions = gtk.ActionGroup("Actions")
        actions.add_actions([
            ('FileMenu', None, '_File'),
            ('EditMenu', None, '_Edit'),
            ('ViewMenu', None, '_View'  ),
            ('ToolsMenu', None, '_Tools'),
            ('SetupMenu', None, '_Setup'),
            ('HelpMenu', None, '_Help'),
            ('Open', gtk.STOCK_OPEN, '_Open', '<control>O',
             'Create a new file',
             self.open),
            ('New', gtk.STOCK_NEW, '_New', '<control>N',
             'New ase.gui window',
             lambda widget: os.system('ag &')),
            ('Save', gtk.STOCK_SAVE, '_Save', '<control>S',
             'Save current file',
             self.save),
            ('Quit', gtk.STOCK_QUIT, '_Quit', '<control>Q',
             'Quit',
             self.exit),
            ('SelectAll', None, 'Select _all', None,
             '',
             self.select_all),
            ('Invert', None, '_Invert selection', None,
             '',
             self.invert_selection),
            ('SelectConstrained', None, 'Select _constrained atoms', None,
             '',
             self.select_constrained_atoms),
            ('First', gtk.STOCK_GOTO_FIRST, '_First image', 'Home',
             '',
             self.step),
            ('Previous', gtk.STOCK_GO_BACK, '_Previous image', 'Page_Up',
             '',
             self.step),
            ('Next', gtk.STOCK_GO_FORWARD, '_Next image', 'Page_Down',
             '',
             self.step),
            ('Last', gtk.STOCK_GOTO_LAST, '_Last image', 'End',
             '',
             self.step),
            ('Repeat', None, 'Repeat ...', None,
             '',
             self.repeat_window),
            ('Rotate', None, 'Rotate ...', None,
             '',
             self.rotate_window),
            ('Focus', gtk.STOCK_ZOOM_FIT, 'Focus', 'F',
             '',
             self.focus),
            ('ZoomIn', gtk.STOCK_ZOOM_IN, 'Zoom in', 'plus',
             '',
             self.zoom),
            ('ZoomOut', gtk.STOCK_ZOOM_OUT, 'Zoom out', 'minus',
             '',
             self.zoom),
            ('Settings', gtk.STOCK_PREFERENCES, 'Settings ...', None,
             '',
             self.settings),
            ('VMD', None, 'VMD', None,
             '',
             self.external_viewer),
            ('RasMol', None, 'RasMol', None,
             '',
             self.external_viewer),
            ('XMakeMol', None, 'xmakemol', None,
             '',
             self.external_viewer),
            ('Graphs', None, 'Graphs ...', None,
             '',
             self.plot_graphs),
            ('Movie', None, 'Movie ...', None,
             '',
             self.movie),
            ('Modify', None, 'Modify ...', None,
             '',
             self.execute),
            ('Constraints', None, 'Constraints ...', None,
             '',
             self.constraints_window),
            ('DFT', None, 'DFT ...', None,
             '',
             self.dft_window),
            ('NEB', None, 'NE_B', None,
             '',
             self.NEB),
            ('BulkModulus', None, 'B_ulk Modulus', None,
             '',
             self.bulk_modulus),
            ('Bulk', None, '_Bulk Crystal', None,
             "Create a bulk crystal with arbitrary orientation",
             self.bulk_window),
            ('Surface', None, '_Surface slab', None,
             "Create the most common surfaces",
             self.surface_window),
            ('Nanoparticle', None, '_Nanoparticle', None,
             "Create a crystalline nanoparticle",
             self.nanoparticle_window),
            ('About', None, '_About', None,
             None,
             self.about),
            ('Webpage', gtk.STOCK_HELP, 'Webpage ...', None, None, webpage),
            ('Debug', None, 'Debug ...', None, None, self.debug)])
        actions.add_toggle_actions([
            ('ShowUnitCell', None, 'Show _unit cell', '<control>U',
             'Bold',
             self.toggle_show_unit_cell,
             show_unit_cell > 0),
            ('ShowAxes', None, 'Show _axes', '<control>A',
             'Bold',
             self.toggle_show_axes,
             True),
            ('ShowBonds', None, 'Show _bonds', '<control>B',
             'Bold',
             self.toggle_show_bonds,
             show_bonds)])
        self.ui = ui = gtk.UIManager()
        ui.insert_action_group(actions, 0)
        self.window.add_accel_group(ui.get_accel_group())

        try:
            mergeid = ui.add_ui_from_string(ui_info)
        except gobject.GError, msg:
            print 'building menus failed: %s' % msg

        vbox.pack_start(ui.get_widget('/MenuBar'), False, False, 0)
        
        View.__init__(self, vbox, rotations)
        Status.__init__(self, vbox)
        vbox.show()
        #self.window.set_events(gtk.gdk.BUTTON_PRESS_MASK)
        self.window.connect('key-press-event', self.scroll)
        self.window.connect('scroll_event', self.scroll_event)
        self.window.show()
        self.graphs = []
        self.movie_window = None
        self.vulnerable_windows = []

    def run(self, expr=None):
        self.set_colors()
        self.set_coordinates(self.images.nimages - 1, focus=True)

        if self.images.nimages > 1:
            self.movie()

        if expr is None and not np.isnan(self.images.E[0]):
            expr = 'i, e - E[-1]'
            
        if expr is not None and expr != '' and self.images.nimages > 1:
            self.plot_graphs(expr=expr)

        gtk.main()

    def step(self, action):
        d = {'First': -10000000,
             'Previous': -1,
             'Next': 1,
             'Last': 10000000}[action.get_name()]
        i = max(0, min(self.images.nimages - 1, self.frame + d))
        self.set_frame(i)
        if self.movie_window is not None:
            self.movie_window.frame_number.value = i
            
    def _do_zoom(self, x):
        """Utility method for zooming"""
        self.scale *= x
        center = (0.5 * self.width, 0.5 * self.height, 0)
        self.offset = x * (self.offset + center) - center
        self.draw()
        
    def zoom(self, action):
        """Zoom in/out on keypress or clicking menu item"""
        x = {'ZoomIn': 1.2, 'ZoomOut':1 / 1.2}[action.get_name()]
        self._do_zoom(x)

    def scroll_event(self, window, event):
        """Zoom in/out when using mouse wheel"""
        if event.direction == gtk.gdk.SCROLL_UP:
            x = 1.2
        elif event.direction == gtk.gdk.SCROLL_DOWN:
            x = 1 / 1.2
        self._do_zoom(x)

    def settings(self, menuitem):
        Settings(self)
        
    def scroll(self, window, event):
        dxdy = {gtk.keysyms.KP_Add: ('zoom', 1.2),
                gtk.keysyms.KP_Subtract: ('zoom', 1 / 1.2),
                gtk.keysyms.Up:    ( 0, -1),
                gtk.keysyms.Down:  ( 0, +1),
                gtk.keysyms.Right: (+1,  0),
                gtk.keysyms.Left:  (-1,  0)}.get(event.keyval, None)
        if dxdy is None:
            return
        dx, dy = dxdy
        if dx == 'zoom':
            self._do_zoom(dy)
            return
        d = self.scale * 0.1
        self.offset -= (dx * d, dy * d, 0)
        self.draw()
                
    def debug(self, x):
        from ase.gui.debug import Debug
        Debug(self)

    def execute(self, widget=None):
        from ase.gui.execute import Execute
        Execute(self)
        
    def constraints_window(self, widget=None):
        from ase.gui.constraints import Constraints
        Constraints(self)

    def dft_window(self, widget=None):
        from ase.gui.dft import DFT
        DFT(self)

    def select_all(self, widget):
        self.images.selected[:] = True
        self.draw()
        
    def invert_selection(self, widget):
        self.images.selected[:] = ~self.images.selected
        self.draw()
        
    def select_constrained_atoms(self, widget):
        self.images.selected[:] = ~self.images.dynamic
        self.draw()
        
    def movie(self, widget=None):
        from ase.gui.movie import Movie
        self.movie_window = Movie(self)
        
    def plot_graphs(self, x=None, expr=None):
        from ase.gui.graphs import Graphs
        g = Graphs(self)
        if expr is not None:
            g.plot(expr=expr)
        
    def NEB(self, action):
        from ase.gui.neb import NudgedElasticBand
        NudgedElasticBand(self.images)
        
    def bulk_modulus(self, action):
        from ase.gui.bulk_modulus import BulkModulus
        BulkModulus(self.images)
        
    def open(self, button=None, filenames=None):
        if filenames == None:
            chooser = gtk.FileChooserDialog(
                _('Open ...'), None, gtk.FILE_CHOOSER_ACTION_OPEN,
                (gtk.STOCK_CANCEL, gtk.RESPONSE_CANCEL,
                 gtk.STOCK_OPEN, gtk.RESPONSE_OK))
            ok = chooser.run()
            if ok == gtk.RESPONSE_OK:
                filenames = [chooser.get_filename()]
            chooser.destroy()

            if not ok:
                return
            
            
        self.images.read(filenames, slice(None))
        self.set_colors()
        self.set_coordinates(self.images.nimages - 1, focus=True)

    def save(self, action):
        chooser = gtk.FileChooserDialog(
            _('Save ...'), None, gtk.FILE_CHOOSER_ACTION_SAVE,
            (gtk.STOCK_CANCEL, gtk.RESPONSE_CANCEL,
             gtk.STOCK_SAVE, gtk.RESPONSE_OK))

        combo = gtk.combo_box_new_text()
        types = []
        for name, suffix in [('Automatic', None),
                             ('XYZ file', 'xyz'),
                             ('ASE trajectory', 'traj'),
                             ('PDB file', 'pdb'),
                             ('Gaussian cube file', 'cube'),
                             ('Python script', 'py'),
                             ('VNL file', 'vnl'),
                             ('Portable Network Graphics', 'png'),
                             ('Persistance of Vision', 'pov'),
                             ('Encapsulated PostScript', 'eps')]:
            if suffix is None:
                name = _(name)
            else:
                name = '%s (%s)' % (_(name), suffix)
            types.append(suffix)
            combo.append_text(name)

        combo.set_active(0)

        pack(chooser.vbox, combo)

        if self.images.nimages > 1:
            button = gtk.CheckButton('Save current image only (#%d)' %
                                     self.frame)
            pack(chooser.vbox, button)
            entry = pack(chooser.vbox, [gtk.Label(_('Slice: ')),
                                        gtk.Entry(10),
                                        help('Help for slice ...')])[1]
            entry.set_text('0:%d' % self.images.nimages)

        while True:
            if chooser.run() == gtk.RESPONSE_CANCEL:
                chooser.destroy()
                return
            
            filename = chooser.get_filename()

            i = combo.get_active()
            if i == 0:
                suffix = filename.split('.')[-1]
                if suffix not in types:
                   self.xxx(message1='Unknown output format!',
                            message2='Use one of: ' + ', '.join(types[1:]))
                   continue
            else:
                suffix = types[i]
                
            if suffix in ['pdb', 'vnl']:
                self.xxx()
                continue
                
            if self.images.nimages > 1:
                if button.get_active():
                    filename += '@%d' % self.frame
                else:
                    filename += '@' + entry.get_text()

            # Does filename exist?  XXX
            
            break
        
        chooser.destroy()

        bbox = np.empty(4)
        bbox[:2] = self.offset[:2]
        bbox[2:] = bbox[:2] + (self.width, self.height)
        bbox /= self.scale
        suc = self.ui.get_widget('/MenuBar/ViewMenu/ShowUnitCell').get_active()
        self.images.write(filename, self.rotation,
                          show_unit_cell=suc, bbox=bbox)
        
    def bulk_window(self, menuitem):
        SetupBulkCrystal(self)

    def surface_window(self, menuitem):
        SetupSurfaceSlab(self)

    def nanoparticle_window(self, menuitem):
        SetupNanoparticle(self)
        
    def new_atoms(self, atoms):
        "Set a new atoms object."
        self.zap_vulnerable()
        rpt = getattr(self.images, 'repeat', None)
        self.images.repeat_images(np.ones(3, int))
        self.images.initialize([atoms])
        self.frame = 0   # Prevent crashes
        self.images.repeat_images(rpt)
        self.set_colors()
        self.set_coordinates(frame=0, focus=True)

    def zap_vulnerable(self):
        "Delete windows that would break when new_atoms is called."
        for wref in self.vulnerable_windows:
            ref = wref()
            if ref is not None:
                ref.destroy()
        self.vulnerable_windows = []

    def register_vulnerable(self, obj):
        """Register windows that are vulnerable to changing the images.

        Some windows will break if the atoms (and in particular the
        number of images) are changed.  They can register themselves
        and be closed when that happens.
        """
        self.vulnerable_windows.append(weakref.ref(obj))

    def exit(self, button, event=None):
        gtk.main_quit()
        return True

    def xxx(self, x=None,
            message1='Not implemented!',
            message2='do you really need it?'):
        oops(message1, message2)
        
    def about(self, action):
        try:
            dialog = gtk.AboutDialog()
        except AttributeError:
            self.xxx()
        else:
            dialog.run()

def webpage(widget):
    import webbrowser
    webbrowser.open('https://wiki.fysik.dtu.dk/ase/ase/gui.html')
