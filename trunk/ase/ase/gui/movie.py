#!/usr/bin/env python
import gtk
import gobject

from ase.gui.languages import translate as _
from ase.gui.widgets import pack


class Movie(gtk.Window):
    def __init__(self, gui):
        gtk.Window.__init__(self)
        self.set_position(gtk.WIN_POS_NONE)
        self.connect('destroy', self.close)
        #self.connect('delete_event', self.exit2)
        self.set_title('Movie')
        vbox = gtk.VBox()
        pack(vbox, gtk.Label(_('Image number:')))
        self.frame_number = gtk.Adjustment(gui.frame, 0,
                                           gui.images.nimages - 1,
                                           1.0, 5.0)
        self.frame_number.connect('value-changed', self.new_frame)

        hscale = pack(vbox, gtk.HScale(self.frame_number))
        hscale.set_update_policy(gtk.UPDATE_DISCONTINUOUS)
        hscale.set_digits(0)

        buttons = [gtk.Button(stock=gtk.STOCK_GOTO_FIRST),
                   gtk.Button(stock=gtk.STOCK_GO_BACK),
                   gtk.Button(stock=gtk.STOCK_GO_FORWARD),
                   gtk.Button(stock=gtk.STOCK_GOTO_LAST)]

        buttons[0].connect('clicked', self.click, -10000000)
        buttons[1].connect('clicked', self.click, -1)
        buttons[2].connect('clicked', self.click, 1)
        buttons[3].connect('clicked', self.click, 10000000)

        pack(vbox, buttons)

        play, stop = pack(vbox, [gtk.Button(_('Play')),
                                 gtk.Button('Stop')])
        play.connect('clicked', self.play)
        stop.connect('clicked', self.stop)

        self.rock = pack(vbox, gtk.CheckButton('Rock'))

        self.time = gtk.Adjustment(2.0, 0.5, 9.0, 0.2)
        hscale = pack(vbox, gtk.HScale(self.time))
        hscale.set_update_policy(gtk.UPDATE_DISCONTINUOUS)
            
        self.time.connect('value-changed', self.new_time)

        self.add(vbox)
        if gtk.pygtk_version < (2, 12):
            self.set_tip = gtk.Tooltips().set_tip
            self.set_tip(hscale, _('Adjust play time.'))
        else:
            hscale.set_tooltip_text(_('Adjust play time.'))

        vbox.show()
        self.show()
        self.gui = gui
        #gui.m=self
        self.direction = 1
        self.id = None
        gui.register_vulnerable(self)
        
    def close(self, event):
        self.stop()

    def click(self, button, step):
        i = max(0, min(self.gui.images.nimages - 1, self.gui.frame + step))
        self.gui.set_frame(i)
        self.frame_number.value = i
        self.direction = cmp(step, 0)
        
    def new_frame(self, widget):
        self.gui.set_frame(int(self.frame_number.value))

    def play(self, widget=None):
        if self.id is not None:
            gobject.source_remove(self.id)

        t = int(1000 * self.time.value / (self.gui.images.nimages - 1))
        self.id = gobject.timeout_add(t, self.step)

    def stop(self, widget=None):
        if self.id is not None:
            gobject.source_remove(self.id)
            self.id = None

    def step(self):
        i = self.gui.frame
        nimages = self.gui.images.nimages
        
        if self.rock.get_active():
            if i == 0:
                self.direction = 1
            elif i == nimages - 1:
                self.direction = -1
            i += self.direction
        else:
            i = (i + self.direction + nimages) % nimages
            
        self.gui.set_frame(i)
        self.frame_number.value = i
        return True

    def new_time(self, widget):
        if self.id is not None:
            self.play()
