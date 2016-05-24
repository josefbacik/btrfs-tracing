import gi
gi.require_version('Gtk', '3.0')
gi.require_version('Gdk', '3.0')
from gi.repository import Gtk,Gdk
import cairo
from bisect import bisect_left

NSECS_IN_SEC = 1000000000
class GraphScreen(Gtk.DrawingArea):
    # Taken from the defintion of cairo_text_extents_t
    class Extents():
        def __init__(self, extents):
            self.x_bearing = extents[0]
            self.y_bearing = extents[1]
            self.width = extents[2]
            self.height = extents[3]
            self.x_advance = extents[4]
            self.y_advance = extents[5]

    class DataPoints():
        def __init__(self, name, xpoints, ypoints, color, connected):
            self.name = name
            self.xpoints = xpoints
            self.ypoints = ypoints
            self.color = color
            self.connected = connected
            self.enabled = True

    def __init__(self):
        Gtk.DrawingArea.__init__(self)
        self.set_has_tooltip(True)
        self.connect("draw", self.on_draw)
        self.connect("query-tooltip", self.tooltip)
        self.connect("button-press-event", self.button_press)
        self.connect("button-release-event", self.button_release)
        self.set_events(self.get_events() | Gdk.EventMask.BUTTON_PRESS_MASK |
                        Gdk.EventMask.BUTTON_RELEASE_MASK)
        self.ylabel = "Size"
        self.xlabel = "Time"
        self.width = 0
        self.height = 0
        self.plots = []
        self.xmax = 0
        self.xmin = None
        self.ymax = 0
        self.ymin = None
        self.enabled_plots = 0

        self.rescale_cb = None
        self.cur_rescale_x = None
        self.selection_line = None

    def add_datapoints(self, name, xpoints, ypoints, color, connected=True):
        dp = self.DataPoints(name, xpoints, ypoints, color, connected)
        self.plots.append(dp)
        """
        if xpoints[-1] > self.xmax:
            self.xmax = xpoints[-1]
        if not self.xmin or self.xmin < xpoints[0]:
            self.xmin = xpoints[0]
        if ypoints[-1] > self.ymax:
            self.ymax = ypoints[-1]
        if not self.ymin or self.ymin < ypoints[0]:
            self.ymin = ypoints[0]
        print("added %s xmin %d xmax %d ymin %d ymax %d" %
                (name, self.xmin, self.xmax, self.ymin, self.ymax))
        """

    def _rescale(self):
        self.xmax = 0
        self.xmin = None
        self.ymax = 0
        self.ymin = 0
        self.enabled_plots = 0

        for data in self.plots:
            if not data.enabled:
                continue
            if len(data.xpoints) == 0:
                continue
            self.enabled_plots += 1
            if max(data.xpoints) > self.xmax:
                self.xmax = max(data.xpoints)
            if not self.xmin or self.xmin < min(data.xpoints):
                self.xmin = min(data.xpoints)
            if max(data.ypoints) > self.ymax:
                self.ymax = max(data.ypoints)
            if self.ymin > min(data.ypoints):
                self.ymin = min(data.ypoints)
        self.queue_draw()

    def update_datapoints(self, name, xpoints, ypoints):
        for d in self.plots:
            if d.name != name:
                continue
            d.xpoints = xpoints
            d.ypoints = ypoints
            break
        self._rescale()

    def set_rescale_cb(self, rescale_cb):
        self.rescale_cb = rescale_cb

    def toggle_datapoint(self, name, toggle):
        for data in self.plots:
            if data.name == name:
                data.enabled = toggle
                break
        self._rescale()

    def _adjust_graph_values(self, cr, width, height):
        self.width = width
        self.height = height

        # The graph is relative to the x and y labels
        yextents = self.Extents(cr.text_extents(self.ylabel))
        self.bottomx = yextents.width * 3/2 + cr.get_line_width()

        xextents = self.Extents(cr.text_extents(self.xlabel))
        self.bottomy = height - (xextents.height * 2 + cr.get_line_width())

    def _draw_graph(self, cr, width, height):
        cr.set_source_rgb(0, 0, 0)
        extents = self.Extents(cr.text_extents(self.ylabel))

        gap = extents.width / 4
        cr.move_to(gap, height / 2)
        cr.show_text(self.ylabel)

        time_extents = self.Extents(cr.text_extents(self.xlabel))

        # We want to center the x-axis label with the x-axis line and the label
        # itself
        xpos = ((width + extents.width * 3/2) / 2) - (time_extents.width / 2)
        gap = time_extents.height / 2
        cr.move_to(xpos, height - gap)
        cr.show_text(self.xlabel)

        lw = cr.get_line_width()
        cr.move_to(self.bottomx - lw, 0)
        cr.line_to(self.bottomx - lw, self.bottomy + lw)
        cr.stroke()

        cr.move_to(self.bottomx - lw, self.bottomy + lw)
        cr.line_to(width, self.bottomy + lw)
        cr.stroke()

    def _draw_plots(self, cr, width, height):
        yticks = self.bottomy / (self.ymax - self.ymin)
        xticks = (width - self.bottomx) / (self.xmax - self.xmin)
        for datapoints in self.plots:
            if datapoints.enabled == False:
                continue
            if len(datapoints.xpoints) == 0:
                continue
            cr.set_source_rgb(datapoints.color[0], datapoints.color[1],
                              datapoints.color[2])
            for i in range(0, len(datapoints.xpoints)):
                if i == 0 or not datapoints.connected:
                    lastx = self.bottomx + ((datapoints.xpoints[i] - self.xmin) * xticks)
                    lasty = self.bottomy - (datapoints.ypoints[i] - self.ymin) * yticks
                    last = (lastx, lasty)
                    if i == 0:
                        continue
                lastx = last[0]
                lasty = last[1]
                if not datapoints.connected:
                    curx = lastx
                    cury = lasty
                else:
                    curx = self.bottomx + ((datapoints.xpoints[i] - self.xmin) * xticks)
                    cury = self.bottomy - (datapoints.ypoints[i] - self.ymin) * yticks
                last = (curx, cury)
                cr.move_to(lastx, lasty)
                cr.line_to(curx, cury)
            cr.stroke()

    def _draw_selection_line(self, cr, width, height):
        if self.selection_line < self.xmin or self.selection_line > self.xmax:
            return
        xticks = (width - self.bottomx) / (self.xmax - self.xmin)
        xval = self.bottomx + ((self.selection_line - self.xmin) * xticks)
        cr.set_source_rgb(0, 1, 1)
        cr.move_to(xval, self.bottomy)
        cr.line_to(xval, 0)
        cr.stroke()

    def on_draw(self, widget, cr):
        # Fill the background with gray
        width = widget.get_allocation().width
        height = widget.get_allocation().height
        cr.set_font_size(14)
        if width != self.width or height != self.height:
            self._adjust_graph_values(cr, width, height)
        cr.set_source_rgb(1, 1, 1)
        cr.rectangle(0, 0, width, height)
        cr.fill()

        self._draw_graph(cr, width, height)
        if self.enabled_plots > 0:
            self._draw_plots(cr, width, height)
        if self.selection_line is not None:
            self._draw_selection_line(cr, width, height)

    def _get_xval(self, width, x):
        if self.xmin is None:
            return x
        adjx = x - self.bottomx
        xticks = (width - self.bottomx) / (self.xmax - self.xmin)
        xval = long(self.xmin + (adjx / xticks))
        return xval

    def _bin_search(self, val, l):
        pos = bisect_left(l, val, 0, len(l))
        if pos == len(l):
            return -1
        return pos

    def tooltip(self, widget, x, y, keyboard_mode, tooltip):
        if self.enabled_plots == 0:
            return False
        if x < self.bottomx or y > self.bottomy:
            return False
        if self.xmin is None:
            return False

        # Get the time position our cursor is currently at
        xval = self._get_xval(widget.get_allocation().width, x)

        index = self._bin_search(xval, self.plots[0].xpoints)
        tipstr = ("Time is %f" % (float(xval) / NSECS_IN_SEC))
        for data in self.plots:
            if data.enabled:
                tipstr += (", %s is %s" %
                            (data.name, self.pretty_size(data.ypoints[index])))
        tooltip.set_text(tipstr)
        return True

    def pretty_size(self, size):
        names = ["bytes", "kib", "mib", "gib", "tib"]
        i = 0
        while size > 1024:
            size /= 1024
            i += 1
        return str(size) + names[i]

    def button_press(self, widget, event):
        if event.x < self.bottomx or event.y > self.bottomy:
            return

        xval = self._get_xval(widget.get_allocation().width, event.x)
        self.cur_rescale_x = xval

    def button_release(self, widget, event):
        if self.cur_rescale_x is None:
            return
        width = widget.get_allocation().width
        x = event.x
        if x > width:
            x = width
        if x < self.bottomx:
            x = self.bottomx
        xval = self._get_xval(width, x)
        if xval > self.cur_rescale_x:
            ts_start = self.cur_rescale_x
            ts_end = xval
        elif xval < self.cur_rescale_x:
            ts_start = xval
            ts_end = self.cur_rescale_x
        else:
            ts_start = 0
            ts_end = 0
        self.rescale_cb(ts_start, ts_end)

class GraphWindow(Gtk.Window):
    def __init__(self):
        Gtk.Window.__init__(self, title="Btrfs space utliziation")
        self.set_default_size(1600, 1200)
        mainbox = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        drawbox = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        self.labelbox = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        treebox = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)

        self.darea = GraphScreen()
        drawbox.pack_start(self.darea, True, True, 0)

        mainbox.pack_start(drawbox, True, True, 0)
        mainbox.pack_start(self.labelbox, False, False, 0)
        self.add(mainbox)

        scroll = Gtk.ScrolledWindow()
        self.liststore = Gtk.ListStore(long, int, int, str, str)
        self.tree = Gtk.TreeView(self.liststore)
        self.selection = self.tree.get_selection()
        self.selection.set_mode(Gtk.SelectionMode.SINGLE)
        self.selection.connect("changed", self.selection_changed)

        for i, column_title in enumerate(["Timestamp", "PID", "CPU", "Event", "Value"]):
            renderer = Gtk.CellRendererText()
            column = Gtk.TreeViewColumn(column_title, renderer, text=i)
            self.tree.append_column(column)

        scroll.set_hexpand(True)
        scroll.add(self.tree)
        treebox.add(scroll)
        mainbox.pack_start(treebox, True, True, 0)
        scroll.show_all()
        self.connect("delete-event", Gtk.main_quit)
        self.rescale_cb = None
        self.rescale_data = None
        self.selected_line = None

    def _rescale_cb(self, ts_start, ts_end):
        self.rescale_cb(self, self.rescale_data, ts_start, ts_end)

    def set_rescale_cb(self, rescale_cb, user_data):
        self.rescale_data = user_data
        self.rescale_cb = rescale_cb
        self.darea.set_rescale_cb(self._rescale_cb)

    def add_datapoints(self, name, xpoints, ypoints, color, connected=True):
        self.darea.add_datapoints(name, xpoints, ypoints, color, connected)

        button = Gtk.ToggleButton(name)
        button.connect("toggled", self.on_button_toggled, name)
        button.set_active(True)
        self.labelbox.pack_start(button, True, False, 0)

    def on_button_toggled(self, button, name):
        self.darea.toggle_datapoint(name, button.get_active())

    def add_flush_event(self, event):
        tree_iter = self.liststore.append(event)
        if event[0] == self.selected_line:
            print("setting selected iter")
            self.selection.select_iter(tree_iter)
        self.tree.set_model(self.liststore)

    def selection_changed(self, widget):
        model, pathlist = widget.get_selected_rows()
        for path in pathlist:
            tree_iter = model.get_iter(path)
            self.darea.selection_line = model.get_value(tree_iter, 0)
            self.selected_line = model.get_value(tree_iter, 0)
        self.darea.queue_draw()

    def main(self):
        self.tree.set_model(self.liststore)
        self.show_all()
        Gtk.main()

def run():
    window = GraphWindow()
    Gtk.main()

if __name__ == "__main__":
    run()
