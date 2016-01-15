import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk
import cairo

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

    def __init__(self):
        Gtk.DrawingArea.__init__(self)
        self.set_has_tooltip(True)
        self.connect("draw", self.on_draw)
        self.connect("query-tooltip", self.tooltip)
        self.ylabel = "Size"
        self.xlabel = "Time"
        self.width = 0
        self.height = 0
        self.plots = []
        self.xmax = 0
        self.xmin = None
        self.ymax = 0
        self.ymin = None

    def add_datapoints(self, name, xpoints, ypoints, color, connected=True):
        dp = self.DataPoints(name, xpoints, ypoints, color, connected)
        self.plots.append(dp)
        if xpoints[-1] > self.xmax:
            self.xmax = xpoints[-1]
        if not self.xmin or self.xmin < xpoints[0]:
            self.xmin = xpoints[0]
        if ypoints[-1] > self.ymax:
            self.ymax = ypoints[-1]
        if not self.ymin or self.ymin < ypoints[0]:
            self.ymin = ypoints[0]

    def _adjust_graph_values(self, cr, width, height):
        self.width = width
        self.height = height

        # The graph is relative to the x and y labels
        yextents = self.Extents(cr.text_extents(self.ylabel))
        self.bottomx = yextents.width * 3/2

        xextents = self.Extents(cr.text_extents(self.xlabel))
        self.bottomy = height - (xextents.height * 2)

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

        cr.move_to(self.bottomx, 0)
        cr.line_to(self.bottomx, self.bottomy)
        cr.stroke()

        cr.move_to(self.bottomx, self.bottomy)
        cr.line_to(width, self.bottomy)
        cr.stroke()

    def _draw_plots(self, cr, width, height):
        yticks = self.bottomy / (self.ymax - self.ymin)
        xticks = (width - self.bottomx) / (self.xmax - self.xmin)
        print("xticks is %f, yticks is %f" % (xticks, yticks))
        print("xmin %d, xmax %d, ymin %d, ymax %d" % (self.xmin, self.xmax,
        self.ymin, self.ymax))
        for datapoints in self.plots:
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
        self._draw_plots(cr, width, height)

    def tooltip(self, widget, x, y, keyboard_mode, tooltip):
        if x < self.bottomx or y > self.bottomy:
            return False

        # Get the time position our cursor is currently at
        width = widget.get_allocation().width
        adjx = x - self.bottomx
        xticks = (width - self.bottomx) / (self.xmax - self.xmin)
        xval = int(self.xmin + (adjx / xticks))
        success = 0

        # This is awful but I don't have the energy to be clever
        # It also requires that xpoints be the same across all the plots which
        # again is awful but is true for the btrfs tracing
        data = self.plots[0]
        if xval not in data.xpoints:
            for i in range(0, len(data.xpoints)):
                if data.xpoints[i] > xval:
                    if (data.xpoints[i] - xval) < (data.xpoints[i-1] - xval):
                        index = i
                    else:
                        index = i - 1
                    break
        else:
            index = data.xpoints.index(xval)
        tipstr = ("Time is %d" % xval)
        for data in self.plots:
            tipstr += (", %s is %d" % (data.name, data.ypoints[index]))
        tooltip.set_text(tipstr)
        return True

class GraphWindow(Gtk.Window):
    def __init__(self):
        Gtk.Window.__init__(self, title="Btrfs space utliziation")
        self.set_default_size(800, 600)
        hbox = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)

        self.darea = GraphScreen()
        hbox.pack_start(self.darea, True, True, 0)

        self.add(hbox)

        self.connect("delete-event", Gtk.main_quit)
        self.show_all()


def run():
    window = GraphWindow()
    Gtk.main()

if __name__ == "__main__":
    run()
