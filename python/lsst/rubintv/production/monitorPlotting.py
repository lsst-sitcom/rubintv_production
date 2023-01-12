# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

from lsst.summit.utils import quickSmooth


def plotExp(exp, figure, saveFilename):
    """Render and exposure as a png, saving it to saveFilename.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        The exposure to plot.
    figure : `matplotlib.figure.Figure`
        The figure to use for plotting.
    saveFilename : `str`
        The filename to save the image to.
    """
    data = quickSmooth(exp.image.array, 1)
    vmin = np.percentile(data, 1)
    vmax = np.percentile(data, 99)

    figure.clear()
    ax1 = figure.add_subplot(111)
    im1 = ax1.imshow(data, cmap='gray', origin='lower', vmin=vmin, vmax=vmax)

    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    plt.colorbar(im1, cax=cax)
    plt.tight_layout()
    plt.savefig(saveFilename)
