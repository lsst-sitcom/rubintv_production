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

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm, colors
from mpl_toolkits.axes_grid1 import make_axes_locatable

import lsst.afw.image as afwImage
from lsst.summit.utils import getQuantiles, quickSmooth


def plotExp(exp, figure, saveFilename=None, doSmooth=True, scalingOption="default"):
    """Render and exposure as a png, saving it to saveFilename.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure` or
          `lsst.afw.image.MaskedImage` or
          `lsst.afw.image.Image` or
          `numpy.ndarray`
        The exposure or data to plot.
    figure : `matplotlib.figure.Figure`
        The figure to use for plotting.
    saveFilename : `str`
        The filename to save the image to.
    scalingOption : `int`
        The choice of colormap normalization.
    """
    match exp:
        case afwImage.Exposure():
            data = exp.image.array
        case afwImage.MaskedImage():
            data = exp.image.array
        case afwImage.Image():
            data = exp.array
        case np.ndarray():
            data = exp
        case _:
            raise TypeError(f"Unknown exposure type: {type(exp)}")

    if doSmooth:
        data = quickSmooth(data, 1)

    cmap = cm.gray
    figure.clear()
    ax1 = figure.add_subplot(111)
    allNanColorbar = False
    match scalingOption:
        case "default":
            vmin = np.percentile(data, 1)
            vmax = np.percentile(data, 99)
            im1 = ax1.imshow(data, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax)
        case "CCS":  # The CCS-style scaling
            quantiles = getQuantiles(data, cmap.N)
            norm = colors.BoundaryNorm(quantiles, cmap.N)
            if np.all(np.isnan(quantiles)):
                allNanColorbar = True
            im1 = ax1.imshow(data, cmap=cmap, origin="lower", norm=norm)
        case "asinh":

            def _forward(x):
                return np.arcsinh(x)

            def _inverse(x):
                return np.sinh(x)

            norm = colors.FuncNorm((_forward, _inverse))
            im1 = ax1.imshow(data, cmap=cmap, origin="lower", norm=norm)
        case _:
            raise ValueError(f"Unknown plot scaling option {scalingOption}")

    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    if not allNanColorbar:
        # this is the line which raises for all-nans. Oddly though, for some
        # reason, the two lines above are sufficient to add a 0-1 range
        # all-white colour bar, and so are left outside of this if-block to
        # help people see that something has gone wrong. The labels are set to
        # nan in the else block below.
        plt.colorbar(im1, cax=cax)
    else:
        # set the tick labels on the cax to all nan manually
        cax.set_yticklabels(["nan" for _ in cax.get_yticklabels()])

    plt.tight_layout()
    if saveFilename:
        plt.savefig(saveFilename)
