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

import lsst.afw.image as afwImage
import numpy as np
from matplotlib import cm, colors
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import make_axes_locatable

from lsst.summit.utils import quickSmooth, getQuantiles


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
    data = None
    if isinstance(exp, afwImage.Exposure):
        data = exp.image.array
    elif isinstance(exp, afwImage.MaskedImage):
        data = exp.image.array
    elif isinstance(exp, afwImage.Image):
        data = exp.array
    elif isinstance(exp, np.ndarray):
        data = exp

    if data is None:
        raise TypeError(f"Unknown exposure type: {type(exp)}")

    if doSmooth:
        data = quickSmooth(data, 1)

    cmap = cm.gray
    figure.clear()
    ax1 = figure.add_subplot(111)
    match scalingOption:
        case "default":
            vmin = np.percentile(data, 1)
            vmax = np.percentile(data, 99)
            im1 = ax1.imshow(data, cmap=cmap, origin='lower', vmin=vmin, vmax=vmax)
        case "CCS":  # The CCS-style scaling
            quantiles = getQuantiles(data, cmap.N)
            norm = colors.BoundaryNorm(quantiles, cmap.N)
            im1 = ax1.imshow(data, cmap=cmap, origin='lower', norm=norm)
        case 'asinh':
            def _forward(x):
                return np.arcsinh(x)

            def _inverse(x):
                return np.sinh(x)
            norm = colors.FuncNorm((_forward, _inverse))
            im1 = ax1.imshow(data, cmap=cmap, origin='lower', norm=norm)
        case _:
            raise ValueError(f"Unknown plot scaling option {scalingOption}")

    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    plt.colorbar(im1, cax=cax)
    plt.tight_layout()
    if saveFilename:
        plt.savefig(saveFilename)
