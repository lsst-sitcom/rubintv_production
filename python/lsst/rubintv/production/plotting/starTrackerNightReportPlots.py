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
import matplotlib.dates as md

from .nightReportPlotBase import StarTrackerPlot

# any classes added to __all__ will automatically be added to the night
# report channel, with each being replotted for each image taken.
__all__ = ['SomePlot']


class SomePlot(StarTrackerPlot):
    _PlotName = 'test-plot'
    _PlotGroup = 'TestGroup'

    def __init__(self,
                 dayObs,
                 locationConfig=None,
                 uploader=None):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         locationConfig=locationConfig,
                         uploader=uploader)

    def plot(self, nightReport, metadata, ccdVisitTable):
        """Create the zeropoint plot.

        Parameters
        ----------
        metadata : `pandas.DataFrame`
            The data from all three StarTracker page tables, as a dataframe.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        for item in ['???', '???']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        inds = metadata.index[metadata['Zeropoint'] > 0].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        bands = np.asarray(metadata['Filter'][inds])
        # TODO: generalise this to all bands and add checks for if empty
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')
        zeroPoint = np.array(metadata['Zeropoint'][inds])
        plt.plot(rawDates[gband], zeroPoint[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[rband], zeroPoint[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[iband], zeroPoint[iband], '.', color=icolor, linestyle='-', label='SDSSi')
        plt.xlabel('TAI Date')
        plt.ylabel('Photometric Zeropoint (mag)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True
