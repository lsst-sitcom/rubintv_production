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

from .nightReportPlotBase import StarTrackerPlot

# any classes added to __all__ will automatically be added to the night
# report channel, with each being replotted for each image taken.
__all__ = ['TestPlot']


class TestPlot(StarTrackerPlot):
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

    def plot(self, metadata):
        """Create a sample plot using data from the StarTracker page tables.

        Parameters
        ----------
        metadata : `pandas.DataFrame`
            The data from all three StarTracker page tables, as a dataframe.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        for item in ['Delta Alt Arcsec wide',
                     'Delta Alt Arcsec',
                     'Delta Az Arcsec wide',
                     'Delta Az Arcsec']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(figsize=(16, 8), constrained_layout=True)

        seqNums = metadata.index

        for series in ['Delta Alt Arcsec wide',
                       'Delta Alt Arcsec',
                       'Delta Az Arcsec wide',
                       'Delta Az Arcsec']:

            plt.plot(seqNums, metadata[series]%360*60*60, label=series)

        axisLabelSize = 18
        plt.xlabel('SeqNum', size=axisLabelSize)
        plt.ylabel('Delta (arcseconds)', size=axisLabelSize)

        plt.legend()
        return True
