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

import matplotlib.pyplot as plt

from .nightReportPlotBase import StarTrackerPlot

# any classes added to __all__ will automatically be added to the night
# report channel, with each being replotted for each image taken.
__all__ = ['EverythingPlot']


class EverythingPlot(StarTrackerPlot):
    _PlotName = 'everything-plot'
    _PlotGroup = 'TemporaryGroup'

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
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        nPlots = 18
        axisLabelSize = 18

        fig, axes = plt.subplots(figsize=(16, 8*nPlots), nrows=nPlots, ncols=1, sharex=True)

        mjds = metadata['MJD']

        suffixes = ['', ' wide', ' fast']

        for seriesNum, series in enumerate(['Alt',
                                            'Az',
                                            'Calculated Alt',
                                            'Calculated Az',
                                            'Calculated Dec',
                                            'Calculated Ra',
                                            'Dec',
                                            'Delta Alt Arcsec',
                                            'Delta Az Arcsec',
                                            'Delta Dec Arcsec',
                                            'Delta Ra Arcsec',
                                            'Delta Rot Arcsec',
                                            'Exposure Time',
                                            'RMS scatter arcsec',
                                            'RMS scatter pixels',
                                            'Ra',
                                            'nSources',
                                            'nSources filtered',
                                            ]):

            for suffix in suffixes:
                seriesName = series + suffix
                if seriesName in metadata.columns:
                    data = metadata[seriesName]
                    axes[seriesNum].plot(mjds, data, label=seriesName)
                    axes[seriesNum].legend()
                    axes[seriesNum].set_xlabel('MJD', size=axisLabelSize)
                    axes[seriesNum].set_ylabel(series, size=axisLabelSize)

        return True
