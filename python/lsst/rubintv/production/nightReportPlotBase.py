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

from abc import ABC, abstractmethod
import os
import matplotlib.pyplot as plt
import logging

__all__ = ['BasicPlot']


class BasicPlot(ABC):
    """Base class for basic night report plots.

    Parameters
    ----------
    plotName : `str`
        The name of the plot, used for upload.
    plotGroup : `str`
        The group to put the plot in on the front end.
    nightReportChannel : `lsst.rubintv.production.rubinTv.NightReportChannel`
        The file watcher to use.
    """

    def __init__(self, *,
                 dayObs,
                 plotName,
                 plotGroup,
                 nightReportChannel,
                 ):
        self.dayObs = dayObs
        self.plotName = plotName
        self.plotGroup = plotGroup
        self.nightReportChannel = nightReportChannel
        self.log = logging.getLogger(f'lsst.rubintv.production.nightReportPlots.{plotName}')

    def getSaveFilename(self):
        """XXX docs
        """
        return os.path.join(self.nightReportChannel.locationConfig.nightReportPath,
                            f'{self.plotName}.png')

    @abstractmethod
    def plot(self, nightReport, metadata, ccdVisitTable):
        """Subclasses must implement this method.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        ccdVisitTable : `pandas.DataFrame`
            The visit summary table for the current day.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        raise NotImplementedError()

    def createAndUpload(self, nightReport, metadata, ccdVisitTable):
        """Create the plot defined in ``plot`` and upload it.

        This is the method called by the Night Report channel to create the
        plot and send it to the bucket.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        ccdVisitTable : `pandas.DataFrame`
            The visit summary table for the current day.
        """
        success = self.plot(nightReport, metadata, ccdVisitTable)
        if not success:
            self.log.warning(f'Plot {self.plotName} failed to create')
            return

        saveFile = self.getSaveFilename()
        plt.savefig(saveFile)
        plt.close()

        self.nightReportChannel.uploader.uploadNightReportData(channel=self.nightReportChannel.channelName,
                                                               dayObsInt=self.dayObs,
                                                               filename=saveFile,
                                                               plotGroup=self.plotGroup)
        # if things start failing later you don't want old plots sticking
        # around and getting re-uploaded as if they were new
        os.remove(saveFile)
