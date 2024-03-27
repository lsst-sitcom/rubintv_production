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

__all__ = ['BasePlot',
           'LatissPlot',
           'StarTrackerPlot']


class BasePlot(ABC):
    """Base class for night report plots.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to make the plots for.
    plotName : `str`
        The name of the plot, used for upload.
    plotGroup : `str`
        The group to put the plot in on the front end.
    channelName : `str`
        The channel to upload to, or ``None``, if being used for development.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`, optional
        The locationConfig containing the paths, or ``None`` if being used for
        development
    uploader : `lsst.rubintv.production.Uploader`, optional
        The uploader, or ``None``, if being used for development.
    """

    def __init__(self, *,
                 dayObs,
                 plotName,
                 plotGroup,
                 channelName=None,
                 locationConfig=None,
                 uploader=None,
                 ):
        self.dayObs = dayObs
        self.plotName = plotName
        self.plotGroup = plotGroup
        self.channelName = channelName
        self.locationConfig = locationConfig
        self.uploader = uploader
        self.log = logging.getLogger(f'lsst.rubintv.production.nightReportPlots.{plotName}')

    def getSaveFilename(self):
        """Get the filename to save the plot to.

        Calculated from the locationConfig, the channel name and the plot name.

        Returns
        -------
        filename : `str`
            The full path and filename to save the plot to, such that it can be
            passed to ``plt.savefig()``.
        """
        return os.path.join(self.locationConfig.nightReportPath,
                            f'{self.channelName}-{self.plotName}.png')

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

    @abstractmethod
    def createAndUpload(self, *args):
        """Create the plot defined in ``plot`` and upload it.

        This is the method called by the Night Report channel to create the
        plot and send it to the bucket.

        Parameters
        ----------
        *arg : `any`
            The arguments which are passed to the `plot` function.
        """
        raise NotImplementedError()


class LatissPlot(BasePlot):
    """Base class for LATISS night report plots.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to make the plots for.
    plotName : `str`
        The name of the plot, used for upload.
    plotGroup : `str`
        The group to put the plot in on the front end.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`, optional
        The locationConfig containing the paths, or ``None`` if being used for
        development
    uploader : `lsst.rubintv.production.Uploader`, optional
        The uploader, or ``None``, if being used for development.
    """
    def __init__(self, *,
                 dayObs,
                 plotName,
                 plotGroup,
                 locationConfig,
                 uploader):

        super().__init__(dayObs=dayObs,
                         plotName=plotName,
                         plotGroup=plotGroup,
                         channelName="auxtel_night_reports",
                         locationConfig=locationConfig,
                         uploader=uploader,
                         )

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
        if self.locationConfig is None or self.uploader is None:
            raise RuntimeError('locationConfig and uploader can only be None for development work.')

        success = self.plot(nightReport, metadata, ccdVisitTable)
        if not success:
            self.log.warning(f'Plot {self.plotName} failed to create')
            return

        saveFile = self.getSaveFilename()
        plt.savefig(saveFile)
        plt.close()

        self.uploader.uploadNightReportData(channel=self.channelName,
                                            dayObs=self.dayObs,
                                            filename=saveFile,
                                            plotGroup=self.plotGroup)

        # if things start failing later you don't want old plots sticking
        # around and getting re-uploaded as if they were new
        os.remove(saveFile)


class StarTrackerPlot(BasePlot):
    """Base class for StarTracker night report plots.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to make the plots for.
    plotName : `str`
        The name of the plot, used for upload.
    plotGroup : `str`
        The group to put the plot in on the front end.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`, optional
        The locationConfig containing the paths, or ``None`` if being used for
        development
    uploader : `lsst.rubintv.production.Uploader`, optional
        The uploader, or ``None``, if being used for development.
    """
    def __init__(self, *,
                 dayObs,
                 plotName,
                 plotGroup,
                 locationConfig,
                 uploader):

        super().__init__(dayObs=dayObs,
                         plotName=plotName,
                         plotGroup=plotGroup,
                         channelName="startracker_night_reports",
                         locationConfig=locationConfig,
                         uploader=uploader,
                         )

    def createAndUpload(self, tableData):
        """Create the plot defined in ``plot`` and upload it.

        This is the method called by the Night Report channel to create the
        plot and send it to the bucket.

        Parameters
        ----------
        tableData : `pandas.DataFrame`
            The data from all three StarTracker page tables, as a dataframe.
        """
        if self.locationConfig is None or self.uploader is None:
            raise RuntimeError('locationConfig and uploader can only be None for development work.')

        success = self.plot(tableData)
        if not success:
            self.log.warning(f'Plot {self.plotName} failed to create')
            return

        saveFile = self.getSaveFilename()
        plt.savefig(saveFile)
        plt.close()

        self.uploader.uploadNightReportData(channel=self.channelName,
                                            dayObs=self.dayObs,
                                            filename=saveFile,
                                            plotGroup=self.plotGroup)

        # if things start failing later you don't want old plots sticking
        # around and getting re-uploaded as if they were new
        os.remove(saveFile)
