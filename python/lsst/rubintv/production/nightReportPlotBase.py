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

__all__ = ['BasicPlot']


class BasicPlot(ABC):
    """Base class for basic night report plots.

    Parameters
    ----------
    dayObs : `int`
        The dayObs.
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

    def upload(self, saveFile):
        if not hasattr(self, '_PlotName'):
            raise RuntimeError('Subclasses must define _PlotName')
        if not hasattr(self, '_PlotGroup'):
            raise RuntimeError('Subclasses must define _PlotGroup')

        if os.path.isfile(saveFile):
            print(f'{saveFile} confirmed to exist')
        else:
            print(f'Failed to find {saveFile}')
            return

        print(f'uploading {saveFile}')

    def getSaveFilename(self):
        return os.path.join(self.nightReportChannel.locationConfig.nightReportPath,
                            f'{self.plotName}.png')

    @abstractmethod
    def plot(self, nightReport, metadata):
        """Subclasses must implement this method.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        """
        raise NotImplementedError()

    def createAndUpload(self, nightReport, metadata):
        self.plot(nightReport, metadata)
        saveFile = self.getSaveFilename()
        plt.savefig(saveFile)
        self.upload(saveFile)
        plt.close()
