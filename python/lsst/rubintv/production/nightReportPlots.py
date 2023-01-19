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

from .nightReportPlotBase import BasicPlot

# any classes added to __all__ will automatically be added to the night
# report channel, with each being replotted for each image taken.
__all__ = ['ZeroPointPlot', 'PsfFwhmPlot']


class ZeroPointPlot(BasicPlot):
    _PlotName = 'per-band-zeropoints'
    _PlotGroup = 'photometry'

    def __init__(self,
                 dayObs,
                 nightReportChannel):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         nightReportChannel=nightReportChannel)

    def plot(self, nightReport, metadata):
        """Create the zeropoint plot.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        """

        # TODO: get these colours from somewhere else
        gcolor = 'mediumseagreen'
        rcolor = 'lightcoral'
        icolor = 'mediumpurple'

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = self.nightReportChannel.getDatesForSeqNums()
        rawDates = np.asarray([datesDict[seqNum] for seqNum in sorted(datesDict.keys())])

        # TODO: need to check the Zeropoint column exists - it won't always
        inds = metadata.index[metadata['Zeropoint'] > 0].tolist()  # get the non-nan values
        inds = [i-1 for i in inds]  # pandas uses 1-based indexing
        bandColumn = metadata['Filter']
        bands = bandColumn[inds]
        # TODO: generalise this to all bands and add checks for if empty
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')
        zeroPoint = np.array(metadata['Zeropoint'].iloc[inds])
        plt.plot(rawDates[inds][gband], zeroPoint[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[inds][rband], zeroPoint[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[inds][iband], zeroPoint[iband], '.', color=icolor, linestyle='-', label='SDSSi')
        plt.xlabel('TAI Date')
        plt.ylabel('Photometric Zeropoint (mag)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        plt.legend()
        return True


class PsfFwhmPlot(BasicPlot):
    _PlotName = 'PSF-FWHM'
    _PlotGroup = 'Seeing'

    def __init__(self,
                 dayObs,
                 nightReportChannel):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         nightReportChannel=nightReportChannel)

    def plot(self, nightReport, metadata):
        """Plot filter and airmass corrected PSF FWHM and DIMM seeing for the
        current report.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = self.nightReportChannel.getDatesForSeqNums()
        rawDates = np.asarray([datesDict[seqNum] for seqNum in sorted(datesDict.keys())])

        # TODO: need to check the PSF FWHM column exists - it won't always
        inds = metadata.index[metadata['PSF FWHM'] > 0].tolist()  # get the non-nan values
        inds = [i-1 for i in inds]  # pandas uses 1-based indexing
        psfFwhm = np.array(metadata['PSF FWHM'].iloc[inds])
        seeing = np.array(metadata['DIMM Seeing'])
        bandColumn = metadata['Filter']
        bands = bandColumn[inds]
        # TODO: generalise this to all bands and add checks for if empty
        for i in range(1, len(bands)+1):
            if bands[i] == 'SDSSg':
                psfFwhm[i] = np.round(psfFwhm[i]*(metadata['Airmass'].iloc[i]**(-0.6)) *
                                      ((477./500.)**(0.2)), decimals=4)
            if bands[i] == 'SDSSr':
                psfFwhm[i] = np.round(psfFwhm[i]*(metadata['Airmass'].iloc[i]**(-0.6)) *
                                      ((623./500.)**(0.2)), decimals=4)
            if bands[i] == 'SDSSi':
                psfFwhm[i] = np.round(psfFwhm[i]*(metadata['Airmass'].iloc[i]**(-0.6)) *
                                      ((762./500.)**(0.2)), decimals=4)
        plt.plot_date(rawDates[inds], seeing[inds], '.', color='0.6', linestyle='-', label='DIMM')
        plt.plot_date(rawDates[inds], psfFwhm[inds], '.',
                      color='darkturqouise', linestyle='-', label='LATISS')
        plt.xlabel('TAI Date')
        plt.ylabel('PSF fwhm (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        plt.legend()
        return True
