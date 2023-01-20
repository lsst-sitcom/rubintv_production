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

from lsst.summit.utils.utils import getAirmassSeeingCorrection, getFilterSeeingCorrection

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

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        for item in ['Zeropoint', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get these colours from somewhere else
        gcolor = 'mediumseagreen'
        rcolor = 'lightcoral'
        icolor = 'mediumpurple'

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()
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

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()
        rawDates = np.asarray([datesDict[seqNum] for seqNum in sorted(datesDict.keys())])

        for item in ['PSF FWHM', 'Airmass', 'DIMM Seeing', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF FWHM'] > 0].tolist()  # get the non-nan values
        inds = [i-1 for i in inds]  # pandas uses 1-based indexing
        psfFwhm = np.array(metadata['PSF FWHM'].iloc[inds])
        airmass = np.array(metadata['Airmass'].iloc[inds])
        rawDates = rawDates[inds]
        seeing = np.array(metadata['DIMM Seeing'].iloc[inds])
        bandColumn = metadata['Filter']
        bands = np.array(bandColumn[inds])
        # TODO: generalise this to all bands
        for i in range(0, len(bands)):
            if bands[i] == 'SDSSg_65mm':
                psfFwhm[i] = (psfFwhm[i]*getFilterSeeingCorrection('SDSSg_65mm')
                              * getAirmassSeeingCorrection(airmass[i]))
            if bands[i] == 'SDSSr_65mm':
                psfFwhm[i] = (psfFwhm[i]*getFilterSeeingCorrection('SDSSr_65mm')
                              * getAirmassSeeingCorrection(airmass[i]))
            if bands[i] == 'SDSSi_65mm':
                psfFwhm[i] = (psfFwhm[i]*getFilterSeeingCorrection('SDSSi_65mm')
                              * getAirmassSeeingCorrection(airmass[i]))
            else:
                self.log.warning(f'Cannot correct unknown filter to 500nm seeing {bands[i]}')
                psfFwhm[i] = psfFwhm[i]*getAirmassSeeingCorrection(airmass[i])
        plt.plot(rawDates, seeing, '.', color='0.6', linestyle='-', label='DIMM')
        plt.plot(rawDates, psfFwhm, '.', color='darkturquoise', linestyle='-', label='LATISS')
        plt.xlabel('TAI Date')
        plt.ylabel('PSF FWHM (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        plt.legend()
        return True
