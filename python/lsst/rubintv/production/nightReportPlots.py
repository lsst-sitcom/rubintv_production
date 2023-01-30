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
__all__ = ['ZeroPointPlot', 'PsfFwhmPlot', 'SourceCountsPlot', 'PsfE1Plot', 'PsfE2Plot']

# TODO: get these colours from somewhere else
gcolor = 'mediumseagreen'
rcolor = 'lightcoral'
icolor = 'mediumpurple'


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

    def plot(self, nightReport, metadata, visitSummaryTable):
        """Create the zeropoint plot.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        visitSummaryTable : `pandas.DataFrame`
            The visit summary table for the current day.

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

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        # TODO: need to check the Zeropoint column exists - it won't always
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

    def plot(self, nightReport, metadata, visitSummaryTable):
        """Plot filter and airmass corrected PSF FWHM and DIMM seeing for the
        current report.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        visitSummaryTable : `pandas.DataFrame`
            The visit summary table for the current day.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        for item in ['PSF FWHM', 'Airmass', 'DIMM Seeing', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF FWHM'] > 0].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        psfFwhm = np.array(metadata['PSF FWHM'][inds])
        airmass = np.array(metadata['Airmass'][inds])
        seeing = np.array(metadata['DIMM Seeing'][inds])
        bands = np.array(metadata['Filter'][inds])
        # TODO: generalise this to all bands
        for i in range(0, len(bands)):
            airMassCorr = getAirmassSeeingCorrection(airmass[i])
            if bands[i] == 'SDSSg_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSg_65mm'))
            if bands[i] == 'SDSSr_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSr_65mm'))
            if bands[i] == 'SDSSi_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSi_65mm'))
            else:
                self.log.warning(f'Cannot correct unknown filter to 500nm seeing {bands[i]}')
                psfFwhm[i] = psfFwhm[i]*airMassCorr

        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')

        plt.plot(rawDates, seeing, '.', color='0.6', linestyle='-', label='DIMM', alpha=0.5)
        plt.plot(rawDates[gband], psfFwhm[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[rband], psfFwhm[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[iband], psfFwhm[iband], '.', color=icolor, linestyle='-', label='SDSSi')
        plt.xlabel('TAI Date')
        plt.ylabel('PSF FWHM (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class PsfE1Plot(BasicPlot):
    _PlotName = 'PSF-e1'
    _PlotGroup = 'Seeing'

    def __init__(self,
                 dayObs,
                 nightReportChannel):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         nightReportChannel=nightReportChannel)

    def plot(self, nightReport, metadata, visitSummaryTable):
        """Plot the PSF ellipticity e1 values for the current report.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        visitSummaryTable : `pandas.DataFrame`
            The visit summary table for the current day.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        for item in ['PSF e1', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF e1'] > -100].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        psf_e1 = np.array(metadata['PSF e1'][inds])
        bands = np.array(metadata['Filter'][inds])

        # TODO: generalise this to all bands
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')

        plt.plot(rawDates[gband], psf_e1[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[rband], psf_e1[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[iband], psf_e1[iband], '.', color=icolor, linestyle='-', label='SDSSi')

        plt.xlabel('TAI Date')
        plt.ylabel('PSF Ellipticity e1')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class PsfE2Plot(BasicPlot):
    _PlotName = 'PSF-e2'
    _PlotGroup = 'Seeing'

    def __init__(self,
                 dayObs,
                 nightReportChannel):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         nightReportChannel=nightReportChannel)

    def plot(self, nightReport, metadata, visitSummaryTable):
        """Plot the PSF ellipticity e2 values for the current report.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        visitSummaryTable : `pandas.DataFrame`
            The visit summary table for the current day.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        for item in ['PSF e2', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF e2'] > -100].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        psf_e2 = np.array(metadata['PSF e2'][inds])
        bands = np.array(metadata['Filter'][inds])

        # TODO: generalise this to all bands
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')

        plt.plot(rawDates[gband], psf_e2[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[rband], psf_e2[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[iband], psf_e2[iband], '.', color=icolor, linestyle='-', label='SDSSi')

        plt.xlabel('TAI Date')
        plt.ylabel('PSF Ellipticity e2')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class SourceCountsPlot(BasicPlot):
    _PlotName = 'Source-Counts'
    _PlotGroup = 'Seeing'

    def __init__(self,
                 dayObs,
                 nightReportChannel):
        super().__init__(dayObs=dayObs,
                         plotName=self._PlotName,
                         plotGroup=self._PlotGroup,
                         nightReportChannel=nightReportChannel)

    def plot(self, nightReport, metadata, visitSummaryTable):
        """Plot source counts for sources detected above 5-sigma and sources
        used for PSF fitting.

        Parameters
        ----------
        nightReport : `lsst.rubintv.production.nightReport.NightReport`
            The night report for the current night.
        metadata : `pandas.DataFrame`
            The front page metadata, as a dataframe.
        visitSummaryTable : `pandas.DataFrame`
            The visit summary table for the current day.

        Returns
        -------
        success : `bool`
            Did the plotting succeed, and thus upload should be performed?
        """
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        for item in ['5-sigma source count', 'PSF star count']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF star count'] > 0].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        five_sigma_source_count = np.array(metadata['5-sigma source count'][inds])
        psf_star_count = np.array(metadata['PSF star count'][inds])

        plt.plot(rawDates, five_sigma_source_count, '.', color='0.8', linestyle='-', label='5-sigma Sources')
        plt.plot(rawDates, psf_star_count, '.', color='0.0', linestyle='-', label='PSF Star Sources')
        plt.xlabel('TAI Date')
        plt.ylabel('Number of Sources')
        plt.yscale('log')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True
