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

from .nightReportPlotBase import LatissPlot

# any classes ro functions added to PLOT_FACTORIES will automatically be added
# to the night report channel, with each being replotted for each image taken.
PLOT_FACTORIES = ['ZeroPointPlot',
                  'SkyMeanPlot',
                  'PsfFwhmPlot',
                  'SourceCountsPlot',
                  'PsfE1Plot',
                  'PsfE2Plot',
                  'TelescopeAzElPlot',
                  'MountMotionPlot',
                  'MountMotionVsZenith',
                  'SkyMeanVsSkyRms',
                  'PsfVsSkyRms',
                  'PsfVsZenith',
                  'PsfVsMountMotion',
                  ]

__all__ = PLOT_FACTORIES

# TODO: DM-38287 centralize these, and make a full list/mapping
gcolor = 'mediumseagreen'
rcolor = 'lightcoral'
icolor = 'mediumpurple'


class ZeroPointPlot(LatissPlot):
    _PlotName = 'per-band-zeropoints'
    _PlotGroup = 'Photometry'

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
        for item in ['Zeropoint', 'Filter']:
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


class SkyMeanPlot(LatissPlot):
    _PlotName = 'Per-Band-Sky-Mean'
    _PlotGroup = 'Photometry'

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
        for item in ['Sky mean', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        inds = metadata.index[metadata['Sky mean'] > 0].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        bands = np.asarray(metadata['Filter'][inds])
        # TODO: generalise this to all bands and add checks for if empty
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')
        skyMean = np.array(metadata['Sky mean'][inds])
        plt.plot(rawDates[gband], skyMean[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        plt.plot(rawDates[rband], skyMean[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        plt.plot(rawDates[iband], skyMean[iband], '.', color=icolor, linestyle='-', label='SDSSi')
        plt.xlabel('TAI Date')
        plt.ylabel('Sky Background (counts per pixel)')
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


class PsfFwhmPlot(LatissPlot):
    _PlotName = 'PSF-FWHM'
    _PlotGroup = 'Seeing'

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
        """Plot filter and airmass corrected PSF FWHM and DIMM seeing for the
        current report.

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
            elif bands[i] == 'SDSSr_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSr_65mm'))
            elif bands[i] == 'SDSSi_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSi_65mm'))
            elif bands[i] == 'SDSSy_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSy_65mm'))
            elif bands[i] == 'SDSSz_65mm':
                psfFwhm[i] = (psfFwhm[i]*airMassCorr*getFilterSeeingCorrection('SDSSz_65mm'))
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


class PsfE1Plot(LatissPlot):
    _PlotName = 'PSF-e1'
    _PlotGroup = 'Seeing'

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
        """Plot the PSF ellipticity e1 values for the current report.

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


class PsfE2Plot(LatissPlot):
    _PlotName = 'PSF-e2'
    _PlotGroup = 'Seeing'

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
        """Plot the PSF ellipticity e2 values for the current report.

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


class SourceCountsPlot(LatissPlot):
    _PlotName = 'Source-Counts'
    _PlotGroup = 'Seeing'

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
        """Plot source counts for sources detected above 5-sigma and sources
        used for PSF fitting.

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


class TelescopeAzElPlot(LatissPlot):
    _PlotName = 'Telescope-AzEl-Plot'
    _PlotGroup = 'Seeing'

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
        """Plot Telescope Azimuth and Elevation with Wind Direction and
        Wind Speed as subplots

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
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        datesDict = nightReport.getDatesForSeqNums()

        for item in ['PSF FWHM']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: need to check the Zeropoint column exists - it won't always
        inds = metadata.index[metadata['PSF FWHM'] > 0].tolist()

        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        telAz = np.asarray([nightReport.data[seqNum]['_raw_metadata']['AZSTART'] for seqNum in inds])
        telAz = np.where(telAz > 0, telAz, telAz+360.)
        telEl = np.asarray([nightReport.data[seqNum]['_raw_metadata']['ELSTART'] for seqNum in inds])

        windSpd = np.asarray([nightReport.data[seqNum]['_raw_metadata']['WINDSPD'] for seqNum in inds])
        windDir = np.asarray([nightReport.data[seqNum]['_raw_metadata']['WINDDIR'] for seqNum in inds])

        fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, sharex=True, gridspec_kw={
            'height_ratios': [2, 1, 2]}, figsize=(6.4, 4.8*1.5))
        ax1.plot(rawDates, windDir, '.', color='royalblue', linestyle='-', label='Wind Az', alpha=0.5)
        ax1.plot(rawDates, telAz, '.', color='firebrick', linestyle='-', label='Telescope Az')
        ax1.set_ylim(0, 365)
        ax1.tick_params(which='both', direction='in')
        major_ticks = [0, 90, 180, 270, 360]
        ax1.set_yticks(major_ticks)
        ax1.yaxis.get_ticklocs(minor=True)
        ax1.minorticks_on()
        ax1.set_ylabel('Azimuth (deg)')
        ax1.grid()
        ax1.legend(fontsize='medium', loc='lower right')

        ax2.plot(rawDates, windSpd, '.', color='darkturquoise', linestyle='-', label='Wind Speed')
        ax2.set_ylabel('(m/s)')
        ax2.tick_params(which='both', direction='in')
        ax2.yaxis.get_ticklocs(minor=True)
        ax2.minorticks_on()
        ax2.grid()
        ax2.legend(fontsize='medium', loc='lower right')

        ax3.plot(rawDates, telEl, '.', color='darksalmon', linestyle='-', label='Telescope El')
        ax3.set_ylim(0, 95)
        ax3.tick_params(which='both', direction='in')
        major_ticks = [30, 60, 90]
        ax3.set_yticks(major_ticks)
        ax3.set_ylabel('Elevation (deg)')
        ax3.yaxis.get_ticklocs(minor=True)
        ax3.minorticks_on()
        ax3.grid()
        ax3.legend(fontsize='medium', loc='lower right')

        plt.xlabel('TAI Date')
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax3.xaxis.set_major_formatter(xfmt)
        for label in ax3.get_xticklabels(which='major'):
            label.set(rotation=30, horizontalalignment='right')
        plt.tight_layout()
        return True


class MountMotionPlot(LatissPlot):
    _PlotName = 'Mount-Motion'
    _PlotGroup = 'Seeing'

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
        """Plot the RMS mount motion vs time.

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
        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        datesDict = nightReport.getDatesForSeqNums()

        for item in ['PSF FWHM', 'Mount motion image degradation']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        inds = metadata.index[metadata['PSF FWHM'] > 0].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        mountMotion = np.array(metadata['Mount motion image degradation'][inds])

        plt.plot(rawDates, mountMotion, '.', color='0.6', linestyle='-', label='Mount Motion')
        plt.xlabel('TAI Date')
        plt.ylabel('RMS Mount Motion (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)
        ax.minorticks_on()
        ax.tick_params(which='both', direction='in')
        plt.legend()
        return True


class AstrometricOffsetMeanPlot(LatissPlot):
    _PlotName = 'Per-Band-Astrometry-Offset-Mean'
    _PlotGroup = 'Astrometry'

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
        """Create the astometric offset mean plot.

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
        for item in ['Astrometric bias', 'Filter']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)
        fig, ax1 = plt.subplots(1)

        datesDict = nightReport.getDatesForSeqNums()
        inds = metadata.index[metadata['Astrometric bias'] > -10000].tolist()  # get the non-nan values
        rawDates = np.asarray([datesDict[seqNum] for seqNum in inds])
        bands = np.asarray(metadata['Filter'][inds])
        # TODO: generalise this to all bands and add checks for if empty
        rband = np.where(bands == 'SDSSr_65mm')
        gband = np.where(bands == 'SDSSg_65mm')
        iband = np.where(bands == 'SDSSi_65mm')
        astromOffsetMean = np.array(metadata['Astrometric bias'][inds])
        ax1.plot(rawDates[gband], astromOffsetMean[gband], '.', color=gcolor, linestyle='-', label='SDSSg')
        ax1.plot(rawDates[rband], astromOffsetMean[rband], '.', color=rcolor, linestyle='-', label='SDSSr')
        ax1.plot(rawDates[iband], astromOffsetMean[iband], '.', color=icolor, linestyle='-', label='SDSSi')
        ax1.set_xlabel('TAI Date')
        ax1.set_ylabel('Astrometric Offset Mean (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        ax1.grid()
        xfmt = md.DateFormatter('%m-%d %H:%M:%S')
        ax1.xaxis.set_major_formatter(xfmt)
        ax1.tick_params(which='both', direction='in')
        ax1.minorticks_on()

        ax2 = ax1.twinx()
        ax2.plot(rawDates, metadata['Airmass'][inds], 'k--', label='Airmass', alpha=0.5)
        ax2.set_ylabel('Airmass', color='k')

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc=0)

        return True


class MountMotionVsZenith(LatissPlot):
    _PlotName = 'MountMotion-zenith'
    _PlotGroup = 'Elana'

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
        """Create the Mt Motion vs Zenith angle plot.

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
        for item in ['Zenith angle', 'Filter', 'Mount motion image degradation']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        zenith = np.asarray(metadata['Zenith angle'])
        mountMotion = np.asarray(metadata['Mount motion image degradation'])

        bands = np.asarray(metadata['Filter'])
        band_list = set(metadata['Filter'])

        for band in band_list:
            band_loc = np.where(bands == band)
            plt.plot(zenith[band_loc], mountMotion[band_loc], '.', linestyle='', label=band)

        plt.xlabel('Zenith Angle (degrees)')
        plt.ylabel('Mount Motion Image Degradation (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class SkyMeanVsSkyRms(LatissPlot):
    _PlotName = 'SkyMean-SkyRMS'
    _PlotGroup = 'Elana'

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
        """Create the Sky mean vs SkyRMS plot.

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
        for item in ['Zenith angle', 'Filter', 'PSF FWHM']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        skyRms = np.asarray(metadata['Sky RMS'])
        skyMean = np.asarray(metadata['Sky mean'])

        bands = np.asarray(metadata['Filter'])
        band_list = set(metadata['Filter'])

        for band in band_list:
            band_loc = np.where(bands == band)
            plt.plot(skyMean[band_loc], skyRms[band_loc], '.', linestyle='', label=band)

        plt.xlabel('Sky Mean')
        plt.ylabel('Sky RMS')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class PsfVsSkyRms(LatissPlot):
    _PlotName = 'PSF-SkyRMS'
    _PlotGroup = 'Elana'

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
        """Create the PSF vs SkyRMS plot.

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
        for item in ['Zenith angle', 'Filter', 'PSF FWHM']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        skyRms = np.asarray(metadata['Sky RMS'])
        psf = np.asarray(metadata['PSF FWHM'])

        bands = np.asarray(metadata['Filter'])
        band_list = set(metadata['Filter'])

        for band in band_list:
            band_loc = np.where(bands == band)
            plt.plot(skyRms[band_loc], psf[band_loc], '.', linestyle='', label=band)

        plt.xlabel('Sky RMS')
        plt.ylabel('PSF FWHM (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class PsfVsZenith(LatissPlot):
    _PlotName = 'PSF-zenith'
    _PlotGroup = 'Elana'

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
        """Create the PSF vs Zenith angle plot.

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
        for item in ['Zenith angle', 'Filter', 'PSF FWHM']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        zenith = np.asarray(metadata['Zenith angle'])
        psf = np.asarray(metadata['PSF FWHM'])

        bands = np.asarray(metadata['Filter'])
        band_list = set(metadata['Filter'])

        for band in band_list:
            band_loc = np.where(bands == band)
            plt.plot(zenith[band_loc], psf[band_loc], '.', linestyle='', label=band)

        plt.xlabel('Zenith Angle (degrees)')
        plt.ylabel('PSF FWHM (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True


class PsfVsMountMotion(LatissPlot):
    _PlotName = 'PSF-Mount-motion'
    _PlotGroup = 'Elana'

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
        """Create the PSF vs mount motion ploot.

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
        for item in ['Mount motion image degradation', 'Filter', 'PSF FWHM']:
            if item not in metadata.columns:
                msg = f'Cannot create {self._PlotName} plot as required item {item} is not in the table.'
                self.log.warning(msg)
                return False

        # TODO: get a figure you can reuse to avoid matplotlib memory leak
        plt.figure(constrained_layout=True)

        mountMotion = np.asarray(metadata['Mount motion image degradation'])
        psf = np.asarray(metadata['PSF FWHM'])

        bands = np.asarray(metadata['Filter'])
        band_list = set(metadata['Filter'])

        for band in band_list:
            band_loc = np.where(bands == band)
            plt.plot(mountMotion[band_loc], psf[band_loc], '.', linestyle='', label=band)

        plt.xlabel('Mount Motion Image Degradation (arcsec)')
        plt.ylabel('PSF FWHM (arcsec)')
        plt.xticks(rotation=25, horizontalalignment='right')
        plt.grid()
        ax = plt.gca()
        ax.tick_params(which='both', direction='in')
        ax.minorticks_on()
        plt.legend()
        return True
