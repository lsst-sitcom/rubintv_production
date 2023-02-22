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
__all__ = ['RaDecAltAzOverTime',
           'DeltasPlot',
           'SourcesAndScatters',
           'AltAzCoverageTopDown',
           'AltAzCoverage',
           ]

COLORS = 'bgrcmyk'  # these get use in order to automatically give a series of colors for data series


class RaDecAltAzOverTime(StarTrackerPlot):
    _PlotName = 'ra-dec-alt-az-vs-time'
    _PlotGroup = 'Time-Series'

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
        axisLabelSize = 18
        nPlots = 4

        fig, axes = plt.subplots(figsize=(16, 4*nPlots), nrows=nPlots, ncols=1, sharex=True)
        fig.subplots_adjust(hspace=0)

        mjds = metadata['MJD']

        suffixes = ['', ' wide', ' fast']

        plotPairs = [('Alt', 'Calculated Alt'),
                     ('Az', 'Calculated Az'),
                     ('Ra', 'Calculated Ra'),
                     ('Dec', 'Calculated Dec'),
                     ]

        for plotNum, (quantity, fittedQuantity) in enumerate(plotPairs):
            for seriesNum, suffix in enumerate(suffixes):
                seriesName = quantity + suffix  # do the raw data
                if seriesName in metadata.columns:
                    data = metadata[seriesName]
                    axes[plotNum].plot(mjds, data, f'-{COLORS[seriesNum]}', label=seriesName)

                seriesName = fittedQuantity + suffix  # then try the fitted data
                if seriesName in metadata.columns:
                    data = metadata[seriesName]
                    axes[plotNum].plot(mjds, data, f'--{COLORS[seriesNum+1]}', label=seriesName)

                axes[plotNum].legend()
                axes[plotNum].set_xlabel('MJD', size=axisLabelSize)
                axes[plotNum].set_ylabel(quantity, size=axisLabelSize)
        return True


class DeltasPlot(StarTrackerPlot):
    _PlotName = 'delta-ra-dec-alt-az-rot-vs-time'
    _PlotGroup = 'Time-Series'

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
        axisLabelSize = 18
        nPlots = 5

        colors = 'bgrcmyk'

        fig, axes = plt.subplots(figsize=(16, 4*nPlots), nrows=nPlots, ncols=1, sharex=True)
        fig.subplots_adjust(hspace=0)

        mjds = metadata['MJD']

        suffixes = ['', ' wide', ' fast']

        plots = ['Delta Alt Arcsec',
                 'Delta Az Arcsec',
                 'Delta Dec Arcsec',
                 'Delta Ra Arcsec',
                 'Delta Rot Arcsec']

        for plotNum, quantity in enumerate(plots):
            for seriesNum, suffix in enumerate(suffixes):
                seriesName = quantity + suffix
                if seriesName in metadata.columns:
                    data = metadata[seriesName]
                    axes[plotNum].plot(mjds, data, f'-{colors[seriesNum]}', label=seriesName)

                axes[plotNum].legend()
                axes[plotNum].set_xlabel('MJD', size=axisLabelSize)
                axes[plotNum].set_ylabel(quantity, size=axisLabelSize)
        return True


class SourcesAndScatters(StarTrackerPlot):
    _PlotName = 'sourceCount-and-astrometric-scatter-vs-time'
    _PlotGroup = 'Time-Series'

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
        axisLabelSize = 18
        nPlots = 4

        colors = 'bgrcmyk'

        fig, axes = plt.subplots(figsize=(16, 4*nPlots), nrows=nPlots, ncols=1, sharex=True)
        fig.subplots_adjust(hspace=0)

        mjds = metadata['MJD']

        suffixes = ['', ' wide', ' fast']

        plots = ['RMS scatter arcsec',
                 'RMS scatter pixels',
                 'nSources',
                 'nSources filtered']

        for plotNum, quantity in enumerate(plots):
            for seriesNum, suffix in enumerate(suffixes):
                seriesName = quantity + suffix
                if seriesName in metadata.columns:
                    data = metadata[seriesName]
                    axes[plotNum].plot(mjds, data, f'-{colors[seriesNum]}', label=seriesName)

                axes[plotNum].legend()
                axes[plotNum].set_xlabel('MJD', size=axisLabelSize)
                axes[plotNum].set_ylabel(quantity, size=axisLabelSize)
        return True


class AltAzCoverageTopDown(StarTrackerPlot):
    _PlotName = 'Alt-Az-top-down'
    _PlotGroup = 'Coverage'

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
        _ = plt.figure(figsize=(10, 10))
        ax = plt.subplot(111, polar=True)

        alts = metadata['Alt']
        azes = metadata['Az']

        ax.plot([az*np.pi/180 for az in azes], alts, 'or', label='Pointing')
        if 'Calculated Dec wide' in metadata.columns:
            hasWideSolve = metadata.dropna(subset=['Calculated Dec wide'])
            wideAlts = hasWideSolve['Alt']
            wideAzes = hasWideSolve['Az']
            ax.scatter([az*np.pi/180 for az in wideAzes], wideAlts, marker='o', s=200,
                       facecolors='none', edgecolors='b', label='Wide Solve')

        if 'Calculated Dec' in metadata.columns:
            hasNarrowSolve = metadata.dropna(subset=['Calculated Dec'])
            narrowAlts = hasNarrowSolve['Alt']
            narrowAzes = hasNarrowSolve['Az']
            ax.scatter([az*np.pi/180 for az in narrowAzes], narrowAlts, marker='o', s=400,
                       facecolors='none', edgecolors='g', label='Narrow Solve')
        ax.legend()
        ax.set_title("Axial coverage - azimuth (theta) vs altitude(r)"
                     "\n 'Top down' view with zenith at center", va='bottom')
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_rlim(0, 90)

        ax.invert_yaxis()  # puts 90 (the zenith) at the center
        return True


class AltAzCoverage(StarTrackerPlot):
    _PlotName = 'Alt-Az-coverage'
    _PlotGroup = 'Coverage'

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
        _ = plt.figure(figsize=(10, 10))
        ax = plt.subplot(111, polar=True)

        alts = metadata['Alt']
        zenithAngles = [alt for alt in alts]
        azes = metadata['Az']

        ax.plot([az*np.pi/180 for az in azes], zenithAngles, 'o')
        ax.set_title("Axial coverage - azimuth (theta) vs altitude (r)", va='bottom')
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_rlim(0, 90)

        return True
