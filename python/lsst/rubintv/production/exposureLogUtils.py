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

import requests
import logging

EXPOSURE_LOG_URL = 'https://summit-lsp.lsst.codes/exposurelog/messages'

LOG_ITEM_MAPPINGS = {
    'message_text': 'Log message',
    'level': 'Log level',
    'urls': 'Jira ticket',
    'exposure_flag': 'Quality flag'
}


def getLogsForDayObs(dayObs, logger=None):
    """Get a dictionary of log messages for the dayObs, keyed by seqNum.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to get logs for.
    logger : `logging.Logger`, optional
        The logger to use for warning messages. Created if not provided.

    Returns
    -------
    logs : `dict` [`int`, `dict`]
        The logs for the dayObs, keyed by seqNum.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    query = (f'{EXPOSURE_LOG_URL}?'
             f'min_day_obs={dayObs}&max_day_obs={dayObs+1}'
             '&is_human=either&is_valid=true&offset=0&limit=10000'
             )
    response = requests.request('GET', query)
    if not response.ok:
        logger.warning('Reponse from exposureLog REST API was not OK')
        return None

    logs = response.json()
    messages = {log["seq_num"]: log for log in logs}
    return messages
