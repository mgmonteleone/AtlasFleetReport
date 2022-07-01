import os

from atlas_lib import Fleet
import gsheet_lib
from atlasapi.atlas import Atlas
from atlasapi.specs import ReplicaSetTypes, AtlasPeriods, AtlasGranularities, Host, AtlasMeasurementTypes, \
    AtlasMeasurement, AtlasMeasurementValue
from pprint import pprint
from tests import BaseTests
import logging
from datetime import datetime

sheet_uri = 'https://docs.google.com/spreadsheets/d/1qKD9da3BnMJp9kNJenf_D5udsi4EhErAJ6bbZ69Icdw/edit#gid=0'
logger = logging.getLogger('test')
logger.setLevel(logging.DEBUG)


class GsheetOutputTests(BaseTests):

    def test_00_fleet_report(self):
        atlas: Atlas = self.a
        report_obj = gsheet_lib.SingleProjFleetReport(atlas_user=os.getenv('ATLAS_USER'), atlas_key=os.getenv('ATLAS_KEY'),
                                                      sheet_uri=sheet_uri, atlas_group=os.getenv('ATLAS_GROUP'),
                                                      granularity=AtlasGranularities.HOUR, period=AtlasPeriods.HOURS_8,
                                                      include_disk_metrics=True,include_host_metrics=True)
        self.assertIsNotNone(report_obj.spreadsheet.creationTime)
        report_obj.create_sheet_headers_manual()
        report_obj.save_report_data_to_sheet()
