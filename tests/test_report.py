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


class OrgReportClassTests(BaseTests):
    def test_00(self):
        report_obj = gsheet_lib.SingleOrgFleetReport(sheet_uri=sheet_uri, atlas_user=self.USER, atlas_key=self.API_KEY,
                                                     atlas_org=os.getenv('ATLAS_GROUP'), include_host_metrics=False,
                                                     include_disk_metrics=False, include_namespace_metrics=True)
        report_obj.create_sheet_headers()
        report_obj.format_headers()
        report_obj.save_report_data_to_sheet()

    def test_01_just_one(self):
        report_obj = gsheet_lib.SingleProjFleetReport( sheet_uri=sheet_uri, atlas_user=self.USER, atlas_key=self.API_KEY,
                                                      include_host_metrics=False,  atlas_group="5cc887dbf2a30b3c755ac0f0",
                                                      include_disk_metrics=False, include_namespace_metrics=True)
        for each in report_obj.get_report_data(cluster_name='neutrino-xflow-eth-calls'):
            pprint(each)
