from atlas_lib import Fleet
from atlasapi.atlas import Atlas
from atlasapi.specs import ReplicaSetTypes, AtlasPeriods, AtlasGranularities, Host, AtlasMeasurementTypes,\
    AtlasMeasurement, AtlasMeasurementValue
from pprint import pprint
from tests import BaseTests
import logging


logger = logging.getLogger('test')


class MeasurementTests(BaseTests):

    def test_00_fleet_report(self):
        atlas: Atlas = self.a
        current_fleet = Fleet(atlas)
        n = 0
        for each in current_fleet.get_full_report_primary_metrics(granularity=AtlasGranularities.HOUR,
                                                          period=AtlasPeriods.WEEKS_1):
            pprint(each)
            self.assertIsInstance(each, dict)
            n += 1
            if n == 3:
                break

    test_00_fleet_report.basic = True




