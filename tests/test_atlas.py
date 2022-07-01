from atlas_lib import Fleet
from atlasapi.atlas import Atlas
from atlasapi.specs import ReplicaSetTypes, AtlasPeriods, AtlasGranularities, Host, AtlasMeasurementTypes, \
    AtlasMeasurement, AtlasMeasurementValue
from pprint import pprint
from tests import BaseTests
import logging
from datetime import datetime

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

    #    def test_01_fleet_report_dataframe(self):
    #        atlas: Atlas = self.a
    #        current_fleet = Fleet(atlas)
    #
    #        dfout = current_fleet.get_full_report_primary_metrics_df(granularity=AtlasGranularities.HOUR,
    #                                                          period=AtlasPeriods.WEEKS_1)
    #        print(dfout)

    def test_01_store_measurements(self):
        atlas: Atlas = self.a
        current_fleet = Fleet(atlas)

        for each_cluster in current_fleet.clusters_list:
            if '-' not in each_cluster.name:
                try:
                    print(
                        f'Cluster : {each_cluster.name}... Primary Host = {each_cluster.primary(atlas).hostname_alias} - {each_cluster.primary(atlas).hostname}')
                except AttributeError:
                    print(
                        f'Cluster : {each_cluster.name}... Primary Host = {each_cluster.primary(atlas)} - {each_cluster.primary(atlas)}')
                for each_host in each_cluster.hosts(self.a):
                    print(f'-----------------{each_host.hostname}:{each_host.port}: role: {each_host.type.value}')
            else:
                print('------------------')

    def test_02_get_measurements(self):
        atlas: Atlas = self.a
        events =atlas.Events.since(datetime(2022,6,20))
        for each_event in events:
            pprint(each_event)