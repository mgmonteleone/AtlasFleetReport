import datetime

from atlas_lib import Fleet, Atlas
from gsheet_lib import SingleProjFleetReport, SingleOrgFleetReport
from atlasapi.specs import AtlasGranularities, AtlasPeriods
import os
from enum import Enum
from pymongo import MongoClient
from pprint import pprint

atlas_org = os.getenv('ATLAS_ORG')
atlas_user = os.getenv('ATLAS_USER')
atlas_key = os.getenv('ATLAS_KEY')
destination_db_string = os.getenv('ATLAS_DB_CONN')
REGION = os.getenv('ATLAS_REGION', 'US_EAST_1')

atlas: Atlas = Atlas(atlas_user, atlas_key)
org_obj = atlas.Organizations.organization_by_id(atlas_org)
project_list = list()
sheet_uri = 'https://docs.google.com/spreadsheets/d/1qKD9da3BnMJp9kNJenf_D5udsi4EhErAJ6bbZ69Icdw/edit#gid=1217399247'


class SaveTo(Enum):
    google = "Google Sheet"
    mongodb = "MongoDB Database"


def main(granularity: AtlasGranularities = AtlasGranularities.HOUR,
         period: AtlasPeriods = AtlasPeriods.HOURS_1, ns_metrics: bool = True, disk_metrics: bool = False,
         host_metrics: bool = False, save_to: SaveTo = SaveTo.mongodb):
    report_obj = SingleOrgFleetReport(sheet_uri=sheet_uri, granularity=granularity, period=period,
                                      atlas_user=atlas_user,
                                      atlas_org=atlas_org, atlas_key=atlas_key,
                                      include_namespace_metrics=ns_metrics, include_disk_metrics=disk_metrics,
                                      include_host_metrics=host_metrics)
    if save_to == SaveTo.google:

        report_obj.create_sheet_headers()
        report_obj.save_report_data_to_sheet(include_host_metrics=host_metrics, include_namespace_metrics=ns_metrics,
                                             include_disk_metrics=disk_metrics)
        report_obj.format_headers()
    elif save_to == SaveTo.mongodb:
        run_uct_date = datetime.datetime.utcnow()
        run_timestamp = run_uct_date.timestamp()
        run_date_string = run_uct_date.isoformat()
        m_client = MongoClient(destination_db_string)
        for each_cluster in report_obj.get_report_data():
            for k, v in each_cluster.items():
                if isinstance(v, Enum):
                    each_cluster[k] = v.name
            each_cluster["run_utc_date"] = run_uct_date
            each_cluster["run_timestamp"] = run_timestamp
            each_cluster["run_datestring"] = run_date_string
            each_cluster["run_date"] = datetime.datetime.combine(run_uct_date.date(), datetime.datetime.min.time())
            m_client.fleetReport[report_obj.org_obj.name.replace(" ", "_")].insert_one(each_cluster)


if __name__ == '__main__':
    main(save_to=SaveTo.mongodb)
