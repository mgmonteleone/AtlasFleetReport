import gspread
from atlasapi.atlas import Atlas, AtlasGranularities, AtlasPeriods
from atlas_lib import Fleet, METRICS, DISK_METRICS
from xlsxwriter.utility import xl_col_to_name
from enum import Enum
import os
from time import time
import logging
from pprint import pprint

report_uri = 'https://docs.google.com/spreadsheets/d/1qKD9da3BnMJp9kNJenf_D5udsi4EhErAJ6bbZ69Icdw/edit#gid=0'

logger = logging.getLogger("gsheet")
logger.setLevel(logging.DEBUG)


class SingleProjFleetReport:
    def __init__(self, atlas_user: str,
                 atlas_key: str, atlas_group: str, sheet_uri: str = os.getenv('SHEET_URI'),
                 granularity: AtlasGranularities = AtlasGranularities.HOUR,
                 period: AtlasPeriods = AtlasPeriods.HOURS_8,
                 include_namespace_metrics: bool = True, include_host_metrics: bool = False,
                 include_disk_metrics: bool = False):
        """

        :param sheet_uri:
        :param atlas_user:
        :param atlas_key:
        :param atlas_group:
        """
        self.atlas: Atlas = Atlas(atlas_user, atlas_key, atlas_group)
        self.fleet: Fleet = Fleet(self.atlas)
        self.project_obj = self.fleet.atlas.Projects.project_by_id(self.fleet.atlas.group)
        self.gspread_obj = gspread.service_account()
        self.spreadsheet = self.gspread_obj.open_by_url(sheet_uri)
        try:
            self.active_worksheet = self.spreadsheet.worksheet(self.project_obj.name)
            self.spreadsheet.del_worksheet(self.active_worksheet)
        except gspread.exceptions.WorksheetNotFound:
            print("Spreadsheet not there, so will not delete it.")

        print(f"Creating Spreadsheet named {self.project_obj.name}")
        self.active_worksheet = self.spreadsheet.add_worksheet(self.project_obj.name, 2, 1)
        self.granularity: AtlasGranularities = granularity
        self.period: AtlasPeriods = period
        self.include_namespace_metrics = include_namespace_metrics
        self.include_host_metrics = include_host_metrics
        self.include_disk_metrics = include_disk_metrics


    def update_status_in_sheet(self, row_ref: int = 1, col_ref: int = 1, status_text: str = 'OK'):
        self.active_worksheet.update_cell(row_ref, col_ref, value=status_text)

    def create_sheet_headers_manual(self):
        APPENDIX_HEADERS = ['Granularity', 'Period']
        NAMESPACE_HEADERS = ['views',	'objects',	'indexes',	'collections',	'databases']
        BASE_HEADERS = ['ro', 'analytics', 'electable', 'shards', 'io_type', 'IOPS', 'tier', 'disk_size', 'name', \
                       'id', 'project_id', 'project_name']
        if self.include_namespace_metrics is True:
            BASE_HEADERS.extend(NAMESPACE_HEADERS)
        if self.include_host_metrics is True:
            BASE_HEADERS.extend(METRICS)
        if self.include_disk_metrics is True:
            BASE_HEADERS.extend(DISK_METRICS)
        BASE_HEADERS.extend(APPENDIX_HEADERS)
        start = time()
        print(f'Creating Headers...')
        self.active_worksheet.update_cell(1, 1, value='Creating Headers.....')
        self.active_worksheet.append_row(BASE_HEADERS)
        print('Done creating headers')
        self.update_status_in_sheet(status_text=f'Done Creating Headers ({int(time() - start)}s)')
        print(f'Done Creating Headers ({int(time() - start)}s)')



    def create_sheet_headers(self):
        start = time()
        print(f'Creating Headers...')
        self.active_worksheet.update_cell(1, 1, value='Creating Headers.....')
        header = []
        print(
            f'Going to use the following parameters for headers G: {AtlasGranularities.HOUR}, P: {AtlasPeriods.HOURS_8}')
        for each in self.fleet.get_full_report_primary_metrics(granularity=AtlasGranularities.HOUR,
                                                               period=AtlasPeriods.HOURS_8,
                                                               include_namespace_metrics=self.include_namespace_metrics,
                                                               include_disk_metrics=self.include_disk_metrics,
                                                               include_host_metrics=self.include_host_metrics):
            for each_name in each.keys():
                header.append(each_name)
            break

        self.active_worksheet.append_row(header)
        print('Done creating headers')
        self.update_status_in_sheet(status_text=f'Done Creating Headers ({int(time() - start)}s)')
        print(f'Done Creating Headers ({int(time() - start)}s)')
        self.active_worksheet = self.spreadsheet.worksheet(self.project_obj.name)
        print(f"There are {self.active_worksheet.col_count} Columns")

    def format_headers(self):
        format = {
            "wrapStrategy": 'WRAP',
            "backgroundColor": {
                "red": 0.0,
                "green": 0.0,
                "blue": 0.0
            },
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "fontSize": 10,
                "bold": True

            }
        }
        self.active_worksheet.format(f"A2:{xl_col_to_name(self.active_worksheet.col_count)}2", format)

    def get_report_data(self) -> dict:
        for each_cluster in self.fleet.get_full_report_primary_metrics(include_host_metrics=self.include_host_metrics,
                                                                       include_namespace_metrics=self.include_namespace_metrics,
                                                                       include_disk_metrics=self.include_disk_metrics,
                                                                       granularity=self.granularity,
                                                                       period=self.period):
            yield each_cluster

    def save_report_data_to_sheet(self, include_namespace_metrics: bool = True, include_host_metrics: bool = True,
                                  include_disk_metrics: bool = True):
        for each_cluster in self.get_report_data():
            start = time()
            list_holder = []
            self.update_status_in_sheet(status_text=f'Pulling Data for {each_cluster.get("name")}')
            for each_value in each_cluster.values():
                if isinstance(each_value, Enum):  # If the value is  an enum, we need to get the value to be cleaner.
                    list_holder.append(each_value.value)
                else:
                    list_holder.append(each_value)
            print('Appending Row!!!')
            print(f'Values are {list_holder.__str__()}')

            self.active_worksheet.append_row(list_holder)

            self.update_status_in_sheet(status_text=f"Completed {each_cluster.get('name')} ({time() - start})s")


class SingleOrgFleetReport:
    def __init__(self, sheet_uri: str = os.getenv('SHEET_URI'),
                 granularity: AtlasGranularities = AtlasGranularities.HOUR,
                 period: AtlasPeriods = AtlasPeriods.HOURS_8,
                 atlas_user: str = os.getenv('ATLAS_USER'),
                 atlas_key: str = os.getenv('ATLAS_KEY'),
                 atlas_org: str = os.getenv('ATLAS_ORG'),
                 include_namespace_metrics: bool = True, include_host_metrics: bool = False,
                 include_disk_metrics: bool = False):
        """

        :param sheet_uri:
        :param atlas_user:
        :param atlas_key:
        """
        self.atlas: Atlas = Atlas(atlas_user, atlas_key)
        self.org_obj = self.atlas.Organizations.organization_by_id(atlas_org)
        self.project_list = list()
        for each in self.atlas.Organizations.get_all_projects_for_org(org_id=atlas_org):
            self.project_list.append(each)
        self.fleet_list = list()
        for each_project in self.project_list:
            atlas_obj = Atlas(atlas_user, atlas_key, each_project.id)
            self.fleet_list.append(
                dict(fleet_obj=Fleet(atlas_obj), project_obj=each_project, atlas_obj=atlas_obj)
            )
        self.gspread_obj = gspread.service_account()
        self.spreadsheet = self.gspread_obj.open_by_url(sheet_uri)
        try:
            self.active_worksheet = self.spreadsheet.worksheet(self.org_obj.name)
            self.spreadsheet.del_worksheet(self.active_worksheet)
        except gspread.exceptions.WorksheetNotFound:
            print("Spreadsheet not there, so will not delete it.")

        print(f"Creating Spreadsheet named {self.org_obj.name}")
        self.active_worksheet = self.spreadsheet.add_worksheet(self.org_obj.name, 2, 1)
        self.granularity: AtlasGranularities = granularity
        self.period: AtlasPeriods = period
        self.include_namespace_metrics = include_namespace_metrics
        self.include_host_metrics = include_host_metrics
        self.include_disk_metrics = include_disk_metrics

        self.create_sheet_headers()

    def update_status_in_sheet(self, row_ref: int = 1, col_ref: int = 1, status_text: str = 'OK'):
        self.active_worksheet.update_cell(row_ref, col_ref, value=status_text)

    def create_sheet_headers(self):
        start = time()
        print(f'Creating Headers...')
        self.active_worksheet.update_cell(1, 1, value='Creating Headers.....')
        header = []
        print(
            f'Going to use the following parameters for headers G: {AtlasGranularities.HOUR}, P: {AtlasPeriods.HOURS_8}')
        for each in self.fleet_list:
            if each.get('project_obj').cluster_count > 0:
                first_proj = each.get('fleet_obj')
                for each_one in first_proj.clusters_list:
                    pprint(each_one.__dict__)

        for each in first_proj.get_full_report_primary_metrics(granularity=AtlasGranularities.HOUR,
                                                               period=AtlasPeriods.HOURS_8,
                                                               include_namespace_metrics=self.include_namespace_metrics,
                                                               include_disk_metrics=self.include_disk_metrics,
                                                               include_host_metrics=self.include_host_metrics):
            for each_name in each.keys():
                header.append(each_name)
            break

        self.active_worksheet.append_row(header)
        print('Done creating headers')
        self.update_status_in_sheet(status_text=f'Done Creating Headers ({int(time() - start)}s)')
        print(f'Done Creating Headers ({int(time() - start)}s)')
        self.active_worksheet = self.spreadsheet.worksheet(self.org_obj.name)
        print(f"There are {self.active_worksheet.col_count} Columns")

    def format_headers(self):
        format = {
            "wrapStrategy": 'WRAP',
            "backgroundColor": {
                "red": 0.0,
                "green": 0.0,
                "blue": 0.0
            },
            "horizontalAlignment": "CENTER",
            "textFormat": {
                "foregroundColor": {
                    "red": 1.0,
                    "green": 1.0,
                    "blue": 1.0
                },
                "fontSize": 10,
                "bold": True

            }
        }
        self.active_worksheet.format(f"A2:{xl_col_to_name(self.active_worksheet.col_count)}2", format)

    def get_report_data(self) -> dict:
        for each_cluster in self.fleet.get_full_report_primary_metrics(include_host_metrics=self.include_host_metrics,
                                                                       include_namespace_metrics=self.include_namespace_metrics,
                                                                       include_disk_metrics=self.include_disk_metrics,
                                                                       granularity=self.granularity,
                                                                       period=self.period):
            yield each_cluster

    def save_report_data_to_sheet(self, include_namespace_metrics: bool = True, include_host_metrics: bool = True,
                                  include_disk_metrics: bool = True):
        for each_cluster in self.get_report_data():
            start = time()
            list_holder = []
            self.update_status_in_sheet(status_text=f'Pulling Data for {each_cluster.get("name")}')
            for each_value in each_cluster.values():
                if isinstance(each_value, Enum):  # If the value is  an enum, we need to get the value to be cleaner.
                    list_holder.append(each_value.value)
                else:
                    list_holder.append(each_value)
            print('Appending Row!!!')
            print(f'Values are {list_holder.__str__()}')

            self.active_worksheet.append_row(list_holder)

            self.update_status_in_sheet(status_text=f"Completed {each_cluster.get('name')} ({time() - start})s")