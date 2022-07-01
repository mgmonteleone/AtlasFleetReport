from atlas_lib import Fleet, Atlas
from gsheet_lib import SingleProjFleetReport
import os

atlas_org = os.getenv('ATLAS_ORG')
atlas_user = os.getenv('ATLAS_USER')
atlas_key = os.getenv('ATLAS_KEY')

atlas: Atlas = Atlas(atlas_user, atlas_key)
org_obj = atlas.Organizations.organization_by_id(atlas_org)
project_list = list()
sheet_uri = 'https://docs.google.com/spreadsheets/d/1qKD9da3BnMJp9kNJenf_D5udsi4EhErAJ6bbZ69Icdw/edit#gid=1217399247'
for each_project in atlas.Organizations.get_all_projects_for_org(org_id=org_obj.id):
    print(each_project.name)
    project_list.append(each_project)

for each_one in project_list:
    atlas_obj = Atlas(atlas_user,atlas_key,each_one.id)
    fleet_obj = Fleet(atlas_obj)
    report = SingleProjFleetReport(atlas_user=atlas_user,atlas_key=atlas_key,atlas_group=each_one.id, sheet_uri=sheet_uri,include_namespace_metrics=True,
                                   include_disk_metrics=False,include_host_metrics=False)
    report.save_report_data_to_sheet()