import datetime

from atlasapi.atlas import Atlas
import os


atlas_org = os.getenv('ATLAS_ORG')
atlas_user = os.getenv('ATLAS_USER')
atlas_key = os.getenv('ATLAS_KEY')
destination_db_string = os.getenv('ATLAS_DB_CONN')
REGION = os.getenv('ATLAS_REGION', 'US_EAST_1')

created_since = datetime.datetime(2022,12,1, tzinfo=datetime.timezone.utc)

atlas: Atlas = Atlas(atlas_user, atlas_key)
projects = atlas.Organizations.get_all_projects_for_org(org_id=atlas_org)
for each_project in projects:
    if each_project.created_date >= created_since:
        # print(f"Project: {each_project.name}, Created: {each_project.created_date.isoformat()}, id: {each_project.id}")
        print(dict(project=each_project.name, id=each_project.id, created_date = each_project.created_date.isoformat(), clusters=each_project.cluster_count))