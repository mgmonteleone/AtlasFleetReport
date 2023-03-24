import datetime

from atlasapi.atlas import Atlas
from atlasapi.events import AtlasEventTypes, AtlasEvent
from typing import List
import os


atlas_org = os.getenv('ATLAS_ORG')
atlas_user = os.getenv('ATLAS_USER')
atlas_key = os.getenv('ATLAS_KEY')
destination_db_string = os.getenv('ATLAS_DB_CONN')


created_since = datetime.datetime(2022,12,1, tzinfo=datetime.timezone.utc)

atlas: Atlas = Atlas(atlas_user, atlas_key)
events = atlas.Events.since_by_type(since_datetime=created_since, event_type=AtlasEventTypes.CLUSTER_DELETED )
for each_event in events:
    print(each_event)