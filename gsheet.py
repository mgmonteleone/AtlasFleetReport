import gspread
from atlasapi.atlas import Atlas, AtlasGranularities, AtlasPeriods
from atlas_lib import Fleet
from xlsxwriter.utility import xl_col_to_name
from enum import Enum
import os
report_uri = 'https://docs.google.com/spreadsheets/d/1qKD9da3BnMJp9kNJenf_D5udsi4EhErAJ6bbZ69Icdw/edit#gid=0'

atlas = Atlas(user=os.getenv('ATLAS_USER'), password=os.getenv('ATLAS_KEY'),group=os.getenv('ATLAS_GROUP'))

current_fleet = Fleet(atlas)

project_obj = current_fleet.atlas.Projects.project_by_id(current_fleet.atlas.group.replace("'",""))

gc = gspread.service_account()

# Open a sheet from a spreadsheet in one go
wks = gc.open_by_url(report_uri)


try:
    active_ws = wks.worksheet(project_obj.name)
    wks.del_worksheet(active_ws)
except gspread.exceptions.WorksheetNotFound:
    print("Spreadsheet not there, so will not delete it.")

print(f"Creating Spreadsheet named {project_obj.name}")
active_ws = wks.add_worksheet(project_obj.name, 2, 1)

print(f'Creating Headers...')
active_ws.update_cell(1,1, value = 'Creating Headers.....' )
header = []
for each in current_fleet.get_full_report_primary_metrics(granularity=AtlasGranularities.HOUR,period=AtlasPeriods.WEEKS_1):
    for each_name in each.keys():
        header.append(each_name)
    break

active_ws.append_row(header)
print('Done creating headers')
active_ws = wks.worksheet(project_obj.name)

active_ws.update_cell(1,1, value = 'Done Creating Headers' )
active_ws.update_cell(1,1, value = f"There are {active_ws.col_count} Columns")
print(f"There are {active_ws.col_count} Columns" )


format =  {
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

for each_cluster in current_fleet.get_full_report_primary_metrics(
        granularity=AtlasGranularities.HOUR,period=AtlasPeriods.WEEKS_1):
    list_holder = []
    active_ws.update_cell(1, 1, value=f'Pulling Data for {each_cluster.get("name")}')
    for each_value in each_cluster.values():
        if isinstance(each_value, Enum): # If the value is  an enum, we need to get the value to be cleaner.
            list_holder.append(each_value.value)
        else:
            list_holder.append(each_value)
    print('Appending Row!!!')

    print(f'Values are {list_holder.__str__()}')

    active_ws.append_row(list_holder)

active_ws.format(f"A2:{xl_col_to_name(active_ws.col_count)}2",format)
active_ws.update_cell(1, 1, value=f'COMPLETE!')


