import datetime
import os
import json
import pandas as pd
import requests


api_key = os.environ["BEARER_NASA_TOKEN"]
all_objetos = pd.DataFrame()
find_next = True
start_date = "1995-01-01"
start_date_object = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
end_date = "1995-02-28"
end_date_object = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
endpoint = "https://api.nasa.gov/neo/rest/v1/feed"
output_file = "near_earth_objects-january_february_1995-raw.csv"

while find_next:
    tmp_end_date_object = start_date_object + datetime.timedelta(days = 7)
    tmp_end_date = tmp_end_date_object.strftime("%Y-%m-%d")
    tmp_start_date = start_date_object.strftime("%Y-%m-%d")
    print(tmp_start_date, tmp_end_date)
    if tmp_end_date_object > end_date_object:
        tmp_end_date = end_date
    print(tmp_start_date, tmp_end_date)

    params = { 'api_key':api_key, 'start_date':tmp_start_date, 'end_date':tmp_end_date }
    try:
        r= requests.get(endpoint, params=params)
    except requests.exceptions.RequestException as e:
        print(r.status_code)
        raise SystemExit(e)

    data = r.json()["near_earth_objects"]
    print(data)

    objetos = pd.DataFrame(columns=["id", "neo_reference", "name", "absolute_magnitude", "estimated_dia_meters_min", "estimated_dia_meters_max",
                                    "is_potencial_hazardous", "is_sentry_object", "close_approach_date", "miss_distance_kilometers"])
    for date in data.keys():
        pretty = json.dumps(data[date], indent=4)
        print("Data:", pretty)
        objects_dict = {
            "id": data[date][0]["id"],
            "new_reference": data[date][0]["neo_reference_id"],
            "name": data[date][0]["name"],
            "absolute_magnitude": data[date][0]["absolute_magnitude_h"],
            "estimated_dia_meters_min": data[date][0]["estimated_diameter"]["meters"]["estimated_diameter_min"],
            "estimated_dia_meters_max": data[date][0]["estimated_diameter"]["meters"]["estimated_diameter_max"],
            "is_potencial_hazardous": data[date][0]["is_potentially_hazardous_asteroid"],
            "is_sentry_object": data[date][0]["is_sentry_object"],
            "close_approach_date": data[date][0]["close_approach_data"][0]["close_approach_date_full"],
            "miss_distance_kilometers": data[date][0]["close_approach_data"][0]["miss_distance"]["kilometers"] 
        }
        objetos = pd.DataFrame([objects_dict])
        print("Shape new DF:", objetos.shape)
        print("Shape all DF:", all_objetos.shape)
        all_objetos = pd.concat([all_objetos, objetos], ignore_index=True)

    if tmp_end_date_object > end_date_object:
        find_next = False
    else:
        start_date_object = tmp_end_date_object + datetime.timedelta(days = 1)

all_objetos.to_csv(output_file, index=False, encoding='utf-8')
