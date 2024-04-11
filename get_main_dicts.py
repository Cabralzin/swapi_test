import os
import requests
import json
import swapi


luke = swapi.get_person(1)

print(luke)

def get_swapi_data():
    url = "https://swapi.dev/api/"
    all_data = {}
    while url:
        response = requests.get(url)
        data = response.json()
        for key, value in data.items():
            if key not in all_data:
                all_data[key] = value
            elif isinstance(value, list):
                all_data[key].extend(value)
            else:
                all_data[key].update(value)
        url = data.get('next')
    return all_data

def save_data_to_file(data, folder):
    for key, url in data.items():
        response = requests.get(url)
        response_data = response.json()
        filename = f"{folder}/{key}.json"
        with open(filename, "w") as file:
            json.dump(response_data, file, indent=4)
        print(f"Data saved to {filename}")

if __name__ == "__main__":
    swapi_data = get_swapi_data()
    folder_name = "swapi_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    save_data_to_file(swapi_data, folder_name)
