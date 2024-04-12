import os
import requests
import json
import requests

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

        url = data.get('next')  # Update the URL to the next page

    return all_data

def save_data_to_file(data, folder):
    for key, url in data.items():
        page_number = 1
        accumulated_results = []  # List to accumulate results from all pages
        while url:
            response = requests.get(url)
            response_data = response.json()

            url_results = response_data.get('results')

            print(url_results)

            for result in url_results:
                accumulated_results.append(result)  # Append result to accumulated list

            next_url = response_data.get('next')
            if next_url:
                print("Next URL:", next_url)
                url = next_url
            else:
                break

            page_number += 1

        # Write accumulated results to file
        filename = f"{folder}/{key}.json"
        with open(filename, "w") as file:
            json.dump(accumulated_results, file, indent=4)  # Dump the entire list at once

        print(f"Data saved to {filename}")

if __name__ == "__main__":
    swapi_data = get_swapi_data()
    folder_name = "swapi_data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    save_data_to_file(swapi_data, folder_name)
