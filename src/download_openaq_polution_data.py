import io
import json
import os
import time
from typing import Any
import httpcore
import openaq
import zipfile
from itertools import product
from argparse import ArgumentParser


class TimeoutErrorExt(openaq.TimeoutError):
    """
    Extension to openaq.TimeoutError to push the expected timeout in seconds up the callstack
    """
    def __init__(self, e: openaq.TimeoutError, timeout_seconds: float):
        self.status_code = e.status_code
        super().__init__(str(e))
        self.timeout_seconds = timeout_seconds


class OpenAQConnectionManager:
    """
    Wrapper around OpenAQ class to easily restart the connection in case of an endless annoying service timeout loop
    """
    client: openaq.OpenAQ | None = None

    def __init__(self, api_key: str):
        self._api_key = api_key

    
    def __del(self):
        if self.client is not None:
            self.client.close()


    def recycle_client(self, timeout: float) -> openaq.OpenAQ:
        """
        Get a client object. If exists, cycle it by closing, sleep for {timeout} seconds, and opening anew

        :return: OpenAQ client object
        :rtype: openaq.OpenAQ
        """
        if self.client is not None:
            self.client.close()
            time.sleep(timeout)
        
        client = openaq.OpenAQ(self._api_key)
        return client


def get_measurements(client: openaq.OpenAQ, sensor_id: int, year: int, page_num: int) -> dict[str, Any]:
    """
    Run a single call to the OpenAQ API to get measurements for a particular parameter for the whole year.
    The service uses paging API so the method retrieves a single page of the results.
    Returns a parsed JSON for the API response.
    
    :param client: OpenAQ API client
    :type client: openaq.OpenAQ
    :param sensor_id: Id of the sensor to query
    :type sensor_id: int
    :param year: The year to retrieve measurements for
    :type year: int
    :param page_num: Result page to retrieve
    :type page_num: int
    :return: Parsed JSON response
    :rtype: dict[str, Any]
    """
    result = {}
    
    limit = 1000
    do_loop = True
    sleep_seconds = 0

    # Initialize measurements variable
    measurements = None

    try:
        # Call the OpenAQ API
        measurements = client.measurements.list(
            sensors_id=sensor_id,
            datetime_from=f'{year}-01-01',
            datetime_to=f'{year}-12-31',
            limit=limit, page=page_num)
        # print(f'  page={page_num} len={len(measurements.results)}', measurements.headers)
        
        # Pages are retrieved until current page returns zero results
        if not len(measurements.results) == 0:
            result = json.loads(measurements.json())
        
        # OpenAQ limits the rate at which the API can be called
        # This is to throttle the requests if close to rate limit
        if measurements.headers.x_ratelimit_remaining is not None and measurements.headers.x_ratelimit_remaining < 5:
            raise openaq.TimeoutError('Timing out to observe rate limits')
        
    except openaq.TimeoutError as e:
        # If timed out or throttling, sleep until the rate limit resets or 60 seconds
        # Raise exception to pass the info up the callstack
        if measurements is not None:
            if measurements.headers.x_ratelimit_reset is None:
                sleep_seconds = 60
            else:
                sleep_seconds = float(measurements.headers.x_ratelimit_reset)
        # print(f'TIMED OUT, sleeping {sleep_seconds} seconds, retry page {page_num}')

        raise TimeoutErrorExt(e, sleep_seconds)

    return result


def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def setup_environment() -> tuple[str, str]:
    # OpenAQ API key
    if is_running_in_databricks():
        print("Running inside Databricks")
        argparse = ArgumentParser()
        argparse.add_argument("--datastore_path", type=str)
        args = argparse.parse_args()

        api_key = dbutils.secrets.get(scope = "air-polution-analytics APIs keys", key = "OPENAQ_API_KEY") # type: ignore
        datastore = args.datastore_path
    else:
        print("Running inside IDE")
        api_key = os.environ['OPENAQ_API_KEY']
        datastore = os.environ['DATASTORE_PATH']

    return (api_key, datastore)


def mark_finished_chunk(datastore: str, name: str) -> None:
    with open(datastore + f'{name}.finished', 'w') as f:
        f.write("done")
        f.flush()


def check_finished_chunk(datastore: str, name: str) -> bool:
    return os.path.exists(datastore + f'{name}.finished')


def main():
    api_key, datastore = setup_environment()

    connection_mgr = OpenAQConnectionManager(api_key)
    client = connection_mgr.recycle_client(0)

    # Geo coords for Belgrade, RS
    belgrade_coordinates = (44.8125, 20.4612)

    # Retrieve data for these years to analyze
    years = [2021, 2022, 2023, 2024, 2025]

    # Retrieve measurements for these parameters of the sensors
    parameters = ['pm1', 'pm10', 'pm25', 'temperature']

    # Find all available locations in the radius of 8 km around Belgrade center
    locations = client.locations.list(coordinates=belgrade_coordinates, radius=8000, limit=1000)

    # locations = client.locations.get(2812630) # This is for debugging purposes

    # Store the locations for processing
    store_zipped(datastore + 'Belgrade-locations.json.zip', json.dumps(locations.json(), indent=4))
    print(f'Got {len(locations.results)} locations')

    for l in locations.results:
        name = l.name.replace(' ', '-')

        # For current location find the ids of the sensors that provide measurements for parameters we are interested in
        # If no sensor measures interesting parameter, skip the parameter
        sensors_parameters = {s.parameter.name: s.id  for s in l.sensors}
        sensor_ids = filter(lambda f: f != 0, map(lambda x: sensors_parameters.get(x, 0), parameters))
        print(f'Downloading measurements for station {name}')

        # years = [2025] # This is for debugging purposes
 
        timed_out = 0
        for year_and_sensor in product(years, sensor_ids):
            year_and_sensor_name = f'{l.id}_{year_and_sensor[1]}_{l.country.code}_{name}_{year_and_sensor[0]}'
            print(f'  Downloading chunk {year_and_sensor_name}')
            if check_finished_chunk(datastore, year_and_sensor_name):
                print(f'  Chunk {year_and_sensor_name} already downloaded, skipping')
                continue

            page_num = 0
            while True:
                page_num += 1
                filename = f'{year_and_sensor_name}_page{page_num}.json'

                # If a page has already been downloaded, skip it
                if os.path.exists(datastore + filename + '.zip'):
                    print(f'    Page {filename}.zip exists, skipping')
                    continue

                try:
                    page = get_measurements(client, year_and_sensor[1], year_and_sensor[0], page_num)
                    timed_out = 0
                    if not page:
                        break

                    # Store the data in zipped format. This shrinks the files about 50 times
                    store_zipped(datastore + filename + '.zip', json.dumps(page, indent=4))
                except TimeoutErrorExt as e:
                    if timed_out == 3:
                        print(f'  Timed out (API) 3 times, skipping page {filename}')
                        client = connection_mgr.recycle_client(0)
                        timed_out += 1
                        continue
                    if timed_out == 5:
                        print(f'  Timed out (API) 5 times, skipping chunk {name}')
                        client = connection_mgr.recycle_client(0)
                        timed_out += 1
                        break
                    if timed_out == 7:    
                        # If the API timed out 7 times in a row, cancel trying and bail the whole thing
                        print('  Timed out (API) 7 times, canceling')
                        raise e
                    else:
                        # If the API timed out, try resetting the connection and wait for the API suggested amount of seconds
                        time_to_sleep = e.timeout_seconds + 60 * timed_out
                        print(f'TIMED OUT OPENAQ, sleeping for {time_to_sleep} seconds, {e}')
                        timed_out += 1
                        client = connection_mgr.recycle_client(time_to_sleep)
                except httpcore.ReadTimeout as e:
                    print(f'TIMED OUT HTTP READ, sleeping for 60 seconds, {e}')
                    client = connection_mgr.recycle_client(60)
                except TimeoutError as e:
                    print(f'TIMED OUT NETWORK/IO, sleeping for 60 seconds, {e}')
                    client = connection_mgr.recycle_client(60)
            mark_finished_chunk(datastore, year_and_sensor_name)


def store_zipped(filename: str, data: str):

    zipped_content = io.BytesIO()
    with zipfile.ZipFile(zipped_content, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as file:
        file.writestr('Belgrade-locations.json', data)

    with open(filename, "wb") as f:
        f.write(zipped_content.getvalue())


if __name__ == "__main__":
    main()