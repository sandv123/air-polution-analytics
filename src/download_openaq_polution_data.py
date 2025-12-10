import json
import os
import time
from typing import Any
import openaq
import zipfile
from itertools import product


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


    def recycle_client(self) -> openaq.OpenAQ:
        """
        Get a client object. If exists, cycle it by closing and opening anew

        :return: OpenAQ client object
        :rtype: openaq.OpenAQ
        """
        if self.client is not None:
            self.client.close()
        
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
    measurements = client.measurements.list(sensors_id=sensor_id, limit=1)

    try:
        # Call the OpenAQ API
        measurements = client.measurements.list(
            sensors_id=sensor_id,
            datetime_from=f'{year}-01-01',
            datetime_to=f'{year}-12-31',
            limit=limit, page=page_num)
        print(f'  page={page_num} len={len(measurements.results)}', measurements.headers)
        
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
        if measurements.headers.x_ratelimit_reset is None:
            sleep_seconds = 60
        else:
            sleep_seconds = float(measurements.headers.x_ratelimit_reset)
        # print(f'TIMED OUT, sleeping {sleep_seconds} seconds, retry page {page_num}')

        raise TimeoutErrorExt(e, sleep_seconds)

    return result


def is_running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


if __name__ == "__main__":

    # OpenAQ API key
    api_key = os.environ['OPENAQ_API_KEY']

    # Datastore path
    datastore = os.environ['DATASTORE_PATH']

    connection_mgr = OpenAQConnectionManager(api_key)
    client = connection_mgr.recycle_client()

    # Geo coords for Belgrade, RS
    belgrade_coordinates = (44.8125, 20.4612)

    # Retrieve data for these years to analyze
    years = [2021, 2022, 2023, 2024, 2025]

    # Retrieve measurements for these parameters of the sensors
    parameters = ['pm1', 'pm10', 'pm25', 'temperature']

    # Find all available locations in the radius of 8 km around Belgrade center
    locations = client.locations.list(coordinates=belgrade_coordinates, radius=8000, limit=1000)

    # Store the locations for processing
    with zipfile.ZipFile(datastore + 'Belgrade-locations.json.zip', 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as file:
        file.writestr('Belgrade-locations.json', json.dumps(locations.json(), indent=4))
    print(f'Got {len(locations.results)} locations')
    
    for l in locations.results:
        name = l.name.replace(' ', '-')

        # For current location find the ids of the sensors that provide measurements for parameters we are interested in
        # If no sensor measures interesting parameter, skip the parameter
        sensors_parameters = {s.parameter.name: s.id  for s in l.sensors}
        sensor_ids = filter(lambda f: f != 0, map(lambda x: sensors_parameters.get(x, 0), parameters))
        
        timed_out = 0
        for args in product(years, sensor_ids):
            page_num = 0
            while True:
                page_num += 1
                filename = f'{l.id}_{args[1]}_{l.country.code}_{name}_{args[0]}_page{page_num}.json'
                print(f'Downloading chunk {filename}')

                # If a page has already been downloaded, skip it
                if os.path.exists(datastore + filename + '.zip'):
                    print(f'  Chunk {filename}.zip exists, skipping')
                    continue

                try:
                    page = get_measurements(client, args[1], args[0], page_num)
                    timed_out = 0
                    if not page:
                        break

                    # Store the data in zipped format. This shrinks the files about 50 times 
                    with zipfile.ZipFile(datastore + filename + '.zip', 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as file:
                        file.writestr(filename, json.dumps(page, indent=4))
                except TimeoutErrorExt as e:
                    if timed_out == 3:
                        # If the API timed out 3 times in a row, cancel trying and bail the whole thing
                        print('  Timed out 3 times, canceling')
                        # client.close()
                        raise e
                    else:
                        # If the API timed out, try resetting the connection and wait for the API suggested amount of seconds
                        print(f'TIMED OUT, sleeping for {e.timeout_seconds} seconds, {e}')
                        timed_out += 1
                        time.sleep(e.timeout_seconds)
                        client = connection_mgr.recycle_client()

    # client.close()