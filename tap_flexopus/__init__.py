#!/usr/bin/env python3
import os
import json
import singer
import requests
import time
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from datetime import datetime, timedelta, date

REQUIRED_CONFIG_KEYS = ["base_url", "api_key", "start_date"]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)

def getNumberOfWeeks(Date):
    startDate = date(int(Date[0:4]),int(Date[5:7]),int(Date[8:10]))
    Today     = datetime.now().date()
    days      = abs(Today - startDate).days
    numberOfWeeks =  days//7
        
    return numberOfWeeks
    
def getStartAndEndDates(weekNumber):
    startDate = datetime.now() - timedelta(weeks=weekNumber)
    endDate  = startDate + timedelta(weeks=1)
    startAndEndDate = {
    'from': str(startDate.date())+'T'+str(startDate.time())+'Z',
    'to': str(endDate.date())+'T'+str(endDate.time())+'Z',
    }       

    return startAndEndDate

def requestAndWriteData(session, api_endpoint, header, stream, bookmark_column, is_sorted, endPoint, max_bookmark, locationId = None, startAndEndDate = None):
    LocationIds     = []
    if endPoint == '/buildings':
        FindLocationIds = True
        try:
            response = session.request("GET", api_endpoint, headers=header)
        except Exception as e:
            LOGGER.error("field to get data from the end point: /buildings")
            LOGGER.error(e)

    else:
 
        FindLocationIds = False
        try:
            response = session.request("GET", api_endpoint, headers=header, params=startAndEndDate)
            while response.reason == 'Too Many Requests':
                time.sleep(60)
                response = session.request("GET", api_endpoint, headers=header, params=startAndEndDate) 
        except Exception as e:
            LOGGER.error("field to get data from the end point: " + api_endpoint)
            LOGGER.error(e)

    tap_data = response.json()
	
    for row in tap_data['data']:
        # if FindLocationIds:
        #     LocationIds.append(row['locations'][0]['id'])
        if FindLocationIds:
            if row.get("locations"):
                for location in row["locations"]:
                    LocationIds.append(location["id"])
            else:
                LOGGER.warning(f"Building without locations: {row.get('id')}")
        elif stream.tap_stream_id == 'bookables' or stream.tap_stream_id == 'bookings':
            row.update({"location_id": locationId})

        singer.write_records(stream.tap_stream_id, [row])
        if bookmark_column:
            if is_sorted:
                # update bookmark to latest value
                singer.write_state({stream.tap_stream_id: row[bookmark_column]})
            else:
                # if data unsorted, save max value until end of writes
                max_bookmark = max(max_bookmark, row[bookmark_column])
    if bookmark_column and not is_sorted:
        singer.write_state({stream.tap_stream_id: max_bookmark})  

    return LocationIds, max_bookmark  
    
        
def sync(config, state, catalog, numberOfWeeks):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    locationIds  = None
    for stream in catalog.get_selected_streams(state):
        time.sleep(0.2)
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value
        try:
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )
        except Exception as e:
            LOGGER.error("Fieled to write schema:")
            LOGGER.error(e)

        base_url = config.get('base_url')
        api_key = config.get('api_key')
        
        authorization : str  = 'Bearer ' + api_key
        header        : dict = {'authorization': authorization,
                                'accept': 'application/json'}
        
        session = requests.Session()    
        
        endPoints    = {'buildings': '/buildings', 'bookables': '/locations/{location_id}/bookables', 'bookings': '/locations/{location_id}/bookings'}
        max_bookmark = None
        
        

        if locationIds == None:
            api_endpoint = base_url + endPoints[stream.tap_stream_id]

            locationIds, max_bookmark  = requestAndWriteData(session, api_endpoint, header, stream, bookmark_column, is_sorted, endPoints[stream.tap_stream_id], max_bookmark)

																			
        
        else:
            for locationId in locationIds:
                for weekNumber in range(numberOfWeeks):
                    startAndEndDate = getStartAndEndDates(weekNumber)
                    temp = endPoints[stream.tap_stream_id]
                    endPoint     = temp.replace("{location_id}", str(locationId))
                    api_endpoint = base_url + endPoint

                    _, max_bookmark = requestAndWriteData(session, api_endpoint, header, stream,
                                                        bookmark_column, is_sorted, endPoint, max_bookmark, locationId, startAndEndDate)

																				
    return


@utils.handle_top_exception(LOGGER)
def main():
    
    # Parse command line arguments

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
            
        try:
            numberOfWeeks = getNumberOfWeeks(args.config.get('start_date')) + 1
        except:
            LOGGER.error("Wrong start date format or value. Acceptable example is: 2023.02.01")
        
        for i in range(len(catalog.streams)):
            if catalog.streams[i].stream == 'buildings':
                catalog.streams[0],catalog.streams[i] = catalog.streams[i], catalog.streams[0]
        try:        
            sync(args.config, args.state, catalog, numberOfWeeks)
            
        except Exception as e:
            LOGGER.error("Fieled to sync the stream:")
            LOGGER.error(e)

if __name__ == "__main__":
    main()
