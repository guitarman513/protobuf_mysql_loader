from typing import TYPE_CHECKING, List, Generator
if TYPE_CHECKING:
    from proj.helper_sxapi_2_mysql import MySQLRecord 
from proj.helper_scraper_state import UsefulGlobalState
from proj.helper_logging import get_logger; logger=get_logger()
from datetime import datetime
import requests
import os
import time


# For understanding the input data format
from proj.spacex.proj_pb2import SomeClass
from proj.helper_sxapi_2_mysql import mysqlify_track

__BASEURL = f"https://someapi.com/api/v1/abc" 

def __generate_api_query(useful_global_state:UsefulGlobalState):
    
    token = useful_global_state.last_token_received_for_data_sucessfully_added_to_db
    # check if token is not a blank string and that it has some length to it
    if bool(token) and len(token) > 10:
        # Then we probably have a good token. Now even if the time is more than 24 hours back, which is the max, Spacex only gives us 24 hours so no harm in just sending it without checking
        url = f"{__BASEURL}?token={token}"
        return url
        
    # if we don't have a token to work with, just get the last 30 seconds of data
    else:
        logger.warning("No token found. This should only happen upon startup if no api_state.json file is present. Will query for the last one hour of data...")
        unixtime = int(datetime.now().timestamp())
        url = f"{__BASEURL}?startTime={unixtime-3600}"
        return url

def get_api_session(): 
    required_location_of_client_cert_and_priv_key = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), # the parent dir of this file
        'api_provider' # Some directory containing the .crt and .key 
    )
    
    client_cert_path = os.path.join(
        required_location_of_client_cert_and_priv_key,
        'client.crt'
    )
    client_priv_key_path = os.path.join(
        required_location_of_client_cert_and_priv_key,
        'client.key'
    )
    
    if not (os.path.exists(client_cert_path) and os.path.exists(client_priv_key_path)):
        raise FileNotFoundError("Can't find both client.crt and client.key in the api provider dir.")     
    
    session = requests.Session()
    # Set the certs 
    session.cert = (client_cert_path, client_priv_key_path)
    
    return session


def query_api(session:requests.Session, useful_global_state:UsefulGlobalState): 
    url = __generate_api_query(useful_global_state)
    response = session.get(url)
    
    if response.status_code == 429:
        logger.warning("Received HTTP code 429; too many requests; watiing 5 seconds")
        time.sleep(5)
        response = session.get(url)
    
    if response.status_code == 500:
        logger.warning("Received status code of 500. Maybe their server is down. Sleeping for 60 seconds and then resuming.")
        time.sleep(60)
    
    if response.status_code != 200:
        logger.error("Received Non-200 Status Code")
        logger.error(f"{response.status_code=}")
        logger.error(f"{response.text=}")
        logger.error(f"{response=}")
        raise ValueError("Received Non-200 Status Code While Querying API")
    
    api_message = SomeClass() 
    api_message.ParseFromString(response.content)
    useful_global_state.last_token_received = api_message.token
    useful_global_state.total_gigabytes_this_session += len(response.content)/(1024*1024*1024) #NOTE: This is assuming one content-character = one byte which I'm not sure about
    useful_global_state.num_successful_api_calls_this_session+=1
    useful_global_state.save_state()
    
    return api_message


def yield_batches_of_docs(api_message, num_tracks_per_batch:int) -> Generator[List["MySQLRecord"], None, None]:
    buffer=[] # This resets after each batch of documents is sent out
    for track in api_message.class_attribute: 
        buffer.append(mysqlify_track(track)) # fill buffer with a track's worth of data
        if len(buffer) == num_tracks_per_batch:
            yield buffer
            buffer = []
    # Now if done iterating thru the message and we have a partial buffer, return that too
    if buffer:
        yield buffer