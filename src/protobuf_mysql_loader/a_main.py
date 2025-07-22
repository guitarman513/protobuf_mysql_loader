import time
import requests
import timeit
import datetime

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mysql.connector import MySQLConnection

from protobuf_mysql_loader.db.mysql_utils import get_mysql_connection_object, check_on_mysql_connection
from protobuf_mysql_loader.db.mysql_creation import add_partitions, create_initial_table, add_tracks_to_db
from protobuf_mysql_loader.helper_scraper_state import UsefulGlobalState
from protobuf_mysql_loader.helper_api_query import get_api_session, query_api, yield_batches_of_docs
from protobuf_mysql_loader.helper_logging import get_logger


logger = get_logger() 

from dotenv import load_dotenv
load_dotenv()


def run_scraper(requests_session:requests.Session, useful_global_state:UsefulGlobalState, mysql_conn:"MySQLConnection", max_threads:int, max_tracks_added_at_once:int, table_name:str, low_bound_num_track_threshold:int):
    start_time = timeit.default_timer()
    spacex_api_message = query_api(requests_session, useful_global_state)
    num_tracks_returned_by_this_spacex_api_query = len(spacex_api_message.udl_observation_responses)
    
    if num_tracks_returned_by_this_spacex_api_query==0: 
        handle_api_returning_zero_tracks(useful_global_state)
        return None
    else:
        useful_global_state.number_of_queries_in_a_row_where_we_didnt_receive_any_tracks = 0
    
    logger.info(f"START: Query returned {num_tracks_returned_by_this_spacex_api_query} tracks. The query completed in {(timeit.default_timer()-start_time):.1f}s")

    # This query returned a bunch of protobuf tracklet data. Time to parse the data from each track and add to the MySQL database.
    # NOTE: honestly I don't think we need threading. We can only get 10k tracks max at a time, and we can probably batch them all at once if we really wanted to.
    for batch_of_docs in yield_batches_of_docs(spacex_api_message, num_tracks_per_batch=max_tracks_added_at_once):
        add_tracks_to_db(batch_of_docs, mysql_conn, useful_global_state, table_name)
        useful_global_state.last_successful_time_we_saved_data_to_db_s = datetime.datetime.now().timestamp()
        useful_global_state.last_token_received_for_data_sucessfully_added_to_db = useful_global_state.last_token_received
    
    logger.info(f"\tEND: Indexing of obs from {num_tracks_returned_by_this_spacex_api_query} tracks finished. Total time from query to indexing took {(timeit.default_timer()-start_time):.1f}s.")
    
    if num_tracks_returned_by_this_spacex_api_query<low_bound_num_track_threshold:
        logger.info(f"Last query we didn't receive many tracks. Sleeping for 10s to prevent log clutter and API throttling...")
        time.sleep(10) 
    
    return None


def handle_api_returning_zero_tracks(useful_global_state:UsefulGlobalState) -> None:
    time_to_sleep_if_we_dont_receive_any_tracks = 30
    num_times_without_track = useful_global_state.number_of_queries_in_a_row_where_we_didnt_receive_any_tracks
    if num_times_without_track > 120: # idk wait an hour then fail? 
        raise Exception("Haven't received any tracks in a while... what's up?")
    logger.warning(f"Queried for tracks but didn't get any in return... Will retry the query with the same token in {time_to_sleep_if_we_dont_receive_any_tracks} seconds.")
    useful_global_state.number_of_queries_in_a_row_where_we_didnt_receive_any_tracks+=1
    time.sleep(time_to_sleep_if_we_dont_receive_any_tracks)
    return None


    
if __name__ == "__main__":
    print("LAUNCH THIS WITH nohup python -u src/a_main.py & ")
    TABLE_NAME = "spacewatch_2024_07_10"
    MAX_THREADS = 4
    # MAX_TRACKS_ADDED_TO_TABLE_AT_A_TIME = 5
    MAX_TRACKS_ADDED_TO_TABLE_AT_A_TIME = 10_000 # Currently takes <2 seconds for 3k tracks (including query and upload to db)
    MAX_NUM_TRACKS_SPACEX_SENDS_PER_API_CALL = 3_000
    logger.info(f"SpaceX usually returns 3k tracks at a time. Num tracks we are processing with each MySQL executemany() call: {MAX_TRACKS_ADDED_TO_TABLE_AT_A_TIME}")
    if MAX_TRACKS_ADDED_TO_TABLE_AT_A_TIME>MAX_NUM_TRACKS_SPACEX_SENDS_PER_API_CALL: logger.info("Uploading all tracks in one go without multiple threads. It may be worth splitting it into smaller chunks and comparing the timing.")
    
    MAX_UNCAUGHT_FAILURES_BEFORE_EXIT = 100
    MIN_NUMBER_OF_TRACKS_RETURNED_BEFORE_10s_SLEEP = 100 # Prevents constant querying. If we only get <100 tracks upon querying, sleep for 10 seconds. This also helps prevent log clutter. Only takes 0.2s to process 100 tracks vs ~2s for 3000
    
    useful_global_state = UsefulGlobalState.from_existing_state_file()
    session = get_api_session()
    mysql_conn = get_mysql_connection_object()
    
    num_failures = 0
    
    create_initial_table(TABLE_NAME, mysql_conn)
    
    while num_failures<MAX_UNCAUGHT_FAILURES_BEFORE_EXIT:
        useful_global_state.number_of_queries_in_a_row_where_we_didnt_receive_any_tracks = 0
        try:
            check_on_mysql_connection(mysql_conn)
            add_partitions(useful_global_state,TABLE_NAME,mysql_conn)
            run_scraper(requests_session=session, useful_global_state=useful_global_state, mysql_conn=mysql_conn, max_threads=MAX_THREADS, max_tracks_added_at_once=MAX_TRACKS_ADDED_TO_TABLE_AT_A_TIME, table_name=TABLE_NAME, low_bound_num_track_threshold=MIN_NUMBER_OF_TRACKS_RETURNED_BEFORE_10s_SLEEP)
        except Exception as e:
            num_failures+=1
            logger.critical("Encountered something bad. Sleeping for 10mins then trying again", e, exc_info=True)
            time.sleep(600)