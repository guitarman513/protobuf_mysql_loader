import json
import os
import logging
logger = logging.getLogger("main_logger")


class UsefulGlobalState:
    desired_state_filename = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),"api_state.json")
    #   root            src             projpath
    
    def __init__(
            self,
            num_successful_api_calls_this_session:int,
            total_gigabytes_this_session:float,
            last_successful_time_we_saved_data_to_db_s:float,
            last_token_received:str,
            last_token_received_for_data_sucessfully_added_to_db:str,
            last_time_partitions_were_created_s:float,
    ):
        self.last_token_received:str = last_token_received
        self.total_gigabytes_this_session:float = total_gigabytes_this_session
        self.num_successful_api_calls_this_session:int = num_successful_api_calls_this_session
        self.last_successful_time_we_saved_data_to_db_s:float = last_successful_time_we_saved_data_to_db_s
        self.last_token_received_for_data_sucessfully_added_to_db:str = last_token_received_for_data_sucessfully_added_to_db
        self.number_of_queries_in_a_row_where_we_didnt_receive_any_tracks:int = 0
        self.last_time_partitions_were_created_s:int = last_time_partitions_were_created_s

    def save_state(self):
        with open(UsefulGlobalState.desired_state_filename, 'w') as output_file:
            output_file.write(
                json.dumps(
                    {
                        "num_successful_api_calls_this_session": self.num_successful_api_calls_this_session,
                        "total_gigabytes_this_session": self.total_gigabytes_this_session,
                        "last_successful_time_we_saved_data_to_db_s": self.last_successful_time_we_saved_data_to_db_s,
                        "last_token_received": self.last_token_received,
                        "last_token_received_for_data_sucessfully_added_to_db": self.last_token_received_for_data_sucessfully_added_to_db,
                        "last_time_partitions_were_created_s": self.last_time_partitions_were_created_s,
                    }
                )
            )
     
    @classmethod   
    def from_existing_state_file(cls):
        try:
            with open(UsefulGlobalState.desired_state_filename, 'r') as input_file:
                state = json.loads(input_file.read())
                logger.warning(f"Loaded state from existing api_state.json file. This should only happen at script startup! Resetting total_gigabytes_this_sessionto zero.")
                return UsefulGlobalState(
                    num_successful_api_calls_this_session=state['num_successful_api_calls_this_session'],
                    total_gigabytes_this_session=0,
                    last_token_received=state['last_token_received'],
                    last_token_received_for_data_sucessfully_added_to_db=state['last_token_received_for_data_sucessfully_added_to_db'],
                    last_successful_time_we_saved_data_to_db_s= state['last_successful_time_we_saved_data_to_db_s'],
                    last_time_partitions_were_created_s=state['last_time_partitions_were_created_s']
                )
        except:
            logger.warning(f"Failed to load state from existing api_state.json file. Will initialize a blank one!")
            return UsefulGlobalState(
                num_successful_api_calls_this_session=0,
                total_gigabytes_this_session=0,
                last_successful_time_we_saved_data_to_db_s=0,
                last_token_received='',
                last_token_received_for_data_sucessfully_added_to_db='',
                last_time_partitions_were_created_s = 0,
            )