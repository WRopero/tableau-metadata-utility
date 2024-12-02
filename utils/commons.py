import tableauserverclient as TSC
import zipfile
from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd
from datetime import datetime
import logging
import json
import os

logging.basicConfig(level=logging.INFO)

def access_token_authenticate(access_token, access_token_secret, site_id,
                              server, version):
    """
    Authenticate to the Tableau Server using the Personal Access Token.
    """

    tableau_auth = TSC.PersonalAccessTokenAuth(access_token, access_token_secret, site_id)
    server_tsc = TSC.Server(server, use_server_version=True)
    server_tsc.version = version
    return server_tsc, tableau_auth


def get_tableau_metadata(server_tsc, tableau_auth, query, app_config, lookback_date):
    """
    Main function to query the Tableau Server and save the metadata of the workbooks in a JSON and CSV file.
    """
    with server_tsc.auth.sign_in(tableau_auth):

        result = server_tsc.metadata.query(query)
        current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
        current_dir = os.getcwd()
        output_dir = os.path.join(current_dir, 'results',f'{current_datetime}')
        os.makedirs(output_dir, exist_ok=True)
        
        data = result["data"]["workbooks"]

        record_count = len(data)
        logging.info(f"Total number of Workbooks in Tableau: {record_count}")

        workbooks = []

        # Flatten the data to use an estructured format
        for workbook in data:
            base_info = {
                "id": workbook["id"],
                "name": workbook["name"],
                "description": workbook["description"],
                "uri": workbook["uri"],
                "createdAt": workbook["createdAt"],
                "updatedAt": workbook["updatedAt"],
                "projectName": workbook["projectName"],
                "owner_id": workbook["owner"]["id"],
                "owner_username": workbook["owner"]["username"],
            }
            for datasource in workbook["embeddedDatasources"]:
                workbook_row = base_info.copy()
                workbook_row.update({
                    "datasource_id":
                    datasource["id"],
                    "datasource_name":
                    datasource["name"],
                    "datasource_hasExtracts":
                    datasource["hasExtracts"],
                    "datasource_extractLastRefreshTime":
                    datasource["extractLastRefreshTime"],
                })
                workbooks.append(workbook_row)

        # Convert to DataFrame
        df = pd.DataFrame(workbooks)

        with open(f'{output_dir}/workbooks_metadata_{current_datetime}.json', 'w') as f:
            json.dump(data, f, indent=4)

            logging.info(f"JSON file saved as workbooks_metadata_{current_datetime}.json")

        # Fetch the TS Events Data Source
        all_datasources, pagination_item = server_tsc.datasources.get()
        ts_events = next(ds for ds in all_datasources
                         if ds.name == "TS Events")
        server_tsc.datasources.download(
            ts_events.id, filepath=f"{output_dir}/TS_Events.hyper")

        # Extract the hyper file
        grouped_wirkbook_events = process_hyper_file(output_dir=output_dir,
                                                     users_ignore_list=app_config['user_ignore_list'],
                                                     lookback_date=lookback_date)

        grouped_wirkbook_events.to_csv(f'{output_dir}/workbooks_usage_events_count_{current_datetime}.csv',
            sep=',',
            index=False)
        logging.info(f"CSV file saved as workbooks_usage_events_count_{current_datetime}.csv")

        merged_df = pd.merge(df, grouped_wirkbook_events, left_on=['name', 'projectName'], 
                             right_on=['workbook_name', 'project_name'], 
                             how='left')

        merged_df.to_csv(f'{output_dir}/merged_workbooks_metadata_{current_datetime}.csv', sep=',', index=False)
        logging.info(f"CSV file saved as merged_workbooks_metadata_{current_datetime}.csv")


def process_hyper_file(output_dir=None, users_ignore_list=None, lookback_date=None, hyper_name=None):
    """
    Extracts the hyper file from the tdsx file
    :param output_dir: The output directory where the hyper file is located
    :param lookback_days: The number of days to look back for the events default is 90 days
    """
    
    with zipfile.ZipFile(f"{output_dir}/TS_Events.hyper.tdsx", 'r') as zip_ref:
        zip_ref.extractall(f"{output_dir}/extracted_tdsx")

    hyper_file_path =f"{output_dir}/extracted_tdsx/Data/Extracts/{hyper_name}.hyper"

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
            query = "SELECT * FROM \"Extract\""
            result = connection.execute_list_query(query)
            columns = [str(column.name).strip('"') for column in connection.catalog.get_table_definition("Extract").columns]
            ts_events_df = pd.DataFrame(result, columns=columns)
            ts_events_df["event_date"] = ts_events_df["event_date"].apply(lambda x: f"{x.year}-{x.month}-{x.day}")

    if users_ignore_list['enabled']:
        ts_events_df = ts_events_df[~ts_events_df["actor_user_name"].isin(users_ignore_list['users_email'])]

    filtered_df = ts_events_df[(ts_events_df["event_date"] >= lookback_date)
                            & (ts_events_df["item_type"] == "Workbook")]
    
    ts_events_df.to_csv(f"{output_dir}/full_hyper_tableau_events_database_data.csv", index=False)

    data_to_export = filtered_df.groupby(["workbook_name", "project_name"]).agg({
        'workbook_name':'count',
        'event_type': lambda x: ', '.join(x),
        'actor_user_name': lambda x: ', '.join(x),
        'event_name': lambda x: ', '.join(x)
        }).rename(
            columns={
                'workbook_name': 'event_count',
                'event_type': 'grouped_events',
                'actor_user_name': 'grouped_actor_user_name',
                'event_name': 'grouped_event_name',
            }).reset_index()

    return data_to_export
