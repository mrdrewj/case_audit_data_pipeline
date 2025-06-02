import time
import database_handler
from report_creator import ExcelMaker
import sql_queries.case_mgmt_docs_query
import sql_queries.case_mgmt_events_query
import sql_queries.family_counseling_docs_query
import sql_queries.family_counseling_events_query
import config
from email_notifications import EmailSender
import traceback  # To capture error details


# _________SETTINGS FOR RUNNING______________________________________________________
testing_mode = True

run_queries = False # Set to True to run the queries, otherwise it will use last collected data.    # 1

create_fc_audits = False  # Set to True to create FC Audits.    # 2
create_cm_audits = False   # Set to True to create CM Audits.    # 3
# ___________________________________________________________________________________

# NOT USING EMAILS CURRENTLY....
send_fc_emails = False   # Set to True to send FC Audits to FC by email. Works with #6, #7, #8 below.     # 4
send_cm_emails = False   # Set to True to send CM Audits to CM by email. Works with #6, #7, #8 below.    # 5

send_supervisor_emails = False   # Set to True to send FC/CM Supervisor Audits by email. Works with #4 and/or #5 above.    # 6
send_worker_emails = False   # Set to True to send FC/CM Worker Audits by email. Works with #4 and/or #5 above.    # 7

send_test_emails = False   # Set to True to send Test Worker Audits to djones by email. Works with #4 and/or #5 above.   # 8


db_handler = database_handler.DatabaseHandler(config.connection_information)
success = True
try:
    run_start = time.time()

    if create_fc_audits:
        if run_queries:
            family_counseling_docs_query = sql_queries.family_counseling_docs_query.family_counseling_docs_query
            fc_documents_columns = sql_queries.family_counseling_docs_query.fc_documents_columns
            fc_documents_columns_numeric = sql_queries.family_counseling_docs_query.fc_documents_columns_numeric
            fc_documents_columns_dates = sql_queries.family_counseling_docs_query.fc_documents_columns_dates
            df_fc_documents = db_handler.process_and_save_df(family_counseling_docs_query, fc_documents_columns, config.start_time, config.end_time, config.fc_documents_pickle, fc_documents_columns_numeric, fc_documents_columns_dates)

            family_counseling_events_query = sql_queries.family_counseling_events_query.family_counseling_events_query
            fc_events_columns = sql_queries.family_counseling_events_query.fc_events_columns
            fc_events_columns_numeric = sql_queries.family_counseling_events_query.fc_events_columns_numeric
            fc_events_columns_dates = sql_queries.family_counseling_events_query.fc_events_columns_dates
            df_fc_events = db_handler.process_and_save_df(family_counseling_events_query, fc_events_columns, config.start_time, config.end_time, config.fc_events_pickle, fc_events_columns_numeric, fc_events_columns_dates)
        if testing_mode:
            fc_excel_maker = ExcelMaker(config.output_directory, config.output_directory)
        else:
            fc_excel_maker = ExcelMaker(config.fc_output_directory, config.supervisor_directory)
        fc_email_df_sups, fc_email_df_workers = fc_excel_maker.create_excel_files('Family Counseling', config.fc_documents_pickle, config.fc_events_pickle)

        if send_fc_emails:
            fc_email_sender = EmailSender()
            if send_supervisor_emails:
                fc_email_sender.send_worker_emails(fc_email_df_workers)
            if send_worker_emails:
                fc_email_sender.send_supervisor_emails(fc_email_df_sups)

        if send_test_emails:
            test_email_sender = EmailSender()
            test_email_sender.send_test_emails(fc_email_df_workers)

    if create_cm_audits:
        if run_queries:
            case_mgmt_docs_query = sql_queries.case_mgmt_docs_query.case_mgmt_docs_query
            cm_documents_columns = sql_queries.case_mgmt_docs_query.cm_documents_columns
            cm_documents_columns_numeric = sql_queries.case_mgmt_docs_query.cm_documents_columns_numeric
            cm_documents_columns_dates = sql_queries.case_mgmt_docs_query.cm_documents_columns_dates
            df_cm_documents = db_handler.process_and_save_df(case_mgmt_docs_query, cm_documents_columns, config.start_time, config.end_time, config.cm_documents_pickle, cm_documents_columns_numeric, cm_documents_columns_dates)

            case_mgmt_events_query = sql_queries.case_mgmt_events_query.case_mgmt_events_query
            cm_events_columns = sql_queries.case_mgmt_events_query.cm_events_columns
            cm_events_columns_numeric = sql_queries.case_mgmt_events_query.cm_events_columns_numeric
            cm_events_columns_dates = sql_queries.case_mgmt_events_query.cm_events_columns_dates
            df_cm_events = db_handler.process_and_save_df(case_mgmt_events_query, cm_events_columns, config.start_time, config.end_time, config.cm_events_pickle, cm_events_columns_numeric, cm_events_columns_dates)
        if testing_mode:
            cm_excel_maker = ExcelMaker(config.output_directory, config.output_directory)
        else: cm_excel_maker = ExcelMaker(config.cm_output_directory, config.supervisor_directory)
        cm_email_df_sups, cm_email_df_workers = cm_excel_maker.create_excel_files('Case Management', config.cm_documents_pickle, config.cm_events_pickle)

        if send_cm_emails:
            cm_email_sender = EmailSender()
            if send_supervisor_emails:
                cm_email_sender.send_worker_emails(cm_email_df_workers)
            if send_worker_emails:
                cm_email_sender.send_supervisor_emails(cm_email_df_sups)

        if send_test_emails:
            test_email_sender = EmailSender()
            test_email_sender.send_test_emails(cm_email_df_workers)

except Exception as e:
    success = False  # Mark failure
    error_details = traceback.format_exc()  # Get full traceback

    # Send an email with error details
    admin_email_sender = EmailSender()
    admin_email_sender.send_email_to_admin("djones@example.org", False, error_details)

    print("An error occurred! You have been notified.")
    print(error_details)

else:
    # If no exception, send success email
    admin_email_sender = EmailSender()
    admin_email_sender.send_email_to_admin("djones@example.org", True)

    print(f'Total run time = {(time.time() - run_start) / 60}')

finally:
    db_handler.disconnect()



