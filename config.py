from datetime import datetime, timedelta
import os
import login_info_drew


# connection_information = {
#     'dbname': 'ENTER DB NAME',
#     'user': 'ENTER USER NAME',             # To Get this you need to get ODBC access to Penelope from their support staff.
#     'password': 'ENTER YOUR PASSWORD',
#     'host': 'ENTER HOST',
#     'port': '5432',
#     'sslmode': 'require'}

connection_information = login_info_drew.connection_information  # Anyone but Drew use connection info above, login_info_drew is not available on github version.

start_time = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

output_directory = r'C:\example_audit_directory\outputs'
fc_output_directory = r"C:\example_audit_directory\FC_Audits"
cm_output_directory = r"C:\example_audit_directory\CM_Audits"
supervisor_directory = r"C:\example_audit_directory\Supervisor_Audits"

fc_documents_pickle = 'pickles/fc_documents.pkl'
fc_documents_excel_path = 'outputs/FC_DOCUMENTS_AUDIT.xlsx'

fc_events_pickle = 'pickles/fc_events.pkl'
fc_events_excel_path = 'outputs/FC_EVENTS_AUDIT.xlsx'

cm_documents_pickle = 'pickles/cm_documents.pkl'
cm_documents_excel_path = 'outputs/CM_DOCUMENTS_AUDIT.xlsx'

cm_events_pickle = 'pickles/cm_events.pkl'
cm_events_excel_path = 'outputs/CM_EVENTS_AUDIT.xlsx'






















# # The time frame is selected automatically as the current month,
# # unless you override this using the commented out variables below...
# current_month = datetime.now().strftime("%B")
# current_year = datetime.now().strftime("%Y")
# #current_year = 2023  # set custom year


# timestamps = {
#     'Fiscal_YTD': (f"{current_year}-07-01 00:00:00", f"{str(int(current_year)+1)}-06-30 23:59:59"),
#     'Last_Fiscal': (f"{str(int(current_year)-1)}-07-01 00:00:00", f"{current_year}-06-30 23:59:59"),
#     'Custom': (f"2023-08-01 00:00:00", f"2024-02-09 23:59:59")
# }   # this is a list of all the timestamps for start and end of each month.

# the_report_folder = f"Report_Outputs\\{current_year}"  # Local folder path, where reports will save.
# the_report_folder = f"C:\\Users\\djones\\PycharmProjects\\CC_Market_FBNN_Monthly_Report\\Report_Outputs\\{current_year}"  # THIS IS Absolute Path, if needed.
