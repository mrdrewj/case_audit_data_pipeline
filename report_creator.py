import os
import string

import pandas as pd
import numpy as np
import re
import datetime
from openpyxl.styles import Font
import xlsxwriter.utility

# Set options permanently
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

class ExcelMaker:
    def __init__(self, output_dir, sup_output_dir):
        self.output_dir = output_dir
        self.sup_output_dir = sup_output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if not os.path.exists(sup_output_dir):
            os.makedirs(sup_output_dir)
        self.red_format = None
        self.yellow_format = None
        self.green_format = None
        self.peach_format = None
        self.grey_format = None

    def create_formats(self, workbook):
        self.red_format = workbook.add_format({'bg_color': '#FF7C80', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        self.yellow_format = workbook.add_format({'bg_color': '#F5F95D', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        self.green_format = workbook.add_format({'bg_color': '#92D050', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        self.peach_format = workbook.add_format({'bg_color': '#FCD5B4', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        self.grey_format = workbook.add_format({'bg_color': '#BFBFBF', 'border': 1, 'align': 'center', 'valign': 'vcenter'})

    def create_formats_with_date(self, workbook):
        self.red_format_with_date = workbook.add_format({'bg_color': '#FF7C80', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': 'mm/dd/yyyy'})
        self.yellow_format_with_date = workbook.add_format({'bg_color': '#F5F95D', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': 'mm/dd/yyyy'})
        self.green_format_with_date = workbook.add_format({'bg_color': '#92D050', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': 'mm/dd/yyyy'})
        self.peach_format_with_date = workbook.add_format({'bg_color': '#FCD5B4', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': 'mm/dd/yyyy'})
        self.grey_format_with_date = workbook.add_format({'bg_color': '#BFBFBF', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'num_format': 'mm/dd/yyyy'})


    def create_excel_files(self, excel_file_type, doc_pickle_path, event_pickle_path, supervisor_column='supervisor_name', worker_column='worker_name'):
        # Load DataFrames from pickle files
        doc_df = pd.read_pickle(doc_pickle_path)
        event_df = pd.read_pickle(event_pickle_path)

        # Initialize an empty DataFrame to store emails
        email_df_sups = pd.DataFrame(columns=["first name", "last name", "email", "filename"])
        email_df_workers = pd.DataFrame(columns=["first name", "last name", "email", "filename"])

        # Create Excel files for each supervisor
        supervisors = doc_df[supervisor_column].unique()
        for supervisor in supervisors:

            if supervisor is not None:
                supervisor_email = doc_df.loc[doc_df[supervisor_column] == supervisor, 'supervisor_email'].values[0]

            supervisor_docs_df = doc_df[doc_df[supervisor_column] == supervisor]
            supervisor_events_df = event_df[event_df[supervisor_column] == supervisor]

            if supervisor:
                last_name, first_name = supervisor.split(', ')

                if excel_file_type == 'Family Counseling':
                    output_file = os.path.join(self.sup_output_dir, f'Supervisor_Audit_{first_name}_{last_name}_FC.xlsx')

                    # Create a dictionary with supervisor info
                    email_dict = {"first name": first_name, "last name": last_name, "email": supervisor_email, "filename": output_file}
                    # Append the dictionary to the email_df as a new row
                    email_df_sups = pd.concat([email_df_sups, pd.DataFrame([email_dict])], ignore_index=True)

                    self.create_FC_excel_with_sheets(output_file, supervisor_docs_df, supervisor_events_df, 'Documents','Events')

                elif excel_file_type == 'Case Management':
                    output_file = os.path.join(self.sup_output_dir, f'Supervisor_Audit_{first_name}_{last_name}_CM.xlsx')

                    # Create a dictionary with supervisor info
                    email_dict = {"first name": first_name, "last name": last_name, "email": supervisor_email, "filename": output_file}
                    # Append the dictionary to the email_df as a new row
                    email_df_sups = pd.concat([email_df_sups, pd.DataFrame([email_dict])], ignore_index=True)

                    self.create_CM_excel_with_sheets(output_file, supervisor_docs_df, supervisor_events_df, 'Documents','Events')


            else:
                output_file = os.path.join(self.sup_output_dir, f'Audit_No_Supervisor_Set.xlsx')

            print(f"Excel file created for supervisor {supervisor} at {output_file}")

        # Create Excel files for each worker
        workers = doc_df[worker_column].unique()
        for worker in workers:
            worker_docs_df = doc_df[doc_df[worker_column] == worker]
            worker_events_df = event_df[event_df[worker_column] == worker]
            worker_email = doc_df.loc[doc_df[worker_column] == worker, 'worker_email'].values[0]
            last_name, first_name = worker.split(', ')

            if excel_file_type == 'Family Counseling':
                output_file = os.path.join(self.output_dir, f'Audit_{first_name}_{last_name}_FC.xlsx')

                # Create a dictionary with supervisor info
                email_dict = {"first name": first_name, "last name": last_name, "email": worker_email, "filename": output_file}
                # Append the dictionary to the email_df as a new row
                email_df_workers = pd.concat([email_df_workers, pd.DataFrame([email_dict])], ignore_index=True)

                self.create_FC_excel_with_sheets(output_file, worker_docs_df, worker_events_df, 'Documents', 'Events')

            elif excel_file_type == 'Case Management':
                output_file = os.path.join(self.output_dir, f'Audit_{first_name}_{last_name}_CM.xlsx')

                # Create a dictionary with supervisor info
                email_dict = {"first name": first_name, "last name": last_name, "email": worker_email, "filename": output_file}
                # Append the dictionary to the email_df as a new row
                email_df_workers = pd.concat([email_df_workers, pd.DataFrame([email_dict])], ignore_index=True)

                self.create_CM_excel_with_sheets(output_file, worker_docs_df, worker_events_df, 'Documents', 'Events')

            print(f"Excel file created for worker {worker} at {output_file}")

        return email_df_sups, email_df_workers

    def create_FC_excel_with_sheets(self, output_file, doc_df, event_df, doc_sheet_name='Documents', event_sheet_name='Events'):
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Transpose the document DataFrame before writing to Excel
            transposed_doc_df = doc_df.T.reset_index()
            transposed_doc_df.columns = ['Field'] + transposed_doc_df.columns.tolist()[1:]
            transposed_doc_df.to_excel(writer, sheet_name=doc_sheet_name, index=False)
            event_df.to_excel(writer, sheet_name=event_sheet_name, index=False)

            workbook = writer.book
            self.create_formats(workbook)
            self.create_formats_with_date(workbook)
            doc_worksheet = writer.sheets[doc_sheet_name]
            event_worksheet = writer.sheets[event_sheet_name]

            event_boolean_columns = ['has_case_note', 'case_note_has_signature', 'was_counseling_session', 'next_session_scheduled']


            # Set standard cell borders
            self.set_regular_borders(workbook, doc_worksheet, transposed_doc_df)
            # Center Text
            self.center_text_except_column_a(workbook, doc_worksheet, transposed_doc_df)

            # Hide some Rows
            self.hide_row(doc_worksheet, transposed_doc_df, 'worker_email')
            self.hide_row(doc_worksheet, transposed_doc_df, 'supervisor_email')

            # Apply conditional formatting based on percent completion
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Individual_profile_pcnt', 95, 90, use_middle=True)

            # Apply conditional formatting based on percent completion
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indiv_profile_additional_info_pcnt', 95, 90, use_middle=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_contact_info')
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_address')
            self.apply_not_required_formatting_text(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_collateral_contacts')

            #
            self.apply_ccm_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CCM_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'CCM_Intake_pcnt', 95, 91, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Youth_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Youth_Intake_pcnt', 62, 31, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'PG_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'PG_Intake_pcnt', 91, 48, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Household_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Household_Intake_pcnt', 91, 48, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info_pcnt', 100, 78, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info_Signature')
            # Apply conditional formatting based expiration
            self.apply_expiration_formatting(workbook, doc_worksheet, transposed_doc_df, 'roi_expired')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Over_18')
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Over_18_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18_pcnt', 100, 73, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services_pcnt', 100, 45, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS_pcnt', 100, 68, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Informed_Consent')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Informed_Consent_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Informed_Consent_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Comprehensive_Evaluation')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Comprehensive_Evaluation_pcnt', 55, 19, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Comprehensive_Evaluation_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Diagnosis')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Diagnosis_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Diagnosis_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Treatment_Plan')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Treatment_Plan_pcnt', 100, 49, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Treatment_Plan_Signature')

            #
            self.apply_casii_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CASII')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'CASII_pcnt', 100, 87, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CASII_Signature')

            #
            self.apply_locus_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'LOCUS')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'LOCUS_pcnt', 100, 86, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'LOCUS_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability')
            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_Stage')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan_pcnt', 89, 86, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Audio_Visual_Consent')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Audio_Visual_Consent_pcnt', 100, 66, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Audio_Visual_Consent_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_Report')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_Report_pcnt', 100, 89, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_Report_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary_pcnt', 100, 78, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary_Signature')

            # Add in attachments to bottom of sheet.
            attachment_row_names = ['individual_attachments', 'case_file_attachments', 'service_file_attachments','non_pres_individual_attachments']
            self.expand_and_write_cells(workbook, doc_worksheet, transposed_doc_df, attachment_row_names)
            # Create Attachment color key
            pastel_colors = [
                '#BAFFD8',  # Pastel Mint
                '#FFFAC8',  # Pastel Cream
                '#FFBAE6',  # Pastel Pink
                '#70E5F7'   # Pastel Blue
            ]
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[0], 'A', pastel_colors[0])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[1], 'A', pastel_colors[1])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[2], 'A', pastel_colors[2])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[3], 'A', pastel_colors[3])

            # Set column widths
            self.set_standard_column_widths(doc_worksheet)

            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 0, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 1, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 2, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 3, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 4, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 5, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 6, 11)
            self.format_basic_doc_sheet_date_row(workbook, doc_worksheet, transposed_doc_df, 7, 11)  # service file start date
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 8, 11)
            self.format_client_name_row(workbook, doc_worksheet, transposed_doc_df, 9, 14)           # client name
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 15, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 16, 11)

            #freeze the first Column
            doc_worksheet.freeze_panes(11, 1)

            # # # Events Sheet # # # -----------------------------------------------------------------------------------

            # Set column widths
            event_worksheet.set_column('A:A', 11)  # case_file_id
            event_worksheet.set_column('B:B', 20)  # case_file_name
            event_worksheet.set_column('C:C', 12)  # individual_id
            event_worksheet.set_column('D:D', 20)  # individual_name
            event_worksheet.set_column('E:E', 14)  # service_file_id
            event_worksheet.set_column('F:F', 17)  # service_file_status
            event_worksheet.set_column('G:G', 17)  # service_name
            event_worksheet.set_column('H:H', 20)  # supervisor_name
            event_worksheet.set_column('I:I', 20)  # worker_name
            event_worksheet.set_column('J:J', 9)  # event_id
            event_worksheet.set_column('K:K', 21)  # event_start_timestamp
            event_worksheet.set_column('L:L', 22)  # service_file_event_type
            event_worksheet.set_column('M:M', 45)  # event_name
            event_worksheet.set_column('N:N', 30)  # was_counseling_session
            event_worksheet.set_column('O:O', 24)  # service_file_event_status
            event_worksheet.set_column('P:P', 23)  # total_event_participants
            event_worksheet.set_column('Q:Q', 13)  # has_case_note
            event_worksheet.set_column('R:R', 22)  # case_note_date
            event_worksheet.set_column('S:S', 18)  # case_note_funding
            event_worksheet.set_column('T:T', 23)  # case_note_has_signature
            event_worksheet.set_column('U:U', 23)  # next_session_scheduled
            event_worksheet.set_column('V:V', 17)  # service_unit_class
            event_worksheet.set_column('W:W', 27)  # cart_item_name
            event_worksheet.set_column('X:X', 9)  # item_qty
            event_worksheet.set_column('Y:Y', 6)  # units
            event_worksheet.set_column('Z:Z', 20)  # cart_item_created_by


            self.apply_initial_event_formatting(workbook, event_worksheet, event_df, event_boolean_columns, 'case_file_id')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'has_case_note')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'case_note_has_signature')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'next_session_scheduled')

            #freeze the first Row
            event_worksheet.freeze_panes(1, 2)

            # self.format_client_name_row(workbook, event_worksheet, transposed_doc_df, 7, 14)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 0, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 1, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 2, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 3, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 4, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 5, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 6, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 13, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 14, 11)

    def create_CM_excel_with_sheets(self, output_file, doc_df, event_df, doc_sheet_name='Documents', event_sheet_name='Events'):
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            # Transpose the document DataFrame before writing to Excel
            transposed_doc_df = doc_df.T.reset_index()
            transposed_doc_df.columns = ['Field'] + transposed_doc_df.columns.tolist()[1:]
            transposed_doc_df.to_excel(writer, sheet_name=doc_sheet_name, index=False)
            event_df.to_excel(writer, sheet_name=event_sheet_name, index=False)
            #print(event_df.info())
            workbook = writer.book
            self.create_formats(workbook)
            self.create_formats_with_date(workbook)
            doc_worksheet = writer.sheets[doc_sheet_name]
            event_worksheet = writer.sheets[event_sheet_name]

            event_boolean_columns = ['has_case_note', 'case_note_has_signature', 'was_case_management_session', 'next_session_scheduled']

            # Set standard cell borders
            self.set_regular_borders(workbook, doc_worksheet, transposed_doc_df)
            # Center Text
            self.center_text_except_column_a(workbook, doc_worksheet, transposed_doc_df)

            # Hide some Rows
            self.hide_row(doc_worksheet, transposed_doc_df, 'worker_email')
            self.hide_row(doc_worksheet, transposed_doc_df, 'supervisor_email')
            self.hide_row(doc_worksheet, transposed_doc_df, 'Food_Stability_Stage')
            self.hide_row(doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary_Signature')

            # Apply conditional formatting based on percent completion
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Individual_profile_pcnt', 95, 90, use_middle=True)
            # Apply conditional formatting based on percent completion
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indiv_profile_additional_info_pcnt', 95, 90, use_middle=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_contact_info')
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_address')
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'indiv_has_collateral_contacts')

            #
            self.apply_ccm_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CCM_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'CCM_Intake_pcnt', 95, 91, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Youth_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Youth_Intake_pcnt', 62, 31, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'PG_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'PG_Intake_pcnt', 91, 48, use_middle=True, hide_pcnt_row=True)

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Household_Intake')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Household_Intake_pcnt', 91, 48, use_middle=True, hide_pcnt_row=True)


            #
            self.apply_180_day_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info_pcnt', 100, 78, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Release_of_Info_Signature')
            # Apply conditional formatting based expiration
            self.apply_expiration_formatting(workbook, doc_worksheet, transposed_doc_df, 'roi_expired')

            #
            self.apply_365_day_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Over_18')
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Over_18_Signature')

            #
            self.apply_365_day_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18_pcnt', 100, 73, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Indemnification_Under_18_Signature')

            #
            self.apply_365_day_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Agreement_for_Services_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Under_18_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Over_18_Signature')

            #
            self.apply_not_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_not_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'ACE_Parent_Signature')

            # Same as FC excel sheet above this point ----------------------------------------------------------------

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Drug')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Drug_pcnt', 100, 69, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Drug_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Alcohol')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Alcohol_pcnt', 100, 80, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'SBIRT_Alcohol_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS_pcnt', 100, 68, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'CSSRS_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability')
            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_Stage')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_pcnt', 100, 90, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Food_Stability_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Case_Plan_Life_Domains')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Case_Plan_Life_Domains_pcnt', 47, 17, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Case_Plan_Life_Domains_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan_pcnt', 89, 86, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Saftey_Plan_Signature')

            #
            self.apply_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_and_Neglect_Report')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_and_Neglect_Report_pcnt', 100, 89, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_maybe_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Abuse_and_Neglect_Report_Signature')

            #
            self.apply_non_null_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary')
            #
            self.apply_pcnt_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary_pcnt', 100, 78, use_middle=True, hide_pcnt_row=True)
            #
            self.apply_signature_required_formatting(workbook, doc_worksheet, transposed_doc_df, 'Service_File_Completion_Summary_Signature')

            # Add in attachments to bottom of sheet.
            attachment_row_names = ['individual_attachments', 'case_file_attachments', 'service_file_attachments','non_pres_individual_attachments']
            self.expand_and_write_cells(workbook, doc_worksheet, transposed_doc_df, attachment_row_names)
            # Create Attachment color key
            pastel_colors = [
                '#BAFFD8',  # Pastel Mint
                '#FFFAC8',  # Pastel Cream
                '#FFBAE6',  # Pastel Pink
                '#70E5F7'   # Pastel Blue

            ]
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[0], 'A', pastel_colors[0])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[1], 'A', pastel_colors[1])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[2], 'A', pastel_colors[2])
            self.format_single_cell(workbook, doc_worksheet, transposed_doc_df, attachment_row_names[3], 'A', pastel_colors[3])


            # Set column widths
            self.set_standard_column_widths(doc_worksheet)

            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 0, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 1, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 2, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 3, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 4, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 5, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 6, 11)
            self.format_basic_doc_sheet_date_row(workbook, doc_worksheet, transposed_doc_df, 7, 11)  # service file start date
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 8, 11)
            self.format_client_name_row(workbook, doc_worksheet, transposed_doc_df, 9, 14)           # client name
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 15, 11)
            self.format_basic_doc_sheet_row(workbook, doc_worksheet, transposed_doc_df, 16, 11)

            #freeze the first Column
            doc_worksheet.freeze_panes(11, 1)

            # # # Events Sheet # # # -----------------------------------------------------------------------------------

            # Set column widths
            event_worksheet.set_column('A:A', 11)  # case_file_id
            event_worksheet.set_column('B:B', 20)  # case_file_name
            event_worksheet.set_column('C:C', 12)  # individual_id
            event_worksheet.set_column('D:D', 20)  # individual_name
            event_worksheet.set_column('E:E', 14)  # service_file_id
            event_worksheet.set_column('F:F', 17)  # service_file_status
            event_worksheet.set_column('G:G', 17)  # service_name
            event_worksheet.set_column('H:H', 20)  # supervisor_name
            event_worksheet.set_column('I:I', 20)  # worker_name
            event_worksheet.set_column('J:J', 9)  # event_id
            event_worksheet.set_column('K:K', 21)  # event_start_timestamp
            event_worksheet.set_column('L:L', 22)  # service_file_event_type
            event_worksheet.set_column('M:M', 45)  # event_name
            event_worksheet.set_column('N:N', 30) #, None, {'hidden': True})  # was_cm_session -- only used for PAT, not currently hidden
            event_worksheet.set_column('O:O', 24)  # service_file_event_status
            event_worksheet.set_column('P:P', 23)  # total_event_participants
            event_worksheet.set_column('Q:Q', 13)  # has_case_note
            event_worksheet.set_column('R:R', 22)  # case_note_date
            event_worksheet.set_column('S:S', 18)  # case_note_funding
            event_worksheet.set_column('T:T', 23)  # case_note_has_signature
            event_worksheet.set_column('U:U', 23)  # next_session_scheduled
            event_worksheet.set_column('V:V', 17)  # service_unit_class
            event_worksheet.set_column('W:W', 27)  # cart_item_name
            event_worksheet.set_column('X:X', 9)  # item_qty
            event_worksheet.set_column('Y:Y', 6)  # units
            event_worksheet.set_column('Z:Z', 20)  # cart_item_created_by
            event_worksheet.set_column('AA:AA', 22)  # number_referrals_given
            event_worksheet.set_column('AB:AB', 22)  # referral_1
            event_worksheet.set_column('AC:AC', 22)  # referral_2
            event_worksheet.set_column('AD:AD', 22)  # referral_3
            event_worksheet.set_column('AE:AE', 22)  # referral_4
            event_worksheet.set_column('AF:AF', 22)  # referral_5
            event_worksheet.set_column('AG:AG', 22)  # referral_6
            event_worksheet.set_column('AH:AH', 22)  # referral_7
            event_worksheet.set_column('AI:AI', 22)  # referral_8


            self.apply_initial_event_formatting(workbook, event_worksheet, event_df, event_boolean_columns, 'case_file_id')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'has_case_note')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'case_note_has_signature')
            self.apply_boolean_color_formatting(workbook, event_worksheet, event_df, 'next_session_scheduled')

            #freeze the first Row
            event_worksheet.freeze_panes(1, 2)

            # self.format_client_name_row(workbook, event_worksheet, transposed_doc_df, 7, 14)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 0, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 1, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 2, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 3, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 4, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 5, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 6, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 13, 11)
            # self.format_basic_doc_sheet_row(workbook, event_worksheet, transposed_doc_df, 14, 11)

    def apply_boolean_color_formatting(self, workbook, worksheet, df, row_name):
        # Find the column index for the given column name
        col_idx = df.columns.get_loc(row_name)
        start_row = 1  # Assuming Excel's 1-based indexing

        # Loop through each cell in the specified column
        for row_idx in range(start_row, start_row + len(df)):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                pass
            elif cell_value in [1, '1', True, 'True', 'TRUE']:
                # Keep as True
                worksheet.write(row_idx, col_idx, True, self.green_format)
            elif cell_value in [0, '0', False, 'False', 'FALSE']:
                # Keep as False
                worksheet.write(row_idx, col_idx, False, self.red_format)
            else:
                pass

    def apply_pcnt_formatting(self, workbook, worksheet, df, row_name, acceptable_percent_val, lowest_percent_val, use_middle=False, hide_pcnt_row=False):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Apply formatting for values less than acceptable_percent_val
        worksheet.conditional_format(row_idx, start_col, row_idx, start_col + len(df.columns) - 2,
                                     {'type': 'cell', 'criteria': '<', 'value': lowest_percent_val, 'format': self.red_format})

        if use_middle:
            worksheet.conditional_format(row_idx, start_col, row_idx, start_col + len(df.columns) - 2,
                                 {'type': 'cell', 'criteria': 'between', 'minimum': lowest_percent_val, 'maximum': acceptable_percent_val - 1, 'format': self.yellow_format})

        # Apply formatting for values greater than or equal to acceptable_percent_val
        worksheet.conditional_format(row_idx, start_col, row_idx, start_col + len(df.columns) - 2,
                                     {'type': 'cell', 'criteria': '>=', 'value': acceptable_percent_val, 'format': self.green_format})

        # Hide the row if hide_pcnt_row is True
        if hide_pcnt_row:
            worksheet.set_row(row_idx, None, None, {'hidden': True})

    def apply_maybe_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                # Handle missing values with "Might be Required" text and peach format
                worksheet.write(row_idx, col_idx, "Might be Required", self.peach_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write_datetime(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_ccm_maybe_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                # Handle missing values with "Might be Required" text and peach format
                worksheet.write(row_idx, col_idx, "Required if Client has CCM", self.peach_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write_datetime(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_casii_maybe_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                # Handle missing values with "Might be Required" text and peach format
                worksheet.write(row_idx, col_idx, "Required if Client is under 18", self.peach_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write_datetime(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_locus_maybe_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                # Handle missing values with "Might be Required" text and peach format
                worksheet.write(row_idx, col_idx, "Required if Client is over 18", self.peach_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write_datetime(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_not_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                worksheet.write(row_idx, col_idx, cell_value, self.grey_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write_datetime(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_not_required_formatting_text(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + len(df.columns) - 1):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isna(cell_value):
                worksheet.write(row_idx, col_idx, cell_value, self.grey_format)
            else:
                # Apply date formatting if the value exists
                worksheet.write(row_idx, col_idx, cell_value, self.green_format)

    def apply_non_null_formatting(self, workbook, worksheet, df, row_name):

        # Find the row index for the given column name in the transposed DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Apply red formatting for values that are Null or None
        worksheet.conditional_format(row_idx, start_col, row_idx, start_col + len(df.columns) - 2,
                                     {'type': 'blanks', 'format': self.red_format})

        # Apply green formatting for values that are not Null or None
        worksheet.conditional_format(row_idx, start_col, row_idx, start_col + len(df.columns) - 2,
                                     {'type': 'no_blanks', 'format': self.green_format})

    def apply_365_day_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Get the current date
        current_date = datetime.datetime.now().date()

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            # Check if the cell_value is a date
            if pd.isnull(cell_value) or cell_value == '':
                # Apply red formatting for null or empty cells
                worksheet.write(row_idx, col_idx, cell_value, self.red_format)
            else:
                cell_date = cell_value
                days_difference = (current_date - cell_date).days

                if days_difference > 365:
                    # Apply red formatting if the date is more than 365 days old
                    worksheet.write(row_idx, col_idx, "EXPIRED", self.red_format)
                elif days_difference <= 365:
                    # Apply green formatting if the date is within 365 days
                    worksheet.write(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_180_day_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Get the current date
        current_date = datetime.datetime.now().date()

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            # Check if the cell_value is a date
            if pd.isnull(cell_value) or cell_value == '':
                # Apply red formatting for null or empty cells
                worksheet.write(row_idx, col_idx, cell_value, self.red_format)
            else:
                cell_date = cell_value
                days_difference = (current_date - cell_date).days

                if days_difference > 180:
                    # Apply red formatting if the date is more than 180 days old
                    worksheet.write(row_idx, col_idx, "EXPIRED", self.red_format)
                    # Update the DataFrame with "EXPIRED"
                    df.iloc[row_idx - 1, col_idx] = "EXPIRED"
                elif days_difference <= 180:
                    # Apply green formatting if the date is within 180 days
                    worksheet.write(row_idx, col_idx, cell_value, self.green_format_with_date)

    def apply_expiration_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing
        start_col = 1

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isnull(cell_value) or cell_value == '':
                # Apply red formatting for null or empty cells
                worksheet.write(row_idx, col_idx, cell_value, self.red_format)
            elif isinstance(cell_value, str):
                if 'YES' in cell_value:
                    # Apply red formatting for expired
                    worksheet.write(row_idx, col_idx, cell_value, self.red_format)
                elif 'NO' in cell_value:
                    match = re.search(r'in (\d+) days', cell_value)
                    if match:
                        days_remaining = int(match.group(1))
                        if days_remaining <= 29:
                            # Apply yellow formatting for expiring within 29 days
                            worksheet.write(row_idx, col_idx, cell_value, self.yellow_format)
                        else:
                            # Apply green formatting for not expiring for at least 30 days
                            worksheet.write(row_idx, col_idx, cell_value, self.green_format)
                    else:
                        # Apply green formatting for not expiring (default case)
                        worksheet.write(row_idx, col_idx, cell_value, self.green_format)

    def apply_signature_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing

        doc_row_name = row_name.replace("_Signature", "")

        row_index_doc = df[df['Field'] == doc_row_name].index[0] + 1
        start_col = 1  # Assuming headers are in the first column

        # Define the base format for borders
        regular_border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index
            doc_cell_value = df.iloc[row_index_doc - 1, col_idx]

            # Check if the document is expired first
            if pd.isnull(doc_cell_value) or doc_cell_value == 'EXPIRED':
                worksheet.write(row_idx, col_idx, cell_value, self.red_format)
            elif pd.isnull(cell_value) or cell_value == 'Not Signed':
                # Apply red formatting for null or 'Not Signed'
                worksheet.write(row_idx, col_idx, cell_value, self.red_format)
            else:
                # Apply regular border format for other cases
                worksheet.write(row_idx, col_idx, cell_value, self.green_format)

    def apply_signature_maybe_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing
        start_col = 1  # Assuming headers are in the first column

        # Define the base format for borders
        regular_border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isnull(cell_value):
                # Apply red formatting for null
                worksheet.write(row_idx, col_idx, "Might be Required", self.peach_format)
            elif cell_value == 'Not Signed':
                # Apply red formatting for 'Not Signed'
                worksheet.write(row_idx, col_idx, cell_value, self.peach_format)
            # elif cell_value == 'Fully Signed':
            #     # Apply green formatting for 'Signed and Dated'
            #     worksheet.write(row_idx, col_idx, cell_value, self.green_format)
            # elif cell_value == 'Partially Signed':
            #     # Apply yellow formatting for 'Partially Signed'
            #     worksheet.write(row_idx, col_idx, cell_value, self.yellow_format)
            else:
                # Apply regular border format for other cases
                worksheet.write(row_idx, col_idx, cell_value, self.green_format)

    def apply_signature_not_required_formatting(self, workbook, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df[df['Field'] == row_name].index[0] + 1  # +1 to match Excel's 1-based indexing
        start_col = 1  # Assuming headers are in the first column

        # Define the base format for borders
        regular_border_format = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})

        # Get the number of columns in the DataFrame
        num_cols = len(df.columns) - 1  # Adjust for 0-based index

        # Loop through each cell in the specified row
        for col_idx in range(start_col, start_col + num_cols):
            cell_value = df.iloc[row_idx - 1, col_idx]  # Adjust for 0-based index

            if pd.isnull(cell_value) or cell_value == 'Not Signed':
                # Apply red formatting for null or 'Not Signed'
                worksheet.write(row_idx, col_idx, cell_value, self.grey_format)
            # elif cell_value == 'Fully Signed':
            #     # Apply green formatting for 'Signed and Dated'
            #     worksheet.write(row_idx, col_idx, cell_value, self.green_format)
            # elif cell_value == 'Partially Signed':
            #     # Apply yellow formatting for 'Partially Signed'
            #     worksheet.write(row_idx, col_idx, cell_value, self.yellow_format)
            else:
                # Apply regular border format for other cases
                worksheet.write(row_idx, col_idx, cell_value, self.green_format)

    def expand_and_write_cells(self, workbook, worksheet, df, row_names):
        # Get the positional indices of the rows based on the 'Field' column
        row_idxs = [df.index.get_loc(df[df['Field'] == row_name].index[0]) for row_name in row_names]
        start_row = min(row_idxs) + 1  # Adjust for Excel's 1-based indexing
        start_col = 1  # Column B in Excel (since column A is index 0)
        num_cols = len(df.columns) - 1  # Exclude 'Field' column

        # Define formats for each 'Field' using pastel colors
        pastel_colors = [
            '#BAFFD8',  # Pastel Mint
            '#FFFAC8',  # Pastel Cream
            '#FFBAE6',  # Pastel Pink
            '#70E5F7'  # Pastel Blue
        ]

        field_formats = {}
        field_names = [df.iloc[row_idx, 0] for row_idx in row_idxs]
        for field_name, color in zip(field_names, pastel_colors):
            field_formats[field_name] = workbook.add_format({
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'bg_color': color
            })

        # Default format for cells without specific formatting
        regular_format = workbook.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter'})

        # Prepare a dictionary to hold values for each column
        # Each value is a tuple: (value, field_name)
        column_values = {}
        max_rows_needed = 0

        # Process each column to collect and expand values
        for col_idx in range(start_col, start_col + num_cols):
            values = []
            for row_idx in row_idxs:
                cell_value = df.iloc[row_idx, col_idx]
                if pd.isnull(cell_value):
                    continue
                # Split the cell value by commas and strip whitespace
                split_values = [v.strip() for v in str(cell_value).split(',')]
                # Get the field name (from column 0)
                field_name = df.iloc[row_idx, 0]
                # Store tuples of (value, field_name)
                values.extend([(value, field_name) for value in split_values])
            column_values[col_idx] = values
            max_rows_needed = max(max_rows_needed, len(values))

        # Write the expanded values back into the worksheet
        for i in range(max_rows_needed):
            # We do not write anything in column A
            for col_idx in range(start_col, start_col + num_cols):
                values = column_values.get(col_idx, [])
                if i < len(values):
                    value, field_name = values[i]
                    # Use the format corresponding to the field name
                    cell_format = field_formats.get(field_name, regular_format)
                    worksheet.write(start_row + i, col_idx, value, cell_format)
                else:
                    # Write a blank cell if there's no value
                    worksheet.write_blank(start_row + i, col_idx, None, regular_format)

        # Optionally, clear the original rows if they extend beyond the new data
        for row_idx in row_idxs:
            excel_row_idx = row_idx + 1  # Adjust for Excel's 1-based indexing
            if excel_row_idx >= start_row + max_rows_needed:
                for col_idx in range(0, start_col + num_cols):
                    worksheet.write_blank(excel_row_idx, col_idx, None, regular_format)

    def hide_row(self, worksheet, df, row_name):
        # Find the row index for the given column name in the DataFrame
        row_idx = df['Field'].tolist().index(row_name) + 1  # +1 to match Excel's 1-based indexing

        # Hide the row using the `set_row` method
        worksheet.set_row(row_idx, None, None, {'hidden': True})


    @staticmethod
    def set_standard_column_widths(worksheet):

        worksheet.set_column('A:A', 62)  # Set width of column A to 62
        worksheet.set_column('B:ZZ', 40)  # Set width of all other columns (B to Z) to 40
    @staticmethod
    def apply_initial_event_formatting(workbook, worksheet, df, bool_columns, case_file_column):
        # Define a list of pastel colors
        pastel_colors = [
            '#BAE1FF',  # Light Blue
            '#E1BAFF',  # Light Purple
            '#BAFFD4',  # Light Mint
            '#BAFFFF',  # Light Aqua
        ]

        # Get the list of unique case file IDs and create a color map
        unique_case_ids = df[case_file_column].unique()
        color_map = {case_id: pastel_colors[i % len(pastel_colors)] for i, case_id in enumerate(unique_case_ids)}

        # Loop through each row in the DataFrame
        for row_idx in range(1, len(df) + 1):
            case_id = df.iloc[row_idx - 1][case_file_column]  # Get the case file ID for this row

            # Determine the color format for this row based on case_id
            if pd.notna(case_id):  # Only apply formatting if case_id is not NaN
                color_format = workbook.add_format({
                    'bg_color': color_map[case_id],
                    'align': 'center',
                    'valign': 'vcenter',
                    'border': 1
                })

            # Handle each boolean column individually
            for row_name in bool_columns:
                if row_name in df.columns:
                    col_idx = df.columns.get_loc(row_name)
                    cell_value = df.iloc[row_idx - 1, col_idx]  # Get the value for the boolean column

                    # Write the boolean value with appropriate formatting
                    if pd.isna(cell_value):
                        worksheet.write(row_idx, col_idx, "", color_format)  # Keep Nulls as is
                    elif cell_value in [1, '1', True, 'True']:
                        worksheet.write(row_idx, col_idx, True, color_format)  # Keep as True
                    elif cell_value in [0, '0', False, 'False']:
                        worksheet.write(row_idx, col_idx, False, color_format)  # Keep as False
                    else:
                        worksheet.write(row_idx, col_idx, cell_value, color_format)  # Keep original value

            # Apply the color format to the rest of the row
            for col_idx in range(len(df.columns)):
                if df.columns[col_idx] not in bool_columns:  # Skip the boolean columns as they're already handled
                    other_cell_value = df.iloc[row_idx - 1, col_idx]
                    if pd.isna(other_cell_value):
                        worksheet.write(row_idx, col_idx, "", color_format)  # Write an empty string for NaN values
                    elif isinstance(other_cell_value, (datetime.datetime, pd.Timestamp)):
                        datetime_format_with_color = workbook.add_format({
                            'bg_color': color_map[case_id],
                            'align': 'center',
                            'valign': 'vcenter',
                            'border': 1,
                            'num_format': 'mm/dd/yyyy hh:mm:ss AM/PM'
                        })
                        worksheet.write_datetime(row_idx, col_idx, other_cell_value, datetime_format_with_color)
                    elif isinstance(other_cell_value, bool):
                        worksheet.write(row_idx, col_idx, other_cell_value, color_format)
                    elif isinstance(other_cell_value, (int, float)):
                        worksheet.write_number(row_idx, col_idx, other_cell_value, color_format)
                    else:
                        worksheet.write(row_idx, col_idx, other_cell_value, color_format)

    @staticmethod
    def format_single_cell(workbook, worksheet, df, row_name, col_name, color_code):
        # Create the cell format inside the function
        cell_format = workbook.add_format({
            'font_size': 12,
            'bg_color': color_code,
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })

        # Find the row index where df['Field'] == row_name
        if row_name not in df['Field'].values:
            raise ValueError(f"Row name '{row_name}' not found in 'Field' column.")
        row_idx = df['Field'].tolist().index(row_name)

        # Determine the column index
        if isinstance(col_name, str):
            # Convert column letter to index (e.g., 'A' -> 0)
            col_idx = ord(col_name.upper()) - ord('A')
        elif isinstance(col_name, int):
            col_idx = col_name
        else:
            raise ValueError("col_name must be a column letter (string) or index (integer)")

        # Retrieve the cell value from the DataFrame
        cell_value = df.iloc[row_idx, col_idx]

        # Adjust for Excel's indexing
        excel_row = row_idx + 1  # Adjust if your data starts from the second row in Excel
        excel_col = col_idx  # Column index remains the same (0-based)

        # Write the cell value back to the worksheet with the specified format
        worksheet.write(excel_row, excel_col, cell_value, cell_format)

    @staticmethod
    def format_client_name_row(workbook, worksheet, df, row_num, font_size, exclude_columns=['A']):
        # Define a cell format with a thick bottom border, center alignment, background color, and bold text
        cell_format = workbook.add_format({
            'font_size': font_size,
            'bold': True,
            'bg_color': '#FFCC66',
            'align': 'center',
            'valign': 'vcenter',
            'border': 2,
            'bottom': 5,  # 5 represents a thick border
            'bottom_color': '#000000'  # Black bottom border
        })

        num_cols = len(df.columns)

        for col_idx in range(num_cols):  # Adjust for 0-based index
            col_letter = chr(65 + col_idx)  # A=65 in ASCII
            if col_letter not in exclude_columns:
                cell_value = df.iloc[row_num, col_idx]
                cell_location = xlsxwriter.utility.xl_rowcol_to_cell(row_num + 1, col_idx)  # Adjust for 0-based index
                worksheet.write(cell_location, cell_value, cell_format)

    @staticmethod   # This formats the "general" rows of the Doc sheet that are blue.
    def format_basic_doc_sheet_row(workbook, worksheet, df, row_num, font_size, exclude_columns=['A']):
        cell_format = workbook.add_format({'font_size': font_size, 'bg_color': '#B8CCE4',  'align': 'center', 'valign': 'vcenter', 'border': 1})
        num_cols = len(df.columns)
        for col_idx in range(num_cols):  # Adjust for 0-based index
            col_letter = chr(65 + col_idx)  # A=65 in ASCII
            if col_letter not in exclude_columns:
                cell_value = df.iloc[row_num, col_idx]
                cell_location = xlsxwriter.utility.xl_rowcol_to_cell(row_num + 1, col_idx)  # Adjust for 0-based index
                worksheet.write(cell_location, cell_value, cell_format)

    @staticmethod  # This formats the "general" rows of the Doc sheet that are blue.
    def format_basic_doc_sheet_date_row(workbook, worksheet, df, row_num, font_size, exclude_columns=['A']):
        # Create the format for the date cells
        cell_format = workbook.add_format({
            'font_size': font_size,
            'num_format': 'mm/dd/yyyy',
            'bg_color': '#B8CCE4',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })

        num_cols = len(df.columns)

        for col_idx in range(num_cols):  # Adjust for 0-based index
            col_letter = chr(65 + col_idx)  # A=65 in ASCII
            if col_letter not in exclude_columns:
                cell_value = df.iloc[row_num, col_idx]
                cell_location = xlsxwriter.utility.xl_rowcol_to_cell(row_num + 1, col_idx)  # Adjust for 0-based index

                # If the cell_value is a datetime object, write it with the date format
                if isinstance(cell_value, (pd.Timestamp, datetime.date)):
                    worksheet.write_datetime(row_num + 1, col_idx, cell_value, cell_format)
                else:
                    # For non-date values, use regular write
                    worksheet.write(row_num + 1, col_idx, cell_value, cell_format)

    @staticmethod
    def set_regular_borders(workbook, worksheet, df):
        # Define the format for regular borders
        regular_border_format = workbook.add_format({'border': 1})  # Regular border
        date_format = workbook.add_format({'border': 1, 'num_format': 'mm/dd/yyyy'})  # Regular border with date format

        # Get the number of rows and columns in the DataFrame
        num_rows, num_cols = df.shape

        # Apply regular borders to all cells
        for row in range(num_rows + 1):  # +1 to include the header row
            for col in range(num_cols):
                if row == 0:
                    worksheet.write(row, col, df.columns[col], regular_border_format)  # Header row
                else:
                    value = df.iloc[row - 1, col]  # Data rows
                    if pd.isna(value):
                        value = ''  # Replace NaN with empty string
                    if isinstance(value, (pd.Timestamp, datetime.date)):
                        worksheet.write_datetime(row, col, value, date_format)
                    else:
                        worksheet.write(row, col, value, regular_border_format)

    @staticmethod
    def center_text_except_column_a(workbook, worksheet, df):
        # Define the format for centered text
        centered_text_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})
        date_format_centered = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'num_format': 'mm/dd/yyyy'})

        # Get the number of rows and columns in the DataFrame
        num_rows, num_cols = df.shape

        # Apply centered text format to all cells except column A
        for row in range(num_rows + 1):  # +1 to include the header row
            for col in range(1, num_cols):  # Start from 1 to skip column A
                if row == 0:
                    worksheet.write(row, col, df.columns[col], centered_text_format)  # Header row
                else:
                    value = df.iloc[row - 1, col]  # Data rows
                    if pd.isna(value):
                        value = ''  # Replace NaN with empty string
                    if isinstance(value, (pd.Timestamp, datetime.date)):
                        worksheet.write_datetime(row, col, value, date_format_centered)
                    else:
                        worksheet.write(row, col, value, centered_text_format)





