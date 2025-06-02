fc_documents_columns = [
    'worker_name',
    'worker_email',
    'supervisor_name',
    'supervisor_email',
    'case_file_id',
    'service_name',
    'service_file_id',
    'service_file_start_date',
    'presenting_individual_id',
    'presenting_individual_name',
    'Individual_profile_pcnt',
    'Indiv_profile_additional_info_pcnt',
    'indiv_has_contact_info',
    'indiv_has_address',
    'indiv_has_collateral_contacts',
    'total_service_file_individuals',
    'other_case_members_profile_compltion_has_contact_has_address',
    'CCM_Intake',
    'CCM_Intake_pcnt',
    'Youth_Intake',
    'Youth_Intake_pcnt',
    'PG_Intake',
    'PG_Intake_pcnt',
    'Household_Intake',
    'Household_Intake_pcnt',
    'Release_of_Info',
    'Release_of_Info_pcnt',
    'Release_of_Info_Signature',
    'roi_expired',
    'Indemnification_Over_18',
    'Indemnification_Over_18_Signature',
    'Indemnification_Under_18',
    'Indemnification_Under_18_pcnt',
    'Indemnification_Under_18_Signature',
    'Agreement_for_Services',
    'Agreement_for_Services_pcnt',
    'Agreement_for_Services_Signature',
    'ACE_Under_18',
    'ACE_Under_18_pcnt',
    'ACE_Under_18_Signature',
    'ACE_Over_18',
    'ACE_Over_18_pcnt',
    'ACE_Over_18_Signature',
    'ACE_Parent',
    'ACE_Parent_pcnt',
    'ACE_Parent_Signature',
    'CSSRS',
    'CSSRS_pcnt',
    'CSSRS_Signature',
    'Informed_Consent',
    'Informed_Consent_pcnt',
    'Informed_Consent_Signature',
    'Comprehensive_Evaluation',
    'Comprehensive_Evaluation_pcnt',
    'Comprehensive_Evaluation_Signature',
    'Diagnosis',
    'Diagnosis_pcnt',
    'Diagnosis_Signature',
    'Treatment_Plan',
    'Treatment_Plan_pcnt',
    'Treatment_Plan_Signature',
    'CASII',
    'CASII_pcnt',
    'CASII_Signature',
    'LOCUS',
    'LOCUS_pcnt',
    'LOCUS_Signature',
    'Food_Stability',
    'Food_Stability_Stage',
    'Food_Stability_pcnt',
    'Food_Stability_Signature',
    'Saftey_Plan',
    'Saftey_Plan_pcnt',
    'Saftey_Plan_Signature',
    'Audio_Visual_Consent',
    'Audio_Visual_Consent_pcnt',
    'Audio_Visual_Consent_Signature',
    'Abuse_Report',
    'Abuse_Report_pcnt',
    'Abuse_Report_Signature',
    'Service_File_Completion_Summary',
    'Service_File_Completion_Summary_pcnt',
    'Service_File_Completion_Summary_Signature',
    'individual_attachments',
    'case_file_attachments',
    'service_file_attachments',
    'non_pres_individual_attachments'
]

fc_documents_columns_numeric = [
    'case_file_id',
    'service_file_id',
    'presenting_individual_id',
    'Individual_profile_pcnt',
    'Indiv_profile_additional_info_pcnt',
    'total_service_file_individuals',
    'CCM_Intake_pcnt',
    'Youth_Intake_pcnt',
    'PG_Intake_pcnt',
    'Release_of_Info_pcnt',
    'Indemnification_Under_18_pcnt',
    'Agreement_for_Services_pcnt',
    'ACE_Under_18_pcnt',
    'ACE_Over_18_pcnt',
    'ACE_Parent_pcnt',
    'CSSRS_pcnt',
    'Informed_Consent_pcnt',
    'Comprehensive_Evaluation_pcnt',
    'Diagnosis_pcnt',
    'Treatment_Plan_pcnt',
    'CASII_pcnt',
    'LOCUS_pcnt',
    'Food_Stability_pcnt',
    'Saftey_Plan_pcnt',
    'Audio_Visual_Consent_pcnt',
    'Abuse_Report_pcnt',
    'Service_File_Completion_Summary_pcnt'
]

fc_documents_columns_dates = [
    'service_file_start_date'
]

fc_documents_query = r'''

WITH 
document_completion_percent AS(
    WITH document_questions_answered AS (
        SELECT
            individual_profile.individual_id,
            document.document_id,
            document.document_date,
            document.document_template_id,
            document_body.document_body_id,
            document_answer_details.document_template_question_id,
            document_template_question.question,
            document_answer_details.is_answered,
            ROW_NUMBER() OVER (PARTITION BY individual_profile.individual_id, document.document_template_id ORDER BY document.document_date DESC) as doc_rank,
            ROUND(100.0 * SUM(CASE WHEN document_answer_details.is_answered THEN 1 ELSE 0 END) OVER(PARTITION BY document.document_id) / NULLIF(COUNT(document_answer_details.document_template_question_id) OVER(PARTITION BY document.document_id), 0), 2) AS percentage_answered
        FROM document
        LEFT JOIN document_body ON document.document_id = document_body.document_id
        LEFT JOIN individual_profile ON document_body.entity_id = individual_profile.entity_id
        LEFT JOIN document_answer_details ON document_body.document_body_id = document_answer_details.document_body_id
        LEFT JOIN document_template_question ON document_answer_details.document_template_question_id = document_template_question.document_template_question_id
        
    )
    SELECT
        individual_id,
        document_date,
        document_template_id,
        document_id,
        document_body_id,
        percentage_answered
    FROM document_questions_answered 
    WHERE doc_rank = 1
),
document_signature_check AS (
    SELECT 
        document_id,
        CASE
            WHEN COUNT(document_signature_date) = 0 THEN 'Not Signed'
            ELSE CONCAT(
                COUNT(document_signature_date), ' total: ', 
                COALESCE(
                    STRING_AGG(
                        -- Apply all REGEXP_REPLACE transformations for various replacements
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    document_signature_text, 
                                                    '(Parent/Guardian|Parent/Guardian or Personal Representative|Parent|Guardian|Padre/Guardián/Representante Personal|Padre/Guardián)', 
                                                    'P/G'
                                                ),
                                                'FYI Staff Making Report', 
                                                'Staff'
                                            ), 
                                            'Signature of Immediate Supervisor', 
                                            'Supervisor'
                                        ),
                                        'Signature', 
                                        ''
                                    ),
                                    'Firma del ', 
                                    ''
                                ),
                                '/Representante Personal', 
                                ''
                            ),
                            'Firma de ', 
                            ''
                        ),
                        ', ' ORDER BY document_signature_date ASC
                    ) 
                    FILTER (WHERE document_signature_date IS NOT NULL),
                    'Not Signed'
                )
            )
        END AS signature_status
    FROM document_signatures
    GROUP BY document_id
),
audit_documents AS (
    SELECT DISTINCT
        document.document_id,
        document.document_date,
        document.document_template_id,
        document_template_stage.document_template_stage_type,
        individual_profile.individual_id,
        individual_profile.individual_name,
        case_file_documents.case_file_id,
        service_file_individuals.service_file_id,
        document_signature_check.signature_status,
        document_completion_percent.percentage_answered
    FROM document
    LEFT JOIN document_body ON document.document_id = document_body.document_id
    LEFT JOIN case_file_documents ON document.document_id = case_file_documents.document_id
    LEFT JOIN individual_profile ON document_body.entity_id = individual_profile.entity_id
    LEFT JOIN service_file_individuals ON individual_profile.individual_id = service_file_individuals.individual_id
    LEFT JOIN service_file_documents ON document.document_id = service_file_documents.document_id
    LEFT JOIN document_signature_check ON document.document_id = document_signature_check.document_id
    LEFT JOIN document_completion_percent ON document.document_id = document_completion_percent.document_id
    LEFT JOIN document_template_stage ON document.document_template_stage_id = document_template_stage.document_template_stage_id

),
worker_supervisor AS (
    SELECT 
        worker_profile.worker_id,
        -- wp.worker_name,
        -- wp.worker_report_to_worker_id,
        supervisor.worker_name AS supervisor_name,
        worker_contacts.worker_contact AS supervisor_email

    FROM worker_profile
    LEFT JOIN worker_profile AS supervisor ON worker_profile.worker_report_to_worker_id = supervisor.worker_id
    LEFT JOIN worker_contacts ON worker_profile.worker_report_to_worker_id = worker_contacts.worker_id AND worker_contacts.contact_type = 'Personal Message Settings Email'
),
FC_active_case_load AS(
    SELECT DISTINCT
    service_file.case_file_id,
    service_file_profile.service_file_id,
    service_profile.service_name,
    service_file_profile.service_file_status,
    service_file_profile.service_file_start_date,
    worker_profile.worker_name,
    worker_contacts.worker_contact AS worker_email,
    service_file_workers.service_file_worker_is_primary,
    service_file_workers.service_file_worker_is_attending,
    worker_supervisor.supervisor_name,
    worker_supervisor.supervisor_email,
    service_file_individuals.individual_id

    FROM service_file_profile
        LEFT JOIN service_file ON service_file_profile.service_file_id = service_file.service_file_id 
        LEFT JOIN service_profile ON service_file_profile.service_id = service_profile.service_id
        LEFT JOIN service_file_workers ON service_file_profile.service_file_id = service_file_workers.service_file_id
        LEFT JOIN worker_profile ON service_file_workers.worker_id = worker_profile.worker_id 
        LEFT JOIN worker_contacts ON service_file_workers.worker_id = worker_contacts.worker_id AND worker_contacts.contact_type = 'Personal Message Settings Email'
        LEFT JOIN service_file_individuals ON service_file_profile.service_file_id = service_file_individuals.service_file_id   -- see where below, only using presenting 
        LEFT JOIN worker_supervisor ON worker_profile.worker_id  = worker_supervisor.worker_id 

    WHERE 
        --service_profile.service_name NOT IN ('CC Market')
        service_profile.service_name IN ('Family Counseling')
        AND worker_profile.worker_is_active = 'True'
        AND service_file_workers.service_file_worker_is_primary = 'True'
        AND service_file_individuals.service_file_individual_is_presenting = 'True'
        AND service_file_profile.service_file_status = 'Open'
),
indiv_profile_profile_answered_percent AS (
    SELECT 
        individual_profile.individual_id,
        (COUNT(*) FILTER (WHERE individual_profile.individual_gender IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_date_of_birth IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_language IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_record_drop_down IS NOT NULL) + -- Health Care Coverage
        COUNT(*) FILTER (WHERE individual_profile.partner_is_aware IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.home_is_aware IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_financial_concern_text IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_speaks_english IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_is_safety_concern IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_is_financial_concern IS NOT NULL)

        )::decimal / 10 * 100.0 AS answer_percentage   -- 10 questions 
    FROM individual_profile
    GROUP BY individual_id
),
indiv_profile_additional_info_answered_percent AS (
    SELECT 
        individual_profile.individual_id,
        (COUNT(*) FILTER (WHERE individual_profile.individual_text_1 IS NOT NULL) +-- Preferred Nickname
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_1 IS NOT NULL) +  -- Family Spanish Only
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_2 IS NOT NULL) +  -- Ethnicity
        COUNT(*) FILTER (WHERE individual_profile.individual_checkbox_1 IS NOT NULL) +  -- Referred by SOS
        COUNT(*) FILTER (WHERE individual_profile.individual_text_3 IS NOT NULL) +-- Guardian Name
        COUNT(*) FILTER (WHERE individual_profile.individual_text_4 IS NOT NULL) +-- Guardians Relation
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_3 IS NOT NULL) + -- Race
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_4 IS NOT NULL) + -- Education / Employment Concern
        COUNT(*) FILTER (WHERE individual_profile.individual_checkbox_3 IS NOT NULL) +  -- Subsidy Only
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_5 IS NOT NULL) + -- Victim of Crime
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_6 IS NOT NULL) +  -- Referred by WCJS
        COUNT(*) FILTER (WHERE individual_profile.individual_text_7 IS NOT NULL) +-- Who Filled Referral
        COUNT(*) FILTER (WHERE individual_profile.individual_text_8 IS NOT NULL) +  -- How Did You Find Us
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_7 IS NOT NULL) + -- Substance Use
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_8 IS NOT NULL) + -- Mental Health / Behavior
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_9 IS NOT NULL) + -- Housing
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_10 IS NOT NULL) + -- Suicidal / Homicidal
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_11 IS NOT NULL) + -- Basic Needs
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_12 IS NOT NULL) + -- Food Stability
        COUNT(*) FILTER (WHERE individual_profile.individual_memo_1 IS NOT NULL) + -- Current Concerns
        COUNT(*) FILTER (WHERE individual_profile.individual_memo_2 IS NOT NULL) -- HH Children & Ages
        )::decimal / 21 * 100.0 AS answer_percentage -- 21 questions 
    FROM individual_profile
    GROUP BY individual_id
),
service_file_member_count AS (
    SELECT
        service_file_individuals.service_file_id,
        MAX(CASE 
            WHEN service_file_individual_is_presenting IS TRUE 
            THEN service_file_individuals.individual_id 
            ELSE NULL 
        END) AS presenting_individual_id,
        COUNT(service_file_individuals.individual_id) AS total_service_file_individuals
    FROM service_file_individuals
    GROUP BY service_file_individuals.service_file_id
),
non_presenting_service_file_member_profile_completion AS (
    SELECT
        service_file_individuals.service_file_id,
        service_file_individuals.individual_id AS non_pres_individual_id,
        presenting_indiv.individual_id AS presenting_indiv_id,
        CASE 
            WHEN COUNT(individual_contacts.contact_type) > 0 THEN 'T'
            ELSE 'F'
        END AS has_contact_info,
        CASE
            WHEN 
                COUNT(*) FILTER (WHERE individual_address1 IS NOT NULL) +
                COUNT(*) FILTER (WHERE individual_city IS NOT NULL) +
                COUNT(*) FILTER (WHERE individual_province IS NOT NULL) +
                COUNT(*) FILTER (WHERE individual_zip_pcode IS NOT NULL) = 4 THEN 'T'
            WHEN 
                COUNT(*) FILTER (WHERE individual_address1 IS NOT NULL OR
                                 individual_city IS NOT NULL OR
                                 individual_province IS NOT NULL OR
                                 individual_zip_pcode IS NOT NULL) < 4 THEN 'Inc'
            ELSE 'F'
        END AS has_address_info,
        ROUND((COUNT(*) FILTER (WHERE individual_profile.individual_gender IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_date_of_birth IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_language IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_record_drop_down IS NOT NULL) + -- Health Care Coverage
        COUNT(*) FILTER (WHERE individual_profile.partner_is_aware IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.home_is_aware IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_financial_concern_text IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_speaks_english IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_is_safety_concern IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_is_financial_concern IS NOT NULL) +
        COUNT(*) FILTER (WHERE individual_profile.individual_text_1 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_1 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_2 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_checkbox_1 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_text_3 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_text_4 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_3 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_4 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_checkbox_3 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_5 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_6 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_text_7 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_text_8 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_7 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_8 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_9 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_10 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_11 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_drop_down_12 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_memo_1 IS NOT NULL) + 
        COUNT(*) FILTER (WHERE individual_profile.individual_memo_2 IS NOT NULL)
        )::decimal / 31 * 100.0, 0)::text || '%%' AS profile_completion_pcnt -- 31 questions

    FROM service_file_individuals
        LEFT JOIN (
            SELECT DISTINCT ON (individual_id)
                individual_id,
                contact_type -- or whatever column you want to use to prioritize the contacts
            FROM individual_contacts
        ) AS individual_contacts ON service_file_individuals.individual_id = individual_contacts.individual_id
        LEFT JOIN pw_view_ind_exp ON service_file_individuals.individual_id = pw_view_ind_exp.individual_id
        LEFT JOIN individual_profile ON service_file_individuals.individual_id = individual_profile.individual_id
        LEFT JOIN service_file_individuals AS presenting_indiv ON service_file_individuals.service_file_id = presenting_indiv.service_file_id  AND presenting_indiv.service_file_individual_is_presenting IS TRUE
    
    WHERE service_file_individuals.service_file_individual_is_presenting IS FALSE
    GROUP BY service_file_individuals.service_file_id, service_file_individuals.individual_id, presenting_indiv.individual_id
),
formatted_non_presenting_info AS (
    SELECT
        presenting_indiv_id,
        STRING_AGG(profile_info, ' - ') AS aggregated_info
    FROM (
        SELECT DISTINCT ON (presenting_indiv_id, non_pres_individual_id)
            presenting_indiv_id,
            '(' || profile_completion_pcnt::text || ' ' || has_contact_info || ' ' || has_address_info || ')' AS profile_info
        FROM non_presenting_service_file_member_profile_completion
        -- Potentially add WHERE conditions or additional logic to filter or prioritize rows
    ) AS PreAggregated
    GROUP BY presenting_indiv_id
),
individual_has_contact_info AS (
    SELECT
        individual.individual_id,
        COUNT(individual_contacts.contact_type) AS contact_type_count
    FROM individual
    LEFT JOIN individual_contacts ON individual.individual_id = individual_contacts.individual_id

    GROUP BY individual.individual_id
),
formatted_contact_types AS (
    SELECT
        individual_id,
        CASE 
            WHEN contact_type_count > 0 THEN 'True - ' || contact_type_count::text || ' type/s'
            ELSE NULL
        END AS indiv_has_contact_info
    FROM individual_has_contact_info
),
individual_has_address AS (
    SELECT
        individual_id,
        CASE
            WHEN 
                COUNT(*) FILTER (WHERE individual_address1 IS NOT NULL) +
                -- COUNT(*) FILTER (WHERE individual_address2 IS NOT NULL) +      -- most people don't have this...
                COUNT(*) FILTER (WHERE individual_city IS NOT NULL) +
                COUNT(*) FILTER (WHERE individual_province IS NOT NULL) +
                COUNT(*) FILTER (WHERE individual_zip_pcode IS NOT NULL) = 4 THEN 'True'
            WHEN 
                COUNT(*) FILTER (WHERE individual_address1 IS NOT NULL OR
                                 individual_address2 IS NOT NULL OR
                                 individual_city IS NOT NULL OR
                                 individual_province IS NOT NULL OR
                                 individual_zip_pcode IS NOT NULL) > 0 THEN 'Incomplete'
            ELSE 'False'
        END AS indiv_has_address
    FROM pw_view_ind_exp
    GROUP BY individual_id
),
indiv_has_collateral_contact AS (
SELECT 
individual_profile.individual_id,
COUNT(bluebook_profile.bluebook_name) AS collateral_contact_count

FROM individual_profile
    LEFT JOIN individual_bluebooks ON individual_profile.individual_id = individual_bluebooks.individual_id
    LEFT JOIN bluebook_profile ON individual_bluebooks.bluebook_id = bluebook_profile.bluebook_id

GROUP BY individual_profile.individual_id

),
non_presenting_individuals AS (
    SELECT 
        case_file_individuals.case_file_id,
        case_file_individuals.individual_id AS non_pres_individual_id,
        case_file_individuals.case_file_individual_relationship AS relation
    FROM 
        case_file_individuals
    WHERE 
        case_file_individuals.case_file_individual_relationship IN ('Parent', 'Guardian', 'Grandparent', 'Step Parent')
),
formatted_collateral_contacts AS (
    SELECT
        individual_id,
        CASE 
            WHEN collateral_contact_count > 0 THEN 'True - ' || collateral_contact_count::text || ' contact/s'
            ELSE NULL
        END AS indiv_has_collateral_contacts
    FROM indiv_has_collateral_contact
),
get_ROI_date AS (
    SELECT 
        document.document_id,
        document.document_date,
        document.document_template_id,
        document_body.document_body_id,
        case_file_documents.case_file_id AS case_file_id,
        individual_profile.individual_name AS client_name,
        individual_profile.individual_id AS individual_id,
        ROW_NUMBER() OVER (PARTITION BY case_file_documents.case_file_id ORDER BY document.document_date DESC) AS row_number
    FROM 
        document
    LEFT JOIN document_body ON document.document_id = document_body.document_id
    LEFT JOIN case_file_documents ON document.document_id = case_file_documents.document_id
    LEFT JOIN individual_profile ON document_body.entity_id = individual_profile.entity_id
    LEFT JOIN non_presenting_individuals ON document_body.entity_id = non_presenting_individuals.non_pres_individual_id
    WHERE 
        document.document_template_id IN (1000035, 1000084)
),
roi_info AS (
    SELECT 
        individual_id,
        document_date,
        roi_expiration.answer_date,
        CASE 
            WHEN roi_expiration.answer_date < CURRENT_DATE THEN 'YES - Expired on ' || TO_CHAR(roi_expiration.answer_date, 'MM/DD/YYYY')
            WHEN roi_expiration.answer_date IS NULL THEN 'NO ROI DATE SET'
            ELSE 'NO - Expires in ' || (roi_expiration.answer_date - CURRENT_DATE) || ' days'
        END AS roi_expired
    FROM 
        get_ROI_date
    LEFT JOIN 
        document_answer_date_profile AS roi_expiration ON get_ROI_date.document_body_id = roi_expiration.document_body_id 
        AND roi_expiration.document_template_question_id IN (1001613, 1005602)
    WHERE
        row_number = 1
),
attachments_pivot AS (
    SELECT
        attachkey::integer AS attachkey,
        STRING_AGG(attachname, ', ' ORDER BY slogin DESC) FILTER (WHERE attachentity = 'ind') AS individual_attachments,
        STRING_AGG(attachname, ', ' ORDER BY slogin DESC) FILTER (WHERE attachentity = 'case') AS case_file_attachments,
        STRING_AGG(attachname, ', ' ORDER BY slogin DESC) FILTER (WHERE attachentity = 'cprogprov') AS service_file_attachments
    FROM dtattachmod
    WHERE objectkey IS NOT NULL
      AND attachkey NOT IN ('template')
    GROUP BY attachkey
),

audit_docs_non_pres_indivs AS (
    SELECT * 
    FROM audit_documents
    WHERE individual_id IN (SELECT non_pres_individual_id FROM non_presenting_individuals)
    AND document_template_id IN (
        1000093, -- PG_Intake
        1000021, 1000023, -- ACE_Parent
        1000003  -- Food_Stability
    )
),
audit_docs_service AS (
    SELECT * 
    FROM audit_documents 
    WHERE service_file_id IS NOT NULL
    AND document_template_id IN (
        1000119, 1000079, -- Case_Plan_Life_Domains, Service_File_Completion_Summary
        1000033, 1000083, -- Informed Consent english, spanish
        1000016,          -- Comprehensive Eval
        1000017,          -- Diagnosis
        1000133, 1000046, -- Treatment Plan
        1000020,          -- CASII
        1000024,          -- LOCUS
        1000032,           -- AV consent
        1000003  -- Food_Stability

    )
),
audit_docs_indivs AS (
    SELECT * 
    FROM audit_documents 
    WHERE individual_id IS NOT NULL
    AND document_template_id IN (
        1000035, 1000084, -- Release_of_Info
        1000059, 1000060, -- Indemnification (Over_18, Under_18)
        1000004, 1000006, -- ACE_Under_18
        1000005, 1000007, -- ACE_Over_18
        1000010, 1000008, -- SBIRT_Drug, SBIRT_Alcohol
        1000065,          -- CSSRS
        1000113, 1000092,  -- Youth_Intake
        1000132,           -- Saftey_Plan
        1000003  -- Food_Stability
    )
),
audit_docs_case AS (
    SELECT * 
    FROM audit_documents 
    WHERE case_file_id IS NOT NULL
    AND document_template_id IN (
        1000118, -- CFC_Intake
        1000106, -- HH_Intake
        1000064, 1000076, -- Agreement_for_Services
        1000044 -- Abuse_and_Neglect_Report

    )
)
SELECT
    FC_active_case_load.worker_name,
    FC_active_case_load.worker_email,
    FC_active_case_load.supervisor_name,
    FC_active_case_load.supervisor_email,
    FC_active_case_load.case_file_id,
    FC_active_case_load.service_name,
    FC_active_case_load.service_file_id,
    FC_active_case_load.service_file_start_date,
    individual_profile.individual_id AS presenting_individual_id,
    individual_profile.individual_name AS presenting_individual_name,
    ROUND(indiv_profile_profile_answered_percent.answer_percentage, 1) AS individual_profile_pcnt,
    ROUND(indiv_profile_additional_info_answered_percent.answer_percentage, 1) AS indiv_profile_additional_info_pcnt,
    formatted_contact_types.indiv_has_contact_info,
    individual_has_address.indiv_has_address,
    formatted_collateral_contacts.indiv_has_collateral_contacts,
    service_file_member_count.total_service_file_individuals,
    formatted_non_presenting_info.aggregated_info AS other_case_members_profile_compltion_has_contact_has_address,
    MAX(CASE WHEN (audit_docs_case.document_template_id = 1000118 OR audit_docs_non_pres_indivs.document_template_id = 1000118) THEN audit_docs_case.document_date END) AS CCM_Intake,
    MAX(CASE WHEN (audit_docs_case.document_template_id = 1000118 OR audit_docs_non_pres_indivs.document_template_id = 1000118) THEN audit_docs_case.percentage_answered END) AS CCM_Intake_pcnt,
    MAX(CASE WHEN (audit_docs_indivs.document_template_id = 1000113 OR audit_docs_indivs.document_template_id = 1000092) THEN audit_docs_indivs.document_date END) AS Youth_Intake,
    MAX(CASE WHEN (audit_docs_indivs.document_template_id = 1000113 OR audit_docs_indivs.document_template_id = 1000092) THEN audit_docs_indivs.percentage_answered END) AS Youth_Intake_pcnt,
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000093 THEN audit_docs_case.document_date END) AS PG_Intake,
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000093 THEN audit_docs_case.percentage_answered END) AS PG_Intake_pcnt,
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000106, 1000094) THEN audit_docs_case.document_date END) AS Household_Intake,
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000106, 1000094) THEN audit_docs_case.percentage_answered END) AS Household_Intake_pcnt,
    --ROI
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_indivs.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_non_pres_indivs.document_date END)
    ) AS Release_of_Info,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_indivs.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_non_pres_indivs.percentage_answered END)
    ) AS Release_of_Info_pcnt,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_indivs.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000035, 1000084) THEN audit_docs_non_pres_indivs.signature_status END)
    ) AS Release_of_Info_Signature,
    roi_info.roi_expired,
    -- Indemnification_Over_18
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000059 THEN audit_docs_indivs.document_date END) AS Indemnification_Over_18,
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000059 THEN audit_docs_indivs.signature_status END) AS Indemnification_Over_18_Signature,
    -- Indemnification_Under_18
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000060 THEN audit_docs_indivs.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000060 THEN audit_docs_non_pres_indivs.document_date END)
    ) AS Indemnification_Under_18,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000060 THEN audit_docs_indivs.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000060 THEN audit_docs_non_pres_indivs.percentage_answered END)
    ) AS Indemnification_Under_18_pcnt,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000060 THEN audit_docs_indivs.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000060 THEN audit_docs_non_pres_indivs.signature_status END)
    ) AS Indemnification_Under_18_Signature,
    -- Agreement_for_Services
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000064,1000076) THEN audit_docs_case.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000064,1000076) THEN audit_docs_non_pres_indivs.document_date END)
    ) AS Agreement_for_Services,
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000064,1000076) THEN audit_docs_case.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000064,1000076) THEN audit_docs_non_pres_indivs.percentage_answered END)
    ) AS Agreement_for_Services_pcnt,
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000064,1000076) THEN audit_docs_case.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000064,1000076) THEN audit_docs_non_pres_indivs.signature_status END)
    ) AS Agreement_for_Services_Signature,
    -- ACE_Under_18
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_indivs.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_non_pres_indivs.document_date END)
    ) AS ACE_Under_18,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_indivs.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_non_pres_indivs.percentage_answered END)
    ) AS ACE_Under_18_pcnt,
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_indivs.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000004,1000006) THEN audit_docs_non_pres_indivs.signature_status END)
    ) AS ACE_Under_18_Signature,
    -- ACE_Over_18
    MAX(CASE WHEN (audit_docs_indivs.document_template_id = 1000005 OR audit_docs_indivs.document_template_id = 1000007) THEN audit_docs_indivs.document_date END) AS ACE_Over_18,
    MAX(CASE WHEN (audit_docs_indivs.document_template_id = 1000005 OR audit_docs_indivs.document_template_id = 1000007) THEN audit_docs_indivs.percentage_answered END) AS ACE_Over_18_pcnt,
    MAX(CASE WHEN (audit_docs_indivs.document_template_id = 1000005 OR audit_docs_indivs.document_template_id = 1000007) THEN audit_docs_indivs.signature_status END) AS ACE_Over_18_Signature, 
    -- ACE_Parent (following your snippet’s pattern of checking non_pres_indivs but retrieving columns from audit_docs_case)
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000021,1000023) THEN audit_docs_case.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000021,1000023) THEN audit_docs_case.document_date END)
    ) AS ACE_Parent,
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000021,1000023) THEN audit_docs_case.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000021,1000023) THEN audit_docs_case.percentage_answered END)
    ) AS ACE_Parent_pcnt,
    COALESCE(
    MAX(CASE WHEN audit_docs_case.document_template_id IN (1000021,1000023) THEN audit_docs_case.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id IN (1000021,1000023) THEN audit_docs_case.signature_status END)
    ) AS ACE_Parent_Signature,

    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000065 THEN audit_docs_indivs.document_date END) AS CSSRS,
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000065 THEN audit_docs_indivs.percentage_answered END) AS CSSRS_pcnt,
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000065 THEN audit_docs_indivs.signature_status END) AS CSSRS_Signature,

                                                      --english version                                --spanish version
    MAX(CASE WHEN (audit_docs_service.document_template_id = 1000033 OR audit_docs_service.document_template_id = 1000083) THEN audit_docs_service.document_date END) AS Informed_Consent,
    MAX(CASE WHEN (audit_docs_service.document_template_id = 1000033 OR audit_docs_service.document_template_id = 1000083) THEN audit_docs_service.percentage_answered END) AS Informed_Consent_pcnt,
    MAX(CASE WHEN (audit_docs_service.document_template_id = 1000033 OR audit_docs_service.document_template_id = 1000083) THEN audit_docs_service.signature_status END) AS Informed_Consent_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.document_date END) AS Comprehensive_Evaluation,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.percentage_answered END) AS Comprehensive_Evaluation_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.signature_status END) AS Comprehensive_Evaluation_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.document_date END) AS Diagnosis,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.percentage_answered END) AS Diagnosis_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000016 THEN audit_docs_service.signature_status END) AS Diagnosis_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000046 THEN audit_docs_service.document_date END) AS Treatment_Plan,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000046 THEN audit_docs_service.percentage_answered END) AS Treatment_Plan_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000046 THEN audit_docs_service.signature_status END) AS Treatment_Plan_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000020 THEN audit_docs_service.document_date END) AS CASII,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000020 THEN audit_docs_service.percentage_answered END) AS CASII_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000020 THEN audit_docs_service.signature_status END) AS CASII_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000024 THEN audit_docs_service.document_date END) AS LOCUS,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000024 THEN audit_docs_service.percentage_answered END) AS LOCUS_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000024 THEN audit_docs_service.signature_status END) AS LOCUS_Signature,

    -- Food Stability
    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000003 THEN audit_docs_service.document_date END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000003 THEN audit_docs_service.document_date END)
    ) AS Food_Stability,

    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000003 THEN audit_docs_service.document_template_stage_type END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000003 THEN audit_docs_service.document_template_stage_type END)
    ) AS Food_Stability_Stage,

    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000003 THEN audit_docs_service.percentage_answered END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000003 THEN audit_docs_service.percentage_answered END)
    ) AS Food_Stability_pcnt,

    COALESCE(
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000003 THEN audit_docs_service.signature_status END),
    MAX(CASE WHEN audit_docs_non_pres_indivs.document_template_id = 1000003 THEN audit_docs_service.signature_status END)
    ) AS Food_Stability_Signature,

    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000132 THEN audit_docs_indivs.document_date END) AS Saftey_Plan,
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000132 THEN audit_docs_indivs.percentage_answered END) AS Saftey_Plan_pcnt,
    MAX(CASE WHEN audit_docs_indivs.document_template_id = 1000132 THEN audit_docs_indivs.signature_status END) AS Saftey_Plan_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000032 THEN audit_docs_service.document_date END) AS Audio_Visual_Consent,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000032 THEN audit_docs_service.percentage_answered END) AS Audio_Visual_Consent_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000032 THEN audit_docs_service.signature_status END) AS Audio_Visual_Consent_Signature,

    MAX(CASE WHEN (audit_docs_case.document_template_id = 1000044 OR audit_docs_non_pres_indivs.document_template_id = 1000044) THEN audit_docs_case.document_date END) AS Abuse_and_Neglect_Report,
    MAX(CASE WHEN (audit_docs_case.document_template_id = 1000044 OR audit_docs_non_pres_indivs.document_template_id = 1000044) THEN audit_docs_case.percentage_answered END) AS Abuse_and_Neglect_Report_pcnt,
    MAX(CASE WHEN (audit_docs_case.document_template_id = 1000044 OR audit_docs_non_pres_indivs.document_template_id = 1000044) THEN audit_docs_case.signature_status END) AS Abuse_and_Neglect_Report_Signature,

    MAX(CASE WHEN audit_docs_service.document_template_id = 1000079 THEN audit_docs_service.document_date END) AS Service_File_Completion_Summary,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000079 THEN audit_docs_service.percentage_answered END) AS Service_File_Completion_Summary_pcnt,
    MAX(CASE WHEN audit_docs_service.document_template_id = 1000079 THEN audit_docs_service.signature_status END) AS Service_File_Completion_Summary_Signature,
    attachments_individual.individual_attachments,
    attachments_case.case_file_attachments,
    attachments_service.service_file_attachments,
    (
        SELECT STRING_AGG(sub.attachname, ', ' ORDER BY sub.attachname)
        FROM (
            SELECT DISTINCT dt.attachname
            FROM dtattachmod dt
            JOIN non_presenting_individuals npi 
                ON -- only join rows that are numeric AND not 'template'
                    dt.attachkey ~ '^\d+$'
                    AND npi.non_pres_individual_id = dt.attachkey::integer

            WHERE 
                npi.case_file_id = FC_active_case_load.case_file_id
                AND dt.attachentity = 'ind'
                -- If you prefer the old approach to filter out template,
                -- put it here or in the join condition:
                -- AND dt.attachkey <> 'template'
        ) sub
    ) AS non_pres_individual_attachments




FROM FC_active_case_load 
    LEFT JOIN audit_docs_service ON FC_active_case_load.service_file_id = audit_docs_service.service_file_id
    LEFT JOIN audit_docs_indivs ON FC_active_case_load.individual_id = audit_docs_indivs.individual_id
    LEFT JOIN audit_docs_case ON FC_active_case_load.case_file_id = audit_docs_case.case_file_id
    LEFT JOIN non_presenting_individuals ON FC_active_case_load.case_file_id = non_presenting_individuals.case_file_id
    LEFT JOIN audit_docs_non_pres_indivs ON non_presenting_individuals.non_pres_individual_id = audit_docs_non_pres_indivs.individual_id
    LEFT JOIN indiv_profile_profile_answered_percent ON FC_active_case_load.individual_id = indiv_profile_profile_answered_percent.individual_id
    LEFT JOIN indiv_profile_additional_info_answered_percent ON FC_active_case_load.individual_id = indiv_profile_additional_info_answered_percent.individual_id
    LEFT JOIN service_file_member_count ON FC_active_case_load.service_file_id = service_file_member_count.service_file_id
    LEFT JOIN individual_has_address ON FC_active_case_load.individual_id = individual_has_address.individual_id
    LEFT JOIN formatted_contact_types ON FC_active_case_load.individual_id = formatted_contact_types.individual_id
    LEFT JOIN formatted_collateral_contacts ON FC_active_case_load.individual_id = formatted_collateral_contacts.individual_id
    LEFT JOIN individual_profile ON FC_active_case_load.individual_id = individual_profile.individual_id
    LEFT JOIN formatted_non_presenting_info ON individual_profile.individual_id= formatted_non_presenting_info.presenting_indiv_id
    LEFT JOIN roi_info ON individual_profile.individual_id= roi_info.individual_id
    LEFT JOIN attachments_pivot AS attachments_individual ON attachments_individual.attachkey = individual_profile.individual_id
    LEFT JOIN attachments_pivot AS attachments_case ON attachments_case.attachkey = FC_active_case_load.case_file_id
    LEFT JOIN attachments_pivot AS attachments_service ON attachments_service.attachkey = FC_active_case_load.service_file_id


GROUP BY FC_active_case_load.worker_name,
         FC_active_case_load.worker_email,
         FC_active_case_load.supervisor_email,
         FC_active_case_load.supervisor_name,
         FC_active_case_load.service_name,
         FC_active_case_load.service_file_id,
         FC_active_case_load.service_file_start_date,
         individual_profile.individual_id,
         individual_profile.individual_name,
         FC_active_case_load.case_file_id,
         indiv_profile_profile_answered_percent.answer_percentage, 
         formatted_contact_types.indiv_has_contact_info,
         individual_has_address.indiv_has_address,
         formatted_collateral_contacts.indiv_has_collateral_contacts,
         indiv_profile_additional_info_answered_percent.answer_percentage,
         formatted_non_presenting_info.aggregated_info,
         service_file_member_count.total_service_file_individuals,
         roi_info.roi_expired,
         attachments_individual.individual_attachments,
         attachments_case.case_file_attachments,
         attachments_service.service_file_attachments


ORDER BY FC_active_case_load.worker_name ASC

'''