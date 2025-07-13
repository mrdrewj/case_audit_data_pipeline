cm_events_columns = [
    'case_file_id',
    'case_file_name',
    'individual_id',
    'individual_name',
    'service_file_id',
    'service_file_status',
    'service_name',
    'supervisor_name',
    'worker_name',
    'event_id',
    'event_start_timestamp',
    'service_file_event_type',
    'event_name',
    'was_case_management_session',
    'service_file_event_status',
    'total_event_participants',
    'has_case_note',
    'case_note_date',
    'case_note_funding',
    'case_note_has_signature',
    'next_session_scheduled',
    'service_unit_class',
    'cart_item_name',
    'item_qty',
    'units',
    'cart_item_created_by',
    'number_referrals_given',
    'referral_1',
    'referral_2',
    'referral_3',
    'referral_4',
    'referral_5',
    'referral_6',
    'referral_7',
    'referral_8'
]

cm_events_columns_numeric = [
    'case_file_id',
    'individual_id',
    'service_file_id',
    'event_id',
    'total_event_participants',
    'item_qty',
    'number_referrals_given'
]

cm_events_columns_dates = [
    'event_start_timestamp',
    'case_note_date'
]

cm_events_query = r"""

WITH
vars AS (
    SELECT
        %s::timestamp AS start_time,
        %s::timestamp AS end_time
),
case_notes AS (
    SELECT 
    document.document_id,
    document.document_date,
    document.document_template_id,
    document_body.document_body_id,
    case_file_individuals.case_file_id AS case_file_id,
    event_documents.event_id,
    individual_profile.individual_name AS client_name,
    individual_profile.individual_id AS individual_id

    FROM document
    LEFT JOIN document_body ON document.document_id = document_body.document_id
    LEFT JOIN individual_profile ON document_body.entity_id = individual_profile.entity_id
    LEFT JOIN case_file_individuals ON individual_profile.individual_id = case_file_individuals.individual_id
    LEFT JOIN event_documents ON document.document_id = event_documents.document_id

    WHERE document.document_template_id IN (1000117, 1000052)

),
cm_case_note_notes AS (
    SELECT DISTINCT
    case_notes.document_id,
    case_notes.document_date AS case_note_date,
    --case_notes.document_template_id,
    --case_notes.document_body_id,
    case_notes.case_file_id,
    case_notes.event_id,
    case_notes.individual_id,
    case_notes.client_name,
    was_case_management_session.answer_boolean AS was_case_management_session,
    CASE
        WHEN REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%next session%%'
            OR REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%scheduled for%%'
            OR REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%rescheduled%%'
            OR (REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%close%%' AND (REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%case%%' OR REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%service%%'))
            OR REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%send letter%%'
            OR REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%meet%%' --AND (REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%next%%'))
            OR (REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%next%%' AND (REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%week%%'))
            OR case_note_plan.answer_text ~* '\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b[\s/-]*\b\d{1,2}\s*,?\s*\b\d{4}\b'
            OR case_note_plan.answer_text ~* '\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b'
        THEN TRUE 
        ELSE FALSE
    END AS next_session_scheduled,
    REGEXP_REPLACE(case_note_plan.answer_text, '<[^>]+>', '', 'g') AS case_note_text,
    TRIM(LEADING ', ' FROM 
        CONCAT(
            CASE WHEN twenty_first_century_funding.answer_boolean IS TRUE THEN '21st Cent.' ELSE '' END,
            CASE WHEN bcg_funding.answer_boolean IS TRUE THEN ', BCG' ELSE '' END,
            CASE WHEN cc_market_funding.answer_boolean IS TRUE THEN ', CC Market' ELSE '' END,
            CASE WHEN doj_funding.answer_boolean IS TRUE THEN ', DOJ' ELSE '' END,
            CASE WHEN etv_funding.answer_boolean IS TRUE THEN ', ETV' ELSE '' END,
            CASE WHEN general_cm_funding.answer_boolean IS TRUE THEN ', GEN CM' ELSE '' END,
            CASE WHEN hippy_pat_funding.answer_boolean IS TRUE THEN ', HIPPY/PAT' ELSE '' END,
            CASE WHEN redfield_funding.answer_boolean IS TRUE THEN ', Redfield' ELSE '' END,
            CASE WHEN safe_at_home_funding.answer_boolean IS TRUE THEN ', Safe@Home' ELSE '' END,
            CASE WHEN youthbuild_funding.answer_boolean IS TRUE THEN ', YouthBuild' ELSE '' END,
            CASE WHEN voca_funding.answer_boolean IS TRUE THEN ', VOCA' ELSE '' END,
            CASE WHEN wcjs_funding.answer_boolean IS TRUE THEN ', WCJS' ELSE '' END,
            CASE WHEN other_funding.answer_boolean IS TRUE THEN ', Other' ELSE '' END
        )
    ) AS funding_sources

    FROM case_notes
        LEFT JOIN document_answer_text_profile AS case_note_plan
            ON case_notes.document_body_id = case_note_plan.document_body_id 
            AND case_note_plan.document_template_question_id IN (1002112)

        LEFT JOIN document_answer_boolean_profile AS was_case_management_session
            ON case_notes.document_body_id = was_case_management_session.document_body_id 
            AND was_case_management_session.document_template_question_id IN (1009387)

        LEFT JOIN document_answer_boolean_profile AS twenty_first_century_funding
            ON case_notes.document_body_id = twenty_first_century_funding.document_body_id 
            AND twenty_first_century_funding.document_template_question_id IN (1008663)

        LEFT JOIN document_answer_boolean_profile AS bcg_funding
            ON case_notes.document_body_id = bcg_funding.document_body_id 
            AND bcg_funding.document_template_question_id IN (1002100)

        LEFT JOIN document_answer_boolean_profile AS cc_market_funding
            ON case_notes.document_body_id = cc_market_funding.document_body_id 
            AND cc_market_funding.document_template_question_id IN (1006057)

        LEFT JOIN document_answer_boolean_profile AS etv_funding
            ON case_notes.document_body_id = etv_funding.document_body_id 
            AND etv_funding.document_template_question_id IN (1002746)

        LEFT JOIN document_answer_boolean_profile AS general_cm_funding
            ON case_notes.document_body_id = general_cm_funding.document_body_id 
            AND general_cm_funding.document_template_question_id IN (1002747)

        LEFT JOIN document_answer_boolean_profile AS hippy_pat_funding
            ON case_notes.document_body_id = hippy_pat_funding.document_body_id 
            AND hippy_pat_funding.document_template_question_id IN (1002749)

        LEFT JOIN document_answer_boolean_profile AS redfield_funding
            ON case_notes.document_body_id = redfield_funding.document_body_id 
            AND redfield_funding.document_template_question_id IN (1002750)


        LEFT JOIN document_answer_boolean_profile AS doj_funding
            ON case_notes.document_body_id = doj_funding.document_body_id     -- From Older CM Case Note
            AND doj_funding.document_template_question_id IN (1002103)

        LEFT JOIN document_answer_boolean_profile AS tip_funding
            ON case_notes.document_body_id = tip_funding.document_body_id    -- From Older CM Case Note
            AND tip_funding.document_template_question_id IN (1006090)

        LEFT JOIN document_answer_boolean_profile AS safe_at_home_funding
            ON case_notes.document_body_id = safe_at_home_funding.document_body_id 
            AND safe_at_home_funding.document_template_question_id IN (1002751)

        LEFT JOIN document_answer_boolean_profile AS youthbuild_funding
            ON case_notes.document_body_id = youthbuild_funding.document_body_id 
            AND youthbuild_funding.document_template_question_id IN (1012693, 1002753)

        LEFT JOIN document_answer_boolean_profile AS voca_funding
            ON case_notes.document_body_id = voca_funding.document_body_id 
            AND voca_funding.document_template_question_id IN (1002104)

        LEFT JOIN document_answer_boolean_profile AS wcjs_funding
            ON case_notes.document_body_id = wcjs_funding.document_body_id 
            AND wcjs_funding.document_template_question_id IN (1002105)

        LEFT JOIN document_answer_boolean_profile AS other_funding
            ON case_notes.document_body_id = other_funding.document_body_id 
            AND other_funding.document_template_question_id IN (1002106)


),
cart_item_booking_info AS (
    SELECT 
    kactid as event_id,
    itemname as item_name,
    service_unit_profile.service_unit_class,
    lineqty as item_qty,
    unit_of_measure.unit_of_measure_name AS units,
    slogin AS cart_event_timestamp,
    slogmod AS cart_modify_timestamp, 
    sloginby AS created_by,
    slogmodby AS modified_by

    FROM etactline
        LEFT JOIN service_unit_profile ON etactline.kitemid = service_unit_profile.service_unit_id
        LEFT JOIN unit_of_measure ON service_unit_profile.unit_of_measure_id = unit_of_measure.unit_of_measure_id
),
event_participant_count AS (
    SELECT
        event_attendees.event_id,
        COUNT(event_attendees.entity_id) AS total_event_participants
    FROM event_attendees
    
    WHERE event_attendees.event_attendee_is_primary_worker IS FALSE
    
    GROUP BY event_attendees.event_id
),
worker_supervisor AS (
    SELECT 
        wp.worker_id,
        -- wp.worker_name,
        -- wp.worker_report_to_worker_id,
        supervisor.worker_name AS supervisor_name -- This is the name of the supervisor
    FROM worker_profile wp
    LEFT JOIN worker_profile supervisor ON wp.worker_report_to_worker_id = supervisor.worker_id
),
CM_referrals AS(
SELECT DISTINCT
--case_notes.document_id,
case_notes.document_date AS document_date,
--case_notes.document_template_id,
--case_notes.document_body_id,
case_notes.case_file_id,
case_notes.event_id,
case_notes.individual_id,
case_notes.client_name,
received_referral.answer_boolean AS received_referral,
num_referrals.answer_list AS num_referrals,
COALESCE(
    NULLIF(TRIM(referral_1_1.answer_text), ''),
    NULLIF(TRIM(referral_2_1.answer_text), ''),
    NULLIF(TRIM(referral_3_1.answer_text), ''),
    NULLIF(TRIM(referral_4_1.answer_text), ''),
    NULLIF(TRIM(referral_5_1.answer_text), ''),
    NULLIF(TRIM(referral_6_1.answer_text), ''),
    NULLIF(TRIM(referral_7_1.answer_text), ''),
    NULLIF(TRIM(referral_8_1.answer_text), '')
) AS ref_1,

COALESCE(
    NULLIF(TRIM(referral_2_2.answer_text), ''),
    NULLIF(TRIM(referral_3_2.answer_text), ''),
    NULLIF(TRIM(referral_4_2.answer_text), ''),
    NULLIF(TRIM(referral_5_2.answer_text), ''),
    NULLIF(TRIM(referral_6_2.answer_text), ''),
    NULLIF(TRIM(referral_7_2.answer_text), ''),
    NULLIF(TRIM(referral_8_2.answer_text), '')
) AS ref_2,

COALESCE(
    NULLIF(TRIM(referral_3_3.answer_text), ''),
    NULLIF(TRIM(referral_4_3.answer_text), ''),
    NULLIF(TRIM(referral_5_3.answer_text), ''),
    NULLIF(TRIM(referral_6_3.answer_text), ''),
    NULLIF(TRIM(referral_7_3.answer_text), ''),
    NULLIF(TRIM(referral_8_3.answer_text), '')
) AS ref_3,

COALESCE(
    NULLIF(TRIM(referral_4_4.answer_text), ''),
    NULLIF(TRIM(referral_5_4.answer_text), ''),
    NULLIF(TRIM(referral_6_4.answer_text), ''),
    NULLIF(TRIM(referral_7_4.answer_text), ''),
    NULLIF(TRIM(referral_8_4.answer_text), '')
) AS ref_4,

COALESCE(
    NULLIF(TRIM(referral_5_5.answer_text), ''),
    NULLIF(TRIM(referral_6_5.answer_text), ''),
    NULLIF(TRIM(referral_7_5.answer_text), ''),
    NULLIF(TRIM(referral_8_5.answer_text), '')
) AS ref_5,

COALESCE(
    NULLIF(TRIM(referral_6_6.answer_text), ''),
    NULLIF(TRIM(referral_7_6.answer_text), ''),
    NULLIF(TRIM(referral_8_6.answer_text), '')
) AS ref_6,

COALESCE(
    NULLIF(TRIM(referral_7_7.answer_text), ''),
    NULLIF(TRIM(referral_8_7.answer_text), '')
) AS ref_7,

COALESCE(
    NULLIF(TRIM(referral_8_8.answer_text), '')
) AS ref_8


FROM case_notes
    LEFT JOIN document_answer_boolean_profile AS received_referral ON case_notes.document_body_id = received_referral.document_body_id 
            AND received_referral.document_template_question_id = 1008364
    LEFT JOIN document_answer_list_profile AS num_referrals ON case_notes.document_body_id = num_referrals.document_body_id 
        AND num_referrals.document_template_question_id = 1008419

LEFT JOIN document_answer_text_profile AS referral_1_1 
    ON case_notes.document_body_id = referral_1_1.document_body_id 
    AND referral_1_1.document_template_question_id = 1008365

LEFT JOIN document_answer_text_profile AS referral_2_1 
    ON case_notes.document_body_id = referral_2_1.document_body_id 
    AND referral_2_1.document_template_question_id = 1008420
LEFT JOIN document_answer_text_profile AS referral_2_2 
    ON case_notes.document_body_id = referral_2_2.document_body_id 
    AND referral_2_2.document_template_question_id = 1008421

LEFT JOIN document_answer_text_profile AS referral_3_1 
    ON case_notes.document_body_id = referral_3_1.document_body_id 
    AND referral_3_1.document_template_question_id = 1008422
LEFT JOIN document_answer_text_profile AS referral_3_2 
    ON case_notes.document_body_id = referral_3_2.document_body_id 
    AND referral_3_2.document_template_question_id = 1008433
LEFT JOIN document_answer_text_profile AS referral_3_3 
    ON case_notes.document_body_id = referral_3_3.document_body_id 
    AND referral_3_3.document_template_question_id = 1008434

LEFT JOIN document_answer_text_profile AS referral_4_1 
    ON case_notes.document_body_id = referral_4_1.document_body_id 
    AND referral_4_1.document_template_question_id = 1008435
LEFT JOIN document_answer_text_profile AS referral_4_2 
    ON case_notes.document_body_id = referral_4_2.document_body_id 
    AND referral_4_2.document_template_question_id = 1008436
LEFT JOIN document_answer_text_profile AS referral_4_3 
    ON case_notes.document_body_id = referral_4_3.document_body_id 
    AND referral_4_3.document_template_question_id = 1008437
LEFT JOIN document_answer_text_profile AS referral_4_4 
    ON case_notes.document_body_id = referral_4_4.document_body_id 
    AND referral_4_4.document_template_question_id = 1008438

LEFT JOIN document_answer_text_profile AS referral_5_1 
    ON case_notes.document_body_id = referral_5_1.document_body_id 
    AND referral_5_1.document_template_question_id = 1008439
LEFT JOIN document_answer_text_profile AS referral_5_2 
    ON case_notes.document_body_id = referral_5_2.document_body_id 
    AND referral_5_2.document_template_question_id = 1008440
LEFT JOIN document_answer_text_profile AS referral_5_3 
    ON case_notes.document_body_id = referral_5_3.document_body_id 
    AND referral_5_3.document_template_question_id = 1008441
LEFT JOIN document_answer_text_profile AS referral_5_4 
    ON case_notes.document_body_id = referral_5_4.document_body_id 
    AND referral_5_4.document_template_question_id = 1008442
LEFT JOIN document_answer_text_profile AS referral_5_5 
    ON case_notes.document_body_id = referral_5_5.document_body_id 
    AND referral_5_5.document_template_question_id = 1008443

LEFT JOIN document_answer_text_profile AS referral_6_1 
    ON case_notes.document_body_id = referral_6_1.document_body_id 
    AND referral_6_1.document_template_question_id = 1008444
LEFT JOIN document_answer_text_profile AS referral_6_2 
    ON case_notes.document_body_id = referral_6_2.document_body_id 
    AND referral_6_2.document_template_question_id = 1008445
LEFT JOIN document_answer_text_profile AS referral_6_3 
    ON case_notes.document_body_id = referral_6_3.document_body_id 
    AND referral_6_3.document_template_question_id = 1008446
LEFT JOIN document_answer_text_profile AS referral_6_4 
    ON case_notes.document_body_id = referral_6_4.document_body_id 
    AND referral_6_4.document_template_question_id = 1008447
LEFT JOIN document_answer_text_profile AS referral_6_5 
    ON case_notes.document_body_id = referral_6_5.document_body_id 
    AND referral_6_5.document_template_question_id = 1008448
LEFT JOIN document_answer_text_profile AS referral_6_6 
    ON case_notes.document_body_id = referral_6_6.document_body_id 
    AND referral_6_6.document_template_question_id = 1008449

LEFT JOIN document_answer_text_profile AS referral_7_1 
    ON case_notes.document_body_id = referral_7_1.document_body_id 
    AND referral_7_1.document_template_question_id = 1008450
LEFT JOIN document_answer_text_profile AS referral_7_2 
    ON case_notes.document_body_id = referral_7_2.document_body_id 
    AND referral_7_2.document_template_question_id = 1008451
LEFT JOIN document_answer_text_profile AS referral_7_3 
    ON case_notes.document_body_id = referral_7_3.document_body_id 
    AND referral_7_3.document_template_question_id = 1008452
LEFT JOIN document_answer_text_profile AS referral_7_4 
    ON case_notes.document_body_id = referral_7_4.document_body_id 
    AND referral_7_4.document_template_question_id = 1008453
LEFT JOIN document_answer_text_profile AS referral_7_5 
    ON case_notes.document_body_id = referral_7_5.document_body_id 
    AND referral_7_5.document_template_question_id = 1008454
LEFT JOIN document_answer_text_profile AS referral_7_6 
    ON case_notes.document_body_id = referral_7_6.document_body_id 
    AND referral_7_6.document_template_question_id = 1008455
LEFT JOIN document_answer_text_profile AS referral_7_7 
    ON case_notes.document_body_id = referral_7_7.document_body_id 
    AND referral_7_7.document_template_question_id = 1008456

LEFT JOIN document_answer_text_profile AS referral_8_1 
    ON case_notes.document_body_id = referral_8_1.document_body_id 
    AND referral_8_1.document_template_question_id = 1008457
LEFT JOIN document_answer_text_profile AS referral_8_2 
    ON case_notes.document_body_id = referral_8_2.document_body_id 
    AND referral_8_2.document_template_question_id = 1008458
LEFT JOIN document_answer_text_profile AS referral_8_3 
    ON case_notes.document_body_id = referral_8_3.document_body_id 
    AND referral_8_3.document_template_question_id = 1008459
LEFT JOIN document_answer_text_profile AS referral_8_4 
    ON case_notes.document_body_id = referral_8_4.document_body_id 
    AND referral_8_4.document_template_question_id = 1008460
LEFT JOIN document_answer_text_profile AS referral_8_5 
    ON case_notes.document_body_id = referral_8_5.document_body_id 
    AND referral_8_5.document_template_question_id = 1008461
LEFT JOIN document_answer_text_profile AS referral_8_6 
    ON case_notes.document_body_id = referral_8_6.document_body_id 
    AND referral_8_6.document_template_question_id = 1008462
LEFT JOIN document_answer_text_profile AS referral_8_7 
    ON case_notes.document_body_id = referral_8_7.document_body_id 
    AND referral_8_7.document_template_question_id = 1008463
LEFT JOIN document_answer_text_profile AS referral_8_8 
    ON case_notes.document_body_id = referral_8_8.document_body_id 
    AND referral_8_8.document_template_question_id = 1008464


    WHERE received_referral.answer_boolean = 'True'
)

SELECT
    std_service_file_event.case_file_id,
    std_service_file_event.case_file_name,
    service_file_presenting_individual_id AS individual_id,
    service_file_presenting_individual_name AS individual_name,
    std_service_file_event.service_file_id,
    service_file_profile.service_file_status,
    service_name,
    worker_supervisor.supervisor_name,
    -- event_primary_worker_id,
    event_primary_worker_name AS worker_name,
    -- event_primary_worker_entity_id,
    std_service_file_event.event_id,
    event_start_timestamp,
    service_file_event_type,
    event_name,
    cm_case_note_notes.was_case_management_session,
    service_file_event_status,
    event_participant_count.total_event_participants,
    -- document.document_template_id,
    CASE 
        WHEN (event_documents.document_id IS NOT NULL AND document.document_template_id IN (1000117, 1000052)) THEN TRUE
        ELSE FALSE
    END AS has_case_note,
    cm_case_note_notes.case_note_date,
    cm_case_note_notes.funding_sources AS case_note_funding,
    CASE 
        WHEN document_signatures.document_signature_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS case_note_has_signature,
    cm_case_note_notes.next_session_scheduled,
    -- cm_case_note_notes.session_note_text,
    -- cm_case_note_notes.case_note_text,
    --event_notes,
    cart_item_booking_info.service_unit_class,
    cart_item_booking_info.item_name AS cart_item_name,
    cart_item_booking_info.item_qty,
    cart_item_booking_info.units,
    cart_item_booking_info.created_by AS cart_item_created_by,
    CM_referrals.num_referrals AS number_referrals_given,
    CM_referrals.ref_1 AS referral_1,
    CM_referrals.ref_2 AS referral_2,
    CM_referrals.ref_3 AS referral_3,
    CM_referrals.ref_4 AS referral_4,
    CM_referrals.ref_5 AS referral_5,
    CM_referrals.ref_6 AS referral_6,
    CM_referrals.ref_7 AS referral_7,
    CM_referrals.ref_8 AS referral_8


FROM std_service_file_event
    LEFT JOIN service_file_profile ON std_service_file_event.service_file_id = service_file_profile.service_file_id
    LEFT JOIN cart_item_booking_info ON std_service_file_event.event_id = cart_item_booking_info.event_id 
    LEFT JOIN event_documents ON std_service_file_event.event_id = event_documents.event_id 
    LEFT JOIN document ON event_documents.document_id = document.document_id
    LEFT JOIN document_signatures ON document.document_id = document_signatures.document_id
    LEFT JOIN cm_case_note_notes ON event_documents.event_id = cm_case_note_notes.event_id 
    LEFT JOIN event_participant_count ON cm_case_note_notes.event_id = event_participant_count.event_id 
    LEFT JOIN worker_supervisor ON std_service_file_event.event_primary_worker_id = worker_supervisor.worker_id
    LEFT JOIN worker_profile ON std_service_file_event.event_primary_worker_id = worker_profile.worker_id
    LEFT JOIN CM_referrals ON std_service_file_event.event_id = CM_referrals.event_id

WHERE service_name NOT IN ('CC Market', 'Family Counseling')
    AND service_file_profile.service_file_status = 'Open'
    AND worker_profile.worker_is_active IS TRUE
    AND event_start_timestamp BETWEEN (SELECT start_time FROM vars) AND (SELECT end_time FROM vars)
    -- AND std_service_file_event.service_file_id = 5018  -- FOR TESTING ONLY

ORDER BY event_primary_worker_name, service_file_id, event_start_timestamp DESC


"""