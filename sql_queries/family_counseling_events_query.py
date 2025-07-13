fc_events_columns = [
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
    'was_counseling_session',
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
    'cart_item_created_by'
]

fc_events_columns_numeric = [
    'case_file_id',
    'individual_id',
    'service_file_id',
    'event_id',
    'total_event_participants',
    'item_qty'
]

fc_events_columns_dates = [
    'event_start_timestamp',
    'case_note_date'
]

fc_events_query = r"""
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

    WHERE document.document_template_id = 1000045

),
fc_case_note_notes AS (
    SELECT DISTINCT
    case_notes.document_id,
    case_notes.document_date AS case_note_date,
    --case_notes.document_template_id,
    --case_notes.document_body_id,
    case_notes.case_file_id,
    case_notes.event_id,
    case_notes.individual_id,
    case_notes.client_name,
    was_counseling_session.answer_boolean AS was_counseling_session,
    CASE
        WHEN (REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%next session%%' 
            OR REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%next session%%')
        OR (REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%scheduled for%%' 
            OR REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%scheduled for%%')
        OR (REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%rescheduled%%' 
            OR REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%rescheduled%%')
        OR ((REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%close%%' AND (REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%case%%' OR REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%service%%'))
            OR (REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%close%%' AND (REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%case%%' OR REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%service%%')))
        OR (REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') ILIKE '%%send letter%%' 
            OR REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') ILIKE '%%send letter%%')
        OR (session_note_plan.answer_text ~* '\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b[\s/-]*\b\d{1,2}\s*,?\s*\b\d{4}\b' 
            OR case_note_note.answer_text ~* '\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b[\s/-]*\b\d{1,2}\s*,?\s*\b\d{4}\b')
        OR (session_note_plan.answer_text ~* '\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b'
            OR case_note_note.answer_text ~* '\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b')
        THEN TRUE 
        ELSE FALSE
    END AS next_session_scheduled,
    REGEXP_REPLACE(session_note_plan.answer_text, '<[^>]+>', '', 'g') AS session_note_plan_text,
    REGEXP_REPLACE(case_note_note.answer_text, '<[^>]+>', '', 'g') AS case_note_text,
    TRIM(LEADING ', ' FROM 
        CONCAT(
            CASE WHEN four_b_funding.answer_boolean IS TRUE THEN '4B' ELSE '' END,
            CASE WHEN doj_funding.answer_boolean IS TRUE THEN ', DOJ' ELSE '' END,
            CASE WHEN voca_funding.answer_boolean IS TRUE THEN ', VOCA' ELSE '' END,
            CASE WHEN title_xx_funding.answer_boolean IS TRUE THEN ', TITLE XX' ELSE '' END,
            CASE WHEN general_fc_funding.answer_boolean IS TRUE THEN ', GEN FC' ELSE '' END,
            CASE WHEN viraugh_funding.answer_boolean IS TRUE THEN ', VIRAUGH' ELSE '' END,
            CASE WHEN other_funding.answer_boolean IS TRUE THEN ', OTHER' ELSE '' END
        )
    ) AS funding_sources

    FROM case_notes
        LEFT JOIN document_answer_text_profile AS session_note_plan
            ON case_notes.document_body_id = session_note_plan.document_body_id 
            AND session_note_plan.document_template_question_id = 1001901
        LEFT JOIN document_answer_text_profile AS case_note_note
            ON case_notes.document_body_id = case_note_note.document_body_id 
            AND case_note_note.document_template_question_id = 1001894

        LEFT JOIN document_answer_boolean_profile AS was_counseling_session
            ON case_notes.document_body_id = was_counseling_session.document_body_id 
            AND was_counseling_session.document_template_question_id = 1001873

        LEFT JOIN document_answer_boolean_profile AS four_b_funding
            ON case_notes.document_body_id = four_b_funding.document_body_id 
            AND four_b_funding.document_template_question_id = 1002070
        LEFT JOIN document_answer_boolean_profile AS doj_funding
            ON case_notes.document_body_id = doj_funding.document_body_id 
            AND doj_funding.document_template_question_id = 1001902
        LEFT JOIN document_answer_boolean_profile AS voca_funding
            ON case_notes.document_body_id = voca_funding.document_body_id 
            AND voca_funding.document_template_question_id = 1001903
        LEFT JOIN document_answer_boolean_profile AS title_xx_funding
            ON case_notes.document_body_id = title_xx_funding.document_body_id 
            AND title_xx_funding.document_template_question_id = 1003726
        LEFT JOIN document_answer_boolean_profile AS general_fc_funding
            ON case_notes.document_body_id = general_fc_funding.document_body_id 
            AND general_fc_funding.document_template_question_id = 1006111
        LEFT JOIN document_answer_boolean_profile AS viraugh_funding
            ON case_notes.document_body_id = viraugh_funding.document_body_id 
            AND viraugh_funding.document_template_question_id = 1006112
        LEFT JOIN document_answer_boolean_profile AS other_funding
            ON case_notes.document_body_id = other_funding.document_body_id 
            AND other_funding.document_template_question_id = 1001904
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
        worker_profile.worker_id,
        worker_profile.worker_is_active,
        -- wp.worker_name,
        -- wp.worker_report_to_worker_id,
        supervisor.worker_name AS supervisor_name -- This is the name of the supervisor
    FROM worker_profile
    LEFT JOIN worker_profile supervisor ON worker_profile.worker_report_to_worker_id = supervisor.worker_id
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
    fc_case_note_notes.was_counseling_session,
    service_file_event_status,
    event_participant_count.total_event_participants,
    CASE 
        WHEN (event_documents.document_id IS NOT NULL AND document.document_template_id = 1000045) THEN TRUE
        ELSE FALSE
    END AS has_case_note,
    fc_case_note_notes.case_note_date,
    fc_case_note_notes.funding_sources AS case_note_funding,
    CASE 
        WHEN document_signatures.document_signature_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS case_note_has_signature,
    fc_case_note_notes.next_session_scheduled,
    -- fc_case_note_notes.session_note_plan_text,
    -- fc_case_note_notes.case_note_text,
    --event_notes,
    cart_item_booking_info.service_unit_class,
    cart_item_booking_info.item_name AS cart_item_name,
    cart_item_booking_info.item_qty,
    cart_item_booking_info.units,
    cart_item_booking_info.created_by AS cart_item_created_by

FROM std_service_file_event
    LEFT JOIN service_file_profile ON std_service_file_event.service_file_id = service_file_profile.service_file_id
    LEFT JOIN cart_item_booking_info ON std_service_file_event.event_id = cart_item_booking_info.event_id 
    LEFT JOIN event_documents ON std_service_file_event.event_id = event_documents.event_id 
    LEFT JOIN document ON event_documents.document_id = document.document_id
    LEFT JOIN document_signatures ON document.document_id = document_signatures.document_id
    LEFT JOIN fc_case_note_notes ON event_documents.event_id = fc_case_note_notes.event_id 
    LEFT JOIN event_participant_count ON fc_case_note_notes.event_id = event_participant_count.event_id 
    LEFT JOIN worker_supervisor ON std_service_file_event.event_primary_worker_id = worker_supervisor.worker_id
    LEFT JOIN worker_profile ON std_service_file_event.event_primary_worker_id = worker_profile.worker_id

WHERE service_name IN ('Family Counseling')
    AND worker_profile.worker_is_active IS TRUE
    AND service_file_profile.service_file_status = 'Open'
    AND event_start_timestamp BETWEEN (SELECT start_time FROM vars) AND (SELECT end_time FROM vars)


ORDER BY event_primary_worker_name, individual_name, event_start_timestamp DESC


"""