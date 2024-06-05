-- View: public.locus_tour_brief

-- DROP MATERIALIZED VIEW IF EXISTS public.locus_tour_brief;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.locus_tour_brief
TABLESPACE pg_default
AS
 WITH task_level_details AS (
         SELECT locus_task_brief.tour_id,
            sum(
                CASE
                    WHEN locus_task_brief.status::text = 'COMPLETED'::text THEN locus_task_brief.cod_amount
                    ELSE 0::double precision
                END) AS expected_net_amount,
            sum(
                CASE
                    WHEN locus_task_brief.status::text = 'COMPLETED'::text THEN locus_task_brief.drift_completed_kms
                    ELSE 0::double precision
                END) AS total_completed_drift,
            sum(
                CASE
                    WHEN locus_task_brief.status::text = 'CANCELLED'::text THEN locus_task_brief.drift_cancelled_kms
                    ELSE 0::double precision
                END) AS total_cancelled_drift,
            min(locus_task_brief.task_start_time) AS tour_start_time,
            min(locus_task_brief.task_end_time) AS tour_end_time,
            min(locus_task_brief.dispatch_time) AS dispatch_time
           FROM locus_task_brief
          GROUP BY locus_task_brief.tour_id
        )
 SELECT DISTINCT ON (locus_tour.tour_id) locus_tour.tour_id,
    locus_tour.batch_id,
    locus_tour.actual_net_amount::double precision AS actual_net_amount,
    locus_tour.planned_tour_date::date AS tour_date,
    locus_tour.all_tasks::integer AS total_tasks,
    locus_tour.cancelled_tasks::integer AS cancelled_tasks,
    locus_tour.completed_tasks::integer AS completed_tasks,
    locus_tour.number_of_delayed_taskssla::integer AS number_of_delayed_tasks,
    locus_tour.ongoing_tasks::integer AS ongoing_tasks,
    task_level_details.expected_net_amount,
    locus_tour.live_duration::double precision / 60::double precision AS live_duration,
    locus_tour.planned_distance_kms::double precision / 1000::double precision AS planned_distance,
    locus_tour.planned_time_on_road::double precision / 60::double precision AS planned_time_on_road,
    task_level_details.total_completed_drift,
    task_level_details.total_cancelled_drift,
    locus_tour.travelled_distance_kms::double precision / 1000::double precision AS actual_distance,
    locus_tour.actual_time_on_road::double precision / 1000::double precision AS actual_time_on_road,
    locus_tour.rider_id,
    rider_details.rider_name,
    rider_details.phone AS rider_phone,
    locus_tour.rider_status,
    locus_tour.sequence_edited::integer AS sequence_edited,
    locus_tour.team_name AS lm_team_name,
    locus_tour.tour_name,
    locus_tour.tour_status,
    task_level_details.tour_start_time,
    task_level_details.tour_end_time,
    task_level_details.dispatch_time
   FROM application_db.locus_tour
     LEFT JOIN application_db.rider rider_details ON locus_tour.rider_id::text = rider_details.locus_rider_id
     LEFT JOIN task_level_details ON task_level_details.tour_id::text = locus_tour.tour_id::text
WITH DATA;

ALTER TABLE IF EXISTS public.locus_tour_brief
    OWNER TO postgres;

GRANT SELECT ON TABLE public.locus_tour_brief TO localeuser;
GRANT ALL ON TABLE public.locus_tour_brief TO postgres;

CREATE UNIQUE INDEX tour_id
    ON public.locus_tour_brief USING btree
    (tour_id COLLATE pg_catalog."default")
    TABLESPACE pg_default;