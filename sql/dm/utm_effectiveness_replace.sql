ALTER TABLE dm.utm_effectiveness_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.utm_effectiveness_stage_rep;