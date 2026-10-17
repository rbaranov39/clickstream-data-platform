-- Вход: день в {{ params.day }}.
-- Заменить его партицию в dds.session_rep из dds.session_stage_rep
-- на всём кластере.
ALTER TABLE dds.session_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dds.session_stage_rep;