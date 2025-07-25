-- Вывести все активные сессии
SELECT usename, client_addr, client_port FROM pg_stat_activity;

-- !!!
-- Отключение всех активных сессий кроме текущей (для наладаки)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid();
-- !!!


SELECT *
from standup_raw.process_video;

INSERT INTO standup_raw.process_video (duration, view_count, comment_count, like_count, upload_date)
VALUES (553, 11450, 265118, 290, '20190419')
where video_id = MaVc3dqiEI4;