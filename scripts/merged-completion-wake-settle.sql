-- Settles the completion wakes that 0.1.9-perf.9 merged across parent runs, so a build
-- before 0.1.9-perf.9, which reads only a wake's own parent run, delivers every result exactly once.
-- Stop the server first; run it once before starting the older build. It changes nothing on a
-- database without merged wakes, and running it again changes nothing. See
-- docs/operations/merged-completion-wake-rollback.md.
BEGIN IMMEDIATE;

-- Every message whose wake carries other parent runs' results, and whether its run delivered:
-- still its run's input (a steering restart settles the wake it replaces), and a turn that ended
-- normally. A checkpoint rollback rewrites failed and cancelled runs too, so a rolled-back one
-- counts only if its attempt completed. The doc's table says why nothing else is proof.
CREATE TEMP TABLE merged_wake AS
SELECT wake.message_id, wake.wake_run_id, wake.wake_status, wake.current, wake.delivered,
  -- The older build can cancel a waiting run at its start, and would then send that run's own
  -- results again, so a waiting one settles its own parent run here too.
  wake.delivered AND wake.wake_status = 'waiting' AS settles_own
FROM (
  SELECT message.message_id, message.run_id AS wake_run_id, run.status AS wake_status,
    json_extract(run.payload_json, '$.userMessageId') IS message.message_id AS current,
    json_extract(run.payload_json, '$.userMessageId') IS message.message_id
      AND json_extract(run.payload_json, '$.startedAt') IS NOT NULL
      AND (run.status IN ('completed', 'waiting') OR run.status = 'rolled_back' AND EXISTS (
        SELECT 1 FROM orchestration_v2_projection_run_attempts AS attempt
        WHERE attempt.attempt_id = json_extract(run.payload_json, '$.activeAttemptId')
          AND attempt.status = 'completed'
      )) AS delivered
  FROM orchestration_v2_projection_messages AS message
  JOIN orchestration_v2_projection_runs AS run ON run.run_id = message.run_id
  WHERE json_type(message.payload_json, '$.delegatedCompletion.merged') = 'array'
    AND json_array_length(message.payload_json, '$.delegatedCompletion.merged') > 0
) AS wake;

-- The parent runs whose delivery each wake still holds (own part first, then the merged ones).
CREATE TEMP TABLE merged_part AS
SELECT part.message_id, part.wake_run_id, part.delivered,
  part.own AND NOT part.settles_own AS own, part.parent_run_id
FROM (
  SELECT wake.message_id, wake.wake_run_id, wake.delivered, wake.settles_own, 1 AS own,
    json_extract(message.payload_json, '$.delegatedCompletion.parentRunId') AS parent_run_id,
    json_extract(message.payload_json, '$.delegatedCompletion.generation') AS generation
  FROM merged_wake AS wake
  JOIN orchestration_v2_projection_messages AS message ON message.message_id = wake.message_id
  UNION ALL
  SELECT wake.message_id, wake.wake_run_id, wake.delivered, wake.settles_own, 0,
    json_extract(merged.value, '$.parentRunId'), json_extract(merged.value, '$.generation')
  FROM merged_wake AS wake
  JOIN orchestration_v2_projection_messages AS message ON message.message_id = wake.message_id,
    json_each(message.payload_json, '$.delegatedCompletion.merged') AS merged
) AS part
JOIN orchestration_v2_projection_runs AS parent ON parent.run_id = part.parent_run_id
WHERE json_extract(parent.payload_json, '$.delegatedCompletion.disposition') = 'open'
  AND json_extract(parent.payload_json, '$.delegatedCompletion.delivery.messageId') = part.message_id
  AND json_extract(parent.payload_json, '$.delegatedCompletion.delivery.generation') = part.generation;

-- A delivered wake: each merged parent run settles as 0.1.9-perf.9's finalize would. Its claimed results
-- are delivered; its results still waiting go to one new wake, which the older build sends at start.
-- The wake's own parent run is the older build's to settle, unless the run is waiting.
CREATE TEMP TABLE delivered_task AS
SELECT task.subagent_id
FROM merged_part AS part
JOIN orchestration_v2_projection_runs AS parent ON parent.run_id = part.parent_run_id,
  json_each(parent.payload_json, '$.delegatedCompletion.delivery.taskIds') AS listed
JOIN orchestration_v2_projection_subagents AS task
  ON task.subagent_id = listed.value AND task.run_id = part.parent_run_id
WHERE part.delivered AND NOT part.own AND task.completion_delivery_state = 'claimed';

-- Every result of a parent run that still waits for a wake, as 0.1.9-perf.9's finalize reserves them.
CREATE TEMP TABLE waiting_task AS
SELECT part.parent_run_id, task.subagent_id
FROM merged_part AS part
JOIN orchestration_v2_projection_subagents AS task ON task.run_id = part.parent_run_id
WHERE task.origin = 'app_owned'
  AND task.completion_delivery_state = 'pending'
  AND task.status IN ('completed', 'failed', 'cancelled', 'interrupted')
  AND (part.delivered = 0 OR part.own = 0);

-- A wake that did not deliver: each parent run it held gets its own new wake with the same results
-- (and any still waiting), which the older build sends at start; the wake itself is cancelled.
CREATE TEMP TABLE next_delivery AS
SELECT part.parent_run_id,
  CASE WHEN part.delivered
    THEN json_extract(parent.payload_json, '$.delegatedCompletion.nextGeneration')
    ELSE json_extract(parent.payload_json, '$.delegatedCompletion.delivery.generation')
  END AS generation,
  'message:merged-wake-settle:' || part.wake_run_id || ':' || part.parent_run_id AS message_id,
  (
    SELECT json_group_array(task_id) FROM (
      SELECT listed.value AS task_id
      FROM json_each(parent.payload_json, '$.delegatedCompletion.delivery.taskIds') AS listed
      WHERE NOT part.delivered
      UNION
      SELECT waiting.subagent_id FROM waiting_task AS waiting
      WHERE waiting.parent_run_id = part.parent_run_id
    )
  ) AS task_ids
FROM merged_part AS part
JOIN orchestration_v2_projection_runs AS parent ON parent.run_id = part.parent_run_id
WHERE NOT part.delivered OR NOT part.own;

UPDATE orchestration_v2_projection_subagents
SET completion_delivery_state = 'delivered',
  payload_json = json_set(
    payload_json, '$.completionDelivery', json_object('state', 'delivered', 'observedByRunId', NULL)
  )
WHERE subagent_id IN (SELECT subagent_id FROM delivered_task);

UPDATE orchestration_v2_projection_subagents
SET completion_delivery_state = 'claimed',
  payload_json = json_set(
    payload_json, '$.completionDelivery', json_object('state', 'claimed', 'observedByRunId', NULL)
  )
WHERE subagent_id IN (SELECT subagent_id FROM waiting_task)
  AND run_id IN (
    SELECT parent_run_id FROM next_delivery WHERE json_array_length(task_ids) > 0
  );

UPDATE orchestration_v2_projection_runs
SET payload_json = (
  SELECT json_set(
    orchestration_v2_projection_runs.payload_json,
    '$.delegatedCompletion.delivery',
    CASE WHEN json_array_length(next.task_ids) = 0 THEN json('null')
      ELSE json_object(
        'generation', next.generation,
        'messageId', next.message_id,
        'taskIds', json(next.task_ids)
      )
    END,
    '$.delegatedCompletion.nextGeneration',
    json_extract(orchestration_v2_projection_runs.payload_json, '$.delegatedCompletion.nextGeneration')
      + (json_array_length(next.task_ids) > 0 AND next.generation
        = json_extract(orchestration_v2_projection_runs.payload_json, '$.delegatedCompletion.nextGeneration'))
  )
  FROM next_delivery AS next
  WHERE next.parent_run_id = orchestration_v2_projection_runs.run_id
)
WHERE run_id IN (SELECT parent_run_id FROM next_delivery);

-- Cancel the wakes that did not deliver and have not ended, with their attempt and root node.
-- A run whose input a steering restart replaced is the user's turn now, and is left alone.
CREATE TEMP TABLE cancelled_wake AS
SELECT DISTINCT wake.wake_run_id, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS at
FROM merged_wake AS wake
WHERE wake.current AND NOT wake.delivered
  AND wake.wake_status IN ('queued', 'preparing', 'starting', 'running', 'waiting');

UPDATE orchestration_v2_projection_runs
SET status = 'cancelled',
  completed_at = (SELECT at FROM cancelled_wake WHERE wake_run_id = run_id),
  payload_json = json_set(
    payload_json,
    '$.status', 'cancelled',
    '$.queuePosition', json('null'),
    '$.completedAt', (SELECT at FROM cancelled_wake WHERE wake_run_id = run_id)
  )
WHERE run_id IN (SELECT wake_run_id FROM cancelled_wake);

UPDATE orchestration_v2_projection_run_attempts
SET status = 'cancelled',
  payload_json = json_set(
    payload_json,
    '$.status', 'cancelled',
    '$.completedAt', (SELECT at FROM cancelled_wake WHERE wake_run_id = run_id)
  )
WHERE run_id IN (SELECT wake_run_id FROM cancelled_wake) AND status IN ('pending', 'running');

UPDATE orchestration_v2_projection_provider_turns
SET status = 'cancelled',
  completed_at = (SELECT at FROM cancelled_wake),
  payload_json = json_set(
    payload_json,
    '$.status', 'cancelled',
    '$.completedAt', (SELECT at FROM cancelled_wake)
  )
WHERE status IN ('pending', 'running')
  AND run_attempt_id IN (
    SELECT attempt_id FROM orchestration_v2_projection_run_attempts
    WHERE run_id IN (SELECT wake_run_id FROM cancelled_wake)
  );

UPDATE orchestration_v2_projection_nodes
SET status = 'cancelled',
  completed_at = (SELECT at FROM cancelled_wake WHERE wake_run_id = run_id),
  payload_json = json_set(
    payload_json,
    '$.status', 'cancelled',
    '$.completedAt', (SELECT at FROM cancelled_wake WHERE wake_run_id = run_id)
  )
WHERE run_id IN (SELECT wake_run_id FROM cancelled_wake)
  AND kind = 'root_turn' AND status IN ('pending', 'running', 'waiting');

-- The wake's message keeps only its own parent run, as the older build reads it.
UPDATE orchestration_v2_projection_messages
SET payload_json = json_set(
  json_remove(payload_json, '$.delegatedCompletion.merged'),
  '$.delegatedCompletion.taskIds',
  json((
    SELECT json_group_array(own.value)
    FROM json_each(orchestration_v2_projection_messages.payload_json, '$.delegatedCompletion.taskIds') AS own
    WHERE own.value NOT IN (
      SELECT part_task.value
      FROM json_each(orchestration_v2_projection_messages.payload_json, '$.delegatedCompletion.merged') AS merged,
        json_each(merged.value, '$.taskIds') AS part_task
    )
  ))
)
WHERE message_id IN (SELECT message_id FROM merged_wake);

DROP TABLE temp.merged_wake;
DROP TABLE temp.merged_part;
DROP TABLE temp.delivered_task;
DROP TABLE temp.waiting_task;
DROP TABLE temp.next_delivery;
DROP TABLE temp.cancelled_wake;
COMMIT;
