{{ config(materialized='table') }}

WITH tx AS (
    SELECT
        txn_id,
        txn_ts,
        amount,
        currency_code,
        card_id,
        card_number,
        account_id,
        account_number,
        customer_id,
        customer_city,
        terminal_id,
        terminal_city,
        txn_type,
        status
    FROM {{ ref('ods_fact_atm_transaction') }}
    WHERE txn_type = 'atm_withdrawal'
),

rapid_base AS (
    SELECT
        *,
        lagInFrame(txn_ts) OVER (PARTITION BY card_id ORDER BY txn_ts) AS prev_ts,
        if(prev_ts IS NULL OR dateDiff('second', prev_ts, txn_ts) > 1200, 1, 0) AS new_grp
    FROM tx
    WHERE currency_code IN ('RUB','EUR')
      AND amount BETWEEN 5000 AND 40000
),

rapid_seq AS (
    SELECT
        *,
        sum(new_grp) OVER (PARTITION BY card_id ORDER BY txn_ts) AS grp_id
    FROM rapid_base
),

rapid_groups AS (
    SELECT
        'rapid_withdrawals_diff_terminals' AS fraud_type,
        card_id,
        min(txn_ts) AS fraud_start_ts,
        argMin(txn_id, txn_ts) AS fraud_start_txn_id,
        argMin(terminal_id, txn_ts) AS start_terminal_id,
        argMin(terminal_city, txn_ts) AS start_terminal_city,
        argMin(card_number, txn_ts) AS card_number,
        argMin(account_id, txn_ts) AS account_id,
        argMin(account_number, txn_ts) AS account_number,
        argMin(customer_id, txn_ts) AS customer_id,
        argMin(customer_city, txn_ts) AS customer_city,
        count() AS txns_cnt,
        countDistinct(terminal_id) AS terminals_cnt,
        argMax(status, txn_ts) AS last_status,
        countIf(status = 'approved') AS approved_cnt,
        countIf(status IN ('declined','reversed')) AS fail_cnt,
        dateDiff('second', min(txn_ts), max(txn_ts)) AS span_sec
    FROM rapid_seq
    GROUP BY card_id, grp_id
    HAVING
        txns_cnt BETWEEN 3 AND 6
        AND terminals_cnt >= 2
        AND fail_cnt = 1
        AND approved_cnt = txns_cnt - 1
        AND last_status IN ('declined','reversed')
        AND span_sec <= 1200
),

blocked_tx AS (
    SELECT t.*
    FROM tx t
    INNER JOIN {{ ref('ods_dim_card') }} c
        ON c.card_id = t.card_id
    WHERE c.status = 'blocked'
      AND t.status = 'declined'
),

blocked_base AS (
    SELECT
        *,
        lagInFrame(txn_ts) OVER (PARTITION BY card_id, terminal_id ORDER BY txn_ts) AS prev_ts,
        if(prev_ts IS NULL OR dateDiff('second', prev_ts, txn_ts) > 600, 1, 0) AS new_grp
    FROM blocked_tx
),

blocked_seq AS (
    SELECT
        *,
        sum(new_grp) OVER (PARTITION BY card_id, terminal_id ORDER BY txn_ts) AS grp_id
    FROM blocked_base
),

blocked_groups AS (
    SELECT
        'card_closed_then_used' AS fraud_type,
        card_id,
        min(txn_ts) AS fraud_start_ts,
        argMin(txn_id, txn_ts) AS fraud_start_txn_id,
        terminal_id AS start_terminal_id,
        argMin(terminal_city, txn_ts) AS start_terminal_city,
        argMin(card_number, txn_ts) AS card_number,
        argMin(account_id, txn_ts) AS account_id,
        argMin(account_number, txn_ts) AS account_number,
        argMin(customer_id, txn_ts) AS customer_id,
        argMin(customer_city, txn_ts) AS customer_city,
        count() AS txns_cnt,
        toUInt64(1) AS terminals_cnt,
        dateDiff('second', min(txn_ts), max(txn_ts)) AS span_sec
    FROM blocked_seq
    GROUP BY card_id, terminal_id, grp_id
    HAVING
        txns_cnt BETWEEN 1 AND 3
        AND span_sec <= 600
),

probe_base AS (
    SELECT
        *,
        lagInFrame(txn_ts) OVER (PARTITION BY card_id, terminal_id ORDER BY txn_ts) AS prev_ts,
        if(prev_ts IS NULL OR dateDiff('second', prev_ts, txn_ts) > 600, 1, 0) AS new_grp
    FROM tx
    WHERE currency_code = 'RUB'
      AND status IN ('declined','approved')
),

probe_seq AS (
    SELECT
        *,
        sum(new_grp) OVER (PARTITION BY card_id, terminal_id ORDER BY txn_ts) AS grp_id
    FROM probe_base
),

probe_ranked AS (
    SELECT
        *,
        lagInFrame(amount) OVER (PARTITION BY card_id, terminal_id, grp_id ORDER BY txn_ts) AS prev_amount
    FROM probe_seq
),

probe_groups AS (
    SELECT
        'amount_probe' AS fraud_type,
        card_id,
        min(txn_ts) AS fraud_start_ts,
        argMin(txn_id, txn_ts) AS fraud_start_txn_id,
        terminal_id AS start_terminal_id,
        argMin(terminal_city, txn_ts) AS start_terminal_city,
        argMin(card_number, txn_ts) AS card_number,
        argMin(account_id, txn_ts) AS account_id,
        argMin(account_number, txn_ts) AS account_number,
        argMin(customer_id, txn_ts) AS customer_id,
        argMin(customer_city, txn_ts) AS customer_city,
        count() AS txns_cnt,
        toUInt64(1) AS terminals_cnt,
        dateDiff('second', min(txn_ts), max(txn_ts)) AS span_sec,
        countIf(status = 'approved') AS approved_cnt,
        argMax(status, txn_ts) AS last_status,
        sumIf(1, prev_amount IS NOT NULL AND amount >= prev_amount) AS non_decreasing_steps
    FROM probe_ranked
    GROUP BY card_id, terminal_id, grp_id
    HAVING
        txns_cnt BETWEEN 3 AND 5
        AND span_sec <= 600
        AND non_decreasing_steps = 0
        AND approved_cnt <= 1
        AND (approved_cnt = 0 OR last_status = 'approved')
),

unioned AS (
    SELECT
        fraud_type,
        card_id,
        fraud_start_ts,
        fraud_start_txn_id,
        start_terminal_id,
        start_terminal_city,
        card_number,
        account_id,
        account_number,
        customer_id,
        customer_city,
        txns_cnt,
        terminals_cnt
    FROM rapid_groups

    UNION ALL

    SELECT
        fraud_type,
        card_id,
        fraud_start_ts,
        fraud_start_txn_id,
        start_terminal_id,
        start_terminal_city,
        card_number,
        account_id,
        account_number,
        customer_id,
        customer_city,
        txns_cnt,
        terminals_cnt
    FROM blocked_groups

    UNION ALL

    SELECT
        fraud_type,
        card_id,
        fraud_start_ts,
        fraud_start_txn_id,
        start_terminal_id,
        start_terminal_city,
        card_number,
        account_id,
        account_number,
        customer_id,
        customer_city,
        txns_cnt,
        terminals_cnt
    FROM probe_groups
)

SELECT
    lower(hex(MD5(concat(
        fraud_type, '|',
        toString(card_id), '|',
        toString(start_terminal_id), '|',
        toString(fraud_start_ts)
    )))) AS fraud_event_id,
    fraud_type,
    fraud_start_ts,
    toDate(fraud_start_ts) AS fraud_date,
    fraud_start_txn_id,
    card_id,
    card_number,
    account_id,
    account_number,
    customer_id,
    customer_city,
    start_terminal_id AS terminal_id,
    start_terminal_city AS terminal_city,
    txns_cnt,
    terminals_cnt
FROM unioned