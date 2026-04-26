-- ============================================================
-- PowerHub Billing System — Annotated SQL Excerpts
-- Author: Ngozi Clement
-- Note: Table names and column names have been anonymised.
--       All customer data is synthetic.
-- Stack: Amazon Redshift / LookML
-- ============================================================


-- ============================================================
-- 1. PRORATED FIRST BILLING MONTH
-- ============================================================
-- Every customer gets 30 free days from registration.
-- The first billable month is prorated based on how many days
-- remain after the free period ends.
-- Only months with more than 6 billable days are considered
-- billable — avoids generating near-zero invoices for customers
-- who register very close to month end.

first_billable_month AS (
    SELECT
        customer_id,
        billing_month_start                    AS first_billable_month_start,
        month_days                             AS first_billable_month_days,
        billable_days                          AS first_billable_days,

        CASE
            -- Partial month: prorate by billable days
            WHEN billable_days < month_days AND billable_days > 0
            THEN ROUND(
                    billable_days * (monthly_emi::DECIMAL / month_days),
                 2)
            -- Full month: no proration needed
            ELSE monthly_emi
        END AS first_billable_amount

    FROM monthly_billable
    WHERE billable_days > 6   -- ignore months with ≤6 billable days
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY billing_month_start
    ) = 1
),


-- ============================================================
-- 2. CUMULATIVE PAYMENT COVERAGE WITH EPSILON FIX
-- ============================================================
-- Calculates how many full months a customer's cumulative
-- payments cover, before and after each payment event.
--
-- The + 0.0000001 epsilon prevents a floating point issue where
-- FLOOR(1.0000000) evaluates to 0 instead of 1 when a customer
-- pays an exact multiple of the monthly EMI. Without this fix,
-- a customer paying exactly KES 4,350 on a 4,350/month product
-- would show 0 months covered instead of 1.

payments_with_coverage AS (
    SELECT
        p.customer_id,
        p.payment_hour,
        p.cumulative_paid_so_far,
        COALESCE(p.cumulative_paid_before, 0)  AS cumulative_paid_before,
        f.first_billable_amount,
        c.monthly_emi,

        -- Months covered BEFORE this payment
        CASE
            WHEN COALESCE(p.cumulative_paid_before, 0) < f.first_billable_amount
            THEN 0
            ELSE 1 + FLOOR(
                    (COALESCE(p.cumulative_paid_before, 0) - f.first_billable_amount)
                    / NULLIF(c.monthly_emi, 0)
                    + 0.0000001   -- epsilon: prevents FLOOR(1.0) = 0 edge case
                 )::INT
        END AS months_covered_before,

        -- Months covered AFTER this payment
        CASE
            WHEN p.cumulative_paid_so_far < f.first_billable_amount
            THEN 0
            ELSE 1 + FLOOR(
                    (p.cumulative_paid_so_far - f.first_billable_amount)
                    / NULLIF(c.monthly_emi, 0)
                    + 0.0000001   -- epsilon: same fix applied consistently
                 )::INT
        END AS months_covered_after

    FROM payments_cumulative p
    INNER JOIN customers c         ON p.customer_id = c.customer_id
    INNER JOIN first_billable_month f ON p.customer_id = f.customer_id
),


-- ============================================================
-- 3. LIGHT START DATE LOGIC (PAYMENT TRIGGER)
-- ============================================================
-- Determines the date from which the device should be activated
-- following a payment. Six cases are handled:
--
--   1. Customer has fully paid off the system (unlocked)
--   2. Payment made in a later month than the covering month
--      (arrears payment — start from payment date)
--   3. Payment made 1st–5th of covering month
--      (within grace window — extend light to 5th)
--   4. Payment made 6th+ of covering month, covers only that month
--      (late payment — start from 1st of next month)
--   5. Payment made before first billable month, free period ends mid-month
--      (new customer paying early — start from day after free period ends)
--   6. Default: start from 1st of covering month

CASE
    -- Case 1: Fully paid off
    WHEN total_paid >= unlock_price
    THEN DATEADD(day, 1, free_days_end)::DATE

    -- Case 2: Payment month is after the covering month (arrears)
    WHEN CAST(TO_CHAR(last_payment_date, 'YYYY-MM-01') AS DATE)
       > CAST(TO_CHAR(
             DATEADD(month, months_covered_before, first_billable_month_start),
             'YYYY-MM-01') AS DATE)
    THEN last_payment_date::DATE

    -- Case 3: Payment on 1st–5th of covering month (payment grace window)
    WHEN CAST(TO_CHAR(last_payment_date, 'YYYY-MM-01') AS DATE)
       = CAST(TO_CHAR(
             DATEADD(month, months_covered_before, first_billable_month_start),
             'YYYY-MM-01') AS DATE)
     AND EXTRACT(DAY FROM last_payment_date) BETWEEN 1 AND 5
    THEN DATEADD(day, 5,
             CAST(TO_CHAR(last_payment_date, 'YYYY-MM-01') AS DATE))::DATE

    -- Case 4: Payment on 6th+ of covering month, covers exactly 1 month
    WHEN CAST(TO_CHAR(last_payment_date, 'YYYY-MM-01') AS DATE)
       = CAST(TO_CHAR(
             DATEADD(month, months_covered_before, first_billable_month_start),
             'YYYY-MM-01') AS DATE)
     AND EXTRACT(DAY FROM last_payment_date) >= 6
    THEN CAST(TO_CHAR(
             DATEADD(month, months_covered_before + 1, first_billable_month_start),
             'YYYY-MM-01') AS DATE)

    -- Case 5: Payment before first billing month, free period ends mid-month
    -- (months_covered_before = 0 ensures this only fires on the FIRST payment)
    WHEN CAST(TO_CHAR(last_payment_date, 'YYYY-MM-01') AS DATE)
       < first_billable_month_start
     AND free_days_end >= first_billable_month_start
     AND months_covered_before = 0
    THEN DATEADD(day, 1, free_days_end)::DATE

    -- Case 6: Default — start from 1st of covering month
    ELSE CAST(TO_CHAR(
             DATEADD(month, months_covered_before, first_billable_month_start),
             'YYYY-MM-01') AS DATE)
END AS light_start_date,


-- ============================================================
-- 4. OUTSTANDING DUES CALCULATION
-- ============================================================
-- outstanding_dues = how many billing months have been invoiced
-- but not yet covered by cumulative payments.
--
-- Uses MAX(cumulative_paid_so_far) to get the customer's total
-- payment to date — unrestricted by billing month. This ensures
-- a customer who made a late payment for a previous month shows
-- as current, not overdue.
--
-- The payments_max CTE replaces a correlated subquery for
-- performance — computes MAX per customer once, then joins.

payments_max AS (
    SELECT
        customer_id,
        MAX(cumulative_paid_so_far) AS cumulative_paid_to_date
    FROM payments_cumulative
    GROUP BY customer_id
),

monthly_with_dues AS (
    SELECT
        m.*,
        f.first_billable_amount,

        -- Cumulative months billed (running total)
        SUM(CASE WHEN m.billable_days > 6 THEN 1 ELSE 0 END) OVER (
            PARTITION BY m.customer_id
            ORDER BY m.billing_month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_months_billed,

        -- Total paid to date (from payments_max join — avoids correlated subquery)
        COALESCE(pm.cumulative_paid_to_date, 0) AS cumulative_paid_to_date

    FROM monthly_with_payments m
    INNER JOIN first_billable_month f  ON m.customer_id = f.customer_id
    LEFT JOIN  payments_max pm         ON m.customer_id = pm.customer_id
),

-- Outstanding dues: months billed minus months covered by payments
-- GREATEST(0, ...) prevents negative values for customers who overpaid
final_with_dues AS (
    SELECT
        *,
        GREATEST(0,
            cumulative_months_billed -
            CASE
                WHEN cumulative_paid_to_date < first_billable_amount THEN 0
                ELSE 1 + FLOOR(
                        (cumulative_paid_to_date - first_billable_amount)
                        / NULLIF(monthly_emi, 0)
                        + 0.0000001   -- epsilon fix applied here too
                     )::INT
            END
        )::INT AS outstanding_dues
    FROM monthly_with_dues
),


-- ============================================================
-- 5. GRACE WINDOW ATTRIBUTION
-- ============================================================
-- Monthly grace fires on the 5th of each month.
-- It only applies when:
--   - Customer's free period has ended (free_days_end < billing_month_start)
--     OR customer is in their first prorated month with free period
--     ending after the 5th (has_proration = true)
--   - Customer has exactly 1 month outstanding (not 0, not 2+)
--   - Customer has NOT made a payment before the 5th of this month
--     (payment_made_before_5th = false)
--
-- light_start_date for monthly grace:
--   - Normal: 6th of billing month
--   - Proration exception: day after free period ends
--     (e.g. free period ends 22nd → light starts 23rd)

monthly_grace_rows AS (
    SELECT
        customer_id,
        account_number,
        owner_name,
        phone_number,
        billing_month_start                                    AS grace_month,
        DATEADD(day, 4, billing_month_start)                   AS trigger_date,  -- fires 5th

        CASE
            WHEN has_proration = true
             AND first_billable_month_start = billing_month_start
             AND free_days_end > DATEADD(day, 4, billing_month_start)
            THEN DATEADD(day, 1, free_days_end)::DATE          -- day after free period
            ELSE DATEADD(day, 5, billing_month_start)::DATE    -- standard: 6th
        END AS light_start_date,

        CASE
            WHEN has_proration = true
             AND first_billable_month_start = billing_month_start
             AND free_days_end > DATEADD(day, 4, billing_month_start)
            THEN LAST_DAY(free_days_end)::DATE
            ELSE LAST_DAY(billing_month_start)::DATE
        END AS light_end_date,

        'MONTHLY_GRACE' AS activation_reason

    FROM monthly_grace_attrs
    WHERE is_excluded = false
      AND (
            (free_days_ended = true AND outstanding_dues = 1)
         OR (has_proration = true AND free_days_end_this_month = true AND outstanding_dues = 1)
          )
      AND payment_made_before_5th = false
      -- Production filter (uncomment for scheduled runs):
      -- AND billing_month_start = CAST(TO_CHAR(CURRENT_DATE, 'YYYY-MM-01') AS DATE)
)
