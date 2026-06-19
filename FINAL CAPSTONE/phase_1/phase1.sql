---Q1. Calculate the following aggregate metrics:
---Total Transactions
SELECT 
count(transaction_id) AS total_transactions
FROM bank_fraud

---Total Customers
SELECT 
count(distinct customer_id) AS total_customers
FROM bank_fraud

---Total Fraud Transactions
select 
count(transaction_id) AS fraud_transactions
from bank_fraud
where is_fraud = 1

---Fraud Percentage
with fraud as
    (select
        count(*) as total,
        sum(is_fraud) as total_fraud
    from bank_fraud
    )
select
    total_fraud/total*100 as fraud_percentage
from fraud
    


---Q2. Find the top 10 countries by transaction volume.
select
country,
sum(transaction_amount) AS transaction_volume
from bank_fraud
group by country
order by transaction_volume desc
limit 10

---Q3. Find the top 10 cities generating the highest transaction value.
select
city,
max(transaction_amount) AS highest_transaction_value
from bank_fraud
group by city
order by highest_transaction_value desc
limit 10

---Q5. Determine the percentage distribution of transactions by:
---Payment method
with fraud as (
    select
        payment_method,
        sum(is_fraud) as total_fraud
    from bank_fraud
    group by payment_method
)
select
    payment_method,
    total_fraud * 100.0 / (
        select sum(total_fraud)
        from fraud
    ) as fraud_percentage
from fraud;

---Device type
with fraud as (
    select
        device_type,
        sum(is_fraud) as total_fraud
    from bank_fraud
    group by device_type
)
select
    device_type,
    total_fraud * 100.0 / (
        select sum(total_fraud)
        from fraud
    ) as fraud_percentage
from fraud;

---Merchant Category
with fraud as (
    select
        merchant_category,
        sum(is_fraud) as total_fraud
    from bank_fraud
    group by merchant_category
)
select
    merchant_category,
    total_fraud * 100.0 / (
        select sum(total_fraud)
        from fraud
    ) as fraud_percentage
from fraud;


---Q6. Identify the top 20 customers by SUM(transaction_amount).
SELECT 
customer_id,
sum(transaction_amount)
from bank_fraud
group by customer_id
order by sum(transaction_amount) desc
limit 20

---Q7. Calculate average transaction amount by age group:18–25, 26–35, 36–50,  51–65, 65+
select
    case
        when customer_age < 25 then '18-24'
        when customer_age < 35 then '25-34'
        when customer_age < 50 then '35-49'
        when customer_age < 65 then '50-64'
        else '65+'
    end as age_group,
    avg(transaction_amount) as avg_transaction_amount
from bank_fraud
group by age_group
order by age_group asc;

---Q8. Determine which age group has the highest fraud rate.
with age_fraud as (select
case
    when customer_age >65 then '65+'
    when customer_age >=51 then '51-65'
    when customer_age >=36 then '36-50'
    when customer_age >=26 then '26-35'
    else '18-25'
end as age_group,
count(*) as total_number_transactions,
sum(is_fraud) as total_fraud
from bank_fraud
group by age_group)

select age_group,
100 * total_fraud/total_number_transactions as fraud_rate
from age_fraud
order by fraud_rate desc

---Q10. Calculate average account balance and credit score by country.
select
    country,
    avg(account_balance) as avg_account_balance,
    avg(credit_score) as avg_credit_score
from bank_fraud
group by country;

---Q11. Calculate fraud rate by Merchant Category.

with merchant_fraud as(
    select 
    merchant_category,
    count(*) as marchant_total_transactions,
    sum(is_fraud) as merchant_total_fraud
from bank_fraud
group  by merchant_category)

select 
    merchant_category,
    100 * merchant_total_fraud / marchant_total_transactions as fraud_rate
from merchant_fraud
order by fraud_rate desc

---Q12. Determine fraud rate by Payment Method.

with payment_fraud as(
    select 
    payment_method,
    count(*) as payment_total_transactions,
    sum(is_fraud) as payment_total_fraud
from bank_fraud
group  by payment_method)

select 
    payment_method,
    100 * payment_total_fraud / payment_total_transactions as fraud_rate
from payment_fraud
order by fraud_rate desc

---Q13. Determine fraud rate by Device Type.
with device_fraud as(
    select 
    device_type,
    count(*) as device_total_transactions,
    sum(is_fraud) as device_total_fraud
from bank_fraud
group  by device_type)

select 
    device_type,
    100 * device_total_fraud / device_total_transactions as fraud_rate
from device_fraud
order by fraud_rate desc

---Q14. Find the most common fraud type. Output columns:fraud_type, count, percentage
with fraud_counts as (
    select
        fraud_type,
        count(*) as count_frauds
    from bank_fraud
   where is_fraud = 1 
    group by fraud_type
)
select 
    fraud_type,
    count_frauds,
    count_frauds/(select sum(count_frauds) from fraud_counts)*100 as percentage
from fraud_counts
order by percentage desc
limit 1; 

-----------------------------
WITH fraud AS (
SELECT fraud_type,
COUNT(*) AS total_fraud
FROM bank_fraud
GROUP BY fraud_type)

SELECT fraud_type,
total_fraud,
100 * total_fraud / (SELECT SUM(total_fraud) FROM fraud) AS percentage
FROM fraud
WHERE fraud_type <> 'None'
ORDER BY percentage DESC

---Q15. Analyze fraud transactions occurring during: Night vs. Day, Weekend vs. Weekday
with fraud_transactions as(
select 
    case 
        when is_weekend = 1 then 'weekend'
        else 'weekday'
    end as day_type,
count(*) as total_transactions,
sum(is_fraud) as fraud_transactions
from bank_fraud
group by day_type)

select 
    day_type,
    100 * fraud_transactions / total_transactions as fraud_rate
from fraud_transactions
order  by fraud_rate desc


WITH fraud_transactions AS(
SELECT 
    case 
        when is_night_transaction= 1 then 'night'
        else 'day'
    end as day_type,
count(*) as total_transactions,
sum(is_fraud) as fraud_transactions
from bank_fraud
group by day_type)

select 
    day_type,
    100 * fraud_transactions / total_transactions as fraud_rate
from fraud_transactions
order  by fraud_rate desc

---Q16. Find transactions where transaction_amount exceeds: AVG(transaction_amount) + 3 *STDDEV(transaction_amount). Identify and report potential anomalies.
select
    transaction_id,
    transaction_amount
from bank_fraud
where transaction_amount > (
    select
        avg(transaction_amount) + 3*stddev(transaction_amount) 
    from bank_fraud
)
order by transaction_amount desc;

---Q17. Identify customers having more than 3 failed attempts AND Fraud = 1.
select distinct
    customer_id
from bank_fraud
where failed_attempts > 3 
and is_fraud = 1

---Q19. Analyze fraud rates for: International Transactions, Domestic Transactions
with fraud_transactions as(
select 
    case 
        when is_international= 1 then 'internattional'
        else 'domestic'
    end as type_transaction,
count(*) as total_transactions,
sum(is_fraud) as fraud_transactions
from bank_fraud
group by type_transaction)

select 
    type_transaction,
    100 * fraud_transactions / total_transactions as fraud_rate
from fraud_transactions
order by fraud_rate desc


---Q20. Find customers who performed all of the following, then rank by risk:International Transaction, Night Transaction, Failed Attempts > 

WITH risk_ranking AS (
SELECT customer_id,
CASE
    WHEN fraud_type ='Account Takeover' THEN 6
    WHEN fraud_type ='Synthetic Identity' THEN 5
    WHEN fraud_type ='Identity Theft' THEN 4
    WHEN fraud_type ='Phishing' THEN 3
    WHEN fraud_type ='Card Cloning' THEN 2
    WHEN fraud_type ='Friendly Fraud' THEN 1
    ELSE 0
END AS risk_rank
FROM bank_fraud
WHERE is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2)
SELECT customer_id,
SUM(risk_rank) AS total_risk
FROM risk_ranking
GROUP BY customer_id
ORDER BY total_risk DESC

---Q21. Rank customers by transaction value within each country. Use: RANK()
select
    customer_id,
    country,
    sum(transaction_amount) AS total_transaction,
    rank() over (
        partition by country
        order by sum(transaction_amount) desc
    ) as ranking
from bank_fraud
group by customer_id, country;
    

---Q22. Find the top 5 highest-value transactions in every merchant category. Use: ROW_NUMBER()
with ranked_transactions as (
    select
        transaction_id,
        merchant_category,
        transaction_amount,
        row_number() over (
            partition by merchant_category
            order by transaction_amount desc
        ) as ranking
    from bank_fraud
)
select
    transaction_id,
    merchant_category,
    transaction_amount
from ranked_transactions
WHERE ranking <= 5;

---Q23. Calculate cumulative transaction amount per customer over time. Use: SUM() OVER() ??????
select
    customer_id,
    transaction_date,
    transaction_amount,
    sum(transaction_amount) over (
        partition by customer_id
        order by transaction_date
    ) as cumulative_transaction_amount
from bank_fraud
order by customer_id, transaction_date;

---Q26. Create a View: VW_HIGH_RISK_TRANSACTIONS. Include records where: Transaction is International, Transaction occurred at Night, Failed Attempts > 2
CREATE OR REPLACE VIEW VW_HIGH_RISK_TRANSACTIONS AS
SELECT *
FROM bank_fraud
WHERE is_international = 1
  AND is_night_transaction = 1
  AND failed_attempts > 2;