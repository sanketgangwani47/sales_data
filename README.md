# Sales Data Analysis — SQL Project

## Business Question
How can we evaluate sales KPIs and identify customer purchase patterns to support data-driven business decisions?

## Dataset
- Source: Kaggle
- Size: 185950,8
- Period: 2019-2020

## Tools Used
- SQL (MySQL)

## SQL Techniques Applied
- Window Functions (RANK, ROW_NUMBER, LAG)
- CTEs (Common Table Expressions)
- Subqueries
- GROUP BY aggregations
- JOIN operations

## Key Business Questions Answered
1. Which products/categories drove the highest revenue?
2. Number of new purchases added every month
3. Which products have the highest repeat purchase rate?


## Key Findings
- Identified cross-sell opportunities by extracting customers with ≥3 repeat orders and analysing product co-purchase patterns, surfacing actionable bundling recommendations for the sales team. 
- Identified encouraging product that drove new customers into business.

## SQL Snippet (Sample Query)
```sql
WITH mycte AS
(SELECT DISTINCT PurchaseAddress,FIRST_VALUE(Product) 
OVER(PARTITION BY PurchaseAddress ORDER BY Order_date) AS "Product"
FROM sales_data)
SELECT Product,COUNT(Product) AS "No_Of_Purchases" FROM mycte
GROUP BY Product
ORDER BY COUNT(Product) DESC;
```

## Outcome
Findings were used to evaluate business KPIs and uncover purchase patterns — translating raw transactional data into actionable recommendations.
