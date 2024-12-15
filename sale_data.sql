CREATE DATABASE sales_db;
USE sales_db;

SET SESSION sql_mode = '';

--                 DATA CLEANING                   --

-- REMOVING DUPICATES
CREATE TABLE sales_data AS 
(SELECT * FROM 
(SELECT *, ROW_NUMBER() OVER(PARTITION BY OrderID) AS Row_Num
FROM salesdata) a
WHERE Row_Num<2);

-- ANOTHER METHOD FOR REMOVING DUPLICATES
-- DELETE FROM salesdata WHERE OrderId IN
-- (SELECT OrderId FROM 
-- (SELECT *, ROW_NUMBER() OVER(PARTITION BY OrderID) AS Row_Num
-- FROM salesdata ) a
-- WHERE Row_num>1);

SET SQL_SAFE_UPDATES = 0;

-- SETTING APPROPRIATE DATA-TYPES
ALTER TABLE sales_data
ADD COLUMN Order_date DATETIME ;

UPDATE sales_data 
SET Order_date = (SELECT STR_TO_DATE(OrderDate,"%d-%m-%Y %H:%i") );

SELECT * FROM sales_data;

ALTER TABLE sales_data
DROP COLUMN OrderDate;

DESC sales_data;

ALTER TABLE sales_data
MODIFY OrderID INT PRIMARY KEY,
MODIFY Quantity INT ,
MODIFY PriceEach DECIMAL(6,2),
MODIFY Sales DECIMAL(6,2);

--                   DATA EXPLORATION                    --


-- Total Sales
SELECT SUM(Sales) AS "Total_Sales"
FROM sales_data;

-- Total Quantity Sold
SELECT SUM(Quantity) AS "Total_Quantity"
FROM sales_data;

-- YEARLY SALES
SELECT YEAR(Order_date) AS "Year",SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY YEAR(Order_date);

-- MONTHLY SALES
SELECT MONTHNAME(Order_date) AS "Month",SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY MONTHNAME(Order_date);

-- DAY-WISE SALES
SELECT DAYNAME(Order_date) AS "Day",SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY DAYNAME(Order_date)
ORDER BY SUM(Sales) DESC;

-- HOURLY TREND
SELECT HOUR(Order_date) AS "Hour",SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY HOUR(Order_date);

-- TOP 5 CITIES WITH MAXIMUM QUANTITY SOLD
SELECT City,SUM(Quantity) AS "Total_Quantity"
FROM sales_data
GROUP BY City
ORDER BY SUM(Quantity) DESC
LIMIT 5;

-- TOP 5 CITIES WITH MAXIMUM SALES
SELECT City,SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY City
ORDER BY SUM(Sales) DESC
LIMIT 5;

-- % CONTRIBUTION OF TOP 5 CITIES IN TOTAL SALES
SELECT City,(SUM(Sales)/(SELECT SUM(Sales) FROM sales_data))*100 AS "%_Contribution"
FROM sales_data
GROUP BY City
ORDER BY SUM(Sales) DESC
LIMIT 5;


-- 5 CITIES WITH LEAST QUANTITY SOLD
SELECT City,SUM(Quantity) AS "Total_Quantity"
FROM sales_data
GROUP BY City
ORDER BY SUM(Quantity) 
LIMIT 5;

-- 5 CITIES WITH LEAST SALES
SELECT City,SUM(Sales) AS "Total_Sales"
FROM sales_data
GROUP BY City
ORDER BY SUM(Sales)
LIMIT 5;

-- MOST SELLING PRODUCT IN TERMS OF REVENUE AND IT'S CONTRIBUTION IN TOTAL SALES
SELECT Product,SUM(Quantity) AS "Quantity_Sold",SUM(Sales) AS "Total_Sales",(SUM(Sales)/(SELECT SUM(Sales) FROM sales_data))*100 AS "%_Contribution"
FROM sales_data
GROUP BY Product
ORDER BY SUM(Sales) DESC
LIMIT 1;

-- LEAST SELLING PRODUCT IN TERMS OF REVENUE AND IT'S CONTRIBUTION IN TOTAL SALES
SELECT Product,SUM(Quantity) AS "Quantity_Sold",SUM(Sales) AS "Total_Sales",(SUM(Sales)/(SELECT SUM(Sales) FROM sales_data))*100 AS "%_Contribution"
FROM sales_data
GROUP BY Product
ORDER BY SUM(Sales) 
LIMIT 1;

-- ANALYZING SALES OF BEST SELLING PRODUCT(IN TERMS OF REVENUE) ACROSS DIFFERENT DEMOGRAPHICS
SELECT City,SUM(Quantity),SUM(Sales)
FROM sales_data
WHERE Product = 
(SELECT Product FROM sales_data GROUP BY Product ORDER BY SUM(Sales) DESC LIMIT 1)
GROUP BY City;

-- MOST SELLING PRODUCT IN TERMS OF QUANTITY AND IT'S CONTRIBUTION IN TOTAL SALES
SELECT Product,SUM(Quantity) AS "Quantity_Sold" ,SUM(Sales) AS "Total_Sales",(SUM(Sales)/(SELECT SUM(Sales) FROM sales_data))*100 AS "%_Contribution"
FROM sales_data
GROUP BY Product
ORDER BY SUM(Quantity) DESC
LIMIT 1;

-- ADDRESS WITH MINIMUM 3 ORDERS
SELECT PurchaseAddress
FROM sales_data
GROUP BY PurchaseAddress
HAVING COUNT(PurchaseAddress)>=3;

-- USING ABOVE QUERY IDENTIFYING LINKAGE BETWEEN PRODUCTS THAT ARE PURCHASED 
SELECT Product 
FROM sales_data 
WHERE PurchaseAddress IN 
(SELECT PurchaseAddress
FROM sales_data
GROUP BY PurchaseAddress
HAVING COUNT(PurchaseAddress)>=3);

-- INDIVIDUAL PRODUCT SALES IN DIFFERENT YEARS
SELECT Product,
COUNT(CASE WHEN YEAR(Order_date)=2019 Then Product END) AS "2019_Sales",
COUNT(CASE WHEN YEAR(Order_date)=2020 Then Product END) AS "2020_Sales"
FROM Sales_data
GROUP BY Product;

-- CITY WISE MOST SELLING PRODUCT
SELECT City,Product,No_Of_Orders FROM
(SELECT *,DENSE_RANK() OVER(PARTITION BY City ORDER BY No_Of_Orders DESC) AS "Rnk" FROM
(SELECT City,Product,COUNT(OrderID) AS "No_Of_Orders"
FROM sales_data
GROUP BY City,Product) a) b
WHERE Rnk = 1;

-- CITY WISE LEAST SELLING PRODUCT
SELECT City,Product,No_Of_Orders FROM
(SELECT *,DENSE_RANK() OVER(PARTITION BY City ORDER BY No_Of_Orders ASC) AS "Rnk" FROM
(SELECT City,Product,COUNT(OrderID) AS "No_Of_Orders"
FROM sales_data
GROUP BY City,Product) a) b
WHERE Rnk = 1;

-- INDIVIDUAL PRODUCTS WITH THEIR BEST CITY MARKET AND CONTRIBUTION OF SALES OF THAT MARKET IN TOTAL SALES
SELECT Individual_P.*,All_P.Total_Count,(Individual_P.No_Of_Orders/All_P.Total_Count)*100 AS "%_Contribution_Of_City_Sales_In_Total_Sales"
FROM
(SELECT Product,City,No_Of_Orders
FROM
(SELECT *,DENSE_RANK() OVER(PARTITION BY Product ORDER BY No_Of_Orders DESC) AS "Rnk" FROM
(SELECT Product,City,COUNT(OrderID) AS "No_Of_Orders"
FROM sales_data
GROUP BY Product,City) a) b
WHERE Rnk = 1
ORDER BY Product) Individual_P
JOIN
(SELECT Product,COUNT(OrderID) AS "Total_Count"
FROM sales_data
GROUP BY Product) All_P
ON Individual_P.Product = All_P.Product;

-- INDIVIDUAL PRODUCTS WITH THEIR WORST CITY MARKET AND CONTRIBUTION OF SALES OF THAT MARKET IN TOTAL SALES 
SELECT Individual_P.*,All_P.Total_Count,(Individual_P.No_Of_Orders/All_P.Total_Count)*100 AS "%_Contribution_Of_City_Sales_In_Total_Sales"
FROM
(SELECT Product,City,No_Of_Orders
FROM
(SELECT *,DENSE_RANK() OVER(PARTITION BY Product ORDER BY No_Of_Orders ASC) AS "Rnk" FROM
(SELECT Product,City,COUNT(OrderID) AS "No_Of_Orders"
FROM sales_data
GROUP BY Product,City) a) b
WHERE Rnk = 1
ORDER BY Product) Individual_P
JOIN
(SELECT Product,COUNT(OrderID) AS "Total_Count"
FROM sales_data
GROUP BY Product) All_P
ON Individual_P.Product = All_P.Product;

-- MOST FREQUENT FIRST PURCHASED PRODUCT

WITH mycte AS
(SELECT DISTINCT PurchaseAddress,FIRST_VALUE(Product) 
OVER(PARTITION BY PurchaseAddress ORDER BY Order_date) AS "Product"
FROM sales_data)
SELECT Product,COUNT(Product) AS "No_Of_Purchases" FROM mycte
GROUP BY Product
ORDER BY COUNT(Product) DESC;

-- NUMBER OF NEW PURCHASES EVERY MONTH

SELECT Mnth AS "Month",COUNT(Mnth) AS "No_Of_Purchases" FROM
(SELECT DISTINCT PurchaseAddress,
MONTHNAME(FIRST_VALUE(Order_date) OVER(PARTITION BY PurchaseAddress ORDER BY Order_date)) AS Mnth
FROM sales_data) a
GROUP BY Mnth;
