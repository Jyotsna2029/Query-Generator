CREATE DATABASE superstore_dw;
USE superstore_dw;
SHOW DATABASES;

CREATE TABLE staging_sales (
row_id INT,
order_id VARCHAR(50),
order_date DATE,
ship_date DATE,
ship_mode VARCHAR(50),

customer_id VARCHAR(50),
customer_name VARCHAR(100),
segment VARCHAR(50),

country VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50),
postal_code VARCHAR(20),
region VARCHAR(20),

product_id VARCHAR(50),
category VARCHAR(50),
sub_category VARCHAR(50),
product_name VARCHAR(255),

sales DECIMAL(10,2),
quantity INT,
discount DECIMAL(4,2),
profit DECIMAL(10,2)
);
DESCRIBE staging_sales;

SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

LOAD DATA LOCAL INFILE 'C:/Users/chauh/Downloads/Superstore.csv'
INTO TABLE staging_sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS

(@row_id,@order_id,@order_date,@ship_date,@ship_mode,
@customer_id,@customer_name,@segment,
@country,@city,@state,@postal_code,@region,
@product_id,@category,@sub_category,@product_name,
@sales,@quantity,@discount,@profit)

SET
row_id=@row_id,
order_id=@order_id,
order_date=STR_TO_DATE(@order_date,'%m/%d/%Y'),
ship_date=STR_TO_DATE(@ship_date,'%m/%d/%Y'),
ship_mode=@ship_mode,
customer_id=@customer_id,
customer_name=@customer_name,
segment=@segment,
country=@country,
city=@city,
state=@state,
postal_code=@postal_code,
region=@region,
product_id=@product_id,
category=@category,
sub_category=@sub_category,
product_name=@product_name,
sales=@sales,
quantity=@quantity,
discount=@discount,
profit=@profit;

SELECT order_id, order_date, ship_date
FROM staging_sales
LIMIT 30;

/*Create Dimension Tables*/
CREATE TABLE Dim_Product (
product_key INT AUTO_INCREMENT PRIMARY KEY,
product_id VARCHAR(50),
category VARCHAR(50),
sub_category VARCHAR(50),
product_name VARCHAR(255)
);

CREATE TABLE Dim_Customer (
customer_key INT AUTO_INCREMENT PRIMARY KEY,
customer_id VARCHAR(50),
customer_name VARCHAR(100),
segment VARCHAR(50)
);

CREATE TABLE Dim_Location (
location_id INT AUTO_INCREMENT PRIMARY KEY,
postal_code VARCHAR(20),
city VARCHAR(50),
state VARCHAR(50),
region VARCHAR(20),
country VARCHAR(50)
);

CREATE TABLE Dim_Time (
date_id INT AUTO_INCREMENT PRIMARY KEY,
full_date DATE,
year INT,
month INT,
day INT
);

/*Create the Fact Table*/
CREATE TABLE Fact_Sales (
sales_id INT AUTO_INCREMENT PRIMARY KEY,
order_id VARCHAR(50),

date_id INT,
location_id INT,
product_key INT,
customer_key INT,

sales_amount DECIMAL(10,2),
quantity INT,
discount DECIMAL(4,2),
profit DECIMAL(10,2),

FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id),
FOREIGN KEY (location_id) REFERENCES Dim_Location(location_id),
FOREIGN KEY (product_key) REFERENCES Dim_Product(product_key),
FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key)
);

SHOW TABLES;

/*Insert Data Into Dim_Product*/
INSERT INTO Dim_Product (product_id, category, sub_category, product_name)
SELECT DISTINCT
product_id,
category,
sub_category,
product_name
FROM staging_sales;

SELECT * FROM Dim_Product LIMIT 10;

INSERT INTO Dim_Customer (customer_id, customer_name, segment)
SELECT DISTINCT
customer_id,
customer_name,
segment
FROM staging_sales;

SELECT * FROM Dim_Customer LIMIT 10;

INSERT INTO Dim_Location (postal_code, city, state, region, country)
SELECT DISTINCT
postal_code,
city,
state,
region,
country
FROM staging_sales;

SELECT * FROM Dim_Location LIMIT 10;

INSERT INTO Dim_Time (full_date, year, month, day)
SELECT DISTINCT
order_date,
YEAR(order_date),
MONTH(order_date),
DAY(order_date)
FROM staging_sales
WHERE order_date IS NOT NULL;

SELECT * FROM Dim_Time LIMIT 10;

INSERT INTO Fact_Sales
(order_id, date_id, location_id, product_key, customer_key,
sales_amount, quantity, discount, profit)

SELECT
s.order_id,
t.date_id,
l.location_id,
p.product_key,
c.customer_key,
s.sales,
s.quantity,
s.discount,
s.profit

FROM staging_sales s
JOIN Dim_Time t ON s.order_date = t.full_date
JOIN Dim_Location l ON s.postal_code = l.postal_code
JOIN Dim_Product p ON s.product_id = p.product_id
JOIN Dim_Customer c ON s.customer_id = c.customer_id;

SELECT * FROM Fact_Sales LIMIT 10;

SELECT dl.region, SUM(fs.sales_amount) AS total_sales
FROM Fact_Sales fs
JOIN Dim_Location dl
ON fs.location_id = dl.location_id
GROUP BY dl.region;

/*week6*/
/*Verify Dataset Loaded*/
SELECT COUNT(*) 
FROM staging_sales;
/*check for Missing Data (Data Cleaning)*/
SELECT 
COUNT(*) AS total_rows,
COUNT(order_date) AS order_date_count,
COUNT(product_id) AS product_count,
COUNT(customer_id) AS customer_count
FROM staging_sales;


SET SQL_SAFE_UPDATES = 0;

DELETE FROM staging_sales
WHERE order_date IS NULL
  AND ship_date IS NULL;

UPDATE staging_sales
SET order_date = ship_date
WHERE ship_date IS NOT NULL and order_date IS NULL;

UPDATE staging_sales
SET ship_date = order_date
WHERE order_date IS NOT NULL and ship_date IS NULL;

SET SQL_SAFE_UPDATES = 1;

SELECT order_id, order_date, ship_date
FROM staging_sales
LIMIT 30;

/*Check Data Consistency*/
SELECT COUNT(DISTINCT product_id)
FROM staging_sales;

SELECT COUNT(DISTINCT customer_id)
FROM staging_sales;

SELECT DISTINCT region
FROM staging_sales;

SELECT COUNT(*) FROM Dim_Product;
SELECT COUNT(*) FROM Dim_Customer;
SELECT COUNT(*) FROM Dim_Location;
SELECT COUNT(*) FROM Fact_Sales;

SELECT region, SUM(sales_amount) AS total_sales
FROM Fact_Sales fs
JOIN Dim_Location dl
ON fs.location_id = dl.location_id
GROUP BY region;