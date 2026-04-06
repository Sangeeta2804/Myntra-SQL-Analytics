# Myntra-SQL-Analytics
End-to-end SQL Project for Myntra e-commerce analytics

---

## 📌 Step 1: Database Setup

```sql
USE MyntraDB;
GO
```

---

## 📌 Step 2: Create Schemas

```sql
-- Staging schema (raw data)
CREATE SCHEMA stg;
GO

-- Clean schema (transformed data)
CREATE SCHEMA clean;
GO
```

---

## 📌 Step 3: Create Staging Tables (Raw Data)

```sql
-- Customers
CREATE TABLE stg.customers (
    CustomerID NVARCHAR(20),
    FirstName NVARCHAR(100),
    LastName NVARCHAR(100),
    Email NVARCHAR(200),
    Phone NVARCHAR(30),
    City NVARCHAR(100),
    State NVARCHAR(100),
    Pincode NVARCHAR(10),
    DOB NVARCHAR(20),
    Gender NVARCHAR(20),
    JoinDate NVARCHAR(20),
    LoyaltyTier NVARCHAR(30)
);

-- Products
CREATE TABLE stg.products (
    ProductID VARCHAR(50),
    Category VARCHAR(100),
    SubCategory VARCHAR(100),
    Brand VARCHAR(100),
    ProductName VARCHAR(255),
    MRP VARCHAR(50),
    SellingPrice VARCHAR(50),
    Discount VARCHAR(50),
    StockQty VARCHAR(50),
    Size VARCHAR(50),
    Rating VARCHAR(50)
);

-- Orders
CREATE TABLE stg.orders (
    OrderID NVARCHAR(20),
    CustomerID NVARCHAR(20),
    OrderDate NVARCHAR(20),
    Status NVARCHAR(50),
    PaymentMode NVARCHAR(50),
    DiscountCode NVARCHAR(50),
    ShipCity NVARCHAR(100),
    ShipPincode NVARCHAR(10),
    OrderTotal NVARCHAR(20),
    DeliveryDate NVARCHAR(20),
    Channel NVARCHAR(50)
);

-- Order Items
CREATE TABLE stg.order_items (
    ItemID NVARCHAR(20),
    OrderID NVARCHAR(20),
    ProductID NVARCHAR(20),
    Quantity NVARCHAR(10),
    UnitPrice NVARCHAR(20),
    DiscountPct NVARCHAR(10),
    LineTotal NVARCHAR(20)
);

-- Returns
CREATE TABLE stg.returns (
    ReturnID NVARCHAR(20),
    OrderID NVARCHAR(20),
    ProductID NVARCHAR(20),
    ReturnReason NVARCHAR(200),
    ReturnDate NVARCHAR(20),
    RefundAmount NVARCHAR(20),
    RefundStatus NVARCHAR(50)
);

-- Sellers
CREATE TABLE stg.sellers (
    SellerID NVARCHAR(20),
    SellerName NVARCHAR(200),
    City NVARCHAR(100),
    State NVARCHAR(100),
    GST NVARCHAR(50),
    SellerRating NVARCHAR(10),
    JoinDate NVARCHAR(20),
    PrimaryCategory NVARCHAR(100)
);
```

---

## 📌 Step 4: Bulk Data Loading

```sql
BULK INSERT stg.customers
FROM 'G:\Big Data Projects\MyntraData\customers.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    CODEPAGE = '65001',
    TABLOCK
);
```

---

## 📌 Step 5: Data Quality Checks

```sql
SELECT
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS NullEmails,
    SUM(CASE WHEN LEN(Phone) < 10 THEN 1 ELSE 0 END) AS ShortPhones
FROM stg.customers;
```

---

## 📌 Step 6: Create Clean Tables

```sql
CREATE TABLE clean.customers (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(100),
    LastName NVARCHAR(100),
    Email NVARCHAR(200),
    Phone NVARCHAR(30),
    City NVARCHAR(100),
    State NVARCHAR(100),
    Pincode NVARCHAR(10),
    DOB DATE,
    Gender NVARCHAR(20),
    JoinDate DATE,
    LoyaltyTier NVARCHAR(30)
);
```

---

## 📌 Step 7: Data Cleaning & Transformation

```sql
INSERT INTO clean.customers (
    CustomerID, FirstName, LastName, Email, Phone,
    City, State, Pincode, DOB, Gender, JoinDate, LoyaltyTier
)
SELECT
    TRY_CAST(CustomerID AS INT),
    LTRIM(RTRIM(FirstName)),
    LTRIM(RTRIM(LastName)),
    LOWER(Email),
    Phone,
    City,
    State,
    Pincode,
    TRY_CAST(DOB AS DATE),
    Gender,
    TRY_CAST(JoinDate AS DATE),
    ISNULL(LoyaltyTier,'Bronze')
FROM stg.customers;
```

---

## 📌 Step 8: Remove Duplicates

```sql
WITH cte AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY Email ORDER BY CustomerID) AS rn
    FROM clean.customers
)
DELETE FROM cte WHERE rn > 1;
```

---

## 📌 Step 9: Analytical Views (Power BI Ready)

```sql
CREATE VIEW vw_SalesOverview AS
SELECT
    o.OrderID,
    o.OrderDate,
    o.OrderTotal,
    c.FirstName + ' ' + c.LastName AS CustomerName
FROM clean.orders o
JOIN clean.customers c
ON o.CustomerID = c.CustomerID;
```

---

## 📊 Project Highlights

* ✅ Raw to Clean Data Transformation
* ✅ Data Quality Checks
* ✅ SQL Optimization
* ✅ Power BI Ready Views
* ✅ Real-world E-commerce Dataset

---

## 🚀 Tools Used

* SQL Server
* Power BI





