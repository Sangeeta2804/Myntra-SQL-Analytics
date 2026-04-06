USE MyntraDB;
GO

-- Step 2: Create staging schema (raw dirty data)
CREATE SCHEMA stg;
GO

-- Step 3: Create clean schema (after transformation)
CREATE SCHEMA clean;
GO

-- stg.customers
CREATE TABLE stg.customers (
    CustomerID      NVARCHAR(20),
    FirstName       NVARCHAR(100),
    LastName        NVARCHAR(100),
    Email           NVARCHAR(200),
    Phone           NVARCHAR(30),
    City            NVARCHAR(100),
    State           NVARCHAR(100),
    Pincode         NVARCHAR(10),
    DOB             NVARCHAR(20),
    Gender          NVARCHAR(20),
    JoinDate        NVARCHAR(20),
    LoyaltyTier     NVARCHAR(30)
);


DROP TABLE IF EXISTS clean.customers;

create table clean.customers (
    CustomerID      INT PRIMARY KEY,
    FirstName       NVARCHAR(100),
    LastName        NVARCHAR(100),
    Email           NVARCHAR(200),
    Phone           NVARCHAR(30),
    City            NVARCHAR(100),
    State           NVARCHAR(100),
    Pincode         NVARCHAR(10),
    DOB             DATE,
    Gender          NVARCHAR(20),
    JoinDate        DATE,
    LoyaltyTier     NVARCHAR(30)
);


DROP TABLE IF EXISTS stg.products;

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
    Rating VARCHAR(50)   -- 🔥 IMPORTANT (keep VARCHAR)
);
-- stg.orders
CREATE TABLE stg.orders (
    OrderID         NVARCHAR(20),
    CustomerID      NVARCHAR(20),
    OrderDate       NVARCHAR(20),
    Status          NVARCHAR(50),
    PaymentMode     NVARCHAR(50),
    DiscountCode    NVARCHAR(50),
    ShipCity        NVARCHAR(100),
    ShipPincode     NVARCHAR(10),
    OrderTotal      NVARCHAR(20),
    DeliveryDate    NVARCHAR(20),
    Channel         NVARCHAR(50)
);

-- stg.order_items
CREATE TABLE stg.order_items (
    ItemID          NVARCHAR(20),
    OrderID         NVARCHAR(20),
    ProductID       NVARCHAR(20),
    Quantity        NVARCHAR(10),
    UnitPrice       NVARCHAR(20),
    DiscountPct     NVARCHAR(10),
    LineTotal       NVARCHAR(20)
);

-- stg.returns
CREATE TABLE stg.returns (
    ReturnID        NVARCHAR(20),
    OrderID         NVARCHAR(20),
    ProductID       NVARCHAR(20),
    ReturnReason    NVARCHAR(200),
    ReturnDate      NVARCHAR(20),
    RefundAmount    NVARCHAR(20),
    RefundStatus    NVARCHAR(50)
);

-- stg.sellers
CREATE TABLE stg.sellers (
    SellerID        NVARCHAR(20),
    SellerName      NVARCHAR(200),
    City            NVARCHAR(100),
    State           NVARCHAR(100),
    GST             NVARCHAR(50),
    SellerRating    NVARCHAR(10),
    JoinDate        NVARCHAR(20),
    PrimaryCategory NVARCHAR(100)
);

-- stg.product_seller
CREATE TABLE stg.product_seller (
    PSID            NVARCHAR(20),
    ProductID       NVARCHAR(20),
    SellerID        NVARCHAR(20),
    StockWithSeller NVARCHAR(10),
    LeadTimeDays    NVARCHAR(10)
);

-- stg.reviews
CREATE TABLE stg.reviews (
    ReviewID        NVARCHAR(20),
    CustomerID      NVARCHAR(20),
    ProductID       NVARCHAR(20),
    OrderID         NVARCHAR(20),
    Rating          NVARCHAR(10),
    ReviewText      NVARCHAR(MAX),
    ReviewDate      NVARCHAR(20),
    HelpfulVotes    NVARCHAR(10),
    VerifiedPurchase NVARCHAR(10)
);

-- stg.marketing_campaigns
CREATE TABLE stg.marketing_campaigns (
    CampaignID      NVARCHAR(20),
    CustomerID      NVARCHAR(20),
    CampaignName    NVARCHAR(200),
    Channel         NVARCHAR(100),
    SentDate        NVARCHAR(20),
    Opened          NVARCHAR(5),
    Clicked         NVARCHAR(5),
    Converted       NVARCHAR(5),
    SpendPerContact NVARCHAR(20)
);

-- Run as SA if BULK INSERT gives permission errors
EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
EXEC sp_configure 'Ad Hoc Distributed Queries', 1; RECONFIGURE;

-- 1. customers
    BULK INSERT stg.customers
    FROM 'G:\Big Data Projects\MyntraData\customers.csv'
    WITH (
        FIELDTERMINATOR  = ',',
        ROWTERMINATOR    = '\n',
        FIRSTROW         = 2,
        CODEPAGE         = '65001',
        TABLOCK
    );

-- 2. products
BULK INSERT stg.products
FROM 'G:\Big Data Projects\MyntraData\products.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    TABLOCK,
    ERRORFILE = 'G:\Big Data Projects\MyntraData\products_errors.txt',
    MAXERRORS = 10000
);

SELECT top 10 * FROM stg.products;

-- 3. orders
BULK INSERT stg.orders
FROM 'G:\Big Data Projects\MyntraData\orders.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 4. order_items  (largest file – 2M rows)
BULK INSERT stg.order_items
FROM 'G:\Big Data Projects\MyntraData\order_items.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 5. returns
BULK INSERT stg.returns
FROM 'G:\Big Data Projects\MyntraData\returns.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 6. sellers
BULK INSERT stg.sellers
FROM 'G:\Big Data Projects\MyntraData\sellers.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 7. product_seller
BULK INSERT stg.product_seller
FROM 'G:\Big Data Projects\MyntraData\product_seller.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 8. reviews
BULK INSERT stg.reviews
FROM 'G:\Big Data Projects\MyntraData\reviews.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );

-- 9. marketing_campaigns
BULK INSERT stg.marketing_campaigns
FROM 'G:\Big Data Projects\MyntraData\marketing_campaigns.csv'
WITH ( FIELDTERMINATOR=',', ROWTERMINATOR='\n', FIRSTROW=2, CODEPAGE='65001', TABLOCK );



-- Verify row counts
SELECT 'customers'          AS tbl, COUNT(*) AS rows FROM stg.customers
UNION ALL
SELECT 'products',           COUNT(*) FROM stg.products
UNION ALL
SELECT 'orders',             COUNT(*) FROM stg.orders
UNION ALL
SELECT 'order_items',        COUNT(*) FROM stg.order_items
UNION ALL
SELECT 'returns',            COUNT(*) FROM stg.returns
UNION ALL
SELECT 'sellers',            COUNT(*) FROM stg.sellers
UNION ALL
SELECT 'product_seller',     COUNT(*) FROM stg.product_seller
UNION ALL
SELECT 'reviews',            COUNT(*) FROM stg.reviews
UNION ALL
SELECT 'marketing_campaigns',COUNT(*) FROM stg.marketing_campaigns;


-- Audit customers
SELECT
    COUNT(*)                                     AS TotalRows,
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END)     AS NullEmails,
    SUM(CASE WHEN LEN(Phone) < 10 THEN 1 ELSE 0 END)   AS ShortPhones,
    SUM(CASE WHEN DOB = '00-00-0000' THEN 1 ELSE 0 END) AS BadDOB,
    SUM(CASE WHEN LTRIM(RTRIM(FirstName)) <> FirstName THEN 1 ELSE 0 END) AS LeadingSpaces
FROM stg.customers;

-- Audit products
SELECT
    SUM(CASE WHEN TRY_CAST(SellingPrice AS DECIMAL(12,2)) < 0 THEN 1 ELSE 0 END) AS NegativePrice,
    SUM(CASE WHEN TRY_CAST(Rating AS DECIMAL(3,1)) > 5 THEN 1 ELSE 0 END)        AS InvalidRating,
    SUM(CASE WHEN TRY_CAST(StockQty AS INT) < 0 THEN 1 ELSE 0 END)               AS NegativeStock,
    SUM(CASE WHEN MRP IS NULL THEN 1 ELSE 0 END)                                  AS NullMRP
FROM stg.products;



INSERT INTO clean.customers (
    CustomerID, FirstName, LastName, Email, Phone,
    City, State, Pincode, DOB, Gender, JoinDate, LoyaltyTier
)
SELECT
    TRY_CAST(CustomerID AS INT)                       AS CustomerID,
    INITCAP_WORKAROUND = UPPER(LEFT(LTRIM(RTRIM(FirstName)),1))
        + LOWER(SUBSTRING(LTRIM(RTRIM(FirstName)),2,100)),
    LTRIM(RTRIM(LastName))                            AS LastName,
    CASE WHEN Email LIKE '%@%.%' THEN LOWER(LTRIM(RTRIM(Email)))
         ELSE NULL END                                AS Email,
    CASE WHEN LEN(REPLACE(REPLACE(Phone,'+91-',''),'-','')) >= 10
         THEN Phone ELSE NULL END                     AS Phone,
    UPPER(LEFT(LTRIM(RTRIM(City)),1))
        + LOWER(SUBSTRING(LTRIM(RTRIM(City)),2,100)) AS City,
    LTRIM(RTRIM(State))                               AS State,
    CASE WHEN LEN(Pincode) = 6
              AND TRY_CAST(Pincode AS INT) IS NOT NULL
         THEN Pincode ELSE NULL END                   AS Pincode,
    CASE WHEN TRY_CAST(DOB AS DATE) IS NOT NULL
         THEN TRY_CAST(DOB AS DATE) ELSE NULL END     AS DOB,
    CASE WHEN UPPER(Gender) IN ('MALE','FEMALE','OTHER') THEN UPPER(LEFT(Gender,1))+LOWER(SUBSTRING(Gender,2,10))
         WHEN UPPER(Gender) = 'M' THEN 'Male'
         WHEN UPPER(Gender) = 'F' THEN 'Female'
         ELSE 'Unknown' END                           AS Gender,
    TRY_CAST(JoinDate AS DATE)                        AS JoinDate,
    ISNULL(LoyaltyTier,'Bronze')                      AS LoyaltyTier
FROM stg.customers
WHERE TRY_CAST(CustomerID AS INT) IS NOT NULL;


DROP TABLE IF EXISTS clean.products;

create table clean.products (
    ProductID INT PRIMARY KEY,
    Category NVARCHAR(100),
    SubCategory NVARCHAR(100),
    Brand NVARCHAR(100),
    ProductName NVARCHAR(255),
    MRP DECIMAL(12,2),
    SellingPrice DECIMAL(12,2),
    Discount DECIMAL(5,2),
    StockQty INT,
    Size NVARCHAR(50),
    Rating DECIMAL(3,1)
);

create table clean.orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    Status NVARCHAR(50),
    PaymentMode NVARCHAR(50),
    DiscountCode NVARCHAR(50),
    ShipCity NVARCHAR(100),
    ShipPincode NVARCHAR(10),
    OrderTotal DECIMAL(12,2),
    DeliveryDate DATE,
    Channel NVARCHAR(50)
);

create table clean.order_items (
    ItemID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(12,2),
    DiscountPct DECIMAL(5,2),
    LineTotal DECIMAL(12,2)
);

create table clean.returns (
    ReturnID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    ReturnReason NVARCHAR(200),
    ReturnDate DATE,
    RefundAmount DECIMAL(12,2),
    RefundStatus NVARCHAR(50)
);

create table clean.sellers (
    SellerID INT PRIMARY KEY,
    SellerName NVARCHAR(200),
    City NVARCHAR(100),
    State NVARCHAR(100),
    GST NVARCHAR(50),
    SellerRating DECIMAL(3,1),
    JoinDate DATE,
    PrimaryCategory NVARCHAR(100)
);

create table clean.product_seller (
    PSID INT PRIMARY KEY,
    ProductID INT,
    SellerID INT,
    StockWithSeller INT,
    LeadTimeDays INT
);

CREATE TABLE clean.reviews (
    ReviewID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    OrderID INT,
    Rating DECIMAL(3,1),
    ReviewText NVARCHAR(MAX),
    ReviewDate DATE,
    HelpfulVotes INT,
    VerifiedPurchase BIT
);

create table clean.marketing_campaigns (
    CampaignID INT PRIMARY KEY,
    CustomerID INT,
    CampaignName NVARCHAR(200),
    Channel NVARCHAR(100),
    SentDate DATE,
    Opened BIT,
    Clicked BIT,
    Converted BIT,
    SpendPerContact DECIMAL(12,2)
);

--clean.products
INSERT INTO clean.products (
    ProductID, Category, SubCategory, Brand, ProductName,
    MRP, Discount, SellingPrice, Size, Rating, StockQty
)
SELECT
    TRY_CAST(ProductID AS INT)                          AS ProductID,
    LTRIM(RTRIM(Category))                              AS Category,
    LTRIM(RTRIM(SubCategory))                           AS SubCategory,
    LTRIM(RTRIM(Brand))                                 AS Brand,
    LTRIM(RTRIM(ProductName))                           AS ProductName,
    ABS(TRY_CAST(MRP AS DECIMAL(12,2)))                 AS MRP,
    TRY_CAST(Discount AS INT)                        AS DiscountPct,
    ABS(TRY_CAST(SellingPrice AS DECIMAL(12,2)))        AS SellingPrice,
    LTRIM(RTRIM(Size))                                  AS Size,
    CASE WHEN TRY_CAST(Rating AS DECIMAL(3,1)) BETWEEN 1 AND 5
         THEN TRY_CAST(Rating AS DECIMAL(3,1)) ELSE NULL END AS Rating,
    CASE WHEN TRY_CAST(StockQty AS INT) >= 0
         THEN TRY_CAST(StockQty AS INT) ELSE 0 END      AS StockQty
FROM stg.products
WHERE TRY_CAST(ProductID AS INT) IS NOT NULL;

--clean.orders
INSERT INTO clean.orders (
    OrderID, CustomerID, OrderDate, Status, PaymentMode, DiscountCode, ShipCity, ShipPincode, OrderTotal, DeliveryDate, Channel
)
SELECT
    TRY_CAST(OrderID AS INT)                          AS OrderID,
    TRY_CAST(CustomerID AS INT)                       AS CustomerID,
    TRY_CAST(OrderDate AS DATE)                       AS OrderDate,
    UPPER(LEFT(LTRIM(RTRIM(Status)),1))
        + LOWER(SUBSTRING(LTRIM(RTRIM(Status)),2,50)) AS Status,
    LTRIM(RTRIM(PaymentMode))                         AS PaymentMode,
    DiscountCode,
    LTRIM(RTRIM(ShipCity))                            AS ShipCity,
    ShipPincode,
    ABS(TRY_CAST(OrderTotal AS DECIMAL(12,2)))        AS OrderTotal,
    TRY_CAST(DeliveryDate AS DATE)                    AS DeliveryDate,
    LTRIM(RTRIM(Channel))                             AS Channel
FROM stg.orders
WHERE TRY_CAST(OrderID AS INT) IS NOT NULL
  AND TRY_CAST(CustomerID AS INT) IS NOT NULL;

--clean.order_items
INSERT INTO clean.order_items (
    ItemID, OrderID, ProductID, Quantity, UnitPrice, DiscountPct, LineTotal
)
SELECT
    TRY_CAST(ItemID AS INT)       AS ItemID,
    TRY_CAST(OrderID AS INT)      AS OrderID,
    TRY_CAST(ProductID AS INT)    AS ProductID,
    CASE WHEN TRY_CAST(Quantity AS INT) > 0
         THEN TRY_CAST(Quantity AS INT) ELSE 1 END   AS Quantity,
    ABS(TRY_CAST(UnitPrice AS DECIMAL(12,2)))         AS UnitPrice,
    TRY_CAST(DiscountPct AS INT)                      AS DiscountPct,
    ABS(TRY_CAST(LineTotal AS DECIMAL(14,2)))         AS LineTotal
FROM stg.order_items
WHERE TRY_CAST(ItemID AS INT) IS NOT NULL
  AND TRY_CAST(UnitPrice AS DECIMAL(12,2)) IS NOT NULL;

--clean.returns, clean.sellers, clean.reviews — pattern (apply same logic)
-- returns: fix negative refunds, null reasons
INSERT INTO clean.returns (
    ReturnID, OrderID, ProductID, ReturnReason, ReturnDate, RefundAmount, RefundStatus
)
SELECT TRY_CAST(ReturnID AS INT), TRY_CAST(OrderID AS INT),
       TRY_CAST(ProductID AS INT),
       ISNULL(ReturnReason,'Not Specified') AS ReturnReason,
       TRY_CAST(ReturnDate AS DATE),
       ABS(TRY_CAST(RefundAmount AS DECIMAL(12,2))),
       LTRIM(RTRIM(RefundStatus))
FROM stg.returns WHERE TRY_CAST(ReturnID AS INT) IS NOT NULL;

-- reviews: clamp rating 1-5
INSERT INTO clean.reviews (
    ReviewID, CustomerID, ProductID, OrderID, Rating, ReviewText, ReviewDate, HelpfulVotes, VerifiedPurchase
)
SELECT 
    TRY_CAST(ReviewID AS INT), 
    TRY_CAST(CustomerID AS INT),
    TRY_CAST(ProductID AS INT), 
    TRY_CAST(OrderID AS INT),
    CASE 
        WHEN TRY_CAST(Rating AS INT) BETWEEN 1 AND 5 
        THEN TRY_CAST(Rating AS INT) 
        ELSE NULL 
    END,
    LTRIM(RTRIM(ReviewText)),
    TRY_CAST(ReviewDate AS DATE),
    TRY_CAST(HelpfulVotes AS INT),
    CASE 
        WHEN UPPER(VerifiedPurchase) = 'YES' THEN 1 
        ELSE 0 
    END
FROM stg.reviews 
WHERE TRY_CAST(ReviewID AS INT) IS NOT NULL;


--3.3  Remove Duplicates with ROW_NUMBER()
-- Remove duplicate customers (keep first by CustomerID)
WITH cte AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY Email ORDER BY CustomerID) AS rn
    FROM clean.customers
    WHERE Email IS NOT NULL
)
DELETE FROM cte WHERE rn > 1;

-- Remove duplicate product-seller combinations
WITH cte AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY ProductID, SellerID ORDER BY PSID) AS rn
    FROM clean.product_seller
)
DELETE FROM cte WHERE rn > 1;

--3.4  Create Power BI Ready Views
-- View 1: Sales Overview
CREATE OR ALTER VIEW vw_SalesOverview AS
SELECT
    o.OrderID, o.OrderDate, o.Status, o.PaymentMode, o.Channel,
    o.ShipCity, o.OrderTotal,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.City AS CustomerCity, c.LoyaltyTier,
    YEAR(o.OrderDate)  AS OrderYear,
    MONTH(o.OrderDate) AS OrderMonth,
    FORMAT(o.OrderDate,'yyyy-MM') AS YearMonth
FROM clean.orders o
JOIN clean.customers c ON o.CustomerID = c.CustomerID;
GO

-- View 2: Product Sales
CREATE OR ALTER VIEW vw_ProductSales AS
SELECT
    oi.ItemID, oi.OrderID, oi.Quantity, oi.UnitPrice, oi.LineTotal,
    p.ProductName, p.Category, p.SubCategory, p.Brand
    , p.Size, p.Rating AS ProductRating,
    o.OrderDate, o.Status
FROM clean.order_items oi
JOIN clean.products  p ON oi.ProductID = p.ProductID
JOIN clean.orders    o ON oi.OrderID   = o.OrderID;
GO

-- View 3: Returns Analysis
CREATE OR ALTER VIEW vw_ReturnsAnalysis AS
SELECT
    r.ReturnID, r.ReturnDate, r.ReturnReason, r.RefundAmount, r.RefundStatus,
    p.Category, p.Brand, p.ProductName,
    o.OrderDate, o.Channel, o.PaymentMode,
    c.City, c.LoyaltyTier
FROM clean.returns r
JOIN clean.orders   o ON r.OrderID   = o.OrderID
JOIN clean.products p ON r.ProductID = p.ProductID
JOIN clean.customers c ON o.CustomerID = c.CustomerID;
GO

-- View 4: Customer 360
CREATE OR ALTER VIEW vw_Customer360 AS
SELECT
    c.CustomerID, c.FirstName + ' ' + c.LastName AS CustomerName,
    c.City, c.State, c.Gender, c.LoyaltyTier,
    COUNT(DISTINCT o.OrderID)      AS TotalOrders,
    SUM(o.OrderTotal)              AS TotalSpend,
    AVG(o.OrderTotal)              AS AvgOrderValue,
    MAX(o.OrderDate)               AS LastOrderDate,
    COUNT(DISTINCT rv.ReviewID)    AS TotalReviews,
    AVG(CAST(rv.Rating AS FLOAT))  AS AvgRatingGiven
FROM clean.customers c
LEFT JOIN clean.orders  o  ON c.CustomerID = o.CustomerID
LEFT JOIN clean.reviews rv ON c.CustomerID = rv.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName,
         c.City, c.State, c.Gender, c.LoyaltyTier;
GO

-- View 5: Seller Performance
CREATE OR ALTER VIEW vw_SellerPerformance AS
SELECT
    s.SellerID, s.SellerName, s.City, s.PrimaryCategory,
    s.SellerRating,
    COUNT(DISTINCT ps.ProductID)    AS ProductsListed,
    SUM(ps.StockWithSeller)         AS TotalStock
FROM clean.sellers s
LEFT JOIN clean.product_seller ps ON s.SellerID = ps.SellerID
GROUP BY s.SellerID, s.SellerName, s.City, s.PrimaryCategory, s.SellerRating;
GO
