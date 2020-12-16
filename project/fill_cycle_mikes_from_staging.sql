USE [CycleMikes]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE [dbo].[Sales];
DROP TABLE [dbo].[Customer];
DROP TABLE [dbo].[Date];
DROP TABLE [dbo].[Geography];
DROP TABLE [dbo].[Product];
DROP TABLE [dbo].[ProductSubcategory];
DROP TABLE [dbo].[ProductCategory];
DROP TABLE [dbo].[Territory];

CREATE TABLE [dbo].[Geography](
	[GeographyKey] [int] PRIMARY KEY,
	[City] [varchar](50) NULL,
	--[StateProvinceCode] [varchar](50) NULL,
	[ProvinceName] [varchar](50) NULL,
	[CountryCode] [varchar](50) NULL,
	[CountryName] [varchar](50) NULL,
	--[PostalCode] [varchar](50) NULL,
	[SalesTerritoryKey] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Geography]([GeographyKey],[City],[ProvinceName],
	[CountryCode],[CountryName],[SalesTerritoryKey])
SELECT [GeographyKey],[City],[ProvinceName],[CountryRegionCode],
	[CountryName],[SalesTerritoryKey]
FROM CycleMikes_Staging.dbo.headquarters_geography
EXCEPT
SELECT [GeographyKey],[City],[ProvinceName],
	[CountryCode],[CountryName],[SalesTerritoryKey] FROM [dbo].[Geography]
GO

INSERT INTO [dbo].[Geography]([GeographyKey],[City],[ProvinceName],
	[CountryCode],[CountryName],[SalesTerritoryKey])
SELECT DISTINCT [GeographyKey],[City],[ProvinceName],
	[CountryCode],[CountryName],[SalesTerritoryKey]
FROM CycleMikes_Staging.dbo.europe_sales
EXCEPT
SELECT [GeographyKey],[City],[ProvinceName],
	[CountryCode],[CountryName],[SalesTerritoryKey] FROM [dbo].[Geography]
GO

CREATE TABLE [dbo].[Date](
	[DateKey] [int] PRIMARY KEY,
	--[FullDateAlternateKey] [varchar](50) NULL,
	[DayNumberOfWeek] [int] NULL,
	[DayNameOfWeek] [varchar](50) NULL,
	[DayNumberOfMonth] [int] NULL,
	--[DayNumberOfYear] [varchar](50) NULL,
	--[WeekNumberOfYear] [varchar](50) NULL,
	[MonthNumberOfYear] [int] NULL,
	[MonthName] [varchar](50) NULL,
	[CalendarYear] [int] NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Date]([DateKey],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],[CalendarYear])
SELECT [DateKey],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],RTRIM(REPLACE(REPLACE([CalendarYear], char(13),''),char(10),''))
FROM CycleMikes_Staging.dbo.headquarters_date
EXCEPT
SELECT [DateKey],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],[CalendarYear] FROM [dbo].[Date]
GO

INSERT INTO [dbo].[Date]([DateKey],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],[CalendarYear])
SELECT DISTINCT [DateOfOrder],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],[CalendarYear]
FROM CycleMikes_Staging.dbo.europe_sales
EXCEPT
SELECT [DateKey],[DayNumberOfWeek],[DayNameOfWeek],[DayNumberOfMonth],[MonthNumberOfYear],[MonthName],[CalendarYear] FROM [dbo].[Date]
GO

CREATE TABLE [dbo].[Customer](
	[CustomerKey] [int] PRIMARY KEY,
	[GeographyKey] [int] NULL FOREIGN KEY REFERENCES [dbo].[Geography] ([GeographyKey]),
	[FirstName] [varchar](50) NULL,
	[LastName] [varchar](50) NULL,
	[BirthDate] [varchar](50) NULL,
	[MaritalStatus] [varchar](50) NULL,
	[Gender] [varchar](50) NULL,
	[EmailAddress] [varchar](50) NULL,
	[YearlyIncome] [decimal](18,4) NULL,
	[TotalChildren] [int] NULL,
	[NumberChildrenAtHome] [int] NULL,
	[Education] [varchar](50) NULL,
	[Occupation] [varchar](50) NULL,
	[HouseOwnerFlag] [varchar](1) NULL,
	[NumberCarsOwned] [int] NULL,
	[AddressLine1] [varchar](50) NULL,
	[AddressLine2] [varchar](50) NULL,
	[Customer_Phone] [varchar](50) NULL,
	[DateFirstPurchase] [varchar](50) NULL,
	[CommuteDistance] [varchar](50) NULL,
	[BikeBuyer] [int] NULL,
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Customer]([CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],[BikeBuyer])
SELECT [CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],0
FROM CycleMikes_Staging.dbo.headquarters_customer
EXCEPT
SELECT [CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],[BikeBuyer]
FROM [dbo].[Customer]
GO

UPDATE [dbo].[Customer]
SET [BikeBuyer] = 1
FROM CycleMikes.dbo.Customer
JOIN CycleMikes_Staging.dbo.headquarters_customer c
ON Customer.CustomerKey = c.CustomerKey
JOIN CycleMikes_Staging.dbo.headquarters_sales s
ON c.CustomerKey = s.CustomerKey
JOIN CycleMikes_Staging.dbo.headquarters_product p
ON s.ProductKey = p.ProductKey
JOIN CycleMikes_Staging.dbo.headquarters_product_subcategory ps
ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
JOIN CycleMikes_Staging.dbo.headquarters_product_category pc
ON ps.ProductCategoryKey = pc.ProductCategoryKey
WHERE c.CustomerKey = Customer.CustomerKey AND ps.ProductCategoryKey = 1
GO

INSERT INTO [dbo].[Customer]([CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],[BikeBuyer])
SELECT DISTINCT [CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],0
FROM CycleMikes_Staging.dbo.europe_sales s
EXCEPT
SELECT [CustomerKey],[GeographyKey],[FirstName],[LastName],[BirthDate],
	[MaritalStatus],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],
	[Education],[Occupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],
	[AddressLine2],[Customer_Phone],[DateFirstPurchase],[CommuteDistance],[BikeBuyer]
FROM [dbo].[Customer]
GO

UPDATE [dbo].[Customer]
SET [BikeBuyer] = 1
FROM CycleMikes_Staging.dbo.europe_sales s
JOIN CycleMikes_Staging.dbo.europe_product p
ON s.ProductKey = p.ProductKey
WHERE s.CustomerKey = Customer.CustomerKey AND p.ProductCategoryKey = 1
GO

CREATE TABLE [dbo].[ProductCategory](
	[ProductCategoryKey] [int] PRIMARY KEY,
	[ProductCategoryName] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[ProductCategory]([ProductCategoryKey],[ProductCategoryName])
SELECT [ProductCategoryKey],RTRIM(REPLACE(REPLACE([ProductCategoryName], char(13),''),char(10),''))
FROM CycleMikes_Staging.dbo.headquarters_product_category
EXCEPT
SELECT [ProductCategoryKey],[ProductCategoryName] FROM [dbo].[ProductCategory]
GO

INSERT INTO [dbo].[ProductCategory]([ProductCategoryKey],[ProductCategoryName])
SELECT DISTINCT [ProductCategoryKey],RTRIM(REPLACE(REPLACE([ProductCategoryName], char(13),''),char(10),''))
FROM CycleMikes_Staging.dbo.europe_product
EXCEPT
SELECT [ProductCategoryKey],[ProductCategoryName] FROM [dbo].[ProductCategory]
GO

CREATE TABLE [dbo].[ProductSubcategory](
	[ProductSubcategoryKey] [int] PRIMARY KEY,
	[ProductCategoryKey] [int] FOREIGN KEY REFERENCES [dbo].[ProductCategory] ([ProductCategoryKey]),
	[ProductSubcategoryName] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[ProductSubcategory]([ProductSubcategoryKey],[ProductCategoryKey],[ProductSubcategoryName])
SELECT [ProductSubcategoryKey],[ProductCategoryKey],RTRIM(REPLACE(REPLACE([ProductSubcategoryName], char(13),''),char(10),''))
FROM CycleMikes_Staging.dbo.headquarters_product_subcategory
EXCEPT
SELECT [ProductSubcategoryKey],[ProductCategoryKey],[ProductSubcategoryName] FROM [dbo].[ProductSubcategory]
GO

INSERT INTO [dbo].[ProductSubcategory]([ProductSubcategoryKey],[ProductCategoryKey],[ProductSubcategoryName])
SELECT DISTINCT [ProductSubcategoryKey],[ProductCategoryKey],[ProductSubcategoryName]
FROM CycleMikes_Staging.dbo.europe_product
EXCEPT
SELECT [ProductSubcategoryKey],[ProductCategoryKey],[ProductSubcategoryName] FROM [dbo].[ProductSubcategory]
GO

CREATE TABLE [dbo].[Product](
	[ProductKey] [int] PRIMARY KEY,
	[ProductSubcategoryKey] [int] FOREIGN KEY REFERENCES [dbo].[ProductSubcategory] ([ProductSubcategoryKey]),
	[ProductName] [varchar](50) NULL,
	[StandardCost] [varchar](50) NULL,
	[Color] [varchar](50) NULL,
	[ListPrice] [varchar](50) NULL,
	[Size] [varchar](50) NULL,
	[DealerPrice] [varchar](50) NULL,
	[ModelName] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Product]([ProductKey],[ProductSubcategoryKey],[ProductName],
	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName])
SELECT [ProductKey],[ProductSubcategoryKey],[ProductName],
	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName]
FROM CycleMikes_Staging.dbo.headquarters_product
EXCEPT
SELECT [ProductKey],[ProductSubcategoryKey],[ProductName],
	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName] FROM [dbo].[Product]
GO

INSERT INTO [dbo].[Product]([ProductKey],[ProductSubcategoryKey],[ProductName],
	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName])
SELECT DISTINCT [ProductKey],[ProductSubcategoryKey],[ProductName],
	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName]
FROM CycleMikes_Staging.dbo.europe_product ep
WHERE ep.[ProductKey] NOT IN (SELECT [ProductKey] FROM [dbo].[Product])
-- The EXCEPT didn't work here because the product names differed because one had its comma removed.
--EXCEPT
--SELECT [ProductKey],[ProductSubcategoryKey],[ProductName],
--	[StandardCost],[Color],[ListPrice],[Size],[DealerPrice],[ModelName] FROM [dbo].[Product]
GO

CREATE TABLE [dbo].[Territory](
	[SalesTerritoryKey] [int] PRIMARY KEY,
	[SalesTerritoryRegion] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Territory]([SalesTerritoryKey],[SalesTerritoryRegion])
SELECT [SalesTerritoryKey],[SalesTerritoryRegion]
FROM CycleMikes_Staging.dbo.headquarters_territory
EXCEPT
SELECT [SalesTerritoryKey],[SalesTerritoryRegion] FROM [dbo].[Territory]
GO

INSERT INTO [dbo].[Territory]([SalesTerritoryKey],[SalesTerritoryRegion])
SELECT DISTINCT [SalesTerritoryKey],[SalesTerritoryRegion]
FROM CycleMikes_Staging.dbo.europe_territory
EXCEPT
SELECT [SalesTerritoryKey],[SalesTerritoryRegion] FROM [dbo].[Territory]
GO

CREATE TABLE [dbo].[Sales](
	[ProductKey] [int] FOREIGN KEY REFERENCES [dbo].[Product] ([ProductKey]),
	[OrderDateKey] [int] FOREIGN KEY REFERENCES [dbo].[Date] ([DateKey]),
	-- The incoming format is incorrect for some of these. Mongo changed them. May want to change them back.
	[DueDate] [varchar](50) NULL, -- [int] FOREIGN KEY REFERENCES [dbo].[Date] ([DateKey]),
	[CustomerKey] [int] FOREIGN KEY REFERENCES [dbo].[Customer] ([CustomerKey]),
	[ShipDate] [varchar](50) NULL, -- [int] FOREIGN KEY REFERENCES [dbo].[Date] ([DateKey]),
	[SalesTerritoryKey] [int] FOREIGN KEY REFERENCES [dbo].[Territory] ([SalesTerritoryKey]),
	[SalesOrderNumber] [varchar](50) NULL,
	[SalesOrderLineNumber] [varchar](50) NULL,
	[OrderQuantity] [int] NULL,
	--[UnitPrice] [decimal](18,4) NULL,
	--[ExtendedAmount] [decimal](18,4) NULL,
	--[ProductStandardCost] [decimal](18,4) NULL,
	[TotalProductCost] [decimal](18,4) NULL,
	[SalesAmount] [decimal](18,4) NULL,
	[TaxAmt] [decimal](18,4) NULL,
	[CustomerPONumber] [varchar](50) NULL
) ON [PRIMARY]
GO

INSERT INTO [dbo].[Sales]([ProductKey],[OrderDateKey],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber])
SELECT [ProductKey],[OrderDateKey],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber]
FROM CycleMikes_Staging.dbo.headquarters_sales
EXCEPT
SELECT [ProductKey],[OrderDateKey],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber] FROM [dbo].[Sales]
GO

INSERT INTO [dbo].[Sales]([ProductKey],[OrderDateKey],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber])
SELECT DISTINCT [ProductKey],[DateOfOrder],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber]
FROM CycleMikes_Staging.dbo.europe_sales
EXCEPT
SELECT [ProductKey],[OrderDateKey],[DueDate],[CustomerKey],
	[ShipDate],[SalesTerritoryKey],[SalesOrderNumber],[SalesOrderLineNumber],
	[OrderQuantity],
	[TotalProductCost],[SalesAmount],[TaxAmt],[CustomerPONumber] FROM [dbo].[Sales]
GO
