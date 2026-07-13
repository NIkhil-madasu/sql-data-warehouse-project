/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from created datasets of 6 tables
    It performs the following action:
    - Truncates the bronze tables before loading data.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/


create or alter procedure bronze.load_bronze as
begin
     declare @start_time DATETIME, @end_time DATETIME;
     begin try
		  print '=======================================================';
		  print 'LOADING BRONZE LAYER';
		  print '=======================================================';

          SET @start_time = GETDATE();
		  PRINT '>>TRUNCATING TABLE :bronze.cust_info';
		  TRUNCATE TABLE bronze.cust_info;

		PRINT'>>INSERTING DATA INTO:bronze.cust_info';
		INSERT INTO bronze.cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date) VALUES
(1, 'AW001001', '  vivek', '  nair', 'S', 'F', '2025-10-23'),
(2, 'AW001002', 'kavya', '  reddy', 'S', 'F', '2025-10-06'),
(3, 'AW001003', '  simran', 'joshi   ', 'S', 'F', '2025-10-06'),
(4, 'AW001004', 'dinesh', 'chowdhury   ', 'S', 'M', '2025-10-16'),
(5, 'AW001005', '  arjun', 'gupta   ', 'S', 'F', '2025-10-07'),
(6, 'AW001006', 'ma  hesh', 'gupta', 'S', 'M', '2025-10-26'),
(7, 'AW001007', 'monika', 'yadav', 'M', NULL, '2025-10-13'),
(8, 'AW001008', 'seema   ', 'sharma', 'M', 'F', '2025-10-20'),
(9, 'AW001009', 'vivek', 'singh   ', 'S', 'F', '2025-10-26'),
(10, 'AW001010', 'suresh', 'si  ngh', 'S', 'F', '2025-10-14'),
(11, 'AW001011', 'vivek', 'patel', 'M', 'M', '2025-10-13'),
(12, 'AW001012', 'priya', 'mishra   ', 'S', 'M', '2025-10-16'),
(13, 'AW001013', 'divya', 'mishra', 'M', 'F', '2025-10-10'),
(14, 'AW001014', 'swati', 'joshi   ', 'M', 'M', '2025-10-18'),
(15, 'AW001015', '  shreya', 'ku  mar', NULL, NULL, '2025-10-09'),
(16, 'AW001016', 'pooja', 'chowdhury', 'S', 'M', '2025-10-20'),
(17, 'AW001017', 'komal   ', 'joshi', NULL, 'M', '2025-10-23'),
(18, 'AW001018', 'dinesh   ', '  verma', 'S', 'F', '2025-10-14'),
(19, 'AW001019', 'harish', 'menon', 'M', 'F', '2025-10-26'),
(20, 'AW001020', 'harish', 'kumar   ', 'S', 'M', '2025-10-22'),
(21, 'AW001021', 'nikhil', '  pillai', 'M', 'M', '2025-10-15'),
(22, 'AW001022', '  swati', 'desai', NULL, 'F', '2025-10-08'),
(23, 'AW001023', 'di  nesh', '  kumar', 'S', 'F', '2025-10-14'),
(24, 'AW001024', 'komal', 'chowdhury', 'M', 'M', '2025-10-12'),
(25, 'AW001025', 'asha   ', 'yadav   ', 'S', 'F', '2025-10-13'),
(26, 'AW001026', 'suresh   ', 'desai', 'M', NULL, '2025-10-26'),
(27, 'AW001027', '  anjali', 'reddy', NULL, 'F', '2025-10-21'),
(28, 'AW001028', 'di  vya', 'desai', 'S', 'F', '2025-10-19'),
(29, 'AW001029', '  rohit', 'chowdhury   ', 'S', 'M', '2025-10-07'),
(30, 'AW001030', 'juhi', '  gupta', 'M', 'M', '2025-10-09'),
(31, 'AW001031', '  swati', 'joshi   ', 'S', 'F', '2025-10-13'),
(32, 'AW001032', 'suresh   ', '  joshi', 'M', 'F', '2025-10-06'),
(33, 'AW001033', 'kavya', '  patel', 'S', 'F', '2025-10-18'),
(34, 'AW001034', '  anjali', 'madasu', 'S', 'M', '2025-10-20'),
(35, 'AW001035', 'sanjay   ', 'joshi', 'S', 'F', '2025-10-12'),
(36, 'AW001036', 'anjali', '  joshi', 'S', NULL, '2025-10-21'),
(37, 'AW001037', 'harish', '  menon', 'M', NULL, '2025-10-11'),
(38, 'AW001038', 'suresh', 'patel   ', 'M', 'M', '2025-10-24'),
(39, 'AW001039', '  tarun', 'sharma   ', 'M', 'F', '2025-10-14'),
(40, 'AW001040', 'divya', '  verma', 'S', 'M', '2025-10-15'),
(41, 'AW001041', 'ruchi   ', '  sharma', 'M', 'M', '2025-10-09'),
(42, 'AW001042', 'su  resh', 'menon   ', 'M', 'M', '2025-10-13'),
(43, 'AW001043', 'shreya   ', 'das', 'M', 'M', '2025-10-26'),
(44, 'AW001044', '  komal', 'joshi   ', 'M', 'M', '2025-10-10'),
(45, 'AW001045', '  karan', 'gupta', 'S', 'F', '2025-10-12'),
(46, 'AW001046', 'asha   ', 'na  ir', 'S', 'M', '2025-10-07'),
(47, 'AW001047', 'kavya', '  nair', 'S', 'F', '2025-10-14'),
(48, 'AW001048', 'amit', 'joshi', 'M', 'F', '2025-10-10'),
(49, 'AW001049', '  yash', 'yadav', 'S', 'F', '2025-10-15'),
(50, 'AW001050', 'shreya', 'reddy', 'S', 'F', '2025-10-09'),
(51, 'AW001051', 'gaurav', 'chowdhury', 'M', 'M', '2025-10-13'),
(52, 'AW001052', 'amit', 'singh', NULL, 'M', '2025-10-16'),
(53, 'AW001053', 'vinay', 'patel   ', 'M', 'F', '2025-10-07'),
(54, 'AW001054', '  abhishek', 'das   ', 'M', 'M', '2025-10-13'),
(55, 'AW001055', 'rahul', 'mishra   ', 'M', 'M', '2025-10-14'),
(56, 'AW001056', 'gaurav', 'mishra', 'M', 'F', '2025-10-06'),
(57, 'AW001057', 'neha', '  nair', 'M', 'F', '2025-10-09'),
(58, 'AW001058', 'tarun   ', 'verma   ', 'M', 'F', '2025-10-24'),
(59, 'AW001059', 'rohit   ', '  chowdhury', 'M', 'F', '2025-10-12'),
(60, 'AW001060', 'shreya   ', 'verma', 'M', 'F', '2025-10-15'),
(61, 'AW001061', 'harish   ', 'chowdhury   ', 'M', 'M', '2025-10-12'),
(62, 'AW001062', 'vinay', 'mishra', 'M', 'M', '2025-10-15'),
(63, 'AW001063', 'ni  sha', 'madasu   ', 'S', 'M', '2025-10-25'),
(64, 'AW001064', 'preeti   ', 'das   ', 'S', 'F', '2025-10-11'),
(65, 'AW001065', '  naveen', 'menon', 'M', NULL, '2025-10-13'),
(66, 'AW001066', 'juhi   ', '  rao', NULL, 'M', '2025-10-25'),
(67, 'AW001067', '  seema', 'chowdhury', 'M', 'M', '2025-10-18'),
(68, 'AW001068', 'tanvi   ', 'kumar', NULL, 'M', '2025-10-09'),
(69, 'AW001069', 'seema   ', 'singh', 'M', 'F', '2025-10-23'),
(70, 'AW001070', 'swati', 'gu  pta', 'M', 'M', '2025-10-23'),
(71, 'AW001071', 'tarun   ', 'das', 'M', 'F', '2025-10-23'),
(72, 'AW001072', 'sameer', 'pi  llai', 'M', 'M', '2025-10-14'),
(73, 'AW001073', 'seema', 'pillai', 'S', NULL, '2025-10-15'),
(74, 'AW001074', 'swati   ', 'verma', NULL, 'F', '2025-10-18'),
(75, 'AW001075', '  siddharth', '  rao', 'S', 'F', '2025-10-19'),
(76, 'AW001076', '  anjali', 'chowdhury   ', 'M', 'M', '2025-10-06'),
(77, 'AW001077', 'dinesh', '  pillai', 'S', 'M', '2025-10-19'),
(78, 'AW001078', 'yash', 'joshi', 'M', 'F', '2025-10-14'),
(79, 'AW001079', 'pa  llavi', 'mishra   ', 'M', 'F', '2025-10-11'),
(80, 'AW001080', 'ruchi', 'bo  se', 'M', 'M', '2025-10-06'),
(81, 'AW001081', 'kavya', 'kumar', 'S', 'F', '2025-10-16'),
(82, 'AW001082', 'di  vya', 'verma', 'S', 'M', '2025-10-19'),
(83, 'AW001083', 'karan', '  pillai', 'S', 'M', '2025-10-17'),
(84, 'AW001084', 'manoj', 'reddy', 'M', 'F', '2025-10-06'),
(85, 'AW001085', '  monika', 'ku  mar', 'S', 'M', '2025-10-20'),
(86, 'AW001086', 'siddharth   ', '  yadav', 'M', 'M', '2025-10-09'),
(87, 'AW001087', 'seema', '  iyer', NULL, 'F', '2025-10-18'),
(88, 'AW001088', 'nisha', '  rao', 'M', 'M', '2025-10-09'),
(89, 'AW001089', 'siddharth', 'bose', 'M', 'M', '2025-10-07'),
(90, 'AW001090', 'ga  urav', '  yadav', 'M', NULL, '2025-10-19'),
(91, 'AW001091', '  tanvi', 'yadav', 'M', 'M', '2025-10-19'),
(92, 'AW001092', 'sneha', 'nair', 'M', 'M', '2025-10-20'),
(93, 'AW001093', 'pallavi', 'desai   ', 'M', 'M', '2025-10-08'),
(94, 'AW001094', 'meera', 'patel', 'M', 'M', '2025-10-16'),
(95, 'AW001095', 'ra  hul', '  verma', 'S', 'M', '2025-10-16'),
(96, 'AW001096', 'meera', 'nair', 'S', 'F', '2025-10-13'),
(97, 'AW001097', 'mahesh   ', 'joshi', 'M', 'M', '2025-10-21'),
(98, 'AW001098', 'sameer', 'sharma   ', 'S', 'F', '2025-10-24'),
(99, 'AW001099', 'sh  reya', 'menon   ', 'M', 'M', '2025-10-17'),
(100, 'AW001100', 'si  ddharth', 'iyer   ', 'S', 'F', '2025-10-09');
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';
----------------------------------
          SET @start_time = GETDATE();
          PRINT '>>TRUNCATING TABLE :bronze.prd_info';
		  TRUNCATE TABLE bronze.prd_info;

		PRINT'>>INSERTING DATA INTO:bronze.prd_info';
		INSERT INTO bronze.prd_info (prd_id, prd_key, prd_num, prd_cost, prd_line, prd_start_date, prd_end_date) VALUES
(1, 'CO-RF-FR-R92B-58', 'HL Road Frame - Black - 58', NULL, 'R', '2003-07-01', NULL),
(2, 'CO-RF-FR-R92R-58', 'HL Road Frame - Red - 58', NULL, 'R', '2003-07-01', NULL),
(3, 'AC-HE-HL-U509-R', 'Sport-100 Helmet - Red', 12, 'S', '2011-07-01', '2007-12-28'),
(4, 'AC-HE-HL-U509-R', 'Sport-100 Helmet - Red', 14, 'S', '2012-07-01', '2008-12-27'),
(5, 'AC-HE-HL-U509-R', 'Sport-100 Helmet - Red', 13, 'S', '2013-07-01', NULL),
(6, 'AC-HE-HL-U509', 'Sport-100 Helmet - Black', 12, 'S', '2011-07-01', '2007-12-28'),
(7, 'AC-HE-HL-U509', 'Sport-100 Helmet - Black', 14, 'S', '2012-07-01', '2008-12-27'),
(8, 'AC-HE-HL-U509', 'Sport-100 Helmet - Black', 13, 'S', '2013-07-01', NULL),
(9, 'CL-SO-SO-B909-M', 'Mountain Bike Socks - M', 3, 'M', '2011-07-01', '2007-12-28'),
(10, 'CL-SO-SO-B909-L', 'Mountain Bike Socks - L', 3, 'M', '2011-07-01', '2007-12-28'),
(11, 'AC-HE-HL-U509-B', 'Sport-100 Helmet - Blue', 12, 'S', '2011-07-01', '2007-12-28'),
(12, 'MT-FR-FR-M783-40', 'HL Mountain Frame - Black - 40', 264, 'M', '2011-07-01', '2012-05-29'),
(13, 'MT-FR-FR-M783-42', 'HL Mountain Frame - Black - 42', 264, 'M', '2012-05-30', NULL),
(14, 'MT-FR-FR-M783-44', 'HL Mountain Frame - Silver - 44', 269, 'M', '2011-07-01', '2012-05-29'),
(15, 'RD-JE-JE-L890-S', 'Long-Sleeve Logo Jersey - S', 38, 'R', '2011-07-01', NULL),
(16, 'RD-JE-JE-L890-M', 'Long-Sleeve Logo Jersey - M', 38, 'R', '2011-07-01', NULL),
(17, 'RD-JE-JE-L890-L', 'Long-Sleeve Logo Jersey - L', 38, 'R', '2011-07-01', NULL),
(18, 'GL-GL-GL-A200-S', 'Half-Finger Gloves - S', 15, 'S', '2011-07-01', '2012-05-29'),
(19, 'GL-GL-GL-A200-M', 'Half-Finger Gloves - M', 15, 'S', '2011-07-01', NULL),
(20, 'GL-GL-GL-A200-L', 'Half-Finger Gloves - L', 15, 'S', '2011-07-01', NULL),
(21, 'TR-TI-TI-T100-26', 'Touring Tire - 26 inch', NULL, 'T', '2012-01-01', NULL),
(22, 'TR-TU-TU-T200-26', 'Touring Tube - 26 inch', NULL, 'T', '2012-01-01', NULL),
(23, 'RD-FR-FR-R928-52', 'LL Road Frame - Black - 52', 142, 'R', '2011-07-01', '2013-05-29'),
(24, 'RD-FR-FR-R928-58', 'LL Road Frame - Black - 58', 142, 'R', '2013-05-30', NULL),
(25, 'RD-FR-FR-R928-60', 'LL Road Frame - Red - 60', 147, 'R', '2011-07-01', NULL),
(26, 'MT-BO-BO-M400-38', 'Mountain Bottle Cage - 38', NULL, 'M', '2011-07-01', NULL),
(27, 'MT-BO-BO-M400-40', 'Mountain Bottle Cage - 40', NULL, 'M', '2011-07-01', NULL),
(28, 'TR-HE-HE-T500-U', 'Touring Helmet - Universal', 35, 'T', '2012-03-01', NULL),
(29, 'CL-SH-SH-S700-M', 'Bike Shorts - M', 45, 'S', '2011-07-01', '2013-01-15'),
(30, 'CL-SH-SH-S700-L', 'Bike Shorts - L', 45, 'S', '2013-01-16', NULL);
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';
------------------------------
          SET @start_time = GETDATE();
          PRINT '>>TRUNCATING TABLE :bronze.sls_details';
		  TRUNCATE TABLE bronze.sls_details;

	PRINT'>>INSERTING DATA INTO:bronze.sls_details';
		INSERT INTO bronze.sls_details
    (sls_ord_num, sls_prd_key, sls_cst_id, sls_ord_date, sls_ship_date, sls_due_date, sls_sales, sls_quantity, sls_price)
VALUES
('SO43697', 'BK-R93R-44', 1, 20101223, 20101230, 20110104, 3578, 1, 3578),
('SO43698', 'BK-R93R-44', 2, 20101223, 20101230, 20110104, 3578, 1, 3578),
('SO43699', 'BK-R50B-58', 3, 20101224, 20101231, 20110105, 699, 1, 699),
('SO43700', 'BK-M82S-38', 4, 20101224, 20101231, 20110105, 3400, 1, 3400),
('SO43701', 'BK-R93R-44', 5, 20101225, 20110101, 20110106, 3578, 1, 3578),
('SO43702', 'BK-M82B-48', 6, 20101225, 20110101, 20110106, 3375, 1, 3375),
('SO43703', 'BK-R93R-62', 7, 20101230, 20110106, 20110111, 3578, 1, 3578),
('SO43704', 'BK-M82B-48', 8, 20101230, 20110106, 20110111, 3375, 1, 3375),
('SO43705', 'BK-M82S-38', 9, 20101230, 20110106, 20110111, 3400, 1, 3400),
('SO43706', 'BK-R93R-48', 10, 20101231, 20110107, 20110112, 3578, 1, 3578),
('SO43707', 'BK-R93R-48', 11, 20101231, 20110107, 20110112, 3578, 1, 3578),
('SO43708', 'BK-R50R-52', 12, 20101231, 20110107, 20110112, 699, 1, 699),
('SO43709', 'BK-R93R-52', 13, 20101231, 20110107, 20110112, 3578, 1, 3578),
('SO43710', 'BK-R93R-56', 14, 20101231, 20110107, 20110112, 3578, 1, 3578),
('SO43711', 'BK-R93R-44', 15, 20110101, 20110108, 20110113, 3578, 1, 3578),
('SO43712', 'BK-R93R-44', 16, 20110101, 20110108, 20110113, 3578, 1, 3578),
('SO43713', 'BK-R93R-62', 17, 20110102, 20110109, 20110114, 3578, 1, 3578),
('SO43714', 'BK-R93R-44', 18, 20110102, 20110109, 20110114, 3578, 1, 3578),
('SO43715', 'BK-R93R-56', 19, 20110102, 20110109, 20110114, 3578, 1, 3578),
('SO43716', 'BK-R93R-44', 20, 20110102, 20110109, 20110114, 3578, 1, 3578),
('SO43717', 'BK-R50B-62', 21, 20110102, 20110109, 20110114, 699, 1, 699),
('SO43718', 'BK-R93R-44', 22, 20110103, 20110110, 20110115, 3578, 1, 3578),
('SO43719', 'BK-R93R-48', 23, 20110103, 20110110, 20110115, 3578, 1, 3578),
('SO43720', 'BK-R50B-58', 24, 20110103, 20110110, 20110115, 699, 1, 699),
('SO43721', 'BK-R93R-62', 25, 20110103, 20110110, 20110115, 3578, 1, 3578),
('SO43722', 'BK-R93R-48', 26, 20110104, 20110111, 20110116, 3578, 1, 3578),
('SO43723', 'BK-R50B-58', 27, 20110104, 20110111, 20110116, 699, 1, 699),
('SO43724', 'BK-R93R-44', 28, 20110104, 20110111, 20110116, 3578, 1, 3578),
('SO43725', 'BK-R93R-44', 29, 20110105, 20110112, 20110117, 3578, 1, 3578),
('SO43726', 'BK-R50B-58', 30, 20110105, 20110112, 20110117, 699, 1, 699),
('SO43727', 'BK-R93R-44', 31, 20110105, 20110112, 20110117, 3578, 1, 3578),
('SO43728', 'BK-R93R-52', 32, 20110106, 20110113, 20110118, 3578, 1, 3578),
('SO43729', 'BK-M82B-48', 33, 20110107, 20110114, 20110119, 3375, 1, 3375),
('SO43730', 'BK-R93R-62', 34, 20110108, 20110115, 20110120, 3578, 1, 3578),
('SO43731', 'BK-R93R-44', 35, 20110109, 20110116, 20110121, 3578, 1, 3578);
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';
----------------------------------
          SET @start_time = GETDATE();
          PRINT '>>TRUNCATING TABLE :bronze.demographics';
		  TRUNCATE TABLE bronze.demographics;

	PRINT'>>INSERTING DATA INTO:bronze.demographics';
	INSERT INTO bronze.demographics (cst_id, bdate, gender)
VALUES
('NASAW001001', '1968-09-13', 'M'),
('AW001002', '1974-05-18', 'F'),
('AW001003', '1975-09-30', NULL),
('NASAW001004', '1982-10-12', 'F'),
('AW001005', '1976-06-30', 'M'),
('AW001006', '1976-10-31', 'M'),
('NASAW001007', '1965-01-27', 'M'),
('AW001008', '1978-02-17', 'F'),
('AW001009', '1968-04-02', 'F'),
('NASAW001010', '1970-03-18', NULL),
('AW001011', '1980-07-25', 'M'),
('AW001012', '1981-07-27', 'F'),
('NASAW001013', '1974-02-15', 'F'),
('AW001014', '1965-10-07', NULL),
('AW001015', '1977-02-11', 'F'),
('NASAW001016', '1967-08-07', 'F'),
('AW001017', '1973-04-07', 'M'),
('AW001018', '1981-08-15', 'F'),
('NASAW001019', '1967-10-20', 'F'),
('AW001020', '1965-06-10', 'F'),
('AW001021', '1977-05-20', 'F'),
('NASAW001022', '1974-01-01', 'F'),
('AW001023', '1969-03-12', 'F'),
('AW001024', '1975-07-17', NULL),
('NASAW001025', '1970-04-09', 'M'),
('AW001026', '1969-06-05', 'M'),
('AW001027', '1983-01-04', NULL),
('NASAW001028', '1981-12-06', NULL),
('AW001029', '1978-01-10', NULL),
('AW001030', '1966-08-05', 'M'),
('NASAW001031', '1975-09-08', 'F'),
('AW001032', '1973-09-14', 'M'),
('AW001033', '1968-04-12', NULL),
('NASAW001034', '1974-03-10', 'M'),
('AW001035', '1975-02-15', 'F'),
('AW001036', '1972-02-18', NULL),
('NASAW001037', '1973-09-02', 'M'),
('AW001038', '1983-09-17', 'M'),
('AW001039', '1981-02-08', 'F'),
('NASAW001040', '1976-02-16', 'M'),
('AW001041', '1966-07-29', 'F'),
('AW001042', '1981-10-13', 'F'),
('NASAW001043', '1977-10-26', 'F'),
('AW001044', '1977-06-25', NULL),
('NASAW001045', '1967-07-13', 'M');
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';
-----------------------------------------
          SET @start_time = GETDATE();
          PRINT '>>TRUNCATING TABLE :bronze.loc_details';
		  TRUNCATE TABLE bronze.loc_details;

         PRINT '>>INSERTING DATA INTO: bronze.loc_details';

         INSERT INTO bronze.loc_details (cst_id, country)
VALUES
('AW-001001','Australia'),
('AW-001002','Canada'),
('AW-001003','France'),
('AW-001004','DE'),
('AW-001005','Germany'),
('AW-001006','USA'),
('AW-001007','US'),
('AW-001008',NULL),
('AW-001009','Australia'),
('AW-001010','Canada'),
('AW-001011','France'),
('AW-001012','DE'),
('AW-001013','Germany'),
('AW-001014','USA'),
('AW-001015','US'),
('AW-001016',NULL),
('AW-001017','Australia'),
('AW-001018','Canada'),
('AW-001019','France'),
('AW-001020','DE'),
('AW-001021','Germany'),
('AW-001022','USA'),
('AW-001023','US'),
('AW-001024',NULL),
('AW-001025','Australia'),
('AW-001026','Canada'),
('AW-001027','France'),
('AW-001028','DE'),
('AW-001029','Germany'),
('AW-001030','USA'),
('AW-001031','US'),
('AW-001032',NULL),
('AW-001033','Australia'),
('AW-001034','Canada'),
('AW-001035','France'),
('AW-001036','DE'),
('AW-001037','Germany'),
('AW-001038','USA'),
('AW-001039','US'),
('AW-001040',NULL);
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';
-----------------------------------------
          SET @start_time = GETDATE();
      PRINT '>>TRUNCATING TABLE :bronze.cat_maintenance';
TRUNCATE TABLE bronze.cat_maintenance;

PRINT '>>INSERTING DATA INTO: bronze.cat_maintenance';

INSERT INTO bronze.cat_maintenance (id, cat, subcat, maintenance)
VALUES
('CO_RF','Components','Road Frame','Yes'),
('CO_RF','Components','Road Frame','No'),
('AC_HE','Accessories','Helmet','Yes'),
('AC_HE','Accessories','Helmet','No'),
('AC_HE','Accessories','Helmet','Yes'),
('AC_HE','Accessories','Helmet','No'),
('AC_HE','Accessories','Helmet','Yes'),
('AC_HE','Accessories','Helmet','No'),
('CL_SO','Clothing','Socks','Yes'),
('CL_SO','Clothing','Socks','No'),
('AC_HE','Accessories','Helmet','Yes'),
('MT_FR','Mountain','Frame','Yes'),
('MT_FR','Mountain','Frame','No'),
('MT_FR','Mountain','Frame','Yes'),
('RD_JE','Road','Jersey','Yes'),
('RD_JE','Road','Jersey','No'),
('RD_JE','Road','Jersey','Yes'),
('GL_GL','Accessories','Gloves','Yes'),
('GL_GL','Accessories','Gloves','No'),
('GL_GL','Accessories','Gloves','Yes'),
('TR_TI','Touring','Tire','Yes'),
('TR_TU','Touring','Tube','No'),
('RD_FR','Road','Frame','Yes'),
('RD_FR','Road','Frame','No'),
('RD_FR','Road','Frame','Yes'),
('MT_BO','Mountain','Bottle Cage','No'),
('MT_BO','Mountain','Bottle Cage','Yes'),
('TR_HE','Touring','Helmet','Yes'),
('CL_SH','Clothing','Shorts','No'),
('CL_SH','Clothing','Shorts','Yes');
SET @end_time = GETDATE();
print'>> Load Duration : ' + CAST(DATEDIFF(second, @start_time , @end_time) as NVARCHAR) + ' Seconds ';

end try
	begin catch
	    PRINT '=============================================='
    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
    PRINT 'Error Message ' + ERROR_MESSAGE();
    PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '=============================================='
	end catch
 end
