
/* ================================Calculating the True Recovery rate =====================================*/ 

--Creating Two target tables with ranks in ASC and DESC

--Ranking target table in asscending order of score_id
CREATE VIEW New_target AS 
SELECT  masked_acct, rank() OVER (PARTITION BY masked_acct ORDER BY score_ind) as ranks, score_ind, RecoveryPctBalanceAtDefaultRatioMACO12
FROM "target"

--DROP VIEW New_target;

SELECT*
FROM New_target;

--Ranking target table in descending order of score_id
CREATE VIEW New_target_max AS 
SELECT  masked_acct, rank() OVER (PARTITION BY masked_acct ORDER BY score_ind DESC) as ranks, score_ind, RecoveryPctBalanceAtDefaultRatioMACO12
FROM "target"

SELECT*
FROM New_target_max;

--Extracting minimum rank (minimum score_id) data from ascending ranks table
CREATE VIEW Min_Recovery AS
SELECT masked_acct, ranks, RecoveryPctBalanceAtDefaultRatioMACO12 as Recovery
FROM New_target
WHERE ranks = 1;

--DROP VIEW Min_Recovery;

SELECT *
FROM Min_Recovery

SELECT COUNT(*)
FROM Min_Recovery;


--Extracting One less from maximum rank (score_id) data from descending ranks table

CREATE VIEW Maxless_Recovery AS
SELECT masked_acct, ranks, RecoveryPctBalanceAtDefaultRatioMACO12 as Recovery
FROM New_target_max
WHERE ranks = 2;

--DROP VIEW Maxless_Recovery;

SELECT COUNT(*)
FROM Maxless_Recovery;

SELECT *
FROM Maxless_Recovery;

--Calculating True Recovery rate
CREATE VIEW True_Recovery AS 
SELECT  a.masked_acct, (a.Recovery + b.Recovery)/2 as True_Recovery
FROM Min_Recovery as a INNER JOIN Maxless_Recovery as b ON a.masked_acct = b.masked_acct

SELECT COUNT(*)
FROM True_Recovery;

SELECT *
FROM True_Recovery;
/*=======================Checking all the tables and their rowcount========================*/ 

SELECT COUNT(*)
FROM input_data_1;

SELECT *
FROM input_data_1
LIMIT 1;

SELECT COUNT(*)
FROM input_data_2;

SELECT *
FROM input_data_2
LIMIT 1;

SELECT COUNT(*)
FROM input_data_3;

SELECT *
FROM input_data_3
LIMIT 1;

-- Union of input data from 1 to 3 checking the row count: 27437
WITH Data1 AS (SELECT *
FROM input_data_1

UNION ALL

SELECT *
FROM input_data_2

UNION ALL

SELECT * 
FROM input_data_3)
SELECT  COUNT(*)
FROM Data1;

--Creating a View for the Data1 which is a union of input data 1 to 3

CREATE VIEW Data1 AS
SELECT *
FROM input_data_1

UNION ALL

SELECT *
FROM input_data_2

UNION ALL

SELECT * 
FROM input_data_3;

SELECT COUNT(*)
FROM Data1;

--Similar step for input data 4 to 6
SELECT COUNT(*)
FROM input_data_4;

SELECT *
FROM input_data_4
LIMIT 1;

SELECT COUNT(*)
FROM input_data_5;

SELECT *
FROM input_data_5
LIMIT 1;

SELECT COUNT(*)
FROM input_data_6;

SELECT *
FROM input_data_6
LIMIT 1;

SELECT COUNT(*)
FROM input_data_4 as a INNER JOIN input_data_5 as b ON a.key_2 = b.key_2 INNER JOIN input_data_6 as c ON b.key_2 = c.key_2;

-- Union of input data from 1 to 3 checking the row count: 27437
WITH Data2 AS (SELECT *
FROM input_data_4

UNION ALL

SELECT *
FROM input_data_5

UNION ALL

SELECT * 
FROM input_data_6)
SELECT  COUNT(*)
FROM Data2;

-- Creating View for Data 2

CREATE VIEW Data2 AS 
SELECT *
FROM input_data_4

UNION ALL

SELECT *
FROM input_data_5

UNION ALL

SELECT * 
FROM input_data_6;

SELECT COUNT(*)
FROM Data2;

--Similar step for input data 7 to 9
SELECT COUNT(*)
FROM input_data_7;

SELECT *
FROM input_data_7
LIMIT 1;

SELECT COUNT(*)
FROM input_data_8;

SELECT *
FROM input_data_8
LIMIT 1;

SELECT COUNT(*)
FROM input_data_9;

SELECT *
FROM input_data_9
LIMIT 1;

SELECT COUNT(*)
FROM input_data_7 as a INNER JOIN input_data_8 as b ON a.key_2 = b.key_2 INNER JOIN input_data_9 as c ON b.key_2 = c.key_2;

-- Union of input data from 1 to 3 checking the row count: 27437
WITH Data3 AS (SELECT *
FROM input_data_7

UNION ALL

SELECT *
FROM input_data_8

UNION ALL

SELECT * 
FROM input_data_9)
SELECT  COUNT(*)
FROM Data3;

-- Creating View for Data 2

CREATE VIEW Data3 AS 
SELECT *
FROM input_data_7

UNION ALL

SELECT *
FROM input_data_8

UNION ALL

SELECT * 
FROM input_data_9;

SELECT COUNT(*)
FROM Data3;

-- Joining the data 1 and 2

/*----- Dense Rank Data1 for removing duplicates ---------*/


SELECT *
FROm Data1;

PRAGMA table_info(Data1);

WITH New_data1 AS (SELECT DISTINCT masked_acct, key_1,Vintage,ChargeOffMonthKey, ChargeOffMOB, Loss_Date_150, BalanceAtDefault, JointIndicator, LCPIndicator, FICOScore, StateApplicant, FICOScorePctAvgFICOLast30DaysBookedLoans,
		ApplicationWeekday, NewUsedIndicator, VehicleManufacturerRebate, VehicleAge, VehicleMileage, VehicleValueBlackBook, TradeInIndicator, TradeInAmt, VehicleMakeNADA, VehicleModelNADA, 
		JDPowerUsedVehicleIndex, MoodysNumUnemployed, MoodysUsedVehicleIndex, MoodysNumEmployed, MoodysGDPReal, MoodysGasoline, MoodysNumLaborForce, MoodysUnemploymentRate, TradeInPctBBVehicleValue, 
		TradeInPctLine3, TradeInPctLine4, TradeInPctLine5, FinancedAmt, LoanAmtLine3, LoanAmtLine4
FROM Data1)
SELECT COUNT(*)
FROM New_data1 as d1 INNER JOIN Data2 as d2 ON d1.key_1 = d2.key_1;

CREATE VIEW New_Data1 AS 
SELECT DISTINCT masked_acct, key_1,Vintage,ChargeOffMonthKey, ChargeOffMOB, Loss_Date_150, BalanceAtDefault, JointIndicator, LCPIndicator, FICOScore, StateApplicant, FICOScorePctAvgFICOLast30DaysBookedLoans,
		ApplicationWeekday, NewUsedIndicator, VehicleManufacturerRebate, VehicleAge, VehicleMileage, VehicleValueBlackBook, TradeInIndicator, TradeInAmt, VehicleMakeNADA, VehicleModelNADA, 
		JDPowerUsedVehicleIndex, MoodysNumUnemployed, MoodysUsedVehicleIndex, MoodysNumEmployed, MoodysGDPReal, MoodysGasoline, MoodysNumLaborForce, MoodysUnemploymentRate, TradeInPctBBVehicleValue, 
		TradeInPctLine3, TradeInPctLine4, TradeInPctLine5, FinancedAmt, LoanAmtLine3, LoanAmtLine4
FROM Data1;

CREATE VIEW Data_final AS 
WITH CTE1 AS (SELECT *
FROM New_Data1 as d1 INNER JOIN Data2 as d2 ON d1.key_1 = d2.key_1)
SELECT *
FROM CTE1 as a INNER JOIN Data3 as b ON a.key_2 = b.key_2;

SELECT *
FROM Data_final;

SELECT COUNT(*)
FROM Data_final as d1 INNER JOIN True_Recovery as d2 ON d1.masked_acct = d2.masked_acct;

CREATE VIEW FINAL_TABLE AS 
SELECT *
FROM Data_final as d1 INNER JOIN True_Recovery as d2 ON d1.masked_acct = d2.masked_acct;

SELECT COUNT(*)
FROM FINAL_TABLE;

SELECT *
FROM FINAL_TABLE;

/*================ Droping Views================*/

DROP VIEW Data1;

DROP VIEW Data2;

DROP VIEW Data3;

DROP VIEW Data_final;

Drop VIEW Maxless_Recovery;

Drop View Min_Recovery;

DROP VIEW New_Data1

DROP VIEW FINAL_TABLE;

DROP VIEW True_Recovery;


