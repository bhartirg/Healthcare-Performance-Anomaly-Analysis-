SELECT * FROM heakthcare_analysis.healthcare_dataset ;

SELECT Count(*) as total_row FROM heakthcare_analysis.healthcare_dataset;

DESCRIBE heakthcare_analysis.healthcare_dataset;

USE heakthcare_analysis;  

ALTER TABLE heakthcare_analysis.healthcare_dataset
MODIFY `Date of Admission` DATE,
MODIFY `Discharge Date` DATE;


ALTER TABLE heakthcare_analysis.healthcare_dataset
CHANGE `Name` patient_name VARCHAR(100),
CHANGE `Age` age INT,
CHANGE `Gender` gender VARCHAR(10),
CHANGE `Blood Type` blood_type VARCHAR(5),
CHANGE `Medical Condition` medical_condition VARCHAR(100),
CHANGE `Date of Admission` admission_date DATE,
CHANGE `Discharge Date` discharge_date DATE,
CHANGE `Billing Amount` billing_amount FLOAT,
CHANGE `Admission Type` admission_type VARCHAR(20),
CHANGE `Insurance Provider` insurance_provider VARCHAR(50),
CHANGE `Room Number` room_number INT,
CHANGE `Test Results` test_results VARCHAR(20);


SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';


USE heakthcare_analysis;
LOAD DATA LOCAL INFILE 'C:/Users/bhart/OneDrive/Desktop/sql/data/healthcare_dataset.csv'
INTO TABLE heakthcare_analysis.healthcare_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@name, @age, @gender, @blood_type, @medical_condition,
 @admission_date, @doctor, @hospital, @insurance_provider,
 @billing_amount, @room_number, @admission_type,
 @discharge_date, @medication, @test_results)
SET
patient_name = TRIM(@name),
age = TRIM(@age),
gender = TRIM(@gender),
blood_type = TRIM(@blood_type),
medical_condition = TRIM(@medical_condition),
admission_date = STR_TO_DATE(TRIM(@admission_date),'%Y-%m-%d'),
doctor = TRIM(@doctor),
hospital = TRIM(@hospital),
insurance_provider = TRIM(@insurance_provider),
billing_amount = @billing_amount,
room_number = TRIM(@room_number),
admission_type = TRIM(@admission_type),
discharge_date = STR_TO_DATE(TRIM(@discharge_date),'%Y-%m-%d'),
medication = TRIM(@medication),
test_results = TRIM(@test_results);

SELECT SUM(billing_amount) as total_sum  FROM heakthcare_analysis.healthcare_dataset; #correct output as "csv file" shows

ALTER TABLE heakthcare_analysis.healthcare_dataset
MODIFY billing_amount DECIMAL(10,2);

SELECT    /*This check which column has null values*/
COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'healthcare_dataset';

select COUNT(test_results) AS test_results_nulls     /*Checked null in each column one by one*/
FROM heakthcare_analysis.healthcare_dataset;


SELECT *
FROM heakthcare_analysis.healthcare_dataset
WHERE test_results IS NULL;
  
SELECT patient_name, age, admission_date,Doctor,discharge_date,room_number, COUNT(*)
FROM heakthcare_analysis.healthcare_dataset
GROUP BY patient_name, age, admission_date,Doctor,discharge_date,room_number
HAVING COUNT(*) > 1;

SHOW KEYS 
FROM heakthcare_analysis.healthcare_dataset 
WHERE Key_name = 'PRIMARY';


ALTER TABLE heakthcare_analysis.healthcare_dataset
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;

select count(*) as duplicates from(
SELECT *
FROM (  
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY patient_name,age,gender,blood_type,medical_condition,admission_date,Doctor,Hospital,
           insurance_provider,billing_amount,room_number,admission_type,discharge_date,Medication,test_results
           order by id
) AS rn
FROM heakthcare_analysis.healthcare_dataset)t where rn>1)x where rn>=2;

USE heakthcare_analysis;                          

DELETE t1                                        
FROM heakthcare_analysis.healthcare_dataset t1   
JOIN (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY patient_name, age, gender, blood_type, medical_condition,
                                admission_date, doctor, hospital, insurance_provider,
                                billing_amount, room_number, admission_type,
                                discharge_date, medication, test_results
                   ORDER BY admission_date
               ) AS rn
        FROM heakthcare_analysis.healthcare_dataset
    ) t
    WHERE rn > 1
) t2
ON t1.id = t2.id;

SELECT 
MIN(age) AS min_age,
MAX(age) AS max_age,
MIN(billing_amount) AS min_bill,
MAX(billing_amount) AS max_bill
FROM healthcare_dataset;            /*Check Data Ranges (Validity Check)*/


select count(*) as total_negative from(
SELECT *
FROM healthcare_dataset
WHERE billing_amount < 0)t;

SELECT admission_type, COUNT(*)
FROM healthcare_dataset
WHERE billing_amount < 0
GROUP BY admission_type;

SELECT 
SUM(billing_amount) AS net_revenue,
SUM(CASE WHEN billing_amount > 0 THEN billing_amount ELSE 0 END) AS total_charges,
SUM(CASE WHEN billing_amount < 0 THEN billing_amount ELSE 0 END) AS total_refunds
FROM healthcare_dataset;

 /*Count anomalies as “NOT expected values”If valid values are only 'Normal' and 'Critical', then anomaly = anything else:*/
SELECT *,    
CASE 
  WHEN billing_amount < 0 THEN 'Refund'
  ELSE 'Charge'
END AS billing_type
FROM healthcare_dataset;


SELECT *          /*Date Columns (Logical Check)*/
FROM healthcare_dataset
WHERE admission_date > discharge_date;

SELECT DISTINCT gender FROM healthcare_dataset;    /*Categorical Columns (Consistency Check)*/
SELECT DISTINCT admission_type FROM healthcare_dataset;
SELECT DISTINCT test_results FROM healthcare_dataset;
SELECT DISTINCT blood_type FROM healthcare_dataset;
SELECT DISTINCT Medication FROM healthcare_dataset;

SET SQL_SAFE_UPDATES = 0;  /*Using safe update mode*/
UPDATE healthcare_dataset
SET Doctor = TRIM(Doctor),
    Hospital = TRIM(Hospital);
SET SQL_SAFE_UPDATES = 1;

SELECT *                                           /*Text Columns (Invalid / Strange Values)*/
FROM healthcare_dataset
WHERE patient_name = '' OR patient_name IS NULL;

SELECT *
FROM healthcare_dataset
WHERE billing_amount > 100000;


SELECT *
FROM healthcare_dataset
ORDER BY billing_amount DESC;

select count(*) from(
SELECT patient_name, admission_date, Doctor,COUNT(*) as cnt
FROM healthcare_dataset
GROUP BY patient_name, admission_date,Doctor
HAVING COUNT(*)>1 )t where cnt>=2;

SELECT patient_name, admission_date, GROUP_CONCAT(age) as ages
FROM healthcare_dataset
GROUP BY patient_name, admission_date
HAVING COUNT(DISTINCT age) > 1;


SELECT patient_name, admission_date,Doctor,
       COUNT(*) as total_records,
       COUNT(DISTINCT age) as age_variation
FROM healthcare_dataset
GROUP BY patient_name, admission_date,Doctor
HAVING COUNT(*) > 1 AND COUNT(DISTINCT age) > 1;

SELECT *,
       CASE 
           WHEN COUNT(age) OVER (PARTITION BY patient_name, admission_date,Doctor) > 1
           THEN 'ANOMALY'
           ELSE 'OK'
       END as status
FROM healthcare_dataset;

SET SQL_SAFE_UPDATES = 0;           /*I identified near-duplicate records where all attributes were same except age. I removed duplicates by grouping on all relevant columns while excluding age, and retained a single record using rn=1*/
DELETE FROM healthcare_dataset     
WHERE id IN (                    /*I used ROW_NUMBER() with partitioning on all relevant columns except age to identify duplicates, and deleted records where row number was greater than 1, ensuring only one unique record per group.*/
    SELECT id FROM (             
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY patient_name, gender, blood_type, medical_condition,
                                admission_date, doctor, hospital, insurance_provider,
                                billing_amount, room_number, admission_type,
                                discharge_date, medication, test_results
                   ORDER BY id
               ) AS rn
        FROM healthcare_dataset
    ) AS temp
    WHERE rn > 1
);

SET SQL_SAFE_UPDATES = 1;

/*Copy data into new table for safest*/
CREATE TABLE healthcare_clean LIKE healthcare_dataset;
INSERT INTO healthcare_clean
SELECT * FROM healthcare_dataset;

SELECT COUNT(*) FROM healthcare_dataset;
SELECT COUNT(*) FROM healthcare_clean;

/*Final check for duplication and unique records*/

SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY patient_name,gender, blood_type, medical_condition,
                            admission_date, doctor, hospital, insurance_provider,
                            billing_amount, room_number, admission_type,
                            discharge_date, medication, test_results
               ORDER BY id
           ) AS rn
    FROM healthcare_dataset
) temp
WHERE rn > 1;


SELECT *
FROM healthcare_dataset
WHERE LENGTH(patient_name) < 2;


ALTER TABLE healthcare_dataset
ADD COLUMN Length_of_Stay INT;

SET SQL_SAFE_UPDATES = 0;
UPDATE healthcare_dataset
SET Length_of_Stay = DATEDIFF(discharge_date,admission_date);
SET SQL_SAFE_UPDATES = 1;


SELECT 
COUNT(*) AS negative_count,
SUM(billing_amount) AS total_loss
FROM heakthcare_analysis.healthcare_dataset
WHERE billing_amount < 0;

SELECT admission_type, COUNT(*)
FROM healthcare_dataset
WHERE billing_amount < 0
GROUP BY admission_type;

SELECT 
MIN(billing_amount), 
MAX(billing_amount)
FROM healthcare_dataset
WHERE billing_amount < 0;

 select * from heakthcare_analysis.healthcare_dataset where hospital in ('Ltd Smith','Smith Ltd');



SELECT *,
CASE 
    WHEN billing_amount < 0 THEN 'Refund/Adjustment'
    ELSE 'Normal'
END AS billing_flag
FROM healthcare_dataset;


SELECT admission_type, COUNT(*) AS loss
FROM healthcare_dataset
WHERE billing_amount < 0
GROUP BY admission_type;


SELECT hospital, COUNT(*) AS refund_cases
FROM healthcare_dataset
WHERE billing_amount < 0
GROUP BY hospital
ORDER BY refund_cases DESC;

SELECT SUM(billing_amount) AS total_revenue
FROM healthcare_dataset
WHERE billing_amount >= 0;

SELECT SUM(billing_amount) AS net_revenue
FROM healthcare_dataset;

SELECT *,
CASE 
    WHEN billing_amount < -1000 THEN 'High Adjustment'
    WHEN billing_amount BETWEEN -1000 AND -100 THEN 'Medium Adjustment'
    ELSE 'Low Adjustment'
END AS adjustment_level
FROM healthcare_dataset
WHERE billing_amount < 0;

SELECT hospital, 
COUNT(*) AS cases,
AVG(billing_amount) AS avg_adjustment
FROM healthcare_dataset
WHERE billing_amount < 0
GROUP BY hospital
ORDER BY avg_adjustment;


/*Patient & Demographic Analysis*/
/*Which age group has highest hospital visits?*/

SELECT 
CASE 
    WHEN age < 18 THEN 'Child'
    WHEN age BETWEEN 18 AND 40 THEN 'Adult'
    WHEN age BETWEEN 41 AND 60 THEN 'Middle Age'
    ELSE 'Senior'
END AS age_group,
COUNT(*) AS total_patients
FROM healthcare_dataset
GROUP BY age_group
ORDER BY total_patients DESC;


/*Gender vs Disease Pattern*/
SELECT gender, medical_condition, COUNT(*) AS cases
FROM healthcare_dataset
GROUP BY gender, medical_condition
ORDER BY cases DESC;

/*Hospital-wise Patient Load (Location proxy)*/
SELECT hospital, COUNT(*) AS patient_count
FROM healthcare_dataset
GROUP BY hospital
ORDER BY patient_count DESC;


/*2. Admission & Discharge Analysis Average Length of Stay (LOS)*/

SELECT AVG(length_of_stay) AS avg_stay
FROM healthcare_dataset;

/*Long Stay Patients (Problem Detection 🚨)*/
SELECT *
FROM healthcare_dataset
WHERE length_of_stay > 10;

/*Admission Type Impact*/
SELECT admission_type, AVG(length_of_stay) AS avg_stay
FROM healthcare_dataset
GROUP BY admission_type;



/*3. Clinical Analysis Most Common Diseases*/

SELECT medical_condition, COUNT(*) AS cases
FROM healthcare_dataset
GROUP BY medical_condition
ORDER BY cases DESC;


/*Test Results Distribution*/

SELECT test_results, COUNT(*) AS total
FROM healthcare_dataset
GROUP BY test_results;


/*Disease vs Length of Stay*/

SELECT medical_condition, AVG(length_of_stay) AS avg_stay
FROM healthcare_dataset
GROUP BY medical_condition
ORDER BY avg_stay DESC;



/*4. Financial Analysis
Total Revenue*/

SELECT SUM(billing_amount) AS total_revenue
FROM healthcare_dataset;

/*Revenue per Patient*/
SELECT AVG(billing_amount) AS avg_revenue
FROM healthcare_dataset;

/*Revenue by Hospital*/
SELECT hospital, SUM(billing_amount) AS revenue
FROM healthcare_dataset
GROUP BY hospital
ORDER BY revenue DESC;

/*Insurance Contribution*/
SELECT insurance_provider, 
COUNT(*) AS patients,
SUM(billing_amount) AS revenue
FROM healthcare_dataset
GROUP BY insurance_provider;



/*5. Administrative Analysis
 Admission Type Distribution*/

SELECT admission_type, COUNT(*) AS total
FROM healthcare_dataset
GROUP BY admission_type;

/* Room Utilization*/
SELECT room_number, COUNT(*) AS usage_count
FROM healthcare_dataset
GROUP BY room_number
ORDER BY usage_count DESC;

/*Doctor Workload*/
SELECT doctor, COUNT(*) AS patient_count
FROM healthcare_dataset
GROUP BY doctor
ORDER BY patient_count DESC;