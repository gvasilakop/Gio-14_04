-- Get Users who registered before 31/10/21 since casino game dates are until then
-- Keep only Greece related data
CREATE TABLE STUDIED_USERS AS
WITH temp AS(
select USERPROFILEID,
        CEIL(months_between(TRUNC(sysdate), BIRTHDATE)/12) as age, 
        ZIPCODE, 
        SEX, 
        TRUNC(sysdate) - TRUNC(REGISTRATION_DATE) as TENURE,
        TRUNC(REGISTRATION_DATE) AS REGISTRATION_DATE,
        row_number() over (partition by USERPROFILEID order by REGISTRATION_DATE) rn
from CASINO_USERS
where STATUSSYSNAME='ACTIVE' AND (COUNTRYID=1 OR COUNTRYNAME='Greece')),
active_users as(
select  USERPROFILEID, 
        age, 
        ZIPCODE, 
        SEX, 
        TENURE,
        REGISTRATION_DATE
from temp
where REGISTRATION_DATE <= TO_TIMESTAMP('31/10/21 00:00:00')
AND RN=1
),

temp2 AS(
select USERPROFILEID, CREATED, METHODSYSNAME,
  row_number() over (partition by USERPROFILEID order by CREATED) rn
from CUSTOMER_WALLET
WHERE TYPE=1),

-- Calculate frequency of deposit method
prefered_method as(
select DISTINCT USERPROFILEID, 
SUM(CASE WHEN METHODSYSNAME = 'PayPal' THEN 1 ELSE 0 END) AS PAYPAL,
SUM(CASE WHEN METHODSYSNAME = 'RapidTransfer' THEN 1 ELSE 0 END) AS RapidTransfer,
SUM(CASE WHEN METHODSYSNAME = 'DIASMyBank' THEN 1 ELSE 0 END) AS DIASMyBank,
SUM(CASE WHEN METHODSYSNAME = 'Trustly' THEN 1 ELSE 0 END) AS Trustly,
SUM(CASE WHEN METHODSYSNAME = 'PSCPaysafeCard' THEN 1 ELSE 0 END) AS PSCPaysafeCard,
SUM(CASE WHEN METHODSYSNAME = 'VivaWallet' THEN 1 ELSE 0 END) AS VivaWallet,
SUM(CASE WHEN METHODSYSNAME = 'Skrill' THEN 1 ELSE 0 END) AS Skrill,
SUM(CASE WHEN METHODSYSNAME = 'MoneySafe' THEN 1 ELSE 0 END) AS MoneySafe,
SUM(CASE WHEN METHODSYSNAME = 'VisaCard' THEN 1 ELSE 0 END) AS VisaCard,
SUM(CASE WHEN METHODSYSNAME = 'Moneybookers' THEN 1 ELSE 0 END) AS Moneybookers,
SUM(CASE WHEN METHODSYSNAME = 'DIASDTM' THEN 1 ELSE 0 END) AS DIASDTM,
SUM(CASE WHEN METHODSYSNAME = 'Envoy' THEN 1 ELSE 0 END) AS Envoy,
SUM(CASE WHEN METHODSYSNAME = 'Neteller' THEN 1 ELSE 0 END) AS Neteller,
SUM(CASE WHEN METHODSYSNAME = 'Skrill1tap' THEN 1 ELSE 0 END) AS Skrill1tap
FROM temp2 
GROUP BY USERPROFILEID
)
SELECT  A.USERPROFILEID,
        AGE,
        ZIPCODE,
        SEX,
        TENURE,
        REGISTRATION_DATE,
        PAYPAL, RapidTransfer, DIASMyBank, Trustly, PSCPaysafeCard, VivaWallet,
        Skrill, MoneySafe, VisaCard, Moneybookers, DIASDTM, Envoy, Neteller, Skrill1tap,
        AVG_DEPOSIT_AMOUNT,
        AVG_WITHDRAWAL_AMOUNT,
FROM active_users A
LEFT JOIN prefered_method B
ON A.USERPROFILEID=B.USERPROFILEID
LEFT JOIN (SELECT USERPROFILEID,
                    ROUND(AVG(CASE WHEN TYPE=1 THEN AMOUNT ELSE 0 END),2) AS AVG_DEPOSIT_AMOUNT,
                    ABS(ROUND(AVG(CASE WHEN TYPE=2 THEN AMOUNT ELSE 0 END),2)) AS AVG_WITHDRAWAL_AMOUNT
                    FROM CUSTOMER_WALLET
                    GROUP BY USERPROFILEID) C
ON A.USERPROFILEID=C.USERPROFILEID;

-- Calculate cost, revenue
-- September records will be used for clustering and training of prediction alforithm and test for validating it
CREATE TABLE GREECE_GAMES AS
SELECT USERID,
CASINO_PROVIDER,
ISJACKPOTWINID,
ISLIVEID,
TO_DATE(GAME_DATE) AS GAME_DATE,
SUM(case when hold>0 then hold ELSE 0 end) as revenue, 
ABS(SUM(case when hold<0 then hold ELSE 0 end)) as cost,
CASE WHEN TO_DATE(GAME_DATE) BETWEEN TO_DATE('01/09/2021 00:00:00') AND TO_DATE('30/09/2021 00:00:00') THEN 'TRAIN' ELSE 'TEST' END AS DATASET
FROM CASINO_GAMES A
INNER JOIN STUDIED_USERS B
ON A.USERID = B.USERPROFILEID
GROUP BY  USERID, CASINO_PROVIDER, ISJACKPOTWINID, ISLIVEID, GAME_DATE;