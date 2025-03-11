WITH /* SQL_ID : KRBH307BM */ AAA AS (
            SELECT *
               FROM IKRUSH_FSS_PD
            UNPIVOT(DET_RT FOR FWD_YR IN (PD_RT_Y01, PD_RT_Y02, PD_RT_Y03, PD_RT_Y04, PD_RT_Y05, PD_RT_Y06, PD_RT_Y07, PD_RT_Y08, PD_RT_Y09, PD_RT_Y10
                                              ,PD_RT_Y11, PD_RT_Y12, PD_RT_Y13, PD_RT_Y14, PD_RT_Y15, PD_RT_Y16, PD_RT_Y17, PD_RT_Y18, PD_RT_Y19, PD_RT_Y20
                                              ,PD_RT_Y21, PD_RT_Y22, PD_RT_Y23, PD_RT_Y24, PD_RT_Y25, PD_RT_Y26, PD_RT_Y27, PD_RT_Y28, PD_RT_Y29, PD_RT_Y30
                                              )                          
                   )                   
              
             WHERE BASE_YM = '$$STD_Y4MM'
            ) 
    ,BBB AS (SELECT 'NICE_'||LPAD(REPLACE(REPLACE(A.GRD_TYP, '등급', NULL),'NIC_',NULL),2,0)  AS SEG_ID
                 , LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))               AS AS_OF_DATE
                 , 'Y'                                                      AS STD_PLAN_YN
                 , TO_NUMBER(SUBSTR(A.FWD_YR,8))                            AS FWD_Y
                 , LEAST(ROUND(A.DEF_RT_NICE * A.DET_RT, 10),1)             AS CUM_PD     
             FROM AAA A
		WHERE DEF_RT_NICE <> 0
              UNION ALL
            SELECT 'KCB_'||LPAD(REPLACE(REPLACE(A.GRD_TYP, '등급', NULL),'KCB_',NULL),2,0)     AS SEG_ID
                 , LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))               AS AS_OF_DATE
                 , 'Y'                                                      AS STD_PLAN_YN
                 , TO_NUMBER(SUBSTR(FWD_YR,8))                              AS FWD_Y
                 , LEAST(ROUND(DEF_RT_KCB * DET_RT, 10),1)                  AS CUM_PD
              FROM AAA A
		WHERE DEF_RT_KCB <> 0
            )
SELECT /* SQL-ID : KRBH307BM */ SEG_ID
     , AS_OF_DATE
     , STD_PLAN_YN
     , FWD_Y
     , NULL        AS TOT_DEFLT_OBJT_CNT
     , NULL        AS DEFLT_CNT
     , NULL        AS FPD
     , NULL        AS FPD_APL   
   	 , CUM_PD                       
     , CUM_PD
     , 'KRBH307BM' AS LAST_MODIFIED_BY
     , SYSDATE     AS LAST_UPDATE_DATE 
  FROM BBB A