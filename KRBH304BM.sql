SELECT /* SQL-ID : KRBH304BM */ 
LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))     AS BASE_DATE
     , B.IRC_ID                               AS IRC_ID
     , A.MAT_CD                               AS MAT_CD
     , '2'                                    AS RATE_TYP
     , A.RATE                                 AS SPOT_RATE
     , NULL                                   AS CREDIT_SPREAD
     , NULL                                   AS DF
     , 'KRBH304BM'                            AS LAST_MODIFIED_BY
     , SYSDATE                                AS LAST_UPDATE_DATE
  FROM Q_IC_FSS_SCEN A
     , Q_CM_IRC B
 WHERE BASE_YM = (SELECT MAX(BASE_YM) FROM Q_IC_FSS_SCEN WHERE BASE_YM < =  '$$STD_Y4MM')
-- WHERE BASE_YM = SUBSTR($$BASE_YM, 1,6)
   AND A.MAT_CD <> 'M0000'
   AND A.FSS_CRVE_CD = B.IRC_ID
   AND A.SCEN_NO = 1
 UNION ALL
SELECT LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))     AS BASE_DATE
     , C.IRC_ID                               AS IRC_ID
     , A.MAT_CD                               AS MAT_CD
     , '2'                                    AS RATE_TYP               
     , A.SPREAD + B.RATE                      AS SPOT_RATE        
     , NULL                                   AS CREDIT_SPREAD   
     , NULL                                   AS DF              
     , 'KRBH304BM'                            AS LAST_MODIFIED_BY
     , SYSDATE                                AS LAST_UPDATE_DATE
  FROM Q_IC_FSS_BOND_SPR_DET A
     , Q_IC_FSS_SCEN B
     , Q_CM_IRC C
 WHERE A.BASE_YM = (SELECT MAX(BASE_YM) FROM Q_IC_FSS_SCEN WHERE BASE_YM < =  '$$STD_Y4MM')
 -- WHERE BASE_YM = SUBSTR($$BASE_YM, 1,6)
   AND B.FSS_CRVE_CD = 'KDSP1000'
   AND B.SCEN_NO = 1
   AND A.BASE_YM = B.BASE_YM
   AND A.MAT_CD = B.MAT_CD      
   AND A.IRC_ID = C.IRC_ID