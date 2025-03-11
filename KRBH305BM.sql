SELECT /* SQL-ID : KRBH305BM */
LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))                     AS BASE_DATE
     , 'KICS_A_SCEN_BASE'                                     AS SCEN_ID
     , B.IRC_ID                                               AS VOL_FACTOR_ID
     , TO_NUMBER(SUBSTR(A.MAT_CD,2,4))                        AS MAT_CD
     , 'M'                                                    AS MAT_TERM_UNIT
     , 1                                                      AS BUCKET_NO
     , LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD')) + 1                 AS BUCKET_START_DATE
     , ADD_MONTHS(LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD')), 1200)   AS BUCKET_END_DATE
     , A.SCEN_NO                                              AS SCEN_NO
     , NULL                                                   AS RAND_SEED
     , A.RATE                                                 AS SCEN_VAL
     , 'KRBH305BM'                                            AS LAST_MODIFIED_BY
     , SYSDATE                                                AS LAST_UPDATE_DATE
  FROM Q_IC_FSS_SCEN A
     , Q_CM_IRC B
 WHERE BASE_YM = (SELECT MAX(BASE_YM) FROM Q_IC_FSS_SCEN WHERE BASE_YM < =  '$$STD_Y4MM')
-- WHERE BASE_YM = SUBSTR($$BASE_YM, 1,6)
   AND A.MAT_CD <> 'M0000'
   AND TO_NUMBER(SUBSTR(A.MAT_CD,2,4)) < 1201
   AND A.FSS_CRVE_CD = B.IRC_ID
 UNION ALL
SELECT LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))                     AS BASE_DATE
     , 'KICS_A_SCEN_BASE'                                     AS SCEN_ID
     , A.IRC_ID                                               AS VOL_FACTOR_ID
     , TO_NUMBER(SUBSTR(A.MAT_CD,2,4))                        AS MAT_CD
     , 'M'                                                    AS MAT_TERM_UNIT
     , 1                                                      AS BUCKET_NO
     , LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD')) + 1                 AS BUCKET_START_DATE
     , ADD_MONTHS(LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD')), 1200)   AS BUCKET_END_DATE
     , B.SCEN_NO                                              AS SCEN_NO
     , NULL                                                   AS RAND_SEED
     , A.SPREAD + B.RATE                                      AS SCEN_VAL
     , 'KRBH305BM'                                            AS LAST_MODIFIED_BY
     , SYSDATE                                                AS LAST_UPDATE_DATE
  FROM Q_IC_FSS_BOND_SPR_DET A
     , Q_IC_FSS_SCEN B
     , Q_CM_IRC C
 WHERE A.BASE_YM = (SELECT MAX(BASE_YM) FROM Q_IC_FSS_SCEN WHERE BASE_YM < =  '$$STD_Y4MM')
 -- WHERE BASE_YM = SUBSTR($$BASE_YM, 1,6)
   AND B.FSS_CRVE_CD = 'KDSP1000'
   AND A.BASE_YM = B.BASE_YM
   AND A.MAT_CD = B.MAT_CD      
   AND A.IRC_ID = C.IRC_ID