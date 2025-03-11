WITH /* SQL-ID : KRBH311BM */ T_AAA AS
(
   SELECT BASE_YM                            AS BASE_YM
        , 'KDSP1000'                         AS FSS_CRVE_CD
        , TO_NUMBER(SUBSTR(SCEN_NO, 7))      AS SCEN_NO
        , MAT_CD                             AS MAT_CD
        , NVL(RATE, 0)                       AS RATE
        , 'KRBH311BM'                        AS LAST_MODIFIED_BY
        , SYSDATE                            AS LAST_MODIFIED_DATE
     FROM IKRUSH_SHCK_IRATE
  UNPIVOT INCLUDE NULLS ( RATE FOR SCEN_NO IN ( SCEN_A01, SCEN_A02, SCEN_A03, SCEN_A04, SCEN_A05, SCEN_A06, SCEN_A07, SCEN_A08, SCEN_A09, SCEN_A10, SCEN_A11, SCEN_A12, SCEN_A13, SCEN_A14 ) )
    WHERE 1=1
      AND BASE_YM = '$$STD_Y4MM'

    UNION ALL

   SELECT BASE_YM                            AS BASE_YM
        , 'KDSP2000'                         AS FSS_CRVE_CD
        , TO_NUMBER(SUBSTR(SCEN_NO, 7))      AS SCEN_NO
        , MAT_CD                             AS MAT_CD
        , NVL(RATE, 0)                       AS RATE
        , 'KRBH311BM'                        AS LAST_MODIFIED_BY
        , SYSDATE                            AS LAST_MODIFIED_DATE
     FROM IKRUSH_SHCK_IRATE
  UNPIVOT INCLUDE NULLS ( RATE FOR SCEN_NO IN ( SCEN_L01, SCEN_L02, SCEN_L03, SCEN_L04, SCEN_L05, SCEN_L06, SCEN_L07, SCEN_L08, SCEN_L09, SCEN_L10, SCEN_L11, SCEN_L12, SCEN_L13, SCEN_L14 ) )
    WHERE 1=1
      AND BASE_YM = '$$STD_Y4MM'

    UNION ALL

   SELECT BASE_YM                            AS BASE_YM
        , 'K'||'KRW'||'1000'                 AS FSS_CRVE_CD
        , TO_NUMBER(SUBSTR(SCEN_NO, 7))      AS SCEN_NO
        , MAT_CD                             AS MAT_CD
        , NVL(RATE, 0)                       AS RATE
        , 'KRBH311BM'                        AS LAST_MODIFIED_BY
        , SYSDATE                            AS LAST_MODIFIED_DATE
     FROM IKRUSH_SHCK_IRATE
  UNPIVOT INCLUDE NULLS ( RATE FOR SCEN_NO IN ( SCEN_A01, SCEN_A02, SCEN_A03, SCEN_A04, SCEN_A05, SCEN_A06, SCEN_A07, SCEN_A08, SCEN_A09, SCEN_A10, SCEN_A11, SCEN_A12, SCEN_A13, SCEN_A14 ) )
    WHERE 1=1
      AND BASE_YM = '$$STD_Y4MM'    

    UNION ALL
   SELECT BASE_YM                            AS BASE_YM
        , 'K'||CRNY_CD||'1000'               AS FSS_CRVE_CD
        , TO_NUMBER(SUBSTR(SCEN_NO, 7))      AS SCEN_NO
        , MAT_CD                             AS MAT_CD
        , NVL(RATE, 0)                       AS RATE
        , 'KRBH311BM'                        AS LAST_MODIFIED_BY
        , SYSDATE                            AS LAST_MODIFIED_DATE
     FROM IKRUSH_SHCK_IRATE_FX
  UNPIVOT INCLUDE NULLS ( RATE FOR SCEN_NO IN ( SCEN_A01, SCEN_A02, SCEN_A03, SCEN_A04, SCEN_A05, SCEN_A06, SCEN_A07, SCEN_A08, SCEN_A09, SCEN_A10, SCEN_A11, SCEN_A12, SCEN_A13, SCEN_A14 ) )
    WHERE 1=1
      AND BASE_YM = '$$STD_Y4MM'
)
SELECT * FROM T_AAA