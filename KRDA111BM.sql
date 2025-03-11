/*20240522. 김민영. 금리위험액, 주식위험액, 외환위험액 반영 로직 변경 */
/*20240531. 김민영. 금리위험액 로직 변경(NVL 처리) 및 IRATE_RPT_COA NOT IN ('1Y_ON') 조건 추가  */


WITH /* SQL ID : KRDA111BM */ IRATE_SMR AS
(
   SELECT *
     FROM Q_IC_IRATE_SMR
    WHERE 1=1
      AND BASE_DATE = '$$STD_Y4MD'
	 AND IRATE_RPT_COA NOT IN ('1Y_ON')
)
, IR_RISK AS
(
   SELECT 'IRATE' AS MRKT_RISK_TPCD
        , GREATEST(ROUND(( RG_T + SQRT(  POWER(GREATEST(NVL(B.UP_T,0) + NVL(C.UP_F,0), NVL(B.DN_T,0) + NVL(C.DN_F,0)), 2)
                              + POWER(GREATEST(NVL(B.FL_T,0) + NVL(C.FL_F,0), NVL(B.ST_T,0) + NVL(C.ST_F,0)), 2) )
                ), 2),0)                                                      AS SCR
     FROM (
             SELECT SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN02)              AS RG_T
               FROM IRATE_SMR
          ) A
        , (
             SELECT SUM(IR_EXPO_SCEN01)                                    AS BS_T
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN03), 0) AS UP_T
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN04), 0) AS DN_T
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN05), 0) AS FL_T
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN06), 0) AS ST_T
               FROM IRATE_SMR
              WHERE 1=1
                AND IRATE_RPT_COA NOT IN ('1N_62B', '1Y_62B', '2N_25B', '2Y_25B')         -- 투자파생을 제외한 전체
          ) B
        , (
             SELECT SUM(IR_EXPO_SCEN01)                                    AS BS_F
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN03), 0) AS UP_F
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN04), 0) AS DN_F
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN05), 0) AS FL_F
                  , GREATEST(SUM(IR_EXPO_SCEN01) - SUM(IR_EXPO_SCEN06), 0) AS ST_F
               FROM IRATE_SMR
              WHERE 1=1
                AND IRATE_RPT_COA     IN ('1N_62B', '1Y_62B', '2N_25B', '2Y_25B')         -- 투자파생
          ) C
)
, STOC_SUM AS
(
   SELECT STOC_RISK_TPCD
        ,  GREATEST(SUM(DECODE(RPT_TPCD, '1', EXPO_AMT, '2', EXPO_AMT, 0)) - SUM(DECODE(RPT_TPCD, '1', SCAF_FAIR_BS_AMT, '2', SCAF_FAIR_BS_AMT, 0)),0)
                  + GREATEST(SUM(DECODE(RPT_TPCD, '3', EXPO_AMT, 0)) - SUM(DECODE(RPT_TPCD, '3', SCAF_FAIR_BS_AMT, 0)),0)             AS SCR
--        ,   GREATEST(SUM(DECODE(RPT_TPCD, '1', SCR, '2', SCR, 0)), 0)
--          + GREATEST(SUM(DECODE(RPT_TPCD, '3', SCR,           0)), 0) AS SCR
     FROM (
             SELECT SUBSTR(STOC_RISK_DTLS_TPCD, 1, 2)                 AS STOC_RISK_TPCD
                  , RPT_TPCD                                          AS RPT_TPCD
                  , DECODE(ACCO_CLCD,'6', -1 * SCR , SCR)             AS SCR                        -- ACCO_CLCD = '6' -> 변액부채
				  , DECODE(ACCO_CLCD,'6', -1 * EXPO_AMT , EXPO_AMT)   AS EXPO_AMT                      
                  , DECODE(ACCO_CLCD,'6', -1 * SCAF_FAIR_BS_AMT , SCAF_FAIR_BS_AMT)   AS SCAF_FAIR_BS_AMT       
               FROM (
                       SELECT A.*
                            , CASE WHEN ACCO_CLCD IN ('1')     THEN '1'
                                   WHEN ACCO_CLCD IN ('2','3') THEN '2'
                                   WHEN ACCO_CLCD IN ('4')     THEN '5'
                                   WHEN ACCO_CLCD IN ('5')     THEN '3'
                                   WHEN ACCO_CLCD IN ('6')     THEN '4'
                                   ELSE '1' END
                              ||SUBSTR(STOC_RISK_DTLS_TPCD,1,2)       AS COA_ID
                            , CASE WHEN CPTC_YN IN ('N') AND FIDE_YN IN ('N') THEN '1'              -- 1: 일반상품(캐피탈콜, 투기목적파생 제외)
                                   WHEN CPTC_YN IN ('Y')                      THEN '2'              -- 2: 캐피탈콜
                                   WHEN FIDE_YN IN ('Y')                      THEN '3'              -- 3: 투기목적파생상품(헷지목적의 CRS, FX 등은 일반상품임)
                                   ELSE '1' END                       AS RPT_TPCD
                         FROM Q_IC_STOC_RSLT A
                        WHERE BASE_DATE = '$$STD_Y4MD'
                          AND ACCO_CLCD <> '4'
                    )
          )
    GROUP BY STOC_RISK_TPCD
)
, STOC_RISK AS
(
   SELECT 'STOC' AS MRKT_RISK_TPCD
        , NVL(ROUND(SQRT(SUM((A.SCR * B.SCR * C.VAL))), 2), 0)        AS SCR
     FROM STOC_SUM A
        , STOC_SUM B
        , (
             SELECT COL_ID, ROW_ID, VAL
               FROM IKRUSH_CORR_COEF
              WHERE '$$STD_Y4MD' BETWEEN APL_STRT_DT AND APL_END_DT
                AND CORR_COEF_KEY = '30'
          ) C
    WHERE A.STOC_RISK_TPCD = C.ROW_ID
      AND B.STOC_RISK_TPCD = C.COL_ID
)
, PRPT_RISK AS
(
   SELECT 'PRPT' AS MRKT_RISK_TPCD
        , NVL(GREATEST(SUM(EXPO_AMT) - SUM(SCAF_FAIR_BS_AMT), 0), 0)  AS SCR
     FROM Q_IC_PRPT_RSLT
    WHERE BASE_DATE = '$$STD_Y4MD'
)
, NET_AMT AS
    (
         SELECT A.CRNY_CD
              , A.RN
              , COALESCE(A.AMT,0) + COALESCE(B.AMT,0) AS NET_POSI
           FROM
                (
                    SELECT CRNY_CD
                         , SUM(EXPO_AMT) AS AMT
                         , 1 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE = '$$STD_Y4MD'
                       AND EXPO_ID NOT IN ( SELECT EXPO_ID
                                              FROM Q_IC_FX_RSLT
                                             WHERE BASE_DATE    = '$$STD_Y4MD'
                                               AND FX_FIDE_TPCD = '1'
                         AND FRM_TPCD IS NULL
                                               AND CRNY_OPT_YN  = 'N'
                                          )
                     GROUP BY CRNY_CD
                     UNION ALL
                    SELECT CRNY_CD, SUM(FXRT_UP_FAIR_AMT)  AS AMT, 2 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE = '$$STD_Y4MD'
                       AND EXPO_ID NOT IN ( SELECT EXPO_ID
                                              FROM Q_IC_FX_RSLT
                                             WHERE BASE_DATE    = '$$STD_Y4MD'
                                               AND FX_FIDE_TPCD = '1'
                         AND FRM_TPCD IS NULL
                                               AND CRNY_OPT_YN  = 'N'
                                          )
                     GROUP BY CRNY_CD
                     UNION ALL
                    SELECT CRNY_CD, SUM(FXRT_DWN_FAIR_AMT) AS AMT, 3 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE = '$$STD_Y4MD'
                       AND EXPO_ID NOT IN ( SELECT EXPO_ID
                                              FROM Q_IC_FX_RSLT
                                             WHERE BASE_DATE    = '$$STD_Y4MD'
                                               AND FX_FIDE_TPCD = '1'
                         AND FRM_TPCD IS NULL
                                               AND CRNY_OPT_YN  = 'N'
                                          )
                     GROUP BY CRNY_CD
                ) A
                LEFT JOIN 
                (
                    SELECT CRNY_CD, SUM(EXPO_AMT)                               AS AMT, 1 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE    = '$$STD_Y4MD'
                       AND FX_FIDE_TPCD = '1'
             AND FRM_TPCD IS NULL
                     GROUP BY CRNY_CD
                     UNION ALL
                    SELECT CRNY_CD, LEAST(SUM(EXPO_AMT), SUM(FXRT_UP_FAIR_AMT)) AS AMT, 2 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE    = '$$STD_Y4MD'
                       AND FX_FIDE_TPCD = '1'
             AND FRM_TPCD IS NULL
                       AND CRNY_OPT_YN  = 'N'
                     GROUP BY CRNY_CD
                     UNION ALL
                    SELECT CRNY_CD, LEAST(SUM(EXPO_AMT), SUM(FXRT_DWN_FAIR_AMT)) AS AMT, 3 AS RN
                      FROM Q_IC_FX_RSLT
                     WHERE BASE_DATE    = '$$STD_Y4MD'
                       AND FX_FIDE_TPCD = '1'
             AND FRM_TPCD IS NULL
                       AND CRNY_OPT_YN  = 'N'
                     GROUP BY CRNY_CD
                ) B
               ON  A.CRNY_CD = B.CRNY_CD
               AND A.RN = B.RN
    )
    , NET_AMT2 AS 
    (
      SELECT *
        FROM NET_AMT
       WHERE 1=1 
         AND RN=1)
    , NET_AMT3 AS
    (
      SELECT A.*, B.NET_POSI AS NET_POSI2
        FROM NET_AMT A , NET_AMT2 B
       WHERE 1=1
        AND A.CRNY_CD=B.CRNY_CD)
    , NET_AMT4 AS
    (
      SELECT A.* ,
             CASE WHEN RN=1 THEN NULL 
                            ELSE GREATEST (NET_POSI2-NET_POSI,0) END NET_POSI3 
                            FROM NET_AMT3 A
    )
    , PRC_VOL_AMT AS
    ( SELECT CRNY_CD AS CRNY_CD
             , SUM(PRC_VOL_RISK_AMT)                            AS VOL_RISK_AMT
          FROM Q_IC_FX_RSLT A
         WHERE A.BASE_DATE = '$$STD_Y4MD'
         GROUP BY CRNY_CD
         ORDER BY 1
    )
    , SMR AS 
    (
      SELECT  A.CRNY_CD
     --     , RN     
            , SUM(CASE WHEN A.RN=2 THEN A.NET_POSI3 ELSE 0 END) AS FX_UP
            , SUM(CASE WHEN A.RN=3 THEN A.NET_POSI3 ELSE 0 END) AS FX_DWN
            , SUM(B.VOL_RISK_AMT)/3                             AS VOL_RISK_AMT
       FROM NET_AMT4 A, PRC_VOL_AMT B
      WHERE 1=1
      AND   A.CRNY_CD=B.CRNY_CD
     GROUP BY A.CRNY_CD 
     ORDER BY A.CRNY_CD
    )
    , COEF AS 
    (
        SELECT
                ROUND(POWER(POWER(SUM(A.FX_UP),2)  * 0.5 + 0.5 * (SUM(A.FX_UP  * A.FX_UP)) ,0.5), 2)  AS FX_UP
              , ROUND(POWER(POWER(SUM(A.FX_DWN),2) * 0.5 + 0.5 * (SUM(A.FX_DWN * A.FX_DWN)),0.5), 2)  AS FX_DWN
              , SUM(A.VOL_RISK_AMT)                                                                   AS VOL_RISK_AMT
         FROM SMR A
         )
,FX_RISK AS
(
     SELECT 'FX' AS MRKT_RISK_TPCD
                 , GREATEST(A.FX_UP, A.FX_DWN) + A.VOL_RISK_AMT AS RQST_CPTL_AMT /* 외환위험액    */
            FROM COEF A
)
, CNCT_RISK AS
(
     SELECT 'CNCT'                                                    AS MRKT_RISK_TPCD
          , NVL(SQRT(SUM(POWER(SCR, 2))), 0)                          AS SCR
       FROM (
               SELECT ASSET_CNCT_TPCD
                    , GRP_CD
                    , SUM(SCR) AS SCR
                 FROM Q_IC_CNCT_RSLT
                WHERE BASE_DATE = '$$STD_Y4MD'
                  AND LMT_OVER_AMT > 0
                  AND ASSET_CNCT_TPCD = '1'
                GROUP BY ASSET_CNCT_TPCD, GRP_CD

                UNION ALL

               SELECT NULL
                    , NULL
                    , MAX(SCR) AS SCR
                 FROM (
                         SELECT ASSET_CNCT_TPCD
                              , SUM(SCR) AS SCR
                           FROM Q_IC_CNCT_RSLT
                          WHERE BASE_DATE = '$$STD_Y4MD'
                            AND LMT_OVER_AMT > 0
                            AND ASSET_CNCT_TPCD IN ('2','3')
                          GROUP BY ASSET_CNCT_TPCD
                      )
             )
)
, MRKT_SMR AS
(
   SELECT * FROM IR_RISK   UNION ALL
   SELECT * FROM STOC_RISK UNION ALL
   SELECT * FROM PRPT_RISK UNION ALL
   SELECT * FROM FX_RISK   UNION ALL
   SELECT * FROM CNCT_RISK
)
--SELECT * FROM MRKT_SMR;
, MRKT_RISK AS
(
   SELECT 'MRKT'                                                      AS MRKT_RISK_TPCD
        , SQRT(SUM(A.SCR * B.SCR * C.VAL))                            AS SCR
     FROM MRKT_SMR A
        , MRKT_SMR B
        , IKRUSH_CORR_COEF C
    WHERE A.MRKT_RISK_TPCD = C.ROW_ID
      AND B.MRKT_RISK_TPCD = C.COL_ID
      AND C.CORR_COEF_KEY = '20'
      AND '$$STD_Y4MD' BETWEEN C.APL_STRT_DT AND C.APL_END_DT
)
--SELECT * FROM MRKT_RISK;
   SELECT '$$STD_Y4MD'                                                AS BASE_DATE
        , MRKT_RISK_TPCD                                              AS MRKT_RISK_TPCD
        , ROUND(SCR, 0)                                               AS SCR
        , 'KRDA111BM'                                                 AS LAST_MODIFIED_BY
        , SYSDATE                                                     AS LAST_UPDATE_DATE
     FROM (
             SELECT * FROM MRKT_RISK UNION ALL
             SELECT * FROM MRKT_SMR  UNION ALL

             SELECT 'DE'                AS MRKT_RISK_TPCD
                  , B.SCR - A.SCR       AS SCR
               FROM MRKT_RISK A
                  , (
                       SELECT SUM(SCR)  AS SCR
                         FROM MRKT_SMR
                    ) B
          )