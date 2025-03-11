WITH /* SQL-ID : KRDA102BM */ 
T_STOC_SHCK AS 
    (
                 SELECT STOC_RISK_DTLS_TPCD AS SR_CLSF_CD
                      , CRGR_KICS
                      , SHCK_VAL
                   FROM IKRUSH_SHCK_STOC
                  WHERE 1=1
                    AND '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
    )
, T_BASE AS 
(
         SELECT IA.*
              , CASE WHEN SUBSTR(IA.KICS_PROD_TPCD, 1, 1) IN ('6', '7') THEN 'Y'                                             
                     ELSE 'N' END                                                          AS FIDE_YN  -- 파생상품해당여부
              , CASE WHEN IA.SR_CLSF_CD IN ('S31')                                              -- 우선주
                     THEN CASE WHEN IA.CRGR_KICS IN ('RF')                 THEN '2'         -- 무위험으로 분류한 건은 국내차주 기준 KICS2등급 처리
                             ELSE NVL(IA.CRGR_KICS,'99') 
                              END
                     ELSE '0' END                                                          AS CRGR_ADJ  -- 그외에는 KICS신용등급과 무관함
           FROM Q_IC_EXPO IA
          WHERE 1=1
            AND IA.BASE_DATE = '$$STD_Y4MD'
            AND IA.LEG_TYPE       IN ('STD', 'NET')
            AND NVL(IA.ISIN_CD, '999999999999')
                              NOT IN (
                                        SELECT ISIN_CD
                                          FROM IKRUSH_FIDE_OPT
                                         WHERE 1=1
                                           AND BASE_DATE = '$$STD_Y4MD'
                                     )
 )
,T_STOC_RST AS
(
   SELECT A.BASE_DATE                 
        , A.EXPO_ID                         
        , A.FUND_CD
        , A.ASSET_TPCD
        , A.KICS_PROD_TPCD
        , A.KICS_PROD_TPNM
        , A.ISIN_CD
        , A.ISIN_NM
        , A.LT_TP
        , A.ACCO_CLCD
        , A.ACCO_CD
        , A.ACCO_NM
        , A.SR_CLSF_CD            AS STOC_RISK_DTLS_TPCD
        , A.CNPT_ID
        , A.CNPT_NM
        , A.CORP_NO
        , A.CRGR_KICS
        , A.CPTC_YN
        , A.FIDE_YN                  AS FIDE_YN                         -- 장내외파생상품
        , 'N'                             AS SRM_YN                          -- 경감상품 없음
--        , CASE WHEN SUBSTR(A.INST_CLCD, 1, 2) IN ('D4')                                                                           -- 지수선물의 경우 BS금액으로 설정
        , CASE WHEN  A.FIDE_YN  = 'Y'
               THEN A.FAIR_BS_AMT
               ELSE A.SR_EXPO_AMT END                 AS EXPO_AMT                        -- SR_EXPO_AMT는 대부분 공정가치와 일치하나 난외약정 등에서는 차이가 있음(-> BS = 0)
        , B.SHCK_VAL                                         AS RISK_COEF
--        , CASE WHEN SUBSTR(A.INST_CLCD, 1, 2) IN ('D4')                                                                           -- D: 파생상품, 4: 지수선물(비고. 5: 지수콜옵션, 6: 지수풋옵션, 3:국채선물)
        , CASE WHEN  A.FIDE_YN  = 'Y'
--               THEN A.SR_EXPO_AMT * B.SHCK_VAL
--                    * DECODE(SUBSTR(A.INST_CLCD, 3, 1), 'S', -1, 1)                                                               -- 지수선물의 경우 매도포지션(S)이면 요구자본 감소
               THEN GREATEST(A.SR_EXPO_AMT * B.SHCK_VAL
                    * DECODE(SUBSTR(A.INST_CLCD, 3, 1), 'S', -1, 1),0)                                                               -- 지수선물의 경우 매도포지션(S)이면 요구자본 감소
               ELSE A.SR_EXPO_AMT * B.SHCK_VAL
               END                                                                             AS SCR
--        , CASE WHEN SUBSTR(A.INST_CLCD, 1, 2) IN ('D4')
        , CASE WHEN  A.FIDE_YN  = 'Y'
               THEN A.FAIR_BS_AMT - A.SR_EXPO_AMT * B.SHCK_VAL
                    * DECODE(SUBSTR(A.INST_CLCD, 3, 1), 'S', -1, 1)                                                               -- 지수선물의 경우 매도포지션(S)이면 충격후공정가치 증가
               ELSE A.SR_EXPO_AMT - A.SR_EXPO_AMT * B.SHCK_VAL
               END                                                                             AS SCAF_FAIR_BS_AMT
        , 'KRDA102BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE
        , NULL                                                                                     AS LVG_RATIO /*레버리지비율*/
     FROM T_BASE      A
        , T_STOC_SHCK B
    WHERE 1=1
      AND A.SR_CLSF_CD NOT IN ('NON', 'S52','S53')   -- 주식형레버리지펀드, 부동산형레버리지펀드 외 주식위험 측정대상 
      AND A.SR_CLSF_CD     = B.SR_CLSF_CD(+)
      AND A.CRGR_ADJ        = B.CRGR_KICS(+)
)
, T_LVG_RST AS 
( /*주식형 레버리지펀드, 부동산형 레버리지펀드 추가 */
  SELECT A.BASE_DATE   
        , A.EXPO_ID          
        , A.FUND_CD
        , A.ASSET_TPCD
        , A.KICS_PROD_TPCD
        , A.KICS_PROD_TPNM
        , A.ISIN_CD
        , A.ISIN_NM
        , A.LT_TP
        , A.ACCO_CLCD
        , A.ACCO_CD
        , A.ACCO_NM
        , A.SR_CLSF_CD                                                                         AS STOC_RISK_DTLS_TPCD
        , A.CNPT_ID
        , A.CNPT_NM
        , A.CORP_NO
        , A.CRGR_KICS
        , A.CPTC_YN
        , 'N'                                                                            AS FIDE_YN                         -- 장내외파생상품
        , 'N'                                                                                  AS SRM_YN                          -- 경감상품 없음
        , A.SR_EXPO_AMT                                                           AS EXPO_AMT                       
/*
        , CASE WHEN A.SR_CLSF_CD = 'S52' THEN LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL,1) 
		     WHEN A.SR_CLSF_CD = 'S53' THEN LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL, 0.75)
		     ELSE NULL 
		      END 	 		                                                              AS RISK_COEF
        , CASE WHEN A.SR_CLSF_CD = 'S52' THEN LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL,1)  * A.SR_EXPO_AMT
		     WHEN A.SR_CLSF_CD = 'S53' THEN LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL, 0.75) * A.SR_EXPO_AMT 
		     ELSE NULL 
		      END 	 		                                                              AS SCR 
        , CASE WHEN A.SR_CLSF_CD = 'S52' THEN (1 - LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL,1))  * A.SR_EXPO_AMT
		     WHEN A.SR_CLSF_CD = 'S53' THEN (1 - LEAST(NVL(A.LVG_RATIO,100) /100 * B.SHCK_VAL, 0.75)) * A.SR_EXPO_AMT 
		     ELSE NULL 
		      END 	 		                                                              AS SCAF_FAIR_BS_AMT */

-- 24.01.30 이선영 주식위험 산출 기준서 개정내용 반영
        , CASE WHEN A.SR_CLSF_CD = 'S52' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 1 -- 주식형레버리지  : 약관상 레버리지 비율을 모르면 100% 적용 
                                                   ELSE GREATEST ( LEAST(A.LVG_RATIO /100 * 0.35 ,1) ,0.49)  END  -- 레버리지 비율을 알면 max{min(LVG RATIO *35% , 100%) , 49%}
		     WHEN A.SR_CLSF_CD = 'S53' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 0.75 -- 부동산레버리지  : 약관상 레버리지 비율을 모르면 75% 적용 
                                                 ELSE GREATEST( LEAST(A.LVG_RATIO /100 * 0.25, 0.75), 0.49) END --  레버리지 비율을 알면 max{min(LVG RATIO *25% , 75%) , 49%}
		     ELSE NULL 
		      END 	 		                                                              AS RISK_COEF
 		
        , ( CASE WHEN A.SR_CLSF_CD = 'S52' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 1 -- 주식형레버리지  : 약관상 레버리지 비율을 모르면 100% 적용 
                                                   ELSE GREATEST ( LEAST(A.LVG_RATIO /100 * 0.35 ,1) ,0.49)  END  -- 레버리지 비율을 알면 max{min(LVG RATIO *35% , 100%) , 49%}
		     WHEN A.SR_CLSF_CD = 'S53' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 0.75 -- 부동산레버리지  : 약관상 레버리지 비율을 모르면 75% 적용 
                                                 ELSE GREATEST( LEAST(A.LVG_RATIO /100 * 0.25, 0.75), 0.49) END --  레버리지 비율을 알면 max{min(LVG RATIO *25% , 75%) , 49%}
		     ELSE NULL 
		      END 	) * A.SR_EXPO_AMT                                          AS SCR 

        , (1 - ( CASE WHEN A.SR_CLSF_CD = 'S52' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 1 -- 주식형레버리지  : 약관상 레버리지 비율을 모르면 100% 적용 
                                                   ELSE GREATEST ( LEAST(A.LVG_RATIO /100 * 0.35 ,1) ,0.49)  END  -- 레버리지 비율을 알면 max{min(LVG RATIO *35% , 100%) , 49%}
		     WHEN A.SR_CLSF_CD = 'S53' THEN CASE WHEN A.LVG_RATIO IS NULL THEN 0.75 -- 부동산레버리지  : 약관상 레버리지 비율을 모르면 75% 적용 
                                                 ELSE GREATEST( LEAST(A.LVG_RATIO /100 * 0.25, 0.75), 0.49) END --  레버리지 비율을 알면 max{min(LVG RATIO *25% , 75%) , 49%}
		     ELSE NULL 
		      END) ) * A.SR_EXPO_AMT                                        AS SCAF_FAIR_BS_AMT 

        , 'KRDA102BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE
        , A.LVG_RATIO                                                                                     AS LVG_RATIO /*레버리지비율*/

     FROM T_BASE A
           , T_STOC_SHCK B
    WHERE 1=1
      AND A.SR_CLSF_CD     IN ('S52','S53')    -- 주식형레버리지펀드, 부동산형레버리지펀드 
      AND A.SR_CLSF_CD     = B.SR_CLSF_CD(+)
)

SELECT *
FROM T_STOC_RST A 
UNION ALL 	  
SELECT *
FROM T_LVG_RST  B