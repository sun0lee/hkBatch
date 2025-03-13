/* 수정사항
MARKET LEDGER를 집계하는 기본적인 원칙은 EXPO ID의 포지션 단위로 집계하는 것이 아닌, NET기준으로 집계하고 있음. => EXPO_ID 당 1건의 데이터 발생 

20240513 (이종통화 처리)
일반적인 외환리스크 측정대상은 기준통화(혹은 상대통화)가 KRW로, EXPO ID당 1개의 외환리스크 산출대상이 발생함.
그러나 이종통화로 구성된 FX스왑은 외환리스크 측정결과가 개별 포지션의 통화별로 산출되기 때문에 2줄이 발생함. 
이 경우 RN을 구분하여 RN = 1은 NET 기준 집계 라인에 매핑하며 , RN=2는 더미 라인을 생성함. 
(RN=2로 생성된 데이터는 외환 리스크만 값의 의미가 있으며, 기타 위험액 및 익스포져, BS 잔액은 NULL처리)

20240514 위험경감미인정파생상품 경감효과 미인식을 위해 환율하락시 평가금액을 공정가치와 동일하게 처리 
20240523 자산 배치 결과만 가져오도록 조건 추가 (금리, 주식, 신용) : EXPO 기준 LEFT JOIN 이라 조건 추가 전후 차이는 없음. 
20250311 금리리스크 익스포져 부호처리대상 : 장내외파생상품 FV < 0 -> 부채 (부채기준 양수로 부호전환)
*/
WITH T_EXPO AS 
(
    SELECT A.*, C.PROD_GRP_TPCD
    FROM Q_IC_EXPO A
    , (
        SELECT KICS_PROD_TPCD, PROD_GRP_TPCD
        FROM IKRUSH_KICS_RISK_MAP 
      ) C
    WHERE A.BASE_DATE = '$$STD_Y4MD'
    AND A.LEG_TYPE IN ('STD','NET')
    AND A.KICS_PROD_TPCD = C.KICS_PROD_TPCD (+)
--    AND    EXPO_ID LIKE 'FIDE_L_G000KRZ502563480_%' --이종통화 처리대상 

)
, T_IR AS 
(
    SELECT * 
    FROM Q_IC_IRATE_RSLT
    WHERE BASE_DATE = '$$STD_Y4MD'
    AND LAST_MODIFIED_BY = 'KRDA101BM' -- 20240523 자산 배치 결과만 가져오도록 조건 추가 (금리, 주식, 신용)
)
, T_STOC AS
( 
    SELECT IC.*
           , CASE WHEN IC.ACCO_CLCD IN ('1', '9') THEN '1'
                  WHEN IC.ACCO_CLCD IN ('2','3') THEN '2'
                  WHEN IC.ACCO_CLCD IN ('5') THEN '3'
                  ELSE NULL
                   END
    --              || DECODE(SUBSTR(IC.STOC_RISK_DTLS_TPCD,1,2),'S3','S31', IC.STOC_RISK_DTLS_TPCD) -- 우선주 변환
    -- 김재우 수정 20240110
			|| CASE WHEN SUBSTR(IC.STOC_RISK_DTLS_TPCD,1,2) = 'S3' THEN 'S31' -- 우선주변환
			WHEN SUBSTR(IC.STOC_RISK_DTLS_TPCD,1,2) = 'S5' THEN 'S51' -- 기타주식 변환
			ELSE IC.STOC_RISK_DTLS_TPCD 
			END 
                        AS STOC_RPT_COA
           , CASE WHEN IC.CPTC_YN = 'Y' THEN '2'   -- 캐피탈콜
                  WHEN IC.FIDE_YN = 'Y' THEN '7'  -- 위험경감미인정파생상품
                  ELSE '1'    -- 보유주식/변액부채
                  END   AS STOC_RPT_TPCD -- 주식리스크보고서유형코드  
        FROM Q_IC_STOC_RSLT IC
      WHERE BASE_DATE = '$$STD_Y4MD'
      AND LAST_MODIFIED_BY = 'KRDA102BM' -- 20240523 자산 배치 결과만 가져오도록 조건 추가 (금리, 주식, 신용)
        AND ACCO_CLCD <> '4'
)
, T_FX AS
(    
     SELECT ID.*
      , CASE WHEN ID.FX_FIDE_TPCD = '1'  --통화선도
                  THEN CASE WHEN ID.FRM_TPCD = '1' THEN '3' --선도 위험경감100%인정
                            WHEN ID.FRM_TPCD = '2' THEN '5' --선도 문서화
                            WHEN ID.FRM_TPCD = '3' THEN '4' --선도 비문서화  
                            ELSE '6' --기타파생
                             END
             WHEN ID.FX_FIDE_TPCD = '2'  --통화옵션
                  THEN CASE WHEN ID.FRM_TPCD = '1' THEN '7' --옵션 위험경감100%인정
                            WHEN ID.FRM_TPCD = '2' THEN '9' --옵션 문서화
                            WHEN ID.FRM_TPCD = '3' THEN '8' --옵션 비문서화
                            ELSE '10' -- 옵션 기타파생     
                             END
             WHEN ID.FX_FIDE_TPCD = '3'  --기타파생
                  THEN '6' -- 기타파생
             WHEN SUBSTR(ID.ACCO_CD,1,1) ='1'  THEN '1' --자산
             WHEN SUBSTR(ID.ACCO_CD,1,1) ='2'  THEN '2' --부채
             ELSE NULL 
              END 
               AS FX_RPT_TPCD -- 외환보고서유형코드
        , CASE WHEN CRNY_OPT_YN = 'N' AND CONT_MATR < 1 THEN '11'  --통화선도 1년미만
               WHEN CRNY_OPT_YN = 'N' AND CONT_MATR >= 1 THEN  '12'  --통화선도 1년이상
               WHEN CRNY_OPT_YN = 'Y' AND CONT_MATR < 1 THEN '13'  --통화옵션 1년미만
               WHEN CRNY_OPT_YN = 'Y' AND CONT_MATR >= 1 THEN  '14'  --통화옵션 1년이상
                 ELSE NULL 
                    END AS CONT_MATR_CLCD -- 계약만기구분코드       
         , ROW_NUMBER () OVER (PARTITION BY  EXPO_ID ORDER BY CRNY_CD DESC ) AS RN -- 20240513 (이종통화 처리) 이선영 수정
   FROM ( 
        SELECT EXPO_ID
             , MAX(ACCO_CD)                          AS ACCO_CD  
--             , MAX(CRNY_CD)                          AS CRNY_CD  -- 20240513 (이종통화 처리) 이선영 수정
             , CRNY_CD-- 20240513 (이종통화 처리) 이선영 수정
             , MAX(CONT_MATR)                     AS CONT_MATR
             , MAX(RMN_MATR_RTO)             AS RMN_MATR_RTO
             , MAX(FX_FIDE_TPCD)                 AS FX_FIDE_TPCD
             , MAX(CRNY_OPT_YN)                 AS CRNY_OPT_YN
             , MAX(FRM_TPCD)                         AS FRM_TPCD
             , MAX(FRM_ADJ_RTO)                  AS FRM_ADJ_RTO
             , SUM(EXPO_AMT)                        AS EXPO_AMT
             , MAX(RISK_COEF)                        AS RISK_COEF
             , SUM(FXRT_UP_FAIR_AMT)        AS FXRT_UP_FAIR_AMT
             , SUM(FXRT_DWN_FAIR_AMT)    AS FXRT_DWN_FAIR_AMT
             , SUM(PRC_VOL_RISK_AMT)        AS PRC_VOL_RISK_AMT
               FROM Q_IC_FX_RSLT
             WHERE BASE_DATE = '$$STD_Y4MD'
             GROUP BY EXPO_ID 
             , CRNY_CD  -- 20240513 (이종통화 처리) 이선영 수정
         ) ID    

)
, T_PRPT AS
( 
        SELECT * 
        FROM Q_IC_PRPT_RSLT
        WHERE BASE_DATE = '$$STD_Y4MD'
)
, T_CR AS
(
    SELECT F1.*
          , DECODE(CRM_AFT_EXPO_AMT,0,0,CRM_AFT_SCR/CRM_AFT_EXPO_AMT) AS CRM_AFT_COEF
       FROM 
          ( SELECT REPLACE(EXPO_ID,'_SNR') AS EXPO_ID
                 , SUM(CRM_AFT_SCR) AS CRM_AFT_SCR   
                 , SUM(CRM_AFT_EXPO_AMT) AS CRM_AFT_EXPO_AMT    
              FROM Q_IC_CR_RSLT
             WHERE BASE_DATE = '$$STD_Y4MD'
             AND LAST_MODIFIED_BY = 'KRDB101BM' -- 20240523 자산 배치 결과만 가져오도록 조건 추가 (금리, 주식, 신용)
             GROUP BY REPLACE(EXPO_ID,'_SNR')
           ) F1
)
SELECT A.BASE_DATE
     , A.EXPO_ID
     , A.LEG_TYPE
     , A.ASSET_TPCD
     , A.FUND_CD
     , A.PROD_TPCD
     , A.PROD_TPNM
     , A.KICS_PROD_TPCD
     , A.KICS_PROD_TPNM
     , A.ISIN_CD
     , A.ISIN_NM
     , A.LT_TP
     , A.PRNT_ISIN_CD
     , A.PRNT_ISIN_NM
     , A.ACCO_CLCD
     , A.ACCO_CD
     , A.ACCO_NM
     , A.ISSU_DATE
     , A.MATR_DATE
     , A.CONT_MATR
     , A.RMN_MATR
     , A.EFFE_MATR
     , A.EFFE_DURA
     , A.IMP_SPRD
     , A.CRNY_CD
     , A.CRNY_FXRT
     , A.CNTY_CD
     , A.NOTL_AMT
     , A.BS_AMT
     , A.FAIR_BS_AMT
     , A.ACCR_AMT
     , A.UERN_AMT
     , A.CNPT_CRNY_CD
     , A.CNPT_CRNY_FXRT
     , A.CNPT_FAIR_BS_AMT
     , A.FV_RPT_COA
     , A.FV_HIER_NODE_ID
     , A.CNPT_ID
     , A.CNPT_NM
     , A.CORP_NO
     , A.CRGR_KIS
     , A.CRGR_NICE
     , A.CRGR_KR
     , A.CRGR_SNP
     , A.CRGR_MOODYS
     , A.CRGR_FITCH
     , A.CRGR_KICS
     , A.CRGR_CB_NICE
     , A.CRGR_CB_KCB
     , A.IND_L_CLSF_CD
     , A.IND_L_CLSF_NM
     , A.IND_CLSF_CD
     , B.IRATE_RPT_COA
     , B.IR_CLSF_CD
     , B.IR_CLSF_NM
     , B.IR_SHCK_WGT
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN01, B.IR_EXPO_SCEN01) AS IR_EXPO_SCEN01
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN02, B.IR_EXPO_SCEN02) AS IR_EXPO_SCEN02
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN03, B.IR_EXPO_SCEN03) AS IR_EXPO_SCEN03
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN04, B.IR_EXPO_SCEN04) AS IR_EXPO_SCEN04
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN05, B.IR_EXPO_SCEN05) AS IR_EXPO_SCEN05
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN06, B.IR_EXPO_SCEN06) AS IR_EXPO_SCEN06
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN07, B.IR_EXPO_SCEN07) AS IR_EXPO_SCEN07
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN08, B.IR_EXPO_SCEN08) AS IR_EXPO_SCEN08
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN09, B.IR_EXPO_SCEN09) AS IR_EXPO_SCEN09
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN10, B.IR_EXPO_SCEN10) AS IR_EXPO_SCEN10
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN11, B.IR_EXPO_SCEN11) AS IR_EXPO_SCEN11
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN12, B.IR_EXPO_SCEN12) AS IR_EXPO_SCEN12
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN13, B.IR_EXPO_SCEN13) AS IR_EXPO_SCEN13
    --  , DECODE(SUBSTR(B.IRATE_RPT_COA,1,2) , '2Y', -1* B.IR_EXPO_SCEN14, B.IR_EXPO_SCEN14) AS IR_EXPO_SCEN14
    /* 2025-03-11 장내외파생상품 FV < 0 인 경우 부채로 분류하고, 부채기준으로 부호(+)처리 */
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN01 ELSE B.IR_EXPO_SCEN01 END  AS IR_EXPO_SCEN01
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN02 ELSE B.IR_EXPO_SCEN02 END  AS IR_EXPO_SCEN02
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN03 ELSE B.IR_EXPO_SCEN03 END  AS IR_EXPO_SCEN03
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN04 ELSE B.IR_EXPO_SCEN04 END  AS IR_EXPO_SCEN04
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN05 ELSE B.IR_EXPO_SCEN05 END  AS IR_EXPO_SCEN05
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN06 ELSE B.IR_EXPO_SCEN06 END  AS IR_EXPO_SCEN06
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN07 ELSE B.IR_EXPO_SCEN07 END  AS IR_EXPO_SCEN07
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN08 ELSE B.IR_EXPO_SCEN08 END  AS IR_EXPO_SCEN08
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN09 ELSE B.IR_EXPO_SCEN09 END  AS IR_EXPO_SCEN09
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN10 ELSE B.IR_EXPO_SCEN10 END  AS IR_EXPO_SCEN10
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN11 ELSE B.IR_EXPO_SCEN11 END  AS IR_EXPO_SCEN11
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN12 ELSE B.IR_EXPO_SCEN12 END  AS IR_EXPO_SCEN12
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN13 ELSE B.IR_EXPO_SCEN13 END  AS IR_EXPO_SCEN13
     , CASE WHEN A.PROD_GRP_TPCD IN ('6','7') AND SUBSTR(B.IRATE_RPT_COA, 1,1) ='2' THEN -1 * B.IR_EXPO_SCEN14 ELSE B.IR_EXPO_SCEN14 END  AS IR_EXPO_SCEN14
     , C.STOC_RPT_COA
     , C.STOC_RPT_TPCD
     , C.STOC_RISK_DTLS_TPCD AS SR_CLSF_CD
     , A.SR_CLSF_NM
     , A.LVG_RATIO
     , C.EXPO_AMT AS SR_EXPO_AMT
     , C.CPTC_YN		 
     , C.FIDE_YN            AS STOC_FIDE_YN
     , C.SRM_YN		        AS SRM_YN		 
     , C.RISK_COEF		    AS SR_SHCK_COEF
     , C.SCR                AS SR_SCR
     , C.SCAF_FAIR_BS_AMT   AS SR_SCAF_FAIR_BS_AMT
     , D.CRNY_CD		    AS FX_CRNY_CD
     , D.FX_RPT_TPCD
     , D.CONT_MATR_CLCD		 
     , A.HDGE_ISIN_CD
     , A.HDGE_MATR_DATE
     , D.FX_FIDE_TPCD
     , D.CRNY_OPT_YN
     , D.FRM_TPCD
     , D.FRM_ADJ_RTO
     , D.EXPO_AMT           AS FR_EXPO_AMT
     , D.RISK_COEF          AS FR_SHCK_COEF
     , D.FXRT_UP_FAIR_AMT 
     , CASE WHEN D.FX_RPT_TPCD = '6' THEN D.EXPO_AMT ELSE D.FXRT_DWN_FAIR_AMT END AS FXRT_DWN_FAIR_AMT -- 24.05.14 이선영 수정 위험경감미인정파생상품 경감효과 미인식을 위해 공정가치와 동일하게 처리 
     , D.PRC_VOL_RISK_AMT
     , E.KICS_PROD_TPCD     AS PR_CLSF_CD  
     , E.KICS_PROD_TPNM     AS PR_CLSF_NM
     , E.EXPO_AMT           AS PR_EXPO_AMT  
     , E.RISK_COEF          AS PR_SHCK_COEF
     , E.SCR                AS PR_SCR
     , E.SCAF_FAIR_BS_AMT   AS PR_SCAF_FAIR_BS_AMT 
     , F.CRM_AFT_COEF
     , F.CRM_AFT_EXPO_AMT
     , 'KRDA115BM'  AS LAST_MODIFIED_BY
     , SYSDATE AS LAST_UPDATE_DATE
   FROM  T_EXPO   A  -- EXPOSURE
      ,  T_IR     B  -- 금리위험
      ,  T_STOC   C  -- 주식위험
      ,  T_FX     D	 -- 외환위험
      ,  T_PRPT   E  -- 부동산위험
      ,  T_CR     F  -- 신용위험 
      
WHERE A.EXPO_ID = B.EXPO_ID(+)
      AND  A.EXPO_ID = C.EXPO_ID(+)
      AND  A.EXPO_ID = D.EXPO_ID(+)
      AND  A.EXPO_ID = E.EXPO_ID(+)
      AND  A.EXPO_ID = F.EXPO_ID(+)
      AND 1 = D.RN(+) -- 20240513 (이종통화 처리) 이선영 수정


-- 20240513 (이종통화 처리) 이선영 수정 RN =2인 데이터 추가 생성 
UNION ALL 
SELECT A.BASE_DATE
     , A.EXPO_ID||CASE WHEN D.RN = 2 THEN '_'||D.RN ELSE NULL END AS EXPO_ID 
     , A.LEG_TYPE     , A.ASSET_TPCD     , A.FUND_CD     , A.PROD_TPCD     , A.PROD_TPNM     , A.KICS_PROD_TPCD     , A.KICS_PROD_TPNM     , A.ISIN_CD     , A.ISIN_NM     , A.LT_TP
     , A.PRNT_ISIN_CD     , A.PRNT_ISIN_NM     , A.ACCO_CLCD     , A.ACCO_CD     , A.ACCO_NM     , A.ISSU_DATE     , A.MATR_DATE     , A.CONT_MATR     , A.RMN_MATR
     , A.EFFE_MATR     , A.EFFE_DURA     , A.IMP_SPRD     , A.CRNY_CD     , A.CRNY_FXRT     , A.CNTY_CD
     , NULL NOTL_AMT     , NULL BS_AMT     , NULL FAIR_BS_AMT     , NULL ACCR_AMT     , NULL UERN_AMT
     , A.CNPT_CRNY_CD     , A.CNPT_CRNY_FXRT
     , NULL CNPT_FAIR_BS_AMT
     , A.FV_RPT_COA     , A.FV_HIER_NODE_ID     , A.CNPT_ID     , A.CNPT_NM     , A.CORP_NO     
     , A.CRGR_KIS     , A.CRGR_NICE     , A.CRGR_KR     , A.CRGR_SNP     , A.CRGR_MOODYS     , A.CRGR_FITCH     , A.CRGR_KICS
     , A.CRGR_CB_NICE     , A.CRGR_CB_KCB     , A.IND_L_CLSF_CD     , A.IND_L_CLSF_NM     , A.IND_CLSF_CD
     , B.IRATE_RPT_COA     , B.IR_CLSF_CD     , B.IR_CLSF_NM     , B.IR_SHCK_WGT
     , NULL  IR_EXPO_SCEN01     , NULL IR_EXPO_SCEN02     , NULL IR_EXPO_SCEN03     , NULL IR_EXPO_SCEN04     , NULL IR_EXPO_SCEN05     , NULL IR_EXPO_SCEN06     , NULL IR_EXPO_SCEN07
     , NULL IR_EXPO_SCEN08     , NULL IR_EXPO_SCEN09     , NULL IR_EXPO_SCEN10     , NULL IR_EXPO_SCEN11     , NULL IR_EXPO_SCEN12     , NULL IR_EXPO_SCEN13     , NULL IR_EXPO_SCEN14
     , C.STOC_RPT_COA     , C.STOC_RPT_TPCD     , C.STOC_RISK_DTLS_TPCD AS SR_CLSF_CD
     , A.SR_CLSF_NM     , A.LVG_RATIO     , NULL SR_EXPO_AMT
     , C.CPTC_YN		 
     , C.FIDE_YN               AS STOC_FIDE_YN
     , C.SRM_YN		    AS SRM_YN		 
     , C.RISK_COEF		    AS SR_SHCK_COEF
     , NULL                      AS SR_SCR
     , NULL                      AS SR_SCAF_FAIR_BS_AMT
     , D.CRNY_CD		   AS FX_CRNY_CD
     , D.FX_RPT_TPCD
     , D.CONT_MATR_CLCD		 
     , A.HDGE_ISIN_CD     , A.HDGE_MATR_DATE
     , D.FX_FIDE_TPCD
     , D.CRNY_OPT_YN
     , D.FRM_TPCD
     , D.FRM_ADJ_RTO
     , D.EXPO_AMT           AS FR_EXPO_AMT
     , D.RISK_COEF           AS FR_SHCK_COEF
     , D.FXRT_UP_FAIR_AMT 
     , CASE WHEN D.FX_RPT_TPCD = '6' THEN D.EXPO_AMT ELSE D.FXRT_DWN_FAIR_AMT END AS FXRT_DWN_FAIR_AMT -- 24.05.14 이선영 수정 위험경감미인정파생상품 경감효과 미인식을 위해 공정가치와 동일하게 처리 
     , D.PRC_VOL_RISK_AMT
     , E.KICS_PROD_TPCD     AS PR_CLSF_CD  
     , E.KICS_PROD_TPNM     AS PR_CLSF_NM
     , NULL           AS PR_EXPO_AMT  
     , E.RISK_COEF          AS PR_SHCK_COEF
     , NULL                AS PR_SCR
     , NULL   AS PR_SCAF_FAIR_BS_AMT 
     , F.CRM_AFT_COEF
     , NULL CRM_AFT_EXPO_AMT
     , 'KRDA115BM'  AS LAST_MODIFIED_BY
     , SYSDATE AS LAST_UPDATE_DATE
   FROM  T_EXPO   A  -- EXPOSURE
      ,  T_IR     B  -- 금리위험
      ,  T_STOC   C  -- 주식위험
      ,  T_FX     D	 -- 외환위험
      ,  T_PRPT   E  -- 부동산위험
      ,  T_CR     F  -- 신용위험 
      
WHERE A.EXPO_ID = B.EXPO_ID(+)
      AND  A.EXPO_ID = C.EXPO_ID(+)
      AND  A.EXPO_ID = D.EXPO_ID(+)
      AND  A.EXPO_ID = E.EXPO_ID(+)
      AND  A.EXPO_ID = F.EXPO_ID(+)
      AND  D.RN = 2