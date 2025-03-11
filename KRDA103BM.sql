/* 수정사항 
- 이종통화 반영을 위해 충격계수 가져오는 조건 수정 : KRW -> 모든 통화별
- 위험경감 미인정대상 처리대상추가 : 730 통화스왑, 731 FX스왑, 733 통화스왑 위험경감불인정 , 734 FX스왑 위험경감불인정
       # 방안 : 1. KICS PROD_TPCD 로 구분하여 처리 
       # 수정대상 : FRM_TPCD  1: 경감100%인정 / 2: 갱신계획 문서화 (경감인정비율 = 잔존만기비율) / 3.갱신계획 비문서화 (경감인정비율 = 잔존만기비율 + (1-잔존만기비율)* 80%) / NULL : 미인정 코드 부재 ====> (영향) FX_RPT_TPCD 외환보고서 유형코드 
                   FRM_ADJ_RTO (위험경감인정비율) = 0 (현재 경감유형 구분 없이 항상 계산하고 있음)
- 2025-02-18 : 외환 익스포져 요건 수정 (상대LEG 공정가치 미반영): NVL(A.FX_CNPT_EXPO_AMT, 0) * DECODE(A.FRM_TPCD, '1', 0, 0)
*/

WITH /* SQL-ID : KRDA103BM */ 
T_SHOCK AS 
(	
       -- SELECT CRNY_CD
       --        , KRW AS RISK_COEF
       -- FROM IKRUSH_SHCK_FX
       -- WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
-- 20240430 박경은 수정 : 이종통화 (EUR/USD, AUD/USD 등) 반영
	SELECT CRNY_CD, CNPT_CRNY_CD, RISK_COEF
	FROM IKRUSH_SHCK_FX
	UNPIVOT ( RISK_COEF FOR CNPT_CRNY_CD IN (AUD,BRL,CAD,CHF,CLP,CNY,COP,CZK,DKK,ETC,EUR,GBP,HKD,HUF,IDR,ILS,INR,JPY,KRW,MXN,MYR,NOK,NZD,PEN,PHP,PLN,RON,RUB,SAR,SEK,SGD,THB,TRY,TWD,USD,ZAR ))
      WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE 
)
, T_BASE AS 
(
             SELECT IIA.*
             , CASE WHEN IIA.BASE_DATE = '20201231'                             -- 헤지목적 파생상품임에도 불구하고 원천에서부터 대상외화채권과 연결고리가 누락된 경우 직접 입력처리
                    THEN CASE WHEN IIA.ISIN_CD IN ('SNDO21010401') THEN 'KR6249811672'
                                  WHEN IIA.ISIN_CD IN ('SNDO21010402') THEN 'KR6249812670'
                                  WHEN IIA.ISIN_CD IN ('SNDO21010403') THEN 'US16876BAA08'
                                  ELSE IIA.HDGE_ISIN_CD END
                    ELSE IIA.HDGE_ISIN_CD END   AS HDGE_ISIN_CD2
             , CASE WHEN IIA.BASE_DATE = '20201231'
                    THEN CASE WHEN IIA.ISIN_CD IN ('SNDO21010401') THEN '20270930'
                                  WHEN IIA.ISIN_CD IN ('SNDO21010402') THEN '20270930'
                                  WHEN IIA.ISIN_CD IN ('SNDO21010403') THEN '20470101'
                                  ELSE IIA.HDGE_MATR_DATE END
                    ELSE IIA.HDGE_MATR_DATE END   AS HDGE_MATR_DATE2
                    
                /* K-ICS 상품분류 세분화 
                    - 위험경감인정   : 통화스왑 (730),  FX스왑 (731) 
                    - 위험경감미인정 : 통화스왑 (733),  FX스왑 (734) : */
              , CASE WHEN ASSET_TPCD IN ('FIDE') AND KICS_PROD_TPCD IN ('730', '731') THEN '1' ELSE '0' END AS RISK_MITI_YN -- 위험경감 대상 : 통화스왑, FX스왑 : KICS_PROD_TPCD 조건 제외해도 동일한 결과임
              , TO_DATE(MATR_DATE, 'YYYYMMDD') - TO_DATE(ISSU_DATE, 'YYYYMMDD')          AS CONT_DDCT
              , MONTHS_BETWEEN(TO_DATE(HDGE_MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD'))/ 12 AS RMN_HDGE_MATR
              
    /* 참고 PRE EXPO 생성시 아래 로직으로 잔여만기와 계약만기를 생성함 (잔여 월도를 기준으로 처리)*/
    --            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS CONT_MATR
    --            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS RMN_MATR

               /*파생의 경우 포지션 구분을 따르고, 그외의 경우 계정과목 구분에서 부채로 구분되는 경우  SHORT POSITION으로 분류 */
            , CASE WHEN ASSET_TPCD IN ('FIDE') --AND KICS_PROD_TPCD IN ('730', '731') -- 24.05.02 조건 주석처리
                      THEN DECODE(LEG_TYPE, 'REC', 'L', 'PAY', 'S', NULL)                                                 -- 파생상품 수취LEG : 매수, 지급LEG : 매도 
                       ELSE DECODE(SUBSTR(ACCO_CD, 1, 1), '1', 'L', '2', 'S', NULL) END  AS POSITION_DV     -- 일반외화상품 자산계정: 매수, 부채계정: 매도
             FROM Q_IC_EXPO IIA
             WHERE 1=1
              AND BASE_DATE = '$$STD_Y4MD'
              AND FR_CLSF_CD     IN ('Y')
              AND LEG_TYPE   NOT IN ('NET')
              AND ACCO_CLCD      IN ('1', '2', '3')                                          -- 4:퇴직연금(실적), 5:변액보험(5) 제외
              -- AND NVL(ISIN_CD, '999999999999')
              --               NOT IN (                                                        -- 신종자본증권 헤지포지션 제외
              --                             SELECT HDGE_ISIN_CD
              --                             FROM IKRUSH_COBD_HDGE
              --                             --WHERE 1=1
              --                             WHERE 1=0                                            /* 20210803 modified (신종자본 헤지포지션 포함하도록 수정) */
              --                             AND BASE_DATE = '$$STD_Y4MD'
              --                      )
)
--SELECT * FROM T_BASE WHERE EXPO_ID ='SECR_O_4000000000476G000000001';
, T_EXPO AS 
(
       SELECT IA.*
                , IA.POSITION_DV                                                                         AS PCHS_SLL_DVCD
                , DECODE(IA.POSITION_DV,'L',1,'S',-1,1) * IA.FR_EXPO_AMT                AS FX_EXPO_AMT
                , DECODE(IA.POSITION_DV,'L',-1,'S',1,1) * IA.CNPT_FAIR_BS_AMT        AS FX_CNPT_EXPO_AMT -- 감독원 가이드라인에 따라 파생상품의 순자산가치를 산출하기 위해 상대LEG공정가치를 가져옴


       /* 위험경감 유형코드 
            RISK_MITI_YN ='1'  위험경감인정   : 통화스왑 (730),  FX스왑 (731)  */
              , CASE WHEN RISK_MITI_YN ='1' AND IA.LT_TP = 'Y' 
                        THEN CASE WHEN IA.RMN_MATR  >= 1.00    THEN '1'                       -- 경감 100%인정   : 파생상품 잔존만기 1년이상
                                        WHEN IA.CONT_DDCT >= 90      THEN '2'                       -- 갱신계획문서화  : 계약만기 3개월이상
                                        ELSE '3'
                                 END 
                        WHEN RISK_MITI_YN ='1' AND IA.HDGE_MATR_DATE IS NULL
                        THEN CASE WHEN IA.RMN_MATR  >= 1.00         THEN '1'                   -- 경감 100%인정   : 파생상품 잔존만기 1년이상
                                        WHEN IA.CONT_DDCT>= 90            THEN '2'
                                        ELSE '3' 
                                        END 	  
                        WHEN RISK_MITI_YN ='1' AND IA.HDGE_MATR_DATE IS NOT NULL
                        THEN CASE WHEN IA.RMN_MATR  >= 1.00                          THEN '1'    -- 경감 100%인정   : 파생상품 잔존만기 1년이상
                                        WHEN IA.MATR_DATE >= IA.HDGE_MATR_DATE   THEN '1'    -- 경감 100%인정   : 파생상품 파생상품만기가 헤지대상만기 이후
                                        WHEN IA.CONT_DDCT >= 90                            THEN '2'    -- 갱신계획문서화  : 계약만기 3개월이상
                                        ELSE '3' END                                                                -- 갱신계획비문서화: 계약만기 3개월미만
                         ELSE NULL END                                                                             AS FRM_TPCD -- 경감불인정대상은 null 

       /* 위험경감 적용비율 */
              , CASE WHEN RISK_MITI_YN ='1' AND IA.LT_TP = 'Y' THEN
                            CASE WHEN IA.RMN_MATR  >= 1.00        THEN 1.0
                                    WHEN IA.CONT_DDCT >= 90          THEN IA.RMN_MATR  + (1- IA.RMN_MATR) * 0.8
                                    ELSE IA.RMN_MATR
                             END        
                             
                       WHEN RISK_MITI_YN ='1' AND IA.HDGE_MATR_DATE IS NULL
                            THEN CASE WHEN IA.RMN_MATR  >= 1.00  THEN 1.0                       -- 경감 100%인정   : 파생상품 잔존만기 1년이상
                                            WHEN IA.CONT_DDCT >= 90    THEN IA.RMN_MATR + ( 1- IA.RMN_MATR) * 0.8		     
                                             ELSE  IA.RMN_MATR
                                     END
                                     
                       WHEN RISK_MITI_YN ='1' AND IA.HDGE_MATR_DATE IS NOT NULL
                                   THEN CASE WHEN IA.RMN_MATR  >= 1.00          THEN 1.0                       -- 경감 100%인정   : 파생상품 잔존만기 1년이상
                                                   WHEN IA.MATR_DATE >= IA.HDGE_MATR_DATE           THEN 1.0                       -- 위험경감기법(선도)만기가 위험경감대상(채권)만기 이후인 경우
                                                   WHEN IA.CONT_DDCT >= 90
                                                   THEN   ( 0 + IA.RMN_MATR / LEAST(IA.RMN_HDGE_MATR, 1)) * 1.0  -- 잔존만기비율
                                                           + ( 1 - IA.RMN_MATR / LEAST(IA.RMN_HDGE_MATR, 1)) * 0.8  -- (1 - 잔존만기비율) x 0.8   --20230314 김재우 수정  기준서 변경
                                            ELSE IA.RMN_MATR END
                       ELSE NULL END                                                                             AS FRM_ADJ_RTO -- 경감불인정대상은 null 

       /* 잔존만기비율 */
              , CASE WHEN RISK_MITI_YN ='1'
                            THEN CASE WHEN IA.LT_TP = 'Y'                             THEN ROUND( IA.RMN_MATR,3)
                                            WHEN IA.HDGE_MATR_DATE IS NULL        THEN ROUND( IA.RMN_MATR,3)
                                            WHEN IA.HDGE_MATR_DATE IS NOT NULL THEN ROUND(  IA.RMN_MATR / LEAST(IA.RMN_HDGE_MATR, 1), 3)
                            --ELSE LEAST(IA.RMN_MATR, 1) END
                            ELSE NULL END
                     ELSE NULL END                                                                             AS RMN_MATR_RTO


              , CASE WHEN IA.KICS_PROD_TPCD IN ('730', '731','733','734' )                    -- 2024.05.02 이선영 730 통화스왑, 731 FX스왑, 733 통화스왑 위험경감불인정 , 734 FX스왑 위험경감불인정
                     THEN CASE WHEN IA.HDGE_MATR_DATE IS NOT NULL       THEN '1'           -- 통화선도(위험경감대상 존재시)
                                                        ELSE '1' END                                                -- 통화선도(위험경감대상 부재시)
                     WHEN IA.KICS_PROD_TPCD IN ('651', '732' ,'753'   )          THEN '2'          -- 통화옵션
                     WHEN SUBSTR(IA.KICS_PROD_TPCD, 1, 1) IN ('6', '7')        THEN '3'         -- 기타파생
                     ELSE '4'                                                              -- 외화현물
                     END                                                                                       AS FX_FIDE_TPCD 

              , CASE WHEN IA.KICS_PROD_TPCD IN ('651', '732' ,'753'   ) THEN 'Y'           -- 651 통화옵션,  732 기타장외통화옵션  753 내재파생옵션-통화관련
                     ELSE 'N' END                                                                              AS CRNY_OPT_YN
              , NVL(IB.CRNY_CD, 'ETC')                                                                         AS CRNY_CD2         -- 통화충격에 없는 통화는 ETC 처리함

        FROM T_BASE IA
              , ( -- 기준서에 충격계수가 제시된 국가와 기타 국가를 구분하기 위해 JOIN 
                     SELECT CRNY_CD
                     FROM IKRUSH_SHCK_FX
                     WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
              ) IB
       WHERE 1=1
              AND IA.CRNY_CD   = IB.CRNY_CD(+)

)
--SELECT * FROM T_EXPO WHERE EXPO_ID ='FIDE_L_G000K55213C39556_FXW215D4M575_002'; 
, T_AAA AS
(
   SELECT A.BASE_DATE                                                                    AS BASE_DATE                       /*  기준일자               */
        , A.EXPO_ID                                                                            AS EXPO_ID                         /*  익스포저ID             */
        , A.PCHS_SLL_DVCD                                                                 AS PCHS_SLL_DVCD
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
        , A.CRNY_CD2                                                                         AS CRNY_CD
        , A.CNPT_ID                                                                            AS CNPT_ID
        , A.CNPT_NM                                                                           AS CNPT_NM
        , A.CORP_NO                                                                           AS CORP_NO
        , A.CONT_MATR                                                                       AS CONT_MATR
        , A.RMN_MATR_RTO                                                                  AS RMN_MATR_RTO
        , A.NOTL_AMT                                                                         AS NOTL_AMT
        , A.FX_FIDE_TPCD                                                                    AS FX_FIDE_TPCD                    /*  파생상품유형코드       */ -- 1 통화선도 2 통화옵션 3 기타파생 4 비파생
        , A.CRNY_OPT_YN                                                                    AS CRNY_OPT_YN
        , A.FRM_TPCD                                                                         AS FRM_TPCD                        /*  위험경감유형코드       */ -- 1 위험경감 100인정 2 갱신계획문서화 3 갱신계획비문서화
        , A.FRM_ADJ_RTO                                                                    AS FRM_ADJ_RTO
        , A.FX_EXPO_AMT + NVL(A.FX_CNPT_EXPO_AMT, 0) * DECODE(A.FRM_TPCD, '1', 0, 0)          AS EXPO_AMT         -- 2025-02-18 : 상대LEG 공정가치 미반영하도록 익스포저 수정
        , B.RISK_COEF                                                                          AS RISK_COEF
        , CASE WHEN A.FRM_TPCD IN ('1', '2', '3')
               THEN A.FX_EXPO_AMT * (1 - A.FRM_ADJ_RTO) + A.FX_EXPO_AMT * (1 + B.RISK_COEF) * A.FRM_ADJ_RTO        -- -> EQUIVALENT TO [A.FX_EXPO_AMT + A.FX_EXPO_AMT * B.RISK_COEF * A.FRM_ADJ_RTO]
               ELSE A.FX_EXPO_AMT * (1 + B.RISK_COEF)
               END
         + NVL(A.FX_CNPT_EXPO_AMT, 0) * DECODE(A.FRM_TPCD, '1', 0, 0)                         AS FXRT_UP_FAIR_AMT -- 2025-02-18 : 상대LEG 공정가치 미반영하도록 익스포저 수정
        , CASE WHEN A.FRM_TPCD IN ('1', '2', '3')
               THEN A.FX_EXPO_AMT * (1 - A.FRM_ADJ_RTO) + A.FX_EXPO_AMT * (1 - B.RISK_COEF) * A.FRM_ADJ_RTO
               ELSE A.FX_EXPO_AMT * (1 - B.RISK_COEF)
               END
          + NVL(A.FX_CNPT_EXPO_AMT, 0) * DECODE(A.FRM_TPCD, '1', 0, 0)                         AS FXRT_DWN_FAIR_AMT-- 2025-02-18 : 상대LEG 공정가치 미반영하도록 익스포저 수정
        , CASE WHEN A.FRM_TPCD IN ('2')                                                                            -- 잔존만기 1년 미만의 갱신계획 문서화 대상 중
--               THEN CASE WHEN MONTHS_BETWEEN(TO_DATE(A.MATR_DATE, 'YYYYMMDD'), TO_DATE(A.ISSU_DATE,'YYYYMMDD')) >= 12.00
               THEN CASE WHEN CONT_MATR >=1.0 --상동 
                         THEN A.NOTL_AMT * A.CRNY_FXRT * 0.01                                                      -- 계약만기 1년 이상 명목계약금액 * 1% (갱신계획문서화인 경우만)
                         ELSE A.NOTL_AMT * A.CRNY_FXRT * 0.02 END                                                  -- 계약만기 1년 미만 명목계약금액 * 2% (갱신계획문서화인 경우만)
               ELSE 0 END                                                                      AS PRC_VOL_RISK_AMT -- 갱신계획 비문서화에 대해서는 가격변동위험액 측정하지 않음(감독원 의견)
        , 'KRDA103BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE
     FROM T_EXPO A
           , T_SHOCK B
      WHERE A.CRNY_CD2 = B.CRNY_CD(+)
      AND NVL(A.CNPT_CRNY_CD,'KRW') = B.CNPT_CRNY_CD (+) -- 20240430 박경은 수정 : 이종통화 (EUR/USD, AUD/USD 등) 반영
)
SELECT * 
FROM T_AAA