/* 수정사항 
- 계정코드별 예외처리 : ACCO_EXCPTN
- SUB TABLE WITH 문 처리

계정과목별 예외 처리 
    1. --comment : 13600402계정(특별자산수익증권)에서 SOC대출_무위험이 아닌, 미수수익 등임 
       : A.ACCO_CD IN ('13600402') // 매도가능금융자산-수익증권_특별자산투자신탁  THEN 'SS4'  -->정부 등 무위험기관 전액보증 인프라사업 => => (CD2)       FROM ACCO WHERE RMK LIKE '%SS4%'
    2. 계좌관리대상의 경우 부채계정은 평가결과가 (-)임 이를 보정하기 위한 부호 -1 처리  : SUBSTR(A1.ACCO_CD, 3, 4) IN ('2760', '2310') => (ACCO_TOBE) FROM ACCO WHERE RMK LIKE '%SIGN-1%'

- T_COA : 유가증권등 SAP 보유원장등에서 입수하는 유가증권 항목은  ACCO의 잔액을 가져오지 않도록 대상 정의  

2024-05-02 : 위험경감불인정 통화선도 추가 
     - 730 : 통화스왑 CRS
     - 731 : FX스왑 
     - 733 : 통화스왑 CRS (위험경감불인정) => 신규추가 
     - 734 : FX스왑 (위험경감불인정 => 신규추가)

2025-02-03 : 기업대출 공정가치 외부 입수대상 처리 
       - 수기입력 : IKRUSH_LT_TOTAL (LT_SEQ =0) -> Q_IC_ASSET_SECR (입수경로 변경) => 외부 공정가치에 따라 내재스프레드 산출 
       - Q_IC_ASSET_LOAN 자동생성대상 제외 

*/
WITH /* SQL-ID : KRDA106BM */ 
ACCO_EXCPTN AS (
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRDA106BM'
)
--SELECT* FROM ACCO_EXCPTN ;
, T_COA AS  -- ACCO에서 제외할 BLNG_LEAF_HIER_NODE_ID 대상     
(
            SELECT HIER_TPCD
                   , HIER_TPNM
                   , ACCO_TPCD
                   , COA_ID
                   , COA_NM
                   , BLNG_LEAF_HIER_NODE_ID
                   , BLNG_SIGN1
                   , BLNG_SIGN2
                   , LAST_MODIFIED_BY
                   , LAST_MODIFIED_DATE
                   , CASE WHEN BLNG_LEAF_HIER_NODE_ID LIKE 'A12%' THEN 'N' ELSE 'Y' END AS ACCO_YN /*시산표의 잔액을 가져오는 대상 구분추가 : 24.02.08 유가증권 ACCO 에서 가져오는 대상에서 제외 */
                   , ACCO_TPCD ||'_'||COA_ID AS ACCO_KEY 
              FROM IKRUSH_COA_MAP
             WHERE HIER_TPCD = '93'	    
)
--SELECT * FROM T_COA ; 
, T_SECR AS  (    
            SELECT BASE_DATE, EXPO_ID, FUND_CD, PROD_TPCD, PROD_TPNM, KICS_PROD_TPCD, INST_TPCD||INST_DTLS_TPCD AS INST_CLCD
                , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN SUBSTR(EXPO_ID, 12, 12) ELSE NULL END AS PRNT_ISIN_CD
                , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN CONT_ID                 ELSE NULL END AS PRNT_ISIN_NM
                , ISIN_CD, ISIN_NM, CONT_ID, ACCO_CD, ACCO_NM, ISSU_DATE, MATR_DATE, IRATE, IRATE_TPCD, EXT_DURA
                , 'STD' AS LEG_TYPE, NULL AS PRPT_CNTY_TPCD
                , CRNY_CD, CRNY_FXRT, BS_AMT, FAIR_BS_AMT, NOTL_AMT, NULL AS CNPT_CRNY_CD, NULL AS CNPT_CRNY_FXRT, NULL AS CNPT_FAIR_BS_AMT
                , ACCR_AMT, UERN_AMT, CNPT_ID, CNPT_NM, CORP_NO
                , CRGR_KIS, CRGR_NICE, CRGR_KR, CRGR_KICS, NULL AS CONT_QNTY, NULL AS CONT_MULT, NULL AS CONT_PRC, NULL AS SPOT_PRC, UNDL_EXEC_PRC, UNDL_SPOT_PRC
                , STOC_TPCD, PRF_STOC_YN, BOND_RANK_CLCD, STOC_LIST_CLCD, CNTY_CD, ASSET_LIQ_CLCD, DEPT_CD
                , CASE WHEN SUBSTR(ACCO_CD, 1, 1) IN ('1') THEN '1'
                       WHEN FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'GNL_FUND_CD')  THEN '1'
                       ELSE '2' END AS IFRS_ACCO_CLCD
                , DECODE(SUBSTR(EXPO_ID, 6, 1), 'L', 'Y', 'N') AS LT_TP, NULL AS HDGE_ISIN_CD, NULL AS HDGE_MATR_DATE
                , CASE WHEN SUBSTR(KICS_PROD_TPCD, 1, 1) IN ('7')
                       THEN CASE WHEN KICS_PROD_TPCD IN ('752', '720', '721'                     ) THEN '1'    -- 금리파생
                                 WHEN KICS_PROD_TPCD IN ('753', '730', '731', '732', '733', '734') THEN '2'    -- 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                                 WHEN KICS_PROD_TPCD IN ('751', '710', '711', '712', '713', '740') THEN '3'    -- 주식파생
                                 WHEN KICS_PROD_TPCD IN ('754'                                   ) THEN '6'    -- 신용파생(기타로 처리하면 안됨)
                                 ELSE '5' END                                                                  -- 기타미분류파생
                       WHEN SUBSTR(KICS_PROD_TPCD, 1, 1) IN ('6')
                       THEN NULL                                                                               -- 장내파생상품은 신용리스크 CCF반영 대상에서 제외
                       ELSE NULL END                                                                                                    AS FIDE_UNDL_CLCD  -- 파생상품 신용리스크 CCF반영 관련
                , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS CONT_MATR
                , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS RMN_MATR
                , CRGR_SNP
                , CRGR_MOODYS
                , CRGR_FITCH
                , IND_L_CLSF_CD
                , IND_L_CLSF_NM
                , IND_CLSF_CD
                , NULL AS OBGT_PSSN_ESTE_YN
                , LVG_RATIO AS LVG_RATIO	--24.01.29 이선영 수정 : 재간접펀드의 레버리지 비율을 읽어오지 못하는 현상 수정.	
                , NULL AS SNR_LTV
                , NULL AS FAIR_VAL_SNR
                , NULL AS DSCR
             FROM Q_IC_ASSET_SECR
            WHERE 1=1
              AND BASE_DATE = '$$STD_Y4MD'
)
--SELECT * FROM T_SECR ;
, T_LOAN AS (
       SELECT BASE_DATE, EXPO_ID, FUND_CD, PROD_TPCD, PROD_TPNM, KICS_PROD_TPCD, INST_TPCD||INST_DTLS_TPCD AS INST_CLCD
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN ISIN_CD ELSE NULL END AS PRNT_ISIN_CD
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN ISIN_NM ELSE NULL END AS PRNT_ISIN_NM
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN NULL ELSE ISIN_CD END AS ISIN_CD
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN NULL ELSE ISIN_NM END AS ISIN_NM
            , CONT_ID, ACCO_CD, ACCO_NM, ISSU_DATE, MATR_DATE, IRATE, IRATE_TPCD, NULL AS EXT_DURA
            , 'STD' AS LEG_TYPE, NULL AS PRPT_CNTY_TPCD
            , CRNY_CD, CRNY_FXRT, BS_AMT, FAIR_BS_AMT, NOTL_AMT, NULL AS CNPT_CRNY_CD, NULL AS CNPT_CRNY_FXRT, NULL AS CNPT_FAIR_BS_AMT
            , ACCR_AMT, UERN_AMT, CNPT_ID, CNPT_NM, CORP_NO
            , CRGR_KIS, CRGR_NICE, CRGR_KR, CRGR_KICS, NULL AS CONT_QNTY, NULL AS CONT_MULT, NULL AS CONT_PRC, NULL AS SPOT_PRC, NULL AS UNDL_EXEC_PRC, NULL AS UNDL_SPOT_PRC
            , NULL STOC_TPCD, NULL PRF_STOC_YN, NULL BOND_RANK_CLCD, NULL STOC_LIST_CLCD, NULL CNTY_CD, NULL ASSET_LIQ_CLCD, DEPT_CD
            , CASE WHEN SUBSTR(ACCO_CD, 1, 1) IN ('1') THEN '1'
                   WHEN FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'GNL_FUND_CD') THEN '1'
                   ELSE '2' END AS IFRS_ACCO_CLCD
            , DECODE(SUBSTR(EXPO_ID, 6, 1), 'L', 'Y', 'N') AS LT_TP, NULL AS HDGE_ISIN_CD, NULL AS HDGE_MATR_DATE
            , NULL AS FIDE_UNDL_CLCD
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS CONT_MATR
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS RMN_MATR
            , CRGR_SNP
            , CRGR_MOODYS
            , CRGR_FITCH
            , IND_L_CLSF_CD
            , IND_L_CLSF_NM
            , IND_CLSF_CD
            , NULL AS OBGT_PSSN_ESTE_YN
            , NULL AS LVG_RATIO	
            , SNR_LTV
            , FAIR_VAL_SNR
            , DSCR
        FROM Q_IC_ASSET_LOAN
        WHERE 1=1
        AND BASE_DATE = '$$STD_Y4MD'
        /*2025-02-03 기업대출 공정가치 외부입수건은 대상은 자동 생성 제외 2025-02-06 ISIN_CD */
	 AND CONT_ID NOT IN  
              (	SELECT ISIN_CD 
                     FROM IKRUSH_LT_TOTAL 
                     WHERE BASE_DATE = '$$STD_Y4MD' 
                     AND LT_SEQ = 0
              )
)
--SELECT * FROM T_LOAN ;
, T_FIDE AS 
(
        SELECT BASE_DATE, EXPO_ID, FUND_CD, PROD_TPCD, PROD_TPNM, KICS_PROD_TPCD
--            , INST_TPCD||INST_DTLS_TPCD||POTN_TPCD AS INST_CLCD 
             ,INST_TPCD||CASE WHEN PROD_TPCD IN ('70E', '70G') THEN '3' ELSE INST_DTLS_TPCD END ||POTN_TPCD AS INST_CLCD  -- 원복 
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN SUBSTR(EXPO_ID, 12, 12) ELSE NULL END AS PRNT_ISIN_CD
            , CASE WHEN SUBSTR(EXPO_ID, 6, 1) IN ('L') THEN CONT_ID                 ELSE NULL END AS PRNT_ISIN_NM
            , ISIN_CD, ISIN_NM, CONT_ID, ACCO_CD, ACCO_NM, ISSU_DATE, MATR_DATE
            , DECODE(LV, 1, NVL(REC_IRATE, 0)    , 2, NVL(PAY_IRATE, 0)    , NVL(REC_IRATE - PAY_IRATE, 0)        )     AS IRATE
            , DECODE(LV, 1, REC_IRATE_TPCD       , 2, PAY_IRATE_TPCD       , REC_IRATE_TPCD                       )     AS IRATE_TPCD
            , DECODE(LV, 1, NVL(REC_EXT_DURA, 0) , 2, NVL(PAY_EXT_DURA, 0) , NVL(EXT_DURA, 0)                     )     AS EXT_DURA
            , DECODE(LV, 1, 'REC'                , 2, 'PAY'                , 'NET'                                )     AS LEG_TYPE
            , NULL AS PRPT_CNTY_TPCD
            , DECODE(LV, 1, REC_CRNY_CD          , 2, PAY_CRNY_CD          , 'KRW'                                )     AS CRNY_CD
            , DECODE(LV, 1, NVL(REC_CRNY_FXRT, 1), 2, NVL(PAY_CRNY_FXRT, 1), 1                                    )     AS CRNY_FXRT
            , DECODE(LV, 1, 0                    , 2, 0                    , NVL(BS_AMT, 0)                       )     AS BS_AMT
            , ROUND(
              DECODE(LV, 1, NVL(REC_EXT_UPRC, 0) , 2, NVL(PAY_EXT_UPRC, 0) , NVL(FAIR_BS_AMT, 0)                  ), 0) AS FAIR_BS_AMT
            , DECODE(LV, 1, NVL(REC_NOTL_AMT, 0) , 2, NVL(PAY_NOTL_AMT, 0) , NVL(REC_NOTL_AMT * REC_CRNY_FXRT, 0) )     AS NOTL_AMT
            , DECODE(LV, 1, PAY_CRNY_CD          , 2, REC_CRNY_CD          , 'KRW'                                )     AS CNPT_CRNY_CD
            , DECODE(LV, 1, NVL(PAY_CRNY_FXRT, 1), 2, NVL(REC_CRNY_FXRT, 1), 1                                    )     AS CNPT_CRNY_FXRT
            , ROUND(
              DECODE(LV, 1, NVL(PAY_EXT_UPRC , 0), 2, NVL(REC_EXT_UPRC , 0), NVL(FAIR_BS_AMT, 0)                  ), 0) AS CNPT_FAIR_BS_AMT
            , ACCR_AMT, UERN_AMT, CNPT_ID, CNPT_NM, CORP_NO
            , CRGR_KIS, CRGR_NICE, CRGR_KR, CRGR_KICS, CONT_QNTY, CONT_MULT, CONT_PRC, SPOT_PRC, UNDL_EXEC_PRC, UNDL_SPOT_PRC
            , STOC_TPCD, PRF_STOC_YN, BOND_RANK_CLCD, STOC_LIST_CLCD, CNTY_CD, ASSET_LIQ_CLCD, DEPT_CD
            , CASE WHEN SUBSTR(ACCO_CD, 1, 1) IN ('1') THEN '1'
                   WHEN FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'GNL_FUND_CD')  THEN '1'
                   ELSE '2' END AS IFRS_ACCO_CLCD
            , DECODE(SUBSTR(EXPO_ID, 6, 1), 'L', 'Y', 'N') AS LT_TP, HDGE_ISIN_CD, HDGE_MATR_DATE
            , CASE WHEN SUBSTR(KICS_PROD_TPCD, 1, 1) IN ('7')
                   THEN CASE WHEN KICS_PROD_TPCD IN ('752', '720', '721'                     ) THEN '1'    -- 금리파생
                             WHEN KICS_PROD_TPCD IN ('753', '730', '731', '732', '733', '734') THEN '2'    -- 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                             WHEN KICS_PROD_TPCD IN ('751', '710', '711', '712', '713', '740') THEN '3'    -- 주식파생
                             WHEN KICS_PROD_TPCD IN ('754'                                   ) THEN '6'    -- 신용파생(기타로 처리하면 안됨)
                             ELSE '5' END                                                                  -- 기타미분류파생
                   WHEN SUBSTR(KICS_PROD_TPCD, 1, 1) IN ('6')
                   THEN NULL                                                                               -- 장내파생상품은 신용리스크 CCF반영 대상에서 제외
                   ELSE NULL END                                                                                                    AS FIDE_UNDL_CLCD  -- 파생상품 신용리스크 CCF반영 관련
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS CONT_MATR
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS RMN_MATR
            , CRGR_SNP
            , CRGR_MOODYS
            , CRGR_FITCH
            , IND_L_CLSF_CD
            , IND_L_CLSF_NM
            , IND_CLSF_CD
            , NULL AS OBGT_PSSN_ESTE_YN
            , NULL AS LVG_RATIO	
            , NULL AS SNR_LTV
            , NULL AS FAIR_VAL_SNR
            , NULL AS DSCR			    
         FROM Q_IC_ASSET_FIDE
            , (
                 SELECT LEVEL AS LV
                   FROM DUAL
                CONNECT BY LEVEL <= 3
              )
        WHERE 1=1
          AND BASE_DATE = '$$STD_Y4MD'
)
--SELECT * FROM T_FIDE ;
, T_ACCO AS 
(
       SELECT BASE_DATE, EXPO_ID, FUND_CD, PROD_TPCD, PROD_TPNM, KICS_PROD_TPCD, INST_TPCD||INST_DTLS_TPCD AS INST_CLCD
            , NULL AS PRNT_ISIN_CD, NULL AS PRNT_ISIN_NM
            , ISIN_CD, ISIN_NM, CONT_ID, ACCO_CD, ACCO_NM, ISSU_DATE, MATR_DATE, NULL AS IRATE, NULL AS IRATE_TPCD, NULL AS EXT_DURA
            , 'STD' AS LEG_TYPE, '1' AS PRPT_CNTY_TPCD
            , CRNY_CD, CRNY_FXRT, BS_AMT, FAIR_BS_AMT, NOTL_AMT, NULL AS CNPT_CRNY_CD, NULL AS CNPT_CRNY_FXRT, NULL AS CNPT_FAIR_BS_AMT
            , ACCR_AMT, UERN_AMT, CNPT_ID, CNPT_NM, CORP_NO
            , CRGR_KIS, CRGR_NICE, CRGR_KR, CRGR_KICS, NULL AS CONT_QNTY, NULL AS CONT_MULT, NULL AS CONT_PRC, NULL AS SPOT_PRC, NULL AS UNDL_EXEC_PRC, NULL AS UNDL_SPOT_PRC
            , STOC_TPCD, PRF_STOC_YN, BOND_RANK_CLCD, STOC_LIST_CLCD, CNTY_CD, ASSET_LIQ_CLCD, DEPT_CD
            , CASE WHEN SUBSTR(ACCO_CD, 1, 1) IN ('1') THEN '1'
                   WHEN FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'GNL_FUND_CD') THEN '1'
                   ELSE '2' END AS IFRS_ACCO_CLCD
            , 'N' AS LT_TP, NULL AS HDGE_ISIN_CD, NULL AS HDGE_MATR_DATE
            , NULL AS FIDE_UNDL_CLCD
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS CONT_MATR
            , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3) AS RMN_MATR
            , NULL AS CRGR_SNP
            , NULL AS CRGR_MOODYS
            , NULL AS CRGR_FITCH
            , NULL AS IND_L_CLSF_CD
            , NULL AS IND_L_CLSF_NM
            , NULL AS IND_CLSF_CD
            , NULL AS OBGT_PSSN_ESTE_YN
            , NULL AS LVG_RATIO	
            , NULL AS SNR_LTV
            , NULL AS FAIR_VAL_SNR
            , NULL AS DSCR			    
     FROM Q_IC_ASSET_ACCO A 
    WHERE 1=1
      AND BASE_DATE = '$$STD_Y4MD'
      AND NOT EXISTS (SELECT 1 FROM T_COA  B WHERE A.ACCO_CD = B.ACCO_KEY AND B.ACCO_YN = 'N') -- 제외대상만 빼기 

)
--SELECT * FROM T_ACCO ;
, T_EXPO AS /*익스포져 통합*/
(
     SELECT A1.BASE_DATE
                  , A1.EXPO_ID
                  , A1.LEG_TYPE
                  , SUBSTR(A1.EXPO_ID, 1, 4)                                                             AS ASSET_TPCD                           -- SECR / LOAN / FIDE / ACCO / PRPT / CASH
--                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3, 4) IN ('2760', '2310')                                                                     -- 계좌관리대상의 경우 부채계정은 평가결과가 (-)임 이를 보정하기 위한 부호
                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SIGN-1' )   --24.04.29 계좌관리대상의 경우 부채계정은 평가결과가 (-)임 이를 보정하기 위한 부호 
                              AND SUBSTR(A1.EXPO_ID, 1, 4) NOT IN ('ACCO') THEN -1 
--                         WHEN SUBSTR(A1.ACCO_CD, 3, 2) IN ('2A') THEN -1                                                                       --  24.02.08  대상 없음 
                              ELSE 1 END            AS AMT_SIGN                             -- 파생상품의 경우 REC / PAY가 아닌 NET을 사용하므로 LEG_TYPE IN ('STD', 'NET') 조건은 따로 처리하지 않음
                  , A1.FUND_CD
                  , A1.PROD_TPCD
                  , A1.PROD_TPNM
                  , CASE WHEN A1.LT_TP = 'Y'
        --20230525 김재우 수정 유동화코드 00으로 바뀜
        --                         THEN CASE WHEN A1.KICS_PROD_TPCD IN ('151')             AND NVL(A1.ASSET_LIQ_CLCD, '0') <> '0' THEN '251'               -- CP중 유동화계열은 재분류( -> '251' 유동화(ABCP))
        --                                   WHEN SUBSTR(A1.KICS_PROD_TPCD, 1, 1) IN ('2') AND NVL(A1.ASSET_LIQ_CLCD, '0') <> '0' THEN '250'               -- 유동화 코드있는 것들은 반영해야할듯(CP이외의 일반채권계열)
                         THEN CASE WHEN A1.KICS_PROD_TPCD IN ('151')             AND NVL(A1.ASSET_LIQ_CLCD, '00') <> '00' THEN '251'               -- CP중 유동화계열은 재분류( -> '251' 유동화(ABCP))
                                   WHEN SUBSTR(A1.KICS_PROD_TPCD, 1, 1) IN ('2') AND NVL(A1.ASSET_LIQ_CLCD, '00') <> '00' THEN '250'               -- 유동화 코드있는 것들은 반영해야할듯(CP이외의 일반채권계열)
                                   WHEN A1.KICS_PROD_TPCD IN ('299')                                                                             -- DET_RPMT_RANK_CLCD IN IDHKRH_BND_INFO --> 3, 4는 후순위/신종에 해당하는 듯함(2는 유동화SPC)
                                   THEN CASE WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('02')
                                             THEN DECODE(SUBSTR(A1.BOND_RANK_CLCD, -1, 1), 'N', '231', 'Y', '233', '230')                        -- '02_N': 231(후순위), '02_Y': 233(조건부후순위), 그외 일반선순위채권 처리
                                             WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('03')
                                             THEN DECODE(SUBSTR(A1.BOND_RANK_CLCD, -1, 1), 'N', '232', 'Y', '234', '230')                        -- '03_N': 232(신종)  , '03_Y': 234(조건부신종)  , 그외 일반선순위채권 처리
                                             ELSE A1.KICS_PROD_TPCD END
                                   WHEN A1.KICS_PROD_TPCD IN ('410', '420', '499')                                                               -- KICS상품분류 중 '410'(상장주식_미분류), '420'(비상장주식_미분류)에 대한 재분류
                                   THEN CASE WHEN A1.STOC_LIST_CLCD = 'Y'                                                                        -- 상장여부 'Y' (계좌원장기준)
                                             THEN CASE WHEN NVL(A3.FTSE_CLCD, '9') = '1'
                                                       THEN DECODE(A1.PRF_STOC_YN, 'Y', '413', '411')                                            -- 선진시장 상장 -> 우선주/보통주
                                                       ELSE DECODE(A1.PRF_STOC_YN, 'Y', '413', '412') END                                        -- 신흥시장 상장 -> 우선주/보통주
                                             WHEN A2.LIST_GB IN ('1', '2')
                                             THEN DECODE(A1.PRF_STOC_YN, 'Y', '413', '411')                                                      -- 국내시장 상장 -> 우선주/보통주
                                             ELSE DECODE(A1.PRF_STOC_YN, 'Y', '422', '421') END                                                  -- 비상장        -> 우선주/보통주
                                   ELSE A1.KICS_PROD_TPCD END

                         WHEN A1.LT_TP = 'N'
        --20230525 김재우 수정 유동화코드 00으로 바뀜
        --                         THEN CASE WHEN A1.KICS_PROD_TPCD IN ('151')             AND NVL(A1.ASSET_LIQ_CLCD, '0') <> '0' THEN '251'               -- CP중 유동화계열은 재분류( -> '251' 유동화(ABCP))
        --                                   WHEN SUBSTR(A1.KICS_PROD_TPCD, 1, 1) IN ('2') AND NVL(A1.ASSET_LIQ_CLCD, '0') <> '0' THEN '250'               -- 유동화 코드있는 것들은 반영해야할듯(CP이외의 일반채권계열)
                         THEN CASE WHEN A1.KICS_PROD_TPCD IN ('151')             AND NVL(A1.ASSET_LIQ_CLCD, '00') <> '00' THEN '251'               -- CP중 유동화계열은 재분류( -> '251' 유동화(ABCP))
                                   WHEN SUBSTR(A1.KICS_PROD_TPCD, 1, 1) IN ('2') AND NVL(A1.ASSET_LIQ_CLCD, '00') <> '00' THEN '250'               -- 유동화 코드있는 것들은 반영해야할듯(CP이외의 일반채권계열)
                                   WHEN A1.KICS_PROD_TPCD IN ('230', '231')                                                                      -- KICS상품분류 중 '230'(선순위채), '231'(후순위채)에 대한 재확인 및 분류(후순위채/신종증권 등)
                                   THEN CASE WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('02')
                                             THEN DECODE(SUBSTR(A1.BOND_RANK_CLCD, -1, 1), 'N', '231', 'Y', '233', A1.KICS_PROD_TPCD)            -- '02_N': 231(후순위), '02_Y': 233(조건부후순위)
                                             WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('03')
                                             THEN DECODE(SUBSTR(A1.BOND_RANK_CLCD, -1, 1), 'N', '232', 'Y', '234', A1.KICS_PROD_TPCD)            -- '03_N': 232(신종)  , '03_Y': 234(조건부신종)
                                             ELSE A1.KICS_PROD_TPCD END
                                   WHEN A1.KICS_PROD_TPCD IN ('410', '420')                                                                      -- KICS상품분류 중 '410'(상장주식_미분류), '420'(비상장주식_미분류)에 대한 재분류
                                   THEN CASE WHEN A1.STOC_LIST_CLCD = 'Y'                                                                        -- 상장여부 'Y' (계좌원장기준)
                                             THEN CASE WHEN NVL(A3.FTSE_CLCD, '9') = '1'
                                                       THEN DECODE(A1.PRF_STOC_YN, 'Y', '413', '411')                                            -- 선진시장 상장 -> 우선주/보통주
                                                       ELSE DECODE(A1.PRF_STOC_YN, 'Y', '413', '412') END                                        -- 신흥시장 상장 -> 우선주/보통주
                                             WHEN A2.LIST_GB IN ('1', '2')
                                             THEN DECODE(A1.PRF_STOC_YN, 'Y', '413', '411')                                                      -- 국내시장 상장 -> 우선주/보통주
                                             ELSE DECODE(A1.PRF_STOC_YN, 'Y', '422', '421') END                                                  -- 비상장        -> 우선주/보통주
                                   WHEN A1.KICS_PROD_TPCD IN ('910')
                                   THEN CASE WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('02') THEN '911'                                           -- 발행채권 중 후순위발행채권 재분류
                                             ELSE A1.KICS_PROD_TPCD END
                                   WHEN A1.KICS_PROD_TPCD IN ('920')
                                   THEN CASE WHEN SUBSTR(A1.BOND_RANK_CLCD, 1, 2) IN ('02') THEN '921'                                           -- 차입금 중 후순위차입금 재분류
                                             ELSE A1.KICS_PROD_TPCD END
                                   ELSE A1.KICS_PROD_TPCD END
                         ELSE A1.KICS_PROD_TPCD END                                                      AS KICS_PROD_TPCD
                  , A1.INST_CLCD                                                                         AS INST_CLCD
                  , A1.ISIN_CD                                                                           AS ISIN_CD
                  , A1.ISIN_NM                                                                           AS ISIN_NM
                  , A1.CONT_ID                                                                           AS CONT_ID
                  , A1.LT_TP                                                                             AS LT_TP
                  , A1.PRNT_ISIN_CD                                                                      AS PRNT_ISIN_CD
                  , A1.PRNT_ISIN_NM                                                                      AS PRNT_ISIN_NM
                  , CASE WHEN SUBSTR(A1.ACCO_CD, 1, 1) IN ('1')                                                       THEN '1:일반'              -- 일반계정
                         ELSE CASE WHEN A1.FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'GNL_FUND_CD'   )   THEN '1:일반'              -- 일반계정(특별계정중 연금저축, 주식연계, 신공시저축 등(A,E,S Respectively)
                                   WHEN A1.FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'SPAC02_FUND_CD')   THEN '2:퇴직보험'          -- 퇴직보험(계좌)
                                   WHEN A1.FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'SPAC03_FUND_CD')   THEN '3:퇴직연금'          -- 퇴직연금원리금보장(계좌)
                                   WHEN A1.FUND_CD IN (SELECT CD FROM IKRASM_CO_CD WHERE GRP_CD = 'SPAC04_FUND_CD')   THEN '4:퇴직연금실적'      -- 퇴직연금실적(계정)
                                   ELSE '5:변액보험' END                                                                                         -- 변액보험(계정)
                         END                                                                             AS ACCO_CLCD
                  , SUBSTR(A1.ACCO_CD, 3)                                                                AS ACCO_CD
                  , A1.ACCO_NM
                  , A4.ASSET_CLSF_CD                                                                     AS CR_PUBI_TPCD
                  , A4.ASSET_CLSF_NM                                                                     AS CR_PUBI_TPNM
                  , A1.ISSU_DATE
                  , A1.MATR_DATE
                  , A1.IRATE
                  , A1.IRATE_TPCD
                  , A1.EXT_DURA
                  , A1.PRPT_CNTY_TPCD
                  , A1.CRNY_CD
                  , NVL(A1.CRNY_FXRT, 1)                                                                 AS CRNY_FXRT
                  --A1.BS_AMT < 0 의 조건은 ISIN_CD = 'KRO5293AF542' (ISIN_NM 대우증권(DLB)255) 와 관련해서 처리한 부분임(장부가는+ , 평가결과는 -인 상황, 다른 계좌와는 룰이 차이가 있음)
--                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3, 4) IN ('2760', '2310') 
                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SIGN-1' )  --24.04.29 
                          AND SUBSTR(A1.EXPO_ID, 1, 4) NOT IN ('ACCO') AND A1.LEG_TYPE IN ('STD', 'NET') AND A1.BS_AMT < 0
                         THEN -A1.BS_AMT           ELSE A1.BS_AMT           END                          AS BS_AMT
--                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3, 4) IN ('2760', '2310') 
                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SIGN-1' )
                          AND SUBSTR(A1.EXPO_ID, 1, 4) NOT IN ('ACCO') AND A1.LEG_TYPE IN ('STD', 'NET')
                         THEN -A1.FAIR_BS_AMT      
                         WHEN A1.LT_TP = 'Y' AND A1.KICS_PROD_TPCD IN ('730','731','733', '734') AND A1.LEG_TYPE = 'REC' THEN A8.REC_AMT -- '733', '734' 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                         WHEN A1.LT_TP = 'Y' AND A1.KICS_PROD_TPCD IN ('730','731','733', '734') AND A1.LEG_TYPE = 'PAY' THEN A8.PAY_AMT -- '733', '734' 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                         ELSE A1.FAIR_BS_AMT      END                          AS FAIR_BS_AMT
                  , NVL(A1.NOTL_AMT, 0)                                                                  AS NOTL_AMT
                  , NVL(A1.ACCR_AMT, 0)                                                                  AS ACCR_AMT
                  , NVL(A1.UERN_AMT, 0)                                                                  AS UERN_AMT
                  , A1.CNPT_CRNY_CD
                  , NVL(A1.CNPT_CRNY_FXRT, 1)                                                            AS CNPT_CRNY_FXRT
--                  , CASE WHEN SUBSTR(A1.ACCO_CD, 3, 4) IN ('2760', '2310') 
                  , CASE WHEN  SUBSTR(A1.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SIGN-1' )
                          AND SUBSTR(A1.EXPO_ID, 1, 4) NOT IN ('ACCO') AND A1.LEG_TYPE IN ('STD', 'NET')
                         THEN -A1.CNPT_FAIR_BS_AMT 
                         WHEN A1.LT_TP = 'Y' AND A1.KICS_PROD_TPCD IN ('730','731','733', '734') AND A1.LEG_TYPE = 'REC' THEN A8.PAY_AMT -- '733', '734' 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                         WHEN A1.LT_TP = 'Y' AND A1.KICS_PROD_TPCD IN ('730','731','733', '734') AND A1.LEG_TYPE = 'PAY' THEN A8.REC_AMT -- '733', '734' 환율파생 이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
                         ELSE A1.CNPT_FAIR_BS_AMT END                          AS CNPT_FAIR_BS_AMT
                  , A1.CNPT_ID
                  , A1.CNPT_NM
                  , A1.CORP_NO
                  , A1.CRGR_KIS
                  , A1.CRGR_NICE
                  , A1.CRGR_KR
                  , A1.CRGR_KICS
                  , A2.GOCO_CD
                  , A2.GOCO_NM
                  , NVL(A1.CONT_QNTY, 0)                                                                 AS CONT_QNTY
                  , NVL(A7.CONT_MULT    , NVL(A1.CONT_MULT    , 1))                                      AS CONT_MULT
                  , NVL(A7.CONT_PRC     , NVL(A1.CONT_PRC     , 0))                                      AS CONT_PRC
                  , NVL(A7.SPOT_PRC     , NVL(A1.SPOT_PRC     , 0))                                      AS SPOT_PRC
                  , NVL(A7.UNDL_EXEC_PRC, NVL(A1.UNDL_EXEC_PRC, 0))                                      AS UNDL_EXEC_PRC
                  , NVL(A7.UNDL_SPOT_PRC, NVL(A1.UNDL_SPOT_PRC, 0))                                      AS UNDL_SPOT_PRC
                  , NVL(A7.UPRC_UNIT, 1)                                                                 AS OPT_UPRC_UNIT
                  , A1.HDGE_ISIN_CD
                  , A1.HDGE_MATR_DATE
                  , A1.STOC_TPCD
                  , A1.PRF_STOC_YN
                  , A1.BOND_RANK_CLCD
                  , A1.STOC_LIST_CLCD
                  , A1.CNTY_CD
                  , A1.ASSET_LIQ_CLCD
                  , A1.IFRS_ACCO_CLCD
                  , A5.ASSET_CLSF_CD                                                                     AS CR_CLSF_CD
                  , A5.ASSET_CLSF_NM                                                                     AS CR_CLSF_NM
                  , A5.CR_ACCO_TPCD                                                                      AS CR_ACCO_TPCD
                  , A5.CR_ACCO_TPNM                                                                      AS CR_ACCO_TPNM
                  , A5.IR_CLSF_CD                                                                        AS IR_CLSF_CD
                  , A5.IR_CLSF_NM                                                                        AS IR_CLSF_NM
                  , A5.SR_CLSF_CD                                                                        AS SR_CLSF_CD
                  , A5.SR_CLSF_NM                                                                        AS SR_CLSF_NM
                  , A1.FIDE_UNDL_CLCD
                  , A1.CONT_MATR
                  , A1.RMN_MATR
                  , NVL(A6.CCF, 0)                                                                       AS CCF
                  , A1.DEPT_CD
                    , A1.CRGR_SNP
                    , A1.CRGR_MOODYS
                    , A1.CRGR_FITCH
                    , A1.IND_L_CLSF_CD
                    , A1.IND_L_CLSF_NM
                    , A1.IND_CLSF_CD
                    , A1.OBGT_PSSN_ESTE_YN
                    , A1.LVG_RATIO
                    , A1.SNR_LTV
                    , A1.FAIR_VAL_SNR
                    , A1.DSCR			    	
               FROM (
                        SELECT * FROM T_SECR 
                        UNION ALL
                        SELECT * FROM T_LOAN
                        UNION ALL
                        SELECT * FROM T_FIDE 
                        UNION ALL
                        SELECT * FROM T_ACCO

                    ) A1
                  , (
                       SELECT ICNO                AS CORP_NO
                            , NVL(STRC_PTCD, '9') AS LIST_GB
                            , GOCO_CD             AS GOCO_CD
                            , GOCO_NM             AS GOCO_NM
                         FROM IDHKRH_COMPANY_INFO
                        WHERE STND_DT = (
                                           SELECT MAX(STND_DT)
                                             FROM IDHKRH_COMPANY_INFO
                                            WHERE STND_DT <= '$$STD_Y4MD'
                                        )
                    ) A2
                  , (
                       SELECT CNTY_CD
                            , FTSE_CLCD
                         FROM IKRUSH_FTSE T1
                        WHERE BASE_DATE  = (
                                              SELECT MAX(BASE_DATE)
                                                FROM IKRUSH_FTSE
                                               WHERE BASE_DATE <= '$$STD_Y4MD'
                                                 AND CNTY_CD = T1.CNTY_CD
                                           )
                    ) A3
                  , (
                       SELECT *
                         FROM (
                                 SELECT A41.CORP_NO
                                      , A41.CORP_NM
                                      , A41.ASSET_CLSF_CD
                                      , A41.ASSET_CLSF_NM
                                      , A41.BASE_DATE
                                      , ROW_NUMBER() OVER (PARTITION BY A41.CORP_NO ORDER BY A41.BASE_DATE DESC) AS RN
                                   FROM IKRUSH_PUBI A41
                                  WHERE A41.BASE_DATE <= '$$STD_Y4MD'
                               )
                         WHERE RN = 1                                                          -- BASE_DATE 역순으로 정렬하여 최신값 적용
                    ) A4
                  , (
                       SELECT *
                         FROM (
                                 SELECT A51.*
                                      , ROW_NUMBER() OVER (PARTITION BY IFRS_ACCO_CLCD, ACCO_CD ORDER BY BASE_DATE DESC) AS RN
                                   FROM IKRUSH_ACCO A51
                                  WHERE 1=1
                                    AND A51.BASE_DATE <= '$$STD_Y4MD'
                              )
                        WHERE RN = 1                                                           -- BASE_DATE 역순으로 정렬하여 최신값 적용
                    ) A5
                  , (
                       SELECT *
                         FROM IKRUSH_CCF
                        WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
                    ) A6
                  , (
                       SELECT *
                         FROM IKRUSH_FIDE_OPT
                        WHERE BASE_DATE = '$$STD_Y4MD'
                    ) A7
                  , (
                       SELECT EXPO_ID
                            , SUM(DECODE(LEG_TYPE,'REC',VALUE,0)) AS REC_AMT
                            , SUM(DECODE(LEG_TYPE,'PAY',VALUE,0)) AS PAY_AMT    
                         FROM Q_IC_ASSET_RSLT
                        WHERE BASE_DATE = '$$STD_Y4MD'
                          AND SCEN_TYPE = 'IR'
                          AND RSLT_TYPE = '14'        -- PV_CLEAN
			                    AND LEG_TYPE  IN ('PAY', 'REC')
                  			  AND SCEN_NUM = 1
			                  GROUP BY EXPO_ID       
                    ) A8

              WHERE 1=1
                AND A1.CORP_NO = A2.CORP_NO(+)
                AND A1.CNTY_CD = A3.CNTY_CD(+)
                AND A1.CORP_NO = A4.CORP_NO(+)
                AND A1.IFRS_ACCO_CLCD     = A5.IFRS_ACCO_CLCD(+)
                AND SUBSTR(A1.ACCO_CD, 3) = A5.ACCO_CD(+)
                AND A1.FIDE_UNDL_CLCD     = A6.FIDE_UNDL_CLCD(+)
                AND A1.RMN_MATR >= A6.STRT_VAL(+)
                AND A1.RMN_MATR  < A6.END_VAL(+)
                AND A1.ISIN_CD = A7.ISIN_CD(+)
                AND A1.EXPO_ID = A8.EXPO_ID(+) 
-- 20230602 김재우 수정
--                AND SUBSTR(A1.ACCO_CD, 3, 1) IN ('1', '2')                                     -- 자산 및 부채계정(자본계정 제외)
                AND SUBSTR(A1.ACCO_CD, 3, 1) <> '3'                                              -- 자본계정제외
)
--SELECT * FROM T_EXPO WHERE LEG_TYPE = 'NET' AND EXPO_ID ='FIDE_O_1000000038674';
, T_ENG_RST AS /*공정가치 평가결과*/
(
             SELECT EXPO_ID                                                                    AS EXPO_ID
                  , LEG_TYPE                                                                   AS LEG_TYPE
                  , GREATEST(NVL(SUM(DECODE(SCEN_TYPE, 'IR', DECODE(RSLT_TYPE, 16, VALUE, 0), 0)), 0.003 ), 0.003)
                                                                                               AS EFFE_MATR
                  , GREATEST(NVL(SUM(DECODE(SCEN_TYPE, 'IR', DECODE(RSLT_TYPE, 17, VALUE, 0), 0)), 0.003 ), 0.003)
                                                                                               AS EFFE_DURA
                  , SUM(DECODE(SCEN_TYPE, 'CL', DECODE(RSLT_TYPE, 61, VALUE, 0), 0))           AS IMP_SPRD
               FROM Q_IC_ASSET_RSLT
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
                AND SCEN_NUM  = '1'
                AND SCEN_TYPE IN ('IR', 'CL')                                                  -- 내재스프레드 CL(캘리브레이션)에서 확보가능
                AND RSLT_TYPE IN ('16', '17', '61')                                            -- 16: 유효만기, 17: 유효듀레이션, 61: 내재스프레드(채권)
              GROUP BY EXPO_ID, LEG_TYPE

              UNION ALL

             SELECT T1.FCONT_ID                                                                AS EXPO_ID
                  , 'STD'                                                                      AS LEG_TYPE
                  , ROUND(GREATEST(NVL(T2.MATR, (T1.MATURITY_DATE - T1.AS_OF_DATE) / 365.0), 0.003), 3)
                                                                                               AS EFFE_MATR
                  , ROUND(GREATEST(NVL(T2.DURA, (T1.MATURITY_DATE - T1.AS_OF_DATE) / 365.0), 0.003), 3)
                                                                                               AS EFFE_DURA
                  , NULL                                                                       AS IMP_SPRD
               FROM Q_CB_INST_BOND_LOAN T1
                  , (
                       SELECT FCONT_ID
                            , CASE WHEN SUM(CF) <> 0
                                   THEN SUM((CF_DATE - CALC_DATE) / 365.0 * CF) / SUM(CF)
                                   ELSE NULL END                                               AS  MATR
                            , CASE WHEN SUM(PV) <> 0
                                   THEN SUM((CF_DATE - CALC_DATE) / 365.0 * PV) / SUM(PV)
                                   ELSE NULL END                                               AS  DURA
                         FROM Q_IF_ER_FV_DET_HIST
                        WHERE CALC_DATE = TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
                        GROUP BY FCONT_ID
                    ) T2
              WHERE 1=1
                AND T1.FCONT_ID   = T2.FCONT_ID(+)
                AND T1.AS_OF_DATE = TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
)
--SELECT * FROM T_ENG_RST WHERE EXPO_ID ='FIDE_O_1000000037794';
, T_IR_RST AS /*금리위험 측정결과*/
( 
      SELECT EXPO_ID                                                                   AS EXPO_ID
           , VALUE1                                                                    AS AMT01
           , VALUE2                                                                    AS AMT02
           , VALUE3                                                                    AS AMT03
           , VALUE4                                                                    AS AMT04
           , VALUE5                                                                    AS AMT05
           , VALUE6                                                                    AS AMT06
           , VALUE7                                                                    AS AMT07
           , VALUE8                                                                    AS AMT08
           , VALUE9                                                                    AS AMT09
           , VALUE10                                                                   AS AMT10
           , VALUE11                                                                   AS AMT11
           , VALUE12                                                                   AS AMT12
           , VALUE13                                                                   AS AMT13
           , VALUE14                                                                   AS AMT14
        FROM
             (
                SELECT EXPO_ID                                                         AS EXPO_ID
                     , SCEN_NUM                                                        AS SCEN_NUM
                     , VALUE                                                           AS VALUE
                  FROM Q_IC_ASSET_RSLT
                 WHERE 1=1
                   AND BASE_DATE = '$$STD_Y4MD'
                   AND SCEN_TYPE = 'IR'
                   AND RSLT_TYPE = '14'        -- PV_CLEAN
                   AND LEG_TYPE  IN ('STD', 'NET')

                 UNION ALL

                SELECT FCONT_ID                                                        AS EXPO_ID
                     , TO_CHAR(SCEN_NUM)                                               AS SCEN_NUM
                     , ROUND(VALUE, 0)                                                 AS VALUE
                  FROM Q_AL_RSLT_A
                 WHERE 1=1
                   AND AS_OF_DATE = TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
                   AND FE_CD      = '9764'     -- PV_DIRTY(상단 쿼리에서 PV_CLEAN으로 변환함)
             )
       PIVOT (SUM(VALUE) FOR SCEN_NUM IN (  1 AS VALUE1
                                          , 2 AS VALUE2
                                          , 3 AS VALUE3
                                          , 4 AS VALUE4
                                          , 5 AS VALUE5
                                          , 6 AS VALUE6
                                          , 7 AS VALUE7
                                          , 8 AS VALUE8
                                          , 9 AS VALUE9
                                          ,10 AS VALUE10
                                          ,11 AS VALUE11
                                          ,12 AS VALUE12
                                          ,13 AS VALUE13
                                          ,14 AS VALUE14 ) )
)
--SELECT * FROM T_IR_RST WHERE EXPO_ID ='FIDE_O_1000000037794';
, T_AAA AS
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , A.EXPO_ID                                                                            AS EXPO_ID                         /*  익스포저ID             */
        , A.LEG_TYPE                                                                           AS LEG_TYPE                        /*  LEG구분(표준/수취/지급/NET)*/
        , A.ASSET_TPCD                                                                         AS ASSET_TPCD                      /*  자산유형코드           */
        , A.FUND_CD                                                                            AS FUND_CD                         /*  펀드코드               */
        , A.PROD_TPCD                                                                          AS PROD_TPCD                       /*  상품유형코드           */
        , A.PROD_TPNM                                                                          AS PROD_TPNM                       /*  상품유형명             */
        , A.KICS_PROD_TPCD                                                                     AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
        , KICS.PROD_TPNM                                                                       AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , A.ISIN_CD                                                                            AS ISIN_CD                         /*  종목코드               */
        , A.ISIN_NM                                                                            AS ISIN_NM                         /*  종목명                 */
        , A.LT_TP                                                                              AS LT_TP                           /*  LOOK THROUGH구분       */
        , A.PRNT_ISIN_CD                                                                       AS PRNT_ISIN_CD                    /*  수익증권종목코드       */
        , NVL(A.PRNT_ISIN_NM, PRNT.ISIN_NM)                                                    AS PRNT_ISIN_NM                    /*  수익증권종목명         */
        , SUBSTR(A.ACCO_CLCD, 1, 1)                                                            AS ACCO_CLCD                       /*  계정구분코드           */
        , A.ACCO_CD                                                                            AS ACCO_CD                         /*  계정과목코드           */
        , A.ACCO_CLCD||'_'||A.ACCO_NM                                                          AS ACCO_NM                         /*  계정과목명             */
        , A.INST_CLCD                                                                          AS INST_CLCD                       /*  인스트루먼트구분코드   */
        , A.ISSU_DATE                                                                          AS ISSU_DATE                       /*  발행일자               */
        , A.MATR_DATE                                                                          AS MATR_DATE                       /*  만기일자               */
        , A.IRATE                                                                              AS IRATE                           /*  금리                   */
        , A.IRATE_TPCD                                                                         AS IRATE_TPCD                      /*  금리유형코드(1:고정)   */
        , A.CONT_MATR                                                                          AS CONT_MATR                       /*  계약만기               */
        , A.RMN_MATR                                                                           AS RMN_MATR                        /*  잔존만기               */
        , CASE WHEN A.ASSET_TPCD = 'FIDE' AND A.LEG_TYPE = 'NET'                                    -- 파생상품의 경우 신용리스크관점의 유효만기를 잔존만기로 설정(계산상 유효만기는 0에 가까움)
               THEN A.RMN_MATR ELSE NVL(B.EFFE_MATR, A.RMN_MATR) END                           AS EFFE_MATR                       /*  유효만기               */
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴
--        , CASE WHEN A.ASSET_TPCD = 'LOAN' AND A.IRATE_TPCD = '2'                                    -- 변동금리대출은 차기금리개정일까지의 기간
          , CASE WHEN A.ASSET_TPCD = 'LOAN' AND A.IRATE_TPCD = '2' AND A.LT_TP = 'N'                -- 변동금리대출은 차기금리개정일까지의 기간
--               THEN D.INT_MATR ELSE NVL(A.EXT_DURA, B.EFFE_DURA) END                           AS EFFE_DURA                       /*  듀레이션(유효)         */
                      THEN B.EFFE_DURA 
                 WHEN A.ASSET_TPCD = 'LOAN' AND A.LT_TP = 'N'
                      THEN F.EFFE_DURA
                 ELSE NVL(A.EXT_DURA, B.EFFE_DURA) END                           AS EFFE_DURA                       /*  듀레이션(유효)         */
        , B.IMP_SPRD                                                                           AS IMP_SPRD                        /*  내재스프레드(채권)     */
        , A.CRNY_CD                                                                            AS CRNY_CD                         /*  통화코드               */
        , A.CRNY_FXRT                                                                          AS CRNY_FXRT                       /*  통화환율(기준일)       */
        , A.CNTY_CD                                                                            AS CNTY_CD                         /*  국가코드               */
        , A.NOTL_AMT                                                                           AS NOTL_AMT                        /*  명목금액(거래통화)/수량*/
        , A.BS_AMT                                                                             AS BS_AMT                          /*  BS금액                 */
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴
--20230504 김재우 수정 부도요건 공정가치 0원처리 
--        , DECODE(A.ASSET_TPCD, 'LOAN', NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0), A.FAIR_BS_AMT)    AS FAIR_BS_AMT                     /*  공정가치B/S금액        */
        , CASE WHEN D.ARR_DAYS >= 90              THEN 0  --연체90일 이상 0원처리
--               WHEN D.FLC_CD IN ('30','40','50') THEN 0  -- 자산건전성 고정이하(고정,회수의문, 추정손실) 0원처리
               WHEN A.ASSET_TPCD = 'LOAN' AND A.LT_TP = 'N' THEN NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0)
               WHEN A.PROD_TPCD IN ('70D','70E','70G') THEN 0  -- 24.03.05 선물계약은 공정가치 0 처리함. 입수된 공정가치 BS잔액은 위험 평가를 위한 내재가치임. 
               ELSE A.FAIR_BS_AMT
                END                                                                            AS FAIR_BS_AMT                     /*  공정가치B/S금액        */
        , A.ACCR_AMT                                                                           AS ACCR_AMT                        /*  미수수익               */
        , A.UERN_AMT                                                                           AS UERN_AMT                        /*  선수수익               */
        , NVL(D.ABD_AMT, 0) + NVL(D.LOCF_AMT, 0) + NVL(D.PV_DISC_AMT, 0)                       AS DEDT_ACCO_AMT                   /*  차감계정합계금액       */
        , A.CNPT_CRNY_CD                                                                       AS CNPT_CRNY_CD                    /*  상대LEG통화코드        */
        , A.CNPT_CRNY_FXRT                                                                     AS CNPT_CRNY_FXRT                  /*  상대LEG통화환율        */
        , A.CNPT_FAIR_BS_AMT                                                                   AS CNPT_FAIR_BS_AMT                /*  상대LEG공정가치B/S금액 */
        , NULL                                                                                 AS FV_CLSF_CD                      /*  공정가치결과분류코드   */

        , CASE WHEN R.CR_CLSF_CD NOT IN ('NON')                                                     -- 신용리스크 측정대상 선별(주식, 부동산, 비용상품(부채), 신종자본/후순위(주식리스크) 제외)
               THEN NVL(A.CR_PUBI_TPCD,                                                             -- 무위험/공공 등의 거래상대방 최우선 분류
                        CASE WHEN A.CRGR_KICS IN ('RF')
                             THEN CASE WHEN A.KICS_PROD_TPCD IN ('120')          THEN 'SS3'         -- RF인 보통예금은 무위험처리(특별자산수익증권 인프라펀드의 예금처리 목적)
--                                       WHEN A.ACCO_CD IN ('13600402')            THEN 'SS4'         -- 13600402계정(특별자산수익증권)에서 SOC대출_무위험이 아닌, 미수수익 등임
                                       WHEN A.ACCO_CD IN  (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SS4' )   THEN 'SS4'  -- 24.04.29  13600402계정(특별자산수익증권)에서 SOC대출_무위험이 아닌, 미수수익 등임
                                       WHEN SUBSTR(R.CR_CLSF_CD, 1, 2) IN ('SS') THEN R.CR_CLSF_CD  -- IKRUSH_KICS_RISK_MAP의 CR_CLSF_CD 가 SS인 건으로 한정하여 제한적 매핑진행
                                       ELSE 'SS3'                                                   -- 'RF'임에도 불구하고 미매핑되는 건은 SS3로 매핑(지급보증)
                                       END
                             WHEN R.CR_CLSF_CD IN ('ZZZ') THEN NVL(A.CR_CLSF_CD, 'NON')             -- 계정처리자산 분류
                             ELSE NVL(R.CR_CLSF_CD, 'NON')
                             END)                                                                   -- 무위험/결손보전 기관에 해당되지 않으면(미매핑 포함) 일반적인 분류진행
               ELSE 'NON' END                                                                  AS CR_CLSF_CD                      /*  신용리스크분류코드     */
        , CASE WHEN R.CR_CLSF_CD NOT IN ('NON')
               THEN NVL(A.CR_PUBI_TPNM,
                        CASE WHEN A.CRGR_KICS IN ('RF')
                             THEN CASE WHEN A.KICS_PROD_TPCD IN ('120')          THEN (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'SS3'       )
----                                       WHEN A.ACCO_CD IN ('13600402')            THEN (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'SS4'       )
                                       WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'SS4' ) THEN (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'SS4'       ) -- 13600402계정(특별자산수익증권)에서 SOC대출_무위험이 아닌, 미수수익 등임
                                       WHEN SUBSTR(R.CR_CLSF_CD, 1, 2) IN ('SS') THEN (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = R.CR_CLSF_CD)
                                       ELSE                                           (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'SS3'       )
                                       END
                             WHEN R.CR_CLSF_CD IN ('ZZZ') THEN NVL(A.CR_CLSF_NM,      (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'NON'       ))
                             ELSE NVL(R.CR_CLSF_NM,                                   (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'NON'       ))
                             END)
               ELSE (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'NON') END
                                                                                               AS CR_CLSF_NM                      /*  신용리스크분류명       */
        , CASE WHEN R.CR_CLSF_CD NOT IN ('NON')
               THEN CASE WHEN R.CR_CLSF_CD IN ('ZZZ')
                         THEN NVL(A.CR_ACCO_TPCD, '9')
                         ELSE NVL(R.CR_ACCO_TPCD, '9') END
               ELSE '9' END                                                                    AS CR_ACCO_TPCD                    /*  신용리스크계정분류코드 */
        , CASE WHEN R.CR_CLSF_CD NOT IN ('NON')
               THEN CASE WHEN R.CR_CLSF_CD IN ('ZZZ')
                         THEN NVL(A.CR_ACCO_TPNM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_ACCO_TPCD' AND CD = '9'))
                         ELSE NVL(R.CR_ACCO_TPNM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_ACCO_TPCD' AND CD = '9')) END
               ELSE (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_ACCO_TPCD' AND CD = '9') END
                                                                                               AS CR_ACCO_TPNM                    /*  신용리스크계정분류명   */
        , CASE WHEN A.LEG_TYPE IN ('STD', 'NET')                                                    -- NET에는 신용리스크 명목금액이 포함됨(주로 REC LEG)
               THEN CASE 
--                         WHEN A.FIDE_UNDL_CLCD IN ('6')                                             -- 신용연계증권(CLN) 중 보장매도 파생상품(CLN은 FIDE_UNDL_CLCD = '6'으로, CCF매핑은 NULL임)
--                         THEN A.NOTL_AMT * A.CRNY_FXRT                                              -- K-ICS요건서에 의하여 보장계약금액만을 신용익스포저로 설정함
                         WHEN A.KICS_PROD_TPCD IN ('240')  THEN A.NOTL_AMT * A.CRNY_FXRT       --20230412 김재우 IFRS9은 분리회계 적용되지 않아, 240 (신용연계채권)의 익스포저 금액은 액면금액으로 처리. 신용파생상품의 보장매도금액은 신용리스크 결과에서 별도 처리
                         WHEN A.KICS_PROD_TPCD IN ('740')                                           -- 주식옵션(ELW)의 경우 명목금액은 익스포저금액은 기초자산의 행사가와 명목수량의 곱임(기초자산 정보: IKRUSH_FIDE_OPT 테이블)
                         THEN GREATEST(DECODE(SUBSTR(A.ACCO_CD, 1, 1), '2', -1, 1) * A.FAIR_BS_AMT, 0) + A.NOTL_AMT * A.OPT_UPRC_UNIT * A.UNDL_EXEC_PRC * A.CRNY_FXRT * A.CCF
                         WHEN SUBSTR(A.KICS_PROD_TPCD ,1,1) = '6' AND A.LT_TP = 'N'
			                   THEN A.CONT_QNTY * A.CONT_MULT * A.SPOT_PRC * DECODE (SUBSTR(A.INST_CLCD,-1),'L',1,-1) 
                         WHEN NVL(A.CCF, 0) <> 0                                                    -- 장외파생상품의 요건서상 명목금액(NET POSITION의 A.NOTL_AMT는 REC LEG의 NOTIONAL 금액(기준통화, 환율 1.0)으로 설정함(A1쿼리 참조))
                         THEN GREATEST(DECODE(SUBSTR(A.ACCO_CD, 1, 1), '2', -1, 1) * A.FAIR_BS_AMT, 0) + A.NOTL_AMT * A.CRNY_FXRT * A.CCF
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴ELSE 문에서 처리됨.
--                         ELSE DECODE(A.ASSET_TPCD, 'LOAN', NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0), A.FAIR_BS_AMT) END
                         ELSE CASE WHEN A.ASSET_TPCD = 'LOAN' AND A.LT_TP = 'N' 
                                  THEN CASE WHEN D.ARR_DAYS >= 90 THEN 0 -- 연체 90일이상 0원처리
                                            ELSE NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0)
                                             END
                                  ELSE A.FAIR_BS_AMT            
--20230511 김재우 수정 부동산담보대출의 경우 공정가치 금액으로 처리
--                                   ELSE CASE WHEN D.ARR_DAYS > 90 OR D.FLC_CD IN ('30', '40', '50') THEN 0 -- 연체 90일이상 OR 자산건전성 고정이하(고정,회수의문, 추정손실) 0원처리
--                                   ELSE CASE WHEN D.ARR_DAYS >= 90 THEN 0 -- 연체 90일이상 0원처리
--                                             ELSE A.FAIR_BS_AMT
--                                              END  
                                    END
                          END          
               ELSE NULL END                                                                   AS CR_EXPO_AMT                     /*  신용익스포저(기준통화) */
        , CASE WHEN A.KICS_PROD_TPCD IN ('ZZZ', '999') AND A.IR_CLSF_CD IS NOT NULL
               THEN A.IR_CLSF_CD
               ELSE NVL(R.IR_CLSF_CD, 'NON') END                                               AS IR_CLSF_CD                      /*  금리리스크분류코드     */
        , CASE WHEN A.KICS_PROD_TPCD IN ('ZZZ', '999') AND A.IR_CLSF_CD IS NOT NULL
               THEN A.IR_CLSF_NM
               ELSE NVL(R.IR_CLSF_NM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'IR_CLSF_CD' AND CD = 'NON')) END
                                                                                               AS IR_CLSF_NM                      /*  금리리스크분류명       */
        , NVL(R.IR_SHCK_WGT, 1.0)                                                              AS IR_SHCK_WGT                     /*  금리리스크적용비율     */
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴
--        , DECODE(A.ASSET_TPCD, 'LOAN', NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0), A.FAIR_BS_AMT)    AS IR_EXPO_AMT                     /*  금리익스포저           */
        , CASE WHEN A.ASSET_TPCD = 'LOAN' AND A.LT_TP = 'N' THEN NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0)
               ELSE A.FAIR_BS_AMT
                END                                                                            AS IR_EXPO_AMT                     /*  금리익스포저           */
        , CASE WHEN R.SR_CLSF_CD NOT IN ('NON')
               THEN CASE WHEN R.SR_CLSF_CD IN ('ZZZ')
                         THEN NVL(A.SR_CLSF_CD, 'NON')                                              -- 계정처리자산 분류: 무형자산 및 12100100(당기손익인식금융자산-주식상장)계정처리자산(실제적용대상은 퇴직연금(실적연동형)만 되어야함)
                         ELSE NVL(R.SR_CLSF_CD, 'NON') END                                          -- 주식리스크 분류(선진/신흥/우선주 등)
               ELSE 'NON' END                                                                  AS SR_CLSF_CD                      /*  주식리스크분류코드     */
        , CASE WHEN R.SR_CLSF_CD NOT IN ('NON')
               THEN CASE WHEN R.SR_CLSF_CD IN ('ZZZ')
                         THEN NVL(A.SR_CLSF_NM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'SR_CLSF_CD' AND CD = 'NON'))
                         ELSE NVL(R.SR_CLSF_NM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'SR_CLSF_CD' AND CD = 'NON')) END
               ELSE (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'SR_CLSF_CD' AND CD = 'NON') END
                                                                                               AS SR_CLSF_NM                      /*  금리리스크분류명       */
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴
--        , CASE WHEN A.ASSET_TPCD IN ('LOAN')
--          , CASE WHEN D.ARR_DAYS > 90 OR D.FLC_CD IN ('30', '40', '50') THEN 0
          , CASE WHEN D.ARR_DAYS >= 90 THEN 0
                 WHEN A.ASSET_TPCD IN ('LOAN') AND A.LT_TP = 'N' 
               THEN NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0)
               ELSE CASE WHEN A.KICS_PROD_TPCD IN ('610')
                         THEN A.CONT_QNTY * A.CONT_MULT * A.SPOT_PRC                                -- 장내지수선물은 수량 x 거래승수 x 현재가격으로 익스포저 금액 결정(LONG/SHORT 포지션정보는 INST_CLCD에서 보유)
                         WHEN A.KICS_PROD_TPCD IN ('740')
                         THEN A.FAIR_BS_AMT                                                         -- 주식관련 장외파생상품 중 ELW의 경우 충격치는 후속배치프로세스에서 산출함(USING IKRUSH_FIDE_OPT 테이블)
                         ELSE A.FAIR_BS_AMT END
               END                                                                             AS SR_EXPO_AMT                     /*  주식익스포저           */
        , CASE WHEN A.CRNY_CD NOT IN ('KRW') THEN R.FR_CLSF_CD  ELSE 'N' END                   AS FR_CLSF_CD                      /*  외환리스크분류코드     */
        , CASE WHEN A.CRNY_CD NOT IN ('KRW') THEN R.FR_CLSF_NM  ELSE '비대상' END              AS FR_CLSF_NM                      /*  외환리스크분류명       */
--20230502 김재우 수정 수익증권 대출은 DCF 로 변경하여 결과 테이블을 다른데서 가져옴
--        , DECODE(A.ASSET_TPCD, 'LOAN', NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0), A.FAIR_BS_AMT)    AS FR_EXPO_AMT                     /*  외환익스포저           */
--        , CASE WHEN D.ARR_DAYS > 90 OR D.FLC_CD IN ('30', '40', '50') THEN 0
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               WHEN A.ASSET_TPCD = 'LOAN' AND A.LT_TP = 'N' THEN NVL(C.AMT01, 0) - NVL(D.ACCR_ADJ, 0)
               ELSE A.FAIR_BS_AMT
                END                                                                            AS FR_EXPO_AMT                     /*  외환익스포저           */
        , D.CNPT_SUM_AMT                                                                       AS CNPT_SUM_AMT                    /*  거래상대방합계금액     */
        , A.CNPT_ID                                                                            AS CNPT_ID                         /*  거래상대방ID           */
        , A.CNPT_NM                                                                            AS CNPT_NM                         /*  거래상대방명           */
        , A.CORP_NO                                                                            AS CORP_NO                         /*  법인등록번호           */
        , A.CRGR_KIS                                                                           AS CRGR_KIS                        /*  신용등급(한신평)       */
        , A.CRGR_NICE                                                                          AS CRGR_NICE                       /*  신용등급(한신정)       */
        , A.CRGR_KR                                                                            AS CRGR_KR                         /*  신용등급(한기평)       */
        , NVL(A.CRGR_KICS, '99')                                                               AS CRGR_KICS                       /*  신용등급(KICS)         */
        , D.CRGR_CB_NICE                                                                       AS CRGR_CB_NICE                    /*  개인신용등급(한신정)   */
        , D.CRGR_CB_KCB                                                                        AS CRGR_CB_KCB                     /*  개인신용등급(KCB)      */
        , A.GOCO_CD                                                                            AS GRP_CD                          /*  그룹코드               */
        , A.GOCO_NM                                                                            AS GRP_NM                          /*  그룹명                 */
        , D.LOAN_CONT_TPCD                                                                     AS LOAN_CONT_TPCD                  /*  대출계약유형코드       */
        , D.COLL_ID                                                                            AS COLL_ID                         /*  담보ID                 */
        , D.COLL_TPCD                                                                          AS COLL_TPCD                       /*  담보유형코드           */
        , D.COLL_SET_AMT                                                                       AS COLL_SET_AMT                    /*  담보설정금액           */
        , D.PRPT_DTLS_TPCD                                                                     AS PRPT_DTLS_TPCD                  /*  부동산상세유형코드     */
        , NVL(D.LTV, E.LTV)                                                                    AS LTV                             /*  LTV                    */
        , D.PRPT_OCPY_TPCD                                                                     AS PRPY_OCPY_TPCD                  /*  부동산점유유형코드     */
        , A.PRPT_CNTY_TPCD                                                                     AS PRPT_CNTY_TPCD                  /*  부동산국가유형코드     */
        , A.CONT_QNTY                                                                          AS CONT_QNTY                       /*  거래수량               */
        , A.CONT_MULT                                                                          AS CONT_MULT                       /*  거래승수               */
        , A.CONT_PRC                                                                           AS CONT_PRC                        /*  체결가격               */
        , A.STOC_TPCD                                                                          AS STOC_TPCD                       /*  주식유형코드           */
        , A.PRF_STOC_YN                                                                        AS PRF_STOC_YN                     /*  KICS우선주여부         */
        , A.STOC_LIST_CLCD                                                                     AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , 'N'                                                                                  AS CPTC_YN                         /*  CAPITAL CALL 여부      */
        , A.BOND_RANK_CLCD                                                                     AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , A.ASSET_LIQ_CLCD                                                                     AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */
        , A.HDGE_ISIN_CD                                                                       AS HDGE_ISIN_CD                    /*  헤지종목코드           */
        , A.HDGE_MATR_DATE                                                                     AS HDGE_MATR_DATE                  /*  헤지만기일자           */
        , NULL                                                                                 AS UUSE_LMT_AMT                    /*  미사용한도금액         */
        , A.CCF                                                                                AS CCF                             /*  신용환산율             */
        , A.FIDE_UNDL_CLCD                                                                     AS FIDE_UNDL_CLCD                  /*  기초자산구분코드       */
        , D.ARR_DAYS                                                                           AS ARR_DAYS                        /*  연체일수               */
        , D.FLC_CD                                                                             AS FLC_CD                          /*  자산건전성코드         */
        , CASE WHEN D.ARR_DAYS >= 90                THEN 'Y'
--               WHEN D.FLC_CD IN ('30', '40', '50') THEN 'Y'
--               WHEN A.CRGR_KICS = '88'             THEN 'Y'
               ELSE 'N' END                                                                    AS DFLT_YN                         /*  부도여부               */

        -- 파생상품부채관련  FAIR_BS_AMT관련 처리는 서브쿼리에서 하였으나, 금리충격치의 경우는 아래의 쿼리로 일관성 확보진행
-- 20230504 김재우 수정 
-- 대출채권 부도요건 반영         
--        , NVL(C.AMT01, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN01                  /*  금리충격후금액(SCEN#01)*/
--        , NVL(C.AMT02, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN02                  /*  금리충격후금액(SCEN#02)*/
--        , NVL(C.AMT03, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN03                  /*  금리충격후금액(SCEN#03)*/
--        , NVL(C.AMT04, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN04                  /*  금리충격후금액(SCEN#04)*/
--        , NVL(C.AMT05, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN05                  /*  금리충격후금액(SCEN#05)*/
--        , NVL(C.AMT06, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN06                  /*  금리충격후금액(SCEN#06)*/
--        , NVL(C.AMT07, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN07                  /*  금리충격후금액(SCEN#07)*/
--        , NVL(C.AMT08, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN08                  /*  금리충격후금액(SCEN#08)*/
--        , NVL(C.AMT09, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN09                  /*  금리충격후금액(SCEN#09)*/
--        , NVL(C.AMT10, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN10                  /*  금리충격후금액(SCEN#10)*/
--        , NVL(C.AMT11, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN11                  /*  금리충격후금액(SCEN#11)*/
--        , NVL(C.AMT12, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN12                  /*  금리충격후금액(SCEN#12)*/
--        , NVL(C.AMT13, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN13                  /*  금리충격후금액(SCEN#13)*/
--        , NVL(C.AMT14, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0)                                    AS IR_EXPO_SCEN14                  /*  금리충격후금액(SCEN#14)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT01, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN01                  /*  금리충격후금액(SCEN#01)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT02, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN02                  /*  금리충격후금액(SCEN#02)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT03, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN03                  /*  금리충격후금액(SCEN#03)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT04, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN04                  /*  금리충격후금액(SCEN#04)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT05, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN05                  /*  금리충격후금액(SCEN#05)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT06, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN06                  /*  금리충격후금액(SCEN#06)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT07, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN07                  /*  금리충격후금액(SCEN#07)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT08, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN08                  /*  금리충격후금액(SCEN#08)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT09, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN09                  /*  금리충격후금액(SCEN#09)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT10, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN10                  /*  금리충격후금액(SCEN#10)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT11, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN11                  /*  금리충격후금액(SCEN#11)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT12, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN12                  /*  금리충격후금액(SCEN#12)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT13, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN13                  /*  금리충격후금액(SCEN#13)*/
        , CASE WHEN D.ARR_DAYS >= 90 THEN 0
               ELSE NVL(C.AMT14, 0) * A.AMT_SIGN - NVL(D.ACCR_ADJ, 0) END                      AS IR_EXPO_SCEN14                  /*  금리충격후금액(SCEN#14)*/
        , A.DEPT_CD                                                                            AS DEPT_CD                         /*  부서코드               */
        , 'KRDA106BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
--20230503 김재우 추가
        , A.CRGR_SNP
        , A.CRGR_MOODYS
        , A.CRGR_FITCH
        , A.IND_L_CLSF_CD
        , A.IND_L_CLSF_NM
        , A.IND_CLSF_CD        
        , A.OBGT_PSSN_ESTE_YN
        , A.LVG_RATIO	
        , COA.COA_ID AS FV_RPT_COA
        , COA.BLNG_LEAF_HIER_NODE_ID  AS FV_HIER_NODE_ID
        , A.SNR_LTV
        , A.FAIR_VAL_SNR
        , A.DSCR
     FROM T_EXPO     A
        , T_ENG_RST  B
        , T_IR_RST   C
        , (
             SELECT ROUND(GREATEST(
                    CASE WHEN T1.IRATE_TPCD = '2'
                         THEN ADD_MONTHS(  TO_DATE(T1.ISSU_DATE, 'YYYYMMDD')
                                         , FLOOR(   MONTHS_BETWEEN(TO_DATE(T1.BASE_DATE, 'YYYYMMDD'), TO_DATE(T1.ISSU_DATE, 'YYYYMMDD'))
                                                  / GREATEST(NVL(T1.IRATE_RPC_CYC, NVL(T1.INT_PAY_CYC, 12)), 1) + 1)
                                           * GREATEST(NVL(T1.IRATE_RPC_CYC, NVL(T1.INT_PAY_CYC, 12)), 1) )
                              - TO_DATE(T1.BASE_DATE, 'YYYYMMDD')
                         ELSE NVL((TO_DATE(T1.MATR_DATE, 'YYYYMMDD') - TO_DATE(T1.BASE_DATE, 'YYYYMMDD')), 0)
                         END  , 1) / 365, 3)                          AS INT_MATR
                  , SUM(T1.BS_AMT) OVER (PARTITION BY T1.CNPT_ID)     AS CNPT_SUM_AMT
                  , CASE WHEN NVL(T1.BS_AMT, 0) - NVL(T1.ACCR_AMT, 0) + NVL(T1.ACCR_ABD_AMT, 0) > 0
                         THEN NVL(T1.ACCR_AMT, 0) - NVL(T1.ACCR_ABD_AMT, 0)
                         ELSE 0 END                                   AS ACCR_ADJ
                  , T1.*
               FROM Q_IC_ASSET_LOAN T1
              WHERE BASE_DATE = '$$STD_Y4MD'
          ) D
        , ( SELECT 'LOAN_L_'||EA.FUND_CD||EA.PRNT_ISIN_CD||'_'||EA.ISIN_CD||'_'||LPAD(EA.LT_SEQ, 3, '0') AS EXPO_ID 
                 , EA.LTV
                 , EA.DSCR   
              FROM IKRUSH_LT_TOTAL EA 
             WHERE BASE_DATE = '$$STD_Y4MD'
           ) E   
        , ( SELECT FCONT_ID
                 ,  (SUM(DECODE( SCEN_NUM, 8, VALUE,0 )) - SUM(DECODE( SCEN_NUM, 7, VALUE,0 )))
                   / SUM(DECODE( SCEN_NUM, 1, VALUE * 2 * 0.01,0)) AS EFFE_DURA
              FROM Q_AL_RSLT_A
             WHERE AS_OF_DATE = TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
               AND SCEN_NUM IN (1,7,8) 
             GROUP BY FCONT_ID
          ) F    
        , (
             SELECT KICS_PROD_TPCD                          AS KICS_PROD_TPCD
                  , MAX(KICS_PROD_TPNM)                     AS KICS_PROD_TPNM
                  , MAX(CR_CLSF_CD)                         AS CR_CLSF_CD
                  , MAX(CR_CLSF_NM)                         AS CR_CLSF_NM
                  , MAX(CR_ACCO_TPCD)                       AS CR_ACCO_TPCD
                  , MAX(CR_ACCO_TPNM)                       AS CR_ACCO_TPNM
                  , MAX(IR_CLSF_CD)                         AS IR_CLSF_CD
                  , MAX(IR_CLSF_NM)                         AS IR_CLSF_NM
                  , MAX(IR_SHCK_WGT)                        AS IR_SHCK_WGT
                  , MAX(SR_CLSF_CD)                         AS SR_CLSF_CD
                  , MAX(SR_CLSF_NM)                         AS SR_CLSF_NM
                  , MAX(FR_CLSF_CD)                         AS FR_CLSF_CD
                  , MAX(FR_CLSF_NM)                         AS FR_CLSF_NM
               FROM IKRUSH_KICS_RISK_MAP
              WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
              GROUP BY KICS_PROD_TPCD
          ) R
        , (
             SELECT CD2                                     AS PROD_TPCD
                  , CD2_NM                                  AS PROD_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_CD'       -- KICS상품분류코드
          ) KICS
        , (
             SELECT STAN_ITMS_CD                            AS ISIN_CD
                  , ITMS_NM                                 AS ISIN_NM
               FROM IKRASH_ITEM_INFO
              WHERE STND_DT   = '$$STD_Y4MD'
                AND CCLT_CLCD = 2
          ) PRNT                                  -- 수익증권종목명 매칭목적
        , T_COA COA
    WHERE 1=1
      AND A.EXPO_ID        = B.EXPO_ID(+)
      AND A.LEG_TYPE       = B.LEG_TYPE(+)
      AND A.EXPO_ID        = C.EXPO_ID(+)
      AND A.EXPO_ID        = D.EXPO_ID(+)
      AND A.EXPO_ID        = E.EXPO_ID(+) -- 20230424 김재우 수정 수익증권의LTV DSCR 추가
      AND A.EXPO_ID        = F.FCONT_ID(+)  
      AND A.KICS_PROD_TPCD = R.KICS_PROD_TPCD(+)
      AND A.KICS_PROD_TPCD = KICS.PROD_TPCD(+)
      AND A.PRNT_ISIN_CD   = PRNT.ISIN_CD(+)
      AND SUBSTR(A.ACCO_CLCD,1,1) = COA.ACCO_TPCD(+)
      AND A.ACCO_CD = COA.COA_ID(+)

)
SELECT * FROM T_AAA 
WHERE 1=1