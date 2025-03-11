/* 수정사항 
- 계정코드별 예외처리 : ACCO_EXCPTN
- SUB TABLE WITH 문 처리 : 재보험자산 주석처리
- 240527 : 금리리스크보고서COA : 금리위험경감인정 파생상품 ('720', '722', '630')  (이자율스왑(IRS) , 채권선도, 국채선물) 
- 240527 : 금리위험경감미인정파생상품을 기타 파생상품으로 재분류 
- 250311 : 장내외파생상품 FV < 0 -> 부채
*/
WITH /* SQL-ID : KRDA101BM */ 
ACCO_EXCPTN AS (
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRDA101BM'
)
--SELECT * FROM ACCO_EXCPTN;
/*, T_RE_INSU AS 
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE
        , 'INSU_O_'||A.ACCO_CLCD||'_'||A.ACCO||'0000_'||A.KICS_PROD_TPCD                       AS EXPO_ID
        , SUBSTR(A.ACCO, 1, 1)||'N'||'_'||SUBSTR(R.IR_CLSF_CD, 2)                              AS IRATE_RPT_COA
        , CASE WHEN A.ACCO_CLCD IN ('1'     ) THEN '1'
               WHEN A.ACCO_CLCD IN ('2', '3') THEN '2'
               WHEN A.ACCO_CLCD IN ('5'     ) THEN '3'
               ELSE '2' END                                                                    AS IRATE_RPT_TPCD
        , NULL                                                                                 AS FUND_CD
        , 'INSU'                                                                               AS ASSET_TPCD
        , A.KICS_PROD_TPCD                                                                     AS KICS_PROD_TPCD
        , R.KICS_PROD_TPNM                                                                     AS KICS_PROD_TPNM
        , NULL                                                                                 AS ISIN_CD
        , NULL                                                                                 AS ISIN_NM
        , 'N'                                                                                  AS LT_TP
        , A.ACCO_CLCD                                                                          AS ACCO_CLCD
        , A.ACCO||'0000'                                                                       AS ACCO_CD
        , R.KICS_PROD_TPNM                                                                     AS ACCO_NM
        , 'KRW'                                                                                AS CRNY_CD
        , A.BS_AMT                                                                             AS BS_AMT
        , CASE WHEN A.ACCO_CLCD NOT IN ('4') THEN NVL(B.SCEN01, 0) ELSE A.BS_AMT END           AS FAIR_BS_AMT
        , R.IR_CLSF_CD                                                                         AS IR_CLSF_CD
        , R.IR_CLSF_NM                                                                         AS IR_CLSF_NM
        , R.IR_SHCK_WGT                                                                        AS IR_SHCK_WGT
        , NVL(B.SCEN01, 0)                                                                     AS IR_EXPO_SCEN01
        , NVL(B.SCEN02, 0)                                                                     AS IR_EXPO_SCEN02
        , NVL(B.SCEN03, 0)                                                                     AS IR_EXPO_SCEN03
        , NVL(B.SCEN04, 0)                                                                     AS IR_EXPO_SCEN04
        , NVL(B.SCEN05, 0)                                                                     AS IR_EXPO_SCEN05
        , NVL(B.SCEN06, 0)                                                                     AS IR_EXPO_SCEN06
        , NVL(B.SCEN07, 0)                                                                     AS IR_EXPO_SCEN07
        , NVL(B.SCEN08, 0)                                                                     AS IR_EXPO_SCEN08
        , NVL(B.SCEN09, 0)                                                                     AS IR_EXPO_SCEN09
        , NVL(B.SCEN10, 0)                                                                     AS IR_EXPO_SCEN10
        , 0                                                                                    AS IR_EXPO_SCEN11
        , 0                                                                                    AS IR_EXPO_SCEN12
        , 0                                                                                    AS IR_EXPO_SCEN13
        , 0                                                                                    AS IR_EXPO_SCEN14
        , 'KRDA101BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE

     FROM (
             SELECT BASE_DATE                                                                  AS BASE_DATE
                  , CASE WHEN SUBSTR(ACCO_CD, 1, 2) IN ('21'  ) THEN 'I60'                     -- 원수보험
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1620') THEN 'I10'                     -- 보험계약대출
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1970') THEN 'I20'                     -- 재보험자산
-- 20230503 김재우 추가
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1Q16', '1R13', '1S10') THEN 'I10'                     -- 보험계약대출
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1CD2') THEN 'I20'                     -- 재보험자산
                         ELSE 'III' END                                                        AS KICS_PROD_TPCD
                  , CASE WHEN SUBSTR(ACCO_CD, 1, 2) IN ('21'  ) THEN '2100'
                         ELSE SUBSTR(ACCO_CD, 1, 4) END                                        AS ACCO
                  , ACCO_CLCD                                                                  AS ACCO_CLCD
                  , SUM(BS_AMT)                                                                AS BS_AMT
               FROM Q_IC_EXPO
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
                AND (    SUBSTR(ACCO_CD, 1, 4) IN ('1620', '1970')
                      OR SUBSTR(ACCO_CD, 1, 3) IN ('211' , '212' , '219') )
              GROUP BY
                    BASE_DATE
                  , CASE WHEN SUBSTR(ACCO_CD, 1, 2) IN ('21'  ) THEN 'I60'
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1620') THEN 'I10'
                         WHEN SUBSTR(ACCO_CD, 1, 4) IN ('1970') THEN 'I20'
                         ELSE 'III' END
                  , CASE WHEN SUBSTR(ACCO_CD, 1, 2) IN ('21'  ) THEN '2100'
                         ELSE SUBSTR(ACCO_CD, 1, 4) END
                  , ACCO_CLCD
          ) A
        , (
             SELECT KICS_PROD_TPCD
                  , CASE WHEN KICS_PROD_TPCD IN ('I20')                  THEN '1'              -- 재보험자산은 변액보험 결과도 모두 일반계정으로 흡수처리
                         WHEN SUBSTR(INSU_RISK_GRP_DVCD, 1, 1) IN ('V')  THEN '5'              -- V: 변액보험
                         WHEN SUBSTR(INSU_RISK_GRP_DVCD, 1, 1) IN ('R')
                         THEN CASE WHEN KICS_COA_ID IN ('R111') THEN '2'
                                   WHEN KICS_COA_ID IN ('R131') THEN '3'
                                   WHEN KICS_COA_ID IN ('R133') THEN '4'                       -- R111: 퇴직보험, R131: 퇴직연금(원리금보장), R133: 퇴직연금(실적배당형)
                                   ELSE '9' END                                                -- V, R이 아닌 나머지는 일반계정으로 분류
                         ELSE '1' END                                                          AS ACCO_CLCD
                  , SUM(SHOCK_PROR_BEL)                                                        AS SCEN01
                  , SUM(INT_BACK_BEL  )                                                        AS SCEN02
                  , SUM(INT_UP_BEL    )                                                        AS SCEN03
                  , SUM(INT_DOWN_BEL  )                                                        AS SCEN04
                  , SUM(INT_SAME_BEL  )                                                        AS SCEN05
                  , SUM(INT_SLOPE_BEL )                                                        AS SCEN06
                  , SUM(INT_SENS1_BEL )                                                        AS SCEN07
                  , SUM(INT_SENS2_BEL )                                                        AS SCEN08
                  , SUM(INT_SENS3_BEL )                                                        AS SCEN09
                  , SUM(INT_SENS4_BEL )                                                        AS SCEN10

               FROM (                                                                          -- INT_[ ]_BEL은 원수보험의 컬럼이고, 보험계약대출은 CL, 재보험자산은 RE로 구분됨
                       SELECT 'I60' AS KICS_PROD_TPCD                                          -- 컬럼순서가 일치하므로, UNION ALL 처리하였음
                            , T1.*
                         FROM IKRASS_INT_RISK_L T1                                             -- 원수보험부채
                        WHERE STND_DT = '$$STD_Y4MD'

                        UNION ALL

                       SELECT 'I10' AS KICS_PROD_TPCD
                            , T2.*
                         FROM IKRASS_INT_RISK_L_CL T2                                          -- 보험계약대출자산
                        WHERE STND_DT = '$$STD_Y4MD'

                        UNION ALL

                       SELECT 'I20' AS KICS_PROD_TPCD
                            , T3.*
                        FROM IKRASS_INT_RISK_L_RE T3                                           -- 재보험자산
                       WHERE STND_DT = '$$STD_Y4MD'
                    )
              WHERE 1=1
              GROUP BY
                    KICS_PROD_TPCD
                  , CASE WHEN KICS_PROD_TPCD IN ('I20')                  THEN '1'
                         WHEN SUBSTR(INSU_RISK_GRP_DVCD, 1, 1) IN ('V')  THEN '5'
                         WHEN SUBSTR(INSU_RISK_GRP_DVCD, 1, 1) IN ('R')
                         THEN CASE WHEN KICS_COA_ID IN ('R111') THEN '2'
                                   WHEN KICS_COA_ID IN ('R131') THEN '3'
                                   WHEN KICS_COA_ID IN ('R133') THEN '4'
                                   ELSE '9' END
                         ELSE '1' END
          ) B
        , (
             SELECT KICS_PROD_TPCD                   AS KICS_PROD_TPCD
                  , MAX(KICS_PROD_TPNM)              AS KICS_PROD_TPNM
                  , MAX(CR_CLSF_CD)                  AS CR_CLSF_CD
                  , MAX(CR_CLSF_NM)                  AS CR_CLSF_NM
                  , MAX(CR_ACCO_TPCD)                AS CR_ACCO_TPCD
                  , MAX(CR_ACCO_TPNM)                AS CR_ACCO_TPNM
                  , MAX(IR_CLSF_CD)                  AS IR_CLSF_CD
                  , MAX(IR_CLSF_NM)                  AS IR_CLSF_NM
                  , MAX(IR_SHCK_WGT)                 AS IR_SHCK_WGT
                  , MAX(SR_CLSF_CD)                  AS SR_CLSF_CD
                  , MAX(SR_CLSF_NM)                  AS SR_CLSF_NM
                  , MAX(FR_CLSF_CD)                  AS FR_CLSF_CD
                  , MAX(FR_CLSF_NM)                  AS FR_CLSF_NM
               FROM IKRUSH_KICS_RISK_MAP
              WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
              GROUP BY KICS_PROD_TPCD
          ) R
    WHERE 1=1
      AND A.ACCO_CLCD      = B.ACCO_CLCD(+)
      AND A.KICS_PROD_TPCD = B.KICS_PROD_TPCD(+)
      AND A.KICS_PROD_TPCD = R.KICS_PROD_TPCD(+)
)*/
, T_AAA AS
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , A.EXPO_ID                                                                            AS EXPO_ID                         /*  익스포저ID             */
----- 금리리스크 COA 기존----------------------------------------------------------------------------------------
/*       
        ,CASE WHEN A.LT_TP = 'Y' AND SUBSTR(A.KICS_PROD_TPCD, 1, 1) = '9' THEN '2'   
              WHEN A.LT_TP = 'Y' AND SUBSTR(A.KICS_PROD_TPCD, 1, 1) IN ('6','7') AND A.FAIR_BS_AMT < 0 THEN '2' 
              ELSE SUBSTR(A.ACCO_CD, 1, 1)
               END 
--          SUBSTR(A.ACCO_CD, 1, 1)
            ||CASE WHEN A.LT_TP= 'N' AND SUBSTR(A.PROD_TPCD,1,1) = 'O' THEN 'Y' -- 미분해 수익증권도 수익증권 처리
                   ELSE A.LT_TP
                    END ||'_'||
          CASE WHEN SUBSTR(A.KICS_PROD_TPCD, 1, 1) IN ('1', '2') AND
--20230503 김재우 수정
-- 계층구조가 변경되어 수정 , 개인대출 기업대출 분리
--                    SUBSTR(A.ACCO_CD, 1, 2) IN ('16') THEN '32'                                                                   -- 대출계정(회계계정 16)은 KICS_PROD_TPCD와 무관하게 대출채권으로 분류(LT내역 중 RP를 대출로 분류하기 위한 목적)
                    SUBSTR(A.ACCO_CD, 1, 2) IN ('1S') THEN 
					                                                      CASE WHEN LT_TP = 'Y' THEN '32'
													                                           ELSE '33'
													                                            END -- 대출계정(회계계정 16)은 KICS_PROD_TPCD와 무관하게 대출채권으로 분류(LT내역 중 RP를 대출로 분류하기 위한 목적)
               WHEN KICS_PROD_TPCD IN ('160', '161') THEN CASE WHEN LT_TP = 'Y' THEN '32'
					                                                     ELSE '33'
															                                  END									  
	             WHEN SUBSTR(A.FV_HIER_NODE_ID,1,3) = 'A13' AND SUBSTR(A.FV_HIER_NODE_ID,-1) = '1' THEN '32'  -- 직접보유 개인대출				    
	             WHEN SUBSTR(A.FV_HIER_NODE_ID,1,3) = 'A13' AND SUBSTR(A.FV_HIER_NODE_ID,-1) = '2' THEN '33'  -- 직접보유 기업대출	
               WHEN SUBSTR(A.KICS_PROD_TPCD, 1, 1) IN ('3') AND A.LT_TP= 'Y' THEN '32' -- 간접보유 대출채권은 기업/개인 구분없음
--               WHEN A.LT_TP = 'N' AND SUBSTR(A.ACCO_CD,1,4) = '1S10' AND SUBSTR(A.ACCO_CD,-1) = '1' THEN '32' -- 직접보유 개인대출은 32. 간접보유는 33 
               WHEN SUBSTR(A.KICS_PROD_TPCD, 1, 1) IN ('6')
--20230508 김재우 수정
-- 장내파생상품중 기초자산이 금리인 경우 금리경감미인정파생
-- 그외의 장내파생상품은 기타파생
-- 모두 자산으로 분류함
--               THEN CASE WHEN SUBSTR(A.ACCO_CD, 1, 1) IN ('1')
--                         THEN DECODE(A.LT_TP, 'Y', '62B', '62A')                                                                  -- 국채선물(in LT) 손실은 부채계정으로 내역을 전환하는게 사실상 불가하므로 자산계정에 음의 가치로 배치
--                         ELSE DECODE(A.LT_TP, 'Y', '25B', '25A') END                                                              -- 장내파생생품(보유는 헷지목적(그러나 바로 정산되므로 장부가는 0임), LT는 투자목적으로 분류)
                    THEN CASE WHEN A.KICS_PROD_TPCD IN ('630','631','640','641') AND LT_TP ='Y' AND A.FAIR_BS_AMT >= 0  THEN '62B' -- 수익증권 위험경감미인정파생(장부가 0과 같거나 크면 자산) 
					                    WHEN A.KICS_PROD_TPCD IN ('630','631','640','641') AND LT_TP ='Y' AND A.FAIR_BS_AMT < 0 THEN '25B'  -- 수익증권 위험경감미인정파생(장부가 0보다 작으면부채) 
                              WHEN A.KICS_PROD_TPCD IN ('630','631','640','641') AND SUBSTR(A.ACCO_CD, 1, 1) IN ('1') --국채선물,국채옵션,이자율선물,이자율옵션
                                   THEN '62B'
                              WHEN SUBSTR(A.ACCO_CD, 1, 1) IN ('1') 
                                   THEN '62C'
                              WHEN A.KICS_PROD_TPCD IN ('630','631','640','641') AND SUBSTR(A.ACCO_CD, 1, 1) IN ('2')       
                                   THEN '25B'
                                   ELSE '25C'
                                    END 
               WHEN SUBSTR(A.KICS_PROD_TPCD, 1, 1) IN ('7')                                                                       -- 장외파생상품
               THEN CASE WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT >= 0  AND A.KICS_PROD_TPCD IN ('720', '722')  THEN '62A'
			                   WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT >= 0  AND A.KICS_PROD_TPCD IN ('721', '752')  THEN '62B'
			                   WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT >= 0  THEN '62C'
			                   WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT < 0  AND A.KICS_PROD_TPCD IN ('720', '722')  THEN '25A'
			                   WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT < 0  AND A.KICS_PROD_TPCD IN ('721', '752')  THEN '25B'
			                   WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT < 0  THEN '25C'
                         WHEN SUBSTR(A.ACCO_CD, 1, 1) IN ('1')
-- 20230503 김재우 수정 
-- 장외파생상품 중 IRS, 채권선도는 금리위험경감 인정 파생상품
-- 그 외 기초자산이 금리인 경우 금리위험경감 미인정 파생상품
-- 기초자산이 금리외의 파생상품은 기타파생상품
--                         THEN DECODE(A.FIDE_UNDL_CLCD, '2', '62A', '62B')                                                         -- 자산 중 1930계정은 62A로 분류(통화관련 파생(CRS)), 그외의 장외파생상품은 투자목적으로 분류(금리/주식/신용/기타 파생)
--                         WHEN SUBSTR(A.ACCO_CD, 1, 4) IN ('2310')
--                         THEN '25B'                                                                                               -- 부채 중 2310계정 중 신종자본증권 헤지목적파생이 존재하지만, 원래 계정분류와 동일하게 CRS임에도 불구하고 투자파생처리
--                         ELSE DECODE(A.FIDE_UNDL_CLCD, '2', '25A', '25B') END                                                     -- 부채 중 2760계정은 25A로 분류(통화관련 파생(CRS))
                         THEN CASE WHEN A.KICS_PROD_TPCD IN ('720', '722') --이자율스왑(IRS) , 채권선도
                                        THEN '62A'
                                   WHEN A.KICS_PROD_TPCD IN ('721', '752') -- 기타장외이자율옵션, 내재파생옵션-이자율관련
                                        THEN '62B'
                                   ELSE '62C'
                                    END       
                         ELSE CASE WHEN A.KICS_PROD_TPCD IN ('720', '722') --이자율스왑(IRS) , 채권선도
                                        THEN '25A'
                                   WHEN A.KICS_PROD_TPCD IN ('721', '752') -- 기타장외이자율옵션, 내재파생옵션-이자율관련
                                        THEN '25B'
                                   ELSE '25C'
                                    END       
                          END
               WHEN A.KICS_PROD_TPCD IN ('ZZZ')
--20230508 김재우 수정
-- IFRS 9 계정기준으로 변경
--               THEN CASE WHEN SUBSTR(A.ACCO_CD, 1, 6) IN ('168077'  , '168088'  , '168099'  )
--               THEN CASE WHEN SUBSTR(A.ACCO_CD, 1, 6) IN ('1S1009', '1S1010', '1S1011'  )
--                         THEN CASE WHEN A.ACCO_CD     IN ('16809901') THEN '31' ELSE '32' END                                     -- 계정처리 자산 중 보험계약대출(31), 대출채권(32) 차감계정
--                         THEN CASE WHEN A.ACCO_CD     IN ('1S101101') THEN '31' ELSE '33' END                                     -- 31약대, 32개인대출, 33 기업대출(차감계정은 개인/기업구분이 어려워 모두 기업분류)
                 THEN CASE WHEN SUBSTR(A.ACCO_CD, 1, 6) IN ('258099'  ) THEN '24'                                                   -- 발행채권(후순위) 할인발행차금
--                         WHEN A.ACCO_CD               IN ('17109911', '17209901', '17209902') THEN '50'                           -- 계정처리 자산 중 부동산관련 차감계정
                         ELSE SUBSTR(A.IR_CLSF_CD, 2) END
               ELSE SUBSTR(A.IR_CLSF_CD, 2) END                                                AS IRATE_RPT_COA
*/
---------------------------------------------------------------------------------------------------
-- 금리리스크 COA 수정 
/* 자산(1) 부채(2) */
        ,CASE WHEN A.LT_TP = 'Y' AND PROD_GRP_TPCD IN ('6','7') AND A.FAIR_BS_AMT < 0 THEN '2' -- 간접보유자산 : 장내장외파생상품 FV<0 -> 부채 
              WHEN PROD_GRP_TPCD IN ('6','7') THEN ( CASE WHEN A.FAIR_BS_AMT < 0 TEHN '2' ELSE '1' END )  --20250311 장내외파생상품 FV < 0 -> 부채  
              ELSE DECODE(SUBSTR(IR_CLSF_CD,1,1),'A','1','L','2','1') 
              END 

/* 직접보유(N) , 간접보유(Y) */
            ||CASE WHEN SUBSTR(A.PROD_TPCD,1,1) = 'O' THEN 'Y' -- 미분해 수익증권도 간접보유 대상으로 분류 
                   ELSE A.LT_TP
              END ||'_'||
                    
/* 금리상품분류 IR_CLSF_CD
    10 현금 및 예치금
    20 채권
    30 대출채권
    40 주식
    50 부동산
    60 비운용자산 
*/
        -- [대출분류]  1. 간접보유(32)  2. 직접보유 개인(32),  기업(33)
              CASE WHEN IR_CLSF_CD ='A33' THEN  --금리상품이 기업대출로 분류된 대상 세부 분류 => (상품그룹 구분보다 금리리스크 분류체계를 우선 적용)
                      CASE WHEN  A.LT_TP= 'Y' THEN '32' -- 간접보유 대출채권은 기업/개인 구분없음 32
                      ELSE                               --직접보유 대출채권(A.LT_TP= 'N' ) =>  개인 : 32  기업 : 33
                          CASE WHEN SUBSTR(A.FV_HIER_NODE_ID,1,3) = 'A13' AND SUBSTR(A.FV_HIER_NODE_ID,-1) = '1' THEN '32'  -- 직접보유 개인대출
                               WHEN SUBSTR(A.FV_HIER_NODE_ID,1,3) = 'A13' AND SUBSTR(A.FV_HIER_NODE_ID,-1) = '2' THEN '33'  -- 직접보유 기업대출
                               ELSE SUBSTR(A.IR_CLSF_CD, 2)  -- default 기업대출 '33'
                               END 
                      END
                /* 원천이 대출에서 입수된 경우 설정과 무관하게 대출로 분류함.  KICS_PROD_TPCD는 IR_CLSF_CD을  'A33' 으로 처리할 것인지 판단 */ 
--                   WHEN PROD_GRP_TPCD IN ('2') THEN 
--                        CASE WHEN ASSET_TPCD = 'LOAN' THEN DECODE(A.LT_TP , 'Y','32','33') -- 채권 중 대출로 분류할 대상 
--                        ELSE SUBSTR(A.IR_CLSF_CD, 2) END -- 그외는 금리 상품분류 기준 따름

        -- [파생상품 분류]
                   WHEN PROD_GRP_TPCD IN ('6','7') THEN--6장내 , 7장외
                   /*자산(62) 부채(25)*/
                         CASE WHEN A.LT_TP = 'Y'  AND A.FAIR_BS_AMT < 0 THEN '25' --장내장외파생상품 FV<0 -> 부채 
                              ELSE  DECODE(SUBSTR(A.ACCO_CD, 1, 1),'1','62','2','25') END-- 그외 계정과목 코드 따름 자산(62) 부채(25) 
                              
                              ||
                   /*위험경감 분류 경감인정금리파생(A), 미인정금리파생(B), 기타(C)*/           
                         -- CASE WHEN A.KICS_PROD_TPCD IN ('720', '722') THEN 'A'  -- 경감인정 금리파생 (이자율스왑(IRS) , 채권선도)
                         CASE WHEN A.KICS_PROD_TPCD IN ('720', '722', '630') THEN 'A'  -- 경감인정 금리파생 (이자율스왑(IRS) , 채권선도, 국채선물) 
                           --   WHEN A.IR_SHCK_WGT = 1 THEN 'B'                   -- 경감미인정 금리파생
                              ELSE 'C'     END                                  -- 기타 파생 
    
            /* 대출, 파생상품 외 나머지는 금리리스크 상품 분류를 따름 */                          
                   ELSE SUBSTR(A.IR_CLSF_CD, 2) END                                                AS IRATE_RPT_COA
               
---------------------------------------------------------------------------------------------------               

        , CASE WHEN A.ACCO_CLCD IN ('1'     ) THEN '1'
               WHEN A.ACCO_CLCD IN ('2', '3') THEN '2'
               WHEN A.ACCO_CLCD IN ('5'     ) THEN '3'
               ELSE '2' END                                                                    AS IRATE_RPT_TPCD
        , A.FUND_CD                                                                            AS FUND_CD
        , A.ASSET_TPCD                                                                         AS ASSET_TPCD
        , A.KICS_PROD_TPCD                                                                     AS KICS_PROD_TPCD
        , A.KICS_PROD_TPNM                                                                     AS KICS_PROD_TPNM
        , A.ISIN_CD                                                                            AS ISIN_CD
        , A.ISIN_NM                                                                            AS ISIN_NM
        , A.LT_TP                                                                              AS LT_TP
        , A.ACCO_CLCD                                                                          AS ACCO_CLCD
        , A.ACCO_CD                                                                            AS ACCO_CD
        , A.ACCO_NM                                                                            AS ACCO_NM
        , A.CRNY_CD                                                                            AS CRNY_CD
        , A.BS_AMT - 0 * NVL(A.DEDT_ACCO_AMT, 0)                                               AS BS_AMT                          -- 대출채권 차감계정을 금리리스크 분류기준에 포함하는 경우 여기서 차감계정을 다시 고려하면 안됨
        , A.FAIR_BS_AMT                                                                        AS FAIR_BS_AMT
        , A.IR_CLSF_CD                                                                         AS IR_CLSF_CD
        , A.IR_CLSF_NM                                                                         AS IR_CLSF_NM
        , NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT)                                                     AS IR_SHCK_WGT
        , A.IR_EXPO_SCEN01 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN01
        , A.IR_EXPO_SCEN02 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN02
        , A.IR_EXPO_SCEN03 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN03
        , A.IR_EXPO_SCEN04 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN04
        , A.IR_EXPO_SCEN05 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN05
        , A.IR_EXPO_SCEN06 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN06
        , A.IR_EXPO_SCEN07 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN07
        , A.IR_EXPO_SCEN08 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN08
        , A.IR_EXPO_SCEN09 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN09
        , A.IR_EXPO_SCEN10 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN10
        , A.IR_EXPO_SCEN11 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN11
        , A.IR_EXPO_SCEN12 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN12
        , A.IR_EXPO_SCEN13 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN13
        , A.IR_EXPO_SCEN14 * NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT) + A.FAIR_BS_AMT * (1.0 - NVL(B.IR_SHCK_WGT,A.IR_SHCK_WGT))             AS IR_EXPO_SCEN14
        , 'KRDA101BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE
--        , C.PROD_GRP_TPCD 
--        , PROD_TPCD
--        , A.FV_HIER_NODE_ID

     FROM (
             SELECT BASE_DATE
                  , EXPO_ID
                  , LEG_TYPE
                  , ASSET_TPCD
                  , FUND_CD
                  , PROD_TPCD
                  , PROD_TPNM
                  , KICS_PROD_TPCD
                  , KICS_PROD_TPNM
                  , ISIN_CD
                  , ISIN_NM
                  --, LT_TP
--20230503 김재우 수정 IFRS9 계정으로 변경
--                  , CASE WHEN ACCO_CD IN ('12600300') THEN 'Y' ELSE LT_TP END AS LT_TP                                            -- MMF는 실제 LT를 수행하지는 않았으나, K-ICS 요건에 따라 반영하였으므로, LT_TP = 'Y' 설정함
--                  , CASE WHEN ACCO_CD IN ('1Q070150', '1R080300', '1S050300') THEN 'Y' ELSE LT_TP END AS LT_TP   -- MMF는 실제 LT를 수행하지는 않았으나, K-ICS 요건에 따라 반영하였으므로, LT_TP = 'Y' 설정함
                    , CASE WHEN ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MMF_LT' ) THEN 'Y' ELSE LT_TP END  AS LT_TP   -- MMF는 실제 LT를 수행하지는 않았으나, K-ICS 요건에 따라 반영하였으므로, LT_TP = 'Y' 설정함
                  , ACCO_CLCD
                  , ACCO_CD
                  , ACCO_NM
                  , CRNY_CD
                  , BS_AMT
                  , FAIR_BS_AMT
                  , ACCR_AMT
                  , DEDT_ACCO_AMT
                  , IR_CLSF_CD
                  , IR_CLSF_NM
                  , IR_SHCK_WGT
                  , FIDE_UNDL_CLCD
                  , IR_EXPO_SCEN01
                  , IR_EXPO_SCEN02
                  , IR_EXPO_SCEN03
                  , IR_EXPO_SCEN04
                  , IR_EXPO_SCEN05
                  , IR_EXPO_SCEN06
                  , IR_EXPO_SCEN07
                  , IR_EXPO_SCEN08
                  , IR_EXPO_SCEN09
                  , IR_EXPO_SCEN10
                  , IR_EXPO_SCEN11
                  , IR_EXPO_SCEN12
                  , IR_EXPO_SCEN13
                  , IR_EXPO_SCEN14
                  , FV_HIER_NODE_ID
               FROM Q_IC_EXPO
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
                AND LEG_TYPE                  IN ('STD', 'NET')                                -- 장외파생상품 REC, PAY LEG는 제외(NET 결과만 준용)
--                AND IR_CLSF_CD            NOT IN ('NON') --  금리리스크 대상이 아닌것도 가져오기 
                AND NVL(FV_HIER_NODE_ID,'0') <>'99999999'  -- 공정가치 미대상
--		        AND FV_HIER_NODE_ID NOT IN ( 'A301' ,'A302', 'B301', 'B302' , 'B101', 'B102')   -- 특별계정미지급금, 특별계정미수금, 보험미수금, 보험미지급금 제외 
                AND NVL(FV_HIER_NODE_ID,'0') NOT IN ( 'A213','B214', 'B101', 'B102')   -- 이연법인세자산, 이연법인세부채, 보험미수금, 보험미지급금 제외 => 정보성 계정이거나 자산 부채 총계에서 수기로 처리하는 항목 
          ) A 
          , ( 
                SELECT * 
                FROM IKRUSH_IR_APPL_RTO            
               WHERE BASE_DATE = '$$STD_Y4MD' 
            ) B         
          ,(
          SELECT KICS_PROD_TPCD, PROD_GRP_TPCD
          FROM IKRUSH_KICS_RISK_MAP 
          ) C
    WHERE 1=1
      AND A.ISIN_CD = B.ISIN_CD(+)
      AND A.KICS_PROD_TPCD = C.KICS_PROD_TPCD (+)
--      UNION ALL
--    SELECT * FROM T_RE_INSU 
      
)
SELECT *
FROM T_AAA
WHERE 1=1