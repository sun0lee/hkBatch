/* 수정사항 
- 계정코드를 매핑하는 SUB쿼리 결과를  WITH 문 처리  

기존 프로그램내 계정과목별 예외 처리 
    1. 공정가치 0처리 대상     : FROM ACCO WHERE RMK LIKE '%FV_0%'  >> SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'FV_0'
    2. B/S 0 처리대상       : FROM ACCO WHERE RMK LIKE 'BV_0' >> SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'BV_0' --'16808802' 기타대출금사모사채(투자)의 경우 현재가치할인차금을 제거함(잔액에 이미 반영됨)
    3. 변액,퇴직  초기자금 제외  : FROM  ACCO WHERE RMK LIKE '%VL_INIT%' >> SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'VL_INIT'
    4. 부동산 분류코드 매핑     : FROM ACCO WHERE RMK  LIKE 'PRPT_%' >> SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_1' 
*/
WITH /* SQL-ID : KRBH104BM */ 
ACCO_MST AS 
 ( /*계정코드 마스터 (신규추가) */
        SELECT ACCO_CD, ACCO_NM
        FROM 
        ( /*레코드 생성일이 다른 경우 동일한 계정코드 정보 중 최신으로 정의된 정보를 가져옴  */
            SELECT SAKNR AS ACCO_CD 
                ,  TXT50 AS ACCO_NM 
                ,  ROW_NUMBER () OVER (PARTITION BY  SAKNR ORDER BY ERDAT DESC ) AS RN
            FROM IBD_SKAT
        )
        WHERE RN = 1
 ) 
, ACCO_EXCPTN AS (
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRBH104BM'
 )
, LEAF AS 
    (
	SELECT 
		 RBUSA||'_'||SAKNR 											AS ACCO_CD
		, TXT50 														AS ACCO_NM
		, RFAREA 													AS FUND_CD
		, CASE WHEN SUBSTR(SAKNR,1,1)='2' THEN (-1)*NVL(HSLXX,0)			 --- 대차변
			ELSE NVL(HSLXX,0) END 									AS BLC
FROM   IBD_FAGLFLEXT
WHERE RPMAX = SUBSTR('$$STD_Y4MD',1,6)
AND SUBSTR(SAKNR,1,1) IN ('1','2')                -- 자산 및 부채계정(그외계정 제외)
)
, CUST AS 
(
                SELECT CORP_NO
                            , KS_CRGR_DMST                                      AS CRGR_KIS
                            , NC_CRGR_DMST                                      AS CRGR_NICE
                            , KR_CRGR_DMST                                      AS CRGR_KR
                            , KS_CRGR_KICS, NC_CRGR_KICS, KR_CRGR_KICS
                            , GREATEST(  DECODE(LEAST(NVL(KS_CRGR_KICS, '999'), NVL(NC_CRGR_KICS, '999')), '999', '0', LEAST(NVL(KS_CRGR_KICS, '999'), NVL(NC_CRGR_KICS, '999')))
                                       , DECODE(LEAST(NVL(KS_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')), '999', '0', LEAST(NVL(KS_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')))
                                       , DECODE(LEAST(NVL(NC_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')), '999', '0', LEAST(NVL(NC_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999'))) )
                                                                                AS CRGR_KICS
                         FROM (
                                 SELECT AA.CORP_NO                              AS CORP_NO
                                      , AA.VLT_CD                               AS VLT_CD
                                      , NVL(BB.CRGR_DMST, '999')                AS CRGR_DMST
                                      , NVL(BB.CRGR_KICS, '999')                AS CRGR_KICS
                                   FROM (
                                           SELECT T1.SCUR_EDPS_CD
                                                , T1.ICNO                                                                         AS CORP_NO
                                                , ROW_NUMBER() OVER(PARTITION BY T1.ICNO, T1.VLT_CO_CD ORDER BY T1.SCUR_EDPS_CD)  AS RN
                                                , T1.VLT_CO_CD                                                                    AS VLT_CD
                                                , T1.TRSN_OTR_CRED_GRDE_CD                                                        AS CD
                                                , NVL(T2.CD_NM, '999')                                                            AS CD_NM
                                             FROM IDHKRH_ISSUER_GRADE T1   --거래상대방신용등급일별정보
                                                , (
                                                     SELECT *
                                                       FROM IKRASM_CO_CD
                                                      WHERE GRP_CD = 'CRGR_EXT'
                                                  ) T2
                                            WHERE 1=1
                                              AND T1.TRSN_OTR_CRED_GRDE_CD = T2.CD(+)
                                              AND T1.STND_DT IN (
                                                                   SELECT MAX(STND_DT)
                                                                     FROM IDHKRH_ISSUER_GRADE
                                                                    WHERE 1=1
                                                                      --AND STND_DT <= TO_CHAR(ADD_MONTHS(TO_DATE('$$STD_Y4MD', 'YYYYMMDD'), +12 ), 'YYYYMMDD')
                                                                      --AND STND_DT >= TO_CHAR(ADD_MONTHS(TO_DATE('$$STD_Y4MD', 'YYYYMMDD'), -24 ), 'YYYYMMDD')
                                                                      AND ICNO = T1.ICNO
                                                                      AND SCUR_EDPS_CD = T1.SCUR_EDPS_CD
                                                                      AND VLT_CO_CD = T1.VLT_CO_CD
                                                                )
                                        ) AA
                                      , (
                                           SELECT CRGR_DMST                     AS CRGR_DMST
                                                , MIN(CRGR_KICS)                AS CRGR_KICS
                                             FROM IKRUSH_MAP_CRGR
                                            GROUP BY CRGR_DMST
                                        ) BB                               -- 평가사 신용등급과 KICS등급간 매핑정보
                                  WHERE 1=1
                                    AND AA.RN = 1
                                    AND AA.CD_NM = BB.CRGR_DMST(+)
                              )
                        PIVOT ( MIN(CRGR_DMST) AS CRGR_DMST, MIN(CRGR_KICS) AS CRGR_KICS FOR VLT_CD IN ('1' AS KR, '2' AS KS, '3' AS NC) )
          )
, T_ACCO AS
( --계정과목별 잔액 정보 적재 
   SELECT '$$STD_Y4MD'                                                                             AS BASE_DATE                       /*  기준일자               */
        , CASE WHEN SUBSTR(LEAF.ACCO_CD, 3, 1) = '1'
               THEN 'ACCO_A_'||LEAF.ACCO_CD||LEAF.FUND_CD
               ELSE 'ACCO_L_'||LEAF.ACCO_CD||LEAF.FUND_CD END                                  AS EXPO_ID                         /*  익스포저ID             */
        , LEAF.FUND_CD                                                                         AS FUND_CD                         /*  펀드코드               */
        , DECODE(SUBSTR(LEAF.ACCO_CD, 3, 1), '1', 'A', 'L')                                    AS PROD_TPCD                       /*  상품유형코드           */
        , '계정처리자산'                                                                           AS PROD_TPNM                       /*  상품유형명             */
        , DECODE(SUBSTR(LEAF.ACCO_CD, 3, 1), '1', 'ZZZ', '999')                                AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */  -- 기본적인 계정처리자산의 방향성을 결정해야함
        , DECODE(SUBSTR(LEAF.ACCO_CD, 3, 1), '1', '기타미분류자산', '기타미분류부채')          AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , NULL                                                                                 AS ISIN_CD                         /*  종목코드               */
        , NULL                                                                                 AS ISIN_NM                         /*  종목명                 */
        , LEAF.ACCO_CD                                                                         AS ACCO_CD                         /*  계정과목코드           */
        , LEAF.ACCO_NM                                                                         AS ACCO_NM                         /*  계정과목명             */
        , NULL                                                                                 AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , 'A'                                                                                  AS INST_TPCD                       /*  인스트루먼트유형코드   */
        , 'Z'                                                                                  AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */
        , '$$STD_Y4MD'                                                                            AS ISSU_DATE                       /*  발행일자               */
        , TO_CHAR(TO_DATE('$$STD_Y4MD', 'YYYYMMDD') + 1, 'YYYYMMDD')                              AS MATR_DATE                       /*  만기일자               */
        , 'KRW'                                                                                AS CRNY_CD                         /*  통화코드               */
        , 1                                                                                    AS CRNY_FXRT                       /*  통화환율(기준일)       */
--	, SUM(LEAF.BLC)								AS NOTL_AMT			 /*  이자계산기준원금       */
	, 0		                                                                       AS NOTL_AMT                        /*  이자계산기준원금       */
        , SUM(LEAF.BLC)                                                                        AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */
	, SUM(CASE WHEN SUBSTR(LEAF.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'BV_0'/*'16808802'*/) THEN 0 ELSE LEAF.BLC END )     AS BS_AMT                          /*  B/S금액                */
        , SUM(CASE WHEN SUBSTR(LEAF.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'BV_0'/*'16808802'*/) THEN 0 ELSE LEAF.BLC END )     AS VLT_AMT                         /*  평가금액               */
--        , SUM(CASE WHEN SUBSTR(LEAF.ACCO_CD, 3) IN ('16808802') THEN 0                                                                                           -- 기타대출금사모사채(투자)의 경우 현재가치할인차금을 제거함(잔액에 이미 반영됨)
--                   --WHEN SUBSTR(LEAF.ACCO_CD, 3) IN ('16809901') THEN LEAF.BLC                                                                                    -- 보험약대 차감계정의 경우는 서브에 위치한 DEDT테이블의 로직에 따라 0이 되나 장부가액 기준 비교모드에서의 활용을 위해 주석처리
--                   ELSE LEAF.BLC * NVL(DEDT.SIGN, 1) END )                                     
       , SUM(CASE WHEN SUBSTR(LEAF.ACCO_CD, 3) IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'FV_0') THEN 0 ELSE LEAF.BLC END)                      AS FAIR_BS_AMT                     /*  공정가치B/S금액(외부)  */  -- 나머지 차감계정은 조인여부에 따라 결정(조인되면 0 처리)
        , NULL                                                                                 AS ACCR_AMT                        /*  미수수익               */
        , NULL                                                                                 AS UERN_AMT                        /*  선수수익               */
        , NULL                                                                                 AS CNPT_ID                         /*  거래상대방ID           */
        , NULL                                                                                 AS CNPT_NM                         /*  거래상대방명           */
        , NULL                                                                                 AS CORP_NO                         /*  법인등록번호           */
        , NULL                                                                                 AS CRGR_KIS                        /*  신용등급(한신평)       */
        , NULL                                                                                 AS CRGR_NICE                       /*  신용등급(한신정)       */
        , NULL                                                                                 AS CRGR_KR                         /*  신용등급(한기평)       */
        , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
        , NULL                                                                                 AS CRGR_KICS                       /*  신용등급(KICS)         */
        , NULL                                                                                 AS STOC_TPCD                       /*  주식유형코드           */
        , NULL                                                                                 AS PRF_STOC_YN                     /*  우선주여부             */
        , NULL                                                                                 AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , NULL                                                                                 AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , 'KR'                                                                                 AS CNTY_CD                         /*  국가코드               */
        , NULL                                                                                 AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */
        , DECODE(SUBSTR(LEAF.FUND_CD, 1, 1), '0', '902472', '902472')                          AS DEPT_CD                         /*  부서코드               */
        , 'KRBH104BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
	, NULL                                                                                     AS OBGT_PSSN_ESTE_YN            /* 의무보유부동산여부 */          	
     FROM  LEAF
    WHERE 1=1
      AND NOT EXISTS
          (
             SELECT *
               FROM (
                       SELECT BASE_DATE
                            , EXPO_ID
                            , FUND_CD
                            , ACCO_CD /* 계정과목 TOBE코드로 입수*/
                         FROM Q_IC_ASSET_SECR
                        WHERE 1=1

                        UNION ALL

                       SELECT BASE_DATE
                            , EXPO_ID
                            , FUND_CD
                            , ACCO_CD /* 계정과목 TOBE코드로 입수*/
                         FROM Q_IC_ASSET_FIDE
                        WHERE 1=1

                        UNION ALL

                       SELECT BASE_DATE
                            , EXPO_ID
                            , FUND_CD
                            , ACCO_CD  --ACCO_CD /* 계정과목 TOBE코드로 입수*/
                         FROM Q_IC_ASSET_LOAN  
                        WHERE 1=1
                    )
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
--                AND TO_CHAR(DECODE(SUBSTR(ACCO_CD, 1, 1), '1', '0000', FUND_CD)||ACCO_CD) = TO_CHAR(LEAF.FUND_CD||LEAF.ACCO_CD)
-- 20230425 김재우 수정 일반계정 0000을 G000으로 변경
                AND TO_CHAR(DECODE(SUBSTR(ACCO_CD, 1, 1), '1', 'G000', FUND_CD)||ACCO_CD) = TO_CHAR(LEAF.FUND_CD||LEAF.ACCO_CD)
          )
      AND LEAF.ACCO_CD NOT IN (
                                 SELECT DISTINCT '1_'||ACCO_CD /* 계정과목 TOBE코드로 입수*/
                                   FROM IKRUSH_CASH_DEP A
                                  WHERE BASE_DATE = '$$STD_Y4MD'
                              )
      AND LEAF.ACCO_CD NOT IN (
                                 SELECT DISTINCT '1_'||ACCO_CD /* 계정과목 TOBE코드로 입수*/
                                   FROM IKRUSH_PRPT A
                                  WHERE BASE_DATE = '$$STD_Y4MD'
                              )
--20230531 김재우 추가
-- 변액초기자금은 B/S 제외      
      AND SUBSTR(LEAF.ACCO_CD, 3) NOT IN ( SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP ='VL_INIT')
      --AND (     BASE_DATE = '20181231' AND SUBSTR(LEAF.ACCO_CD, 3   ) NOT IN ('11155090', '11600300')) -- 현예금테이블 중 계정과목 오입력(?)으로 인한 중복제거
      --AND SUBSTR(LEAF.ACCO_CD, 3   ) NOT IN ('11155090', '11600300')       -- 현예금테이블 중 계정과목 오입력(?)으로 인한 중복제거

    GROUP BY
          LEAF.ACCO_CD
        , LEAF.ACCO_NM
        , LEAF.FUND_CD
   HAVING (SUM(LEAF.BLC) <> 0 OR (LEAF.ACCO_CD IN ('1_1252000010', '1_2519000010')))  -- 이연법인세 자산/부채 계정은 잔액이 0인 경우에도 포함시킴
)
,T_CASH AS 
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , 'CASH_O_'||A.ACCN                                                                    AS EXPO_ID                         /*  익스포저ID             */
--20230525 김재우 수정 0000계정을 G000으로 수정
--        , '0000'                                                                               AS FUND_CD                         /*  펀드코드               */
        , 'G000'                                                                               AS FUND_CD                         /*  펀드코드               */
        , TRIM(A.PROD_TPCD)||'C'                                                               AS PROD_TPCD                       /*  상품유형코드           */
        , KICS.PROD_TPNM                                                                       AS PROD_TPNM                       /*  상품유형명             */
        , NVL(KICS.KICS_TPCD, '199')                                                           AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
        , KICS.KICS_TPNM                                                                       AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , NULL                                                                                 AS ISIN_CD                         /*  종목코드               */
        , NULL                                                                                 AS ISIN_NM                         /*  종목명                 */
        , '1_'||NVL(A.ACCO_CD, '9999999999')                                                     AS ACCO_CD                         /*  계정과목코드           */
        , B.ACCO_NM                                                                    AS ACCO_NM                         /*  계정과목명             */
        , NULL                                                                                 AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , 'A'                                                                                  AS INST_TPCD                       /*  인스트루먼트유형코드   */
        , 'Z'                                                                                  AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */
        , A.BASE_DATE                                                                          AS ISSU_DATE                       /*  발행일자               */
        , NVL(A.MATR_DATE, TO_CHAR(TO_DATE(A.BASE_DATE, 'YYYYMMDD') + 1, 'YYYYMMDD'))          AS MATR_DATE                       /*  만기일자               */
        , NVL(A.CRNY_CD, 'KRW')                                                                AS CRNY_CD                         /*  통화코드               */
        , 1                                                                                    AS CRNY_FXRT                       /*  통화환율(기준일)       */
        , A.BS_AMT                                                                             AS NOTL_AMT                        /*  이자계산기준원금       */
        , A.BS_AMT                                                                             AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */
        , A.BS_AMT                                                                             AS BS_AMT                          /*  B/S금액                */
        , A.BS_AMT                                                                             AS VLT_AMT                         /*  평가금액               */
        , A.BS_AMT                                                                             AS FAIR_BS_AMT                     /*  공정가치B/S금액(외부)  */
        , A.ACCR_AMT                                                                           AS ACCR_AMT                        /*  미수수익               */
        , NULL                                                                                 AS UERN_AMT                        /*  선수수익               */
        , A.CORP_NO                                                                            AS CNPT_ID                         /*  거래상대방ID           */
        , A.CNPT_NM                                                                            AS CNPT_NM                         /*  거래상대방명           */
        , A.CORP_NO                                                                            AS CORP_NO                         /*  법인등록번호           */
        , CUST.CRGR_KIS                                                                        AS CRGR_KIS                        /*  신용등급(한신평)       */
        , CUST.CRGR_NICE                                                                       AS CRGR_NICE                       /*  신용등급(한신정)       */
        , CUST.CRGR_KR                                                                         AS CRGR_KR                         /*  신용등급(한기평)       */
        , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
        , CUST.CRGR_KICS                                                                       AS CRGR_KICS                       /*  신용등급(KICS)         */
        , NULL                                                                                 AS STOC_TPCD                       /*  주식유형코드           */
        , NULL                                                                                 AS PRF_STOC_YN                     /*  우선주여부             */
        , NULL                                                                                 AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , NULL                                                                                 AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , 'KR'                                                                                 AS CNTY_CD                         /*  국가코드               */
        , NULL                                                                                 AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */
        , '902472'                                                                             AS DEPT_CD                         /*  부서코드               */
        , 'KRBH104BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
	, NULL                                                                                     AS OBGT_PSSN_ESTE_YN            /* 의무보유부동산여부 */          	
     FROM IKRUSH_CASH_DEP A                            -- 현예금_기본
        , (
             SELECT CD                                                AS PROD_TPCD
                  , CD_NM                                             AS PROD_TPNM
                  , CD2                                               AS KICS_TPCD
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_MAP_SAP'      -- KICS상품분류매핑(기존분류코드를 KICS상품분류코드로 매핑)
          ) KICS
        ,  ACCO_MST B
        ,  CUST		
    WHERE 1=1
      AND A.BASE_DATE = '$$STD_Y4MD'
      AND NVL(A.PROD_TPCD, '99')||'C' = KICS.PROD_TPCD(+)
      AND A.ACCO_CD    = B.ACCO_CD(+)
      AND A.CORP_NO = CUST.CORP_NO(+)
)
, T_PRPT AS 
( /*부동산 수기 테이블은 신규 재무상태표 코드 기준으로 입수하므로 코드와 관련된 매핑은 신규코드(10자리) 기준으로 작성함. */
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , 'PRPT_O_'||A.ASSET_NO                                                                AS EXPO_ID                         /*  익스포저ID             */
--20230525 김재우 수정 0000계정을 G000으로 수정
--        , '0000'                                                                               AS FUND_CD                         /*  펀드코드               */
        , 'G000'                                                                               AS FUND_CD                         /*  펀드코드               */
        , CASE WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_1') THEN '1'
		WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_2') THEN '2'
		WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_5') THEN '5'
               ELSE '9' END                                        AS PROD_TPCD                       /*  상품유형코드           */
        , CASE WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_1') THEN '토지'
		WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_2') THEN '건물'
		WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'PRPT_5') THEN '건설중인자산'
               ELSE '미분류' END                                        AS PROD_TPNM                       /*  상품유형명             */
	, CASE WHEN A.OBGT_PSSN_ESTE_YN = 'Y' THEN '560' 
	       WHEN A.PRPT_IVSM_TPCD = '1' AND A.PRPT_CNTY_TPCD = '1' THEN '510'
               WHEN A.PRPT_IVSM_TPCD = '1' AND A.PRPT_CNTY_TPCD = '2' THEN '520'
               WHEN A.PRPT_IVSM_TPCD = '2'                            THEN '530'
               ELSE '599' END                                                                  AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
        , CASE WHEN A.OBGT_PSSN_ESTE_YN = 'Y' THEN '의무보유부동산' 
		WHEN A.PRPT_IVSM_TPCD = '1' AND A.PRPT_CNTY_TPCD = '1' THEN '부동산투자용(국내/직접소유)'
               WHEN A.PRPT_IVSM_TPCD = '1' AND A.PRPT_CNTY_TPCD = '2' THEN '부동산투자용(해외/직접소유)'
               WHEN A.PRPT_IVSM_TPCD = '2'                            THEN '부동산업무용(직접소유)'
               ELSE '기타부동산' END                                                           AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , NULL                                                                                 AS ISIN_CD                         /*  종목코드               */
        , A.ASSET_NM                                                                           AS ISIN_NM                         /*  종목명                 */
        , '1_'||NVL(A.ACCO_CD, '9999999999')                                                     AS ACCO_CD                         /*  계정과목코드           */
        , B.ACCO_NM                                                                    AS ACCO_NM                         /*  계정과목명             */
        , A.ASSET_NO                                                                           AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , 'A'                                                                                  AS INST_TPCD                       /*  인스트루먼트유형코드   */
        , 'Z'                                                                                  AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */
        , A.BASE_DATE                                                                          AS ISSU_DATE                       /*  발행일자               */
        , TO_CHAR(TO_DATE(A.BASE_DATE, 'YYYYMMDD') + 1, 'YYYYMMDD')                            AS MATR_DATE                       /*  만기일자               */
        , 'KRW'                                                                                AS CRNY_CD                         /*  통화코드               */
        , 1                                                                                    AS CRNY_FXRT                       /*  통화환율(기준일)       */
        , A.BS_AMT                                                                             AS NOTL_AMT                        /*  이자계산기준원금       */
        , A.BS_AMT                                                                             AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */
        , A.BS_AMT                                                                             AS BS_AMT                          /*  B/S금액                */
        , A.VLT_AMT                                                                            AS VLT_AMT                         /*  평가금액               */
        , A.VLT_AMT                                                                            AS FAIR_BS_AMT                     /*  공정가치B/S금액(외부)  */
        , NULL                                                                                 AS ACCR_AMT                        /*  미수수익               */
        , NULL                                                                                 AS UERN_AMT                        /*  선수수익               */
        , NULL                                                                                 AS CNPT_ID                         /*  거래상대방ID           */
        , A.LOCA                                                                               AS CNPT_NM                         /*  거래상대방명           */
        , NULL                                                                                 AS CORP_NO                         /*  법인등록번호           */
        , NULL                                                                                 AS CRGR_KIS                        /*  신용등급(한신평)       */
        , NULL                                                                                 AS CRGR_NICE                       /*  신용등급(한신정)       */
        , NULL                                                                                 AS CRGR_KR                         /*  신용등급(한기평)       */
        , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
        , NULL                                                                                 AS CRGR_KICS                       /*  신용등급(KICS)         */
        , NULL                                                                                 AS STOC_TPCD                       /*  주식유형코드           */
        , NULL                                                                                 AS PRF_STOC_YN                     /*  우선주여부             */
        , NULL                                                                                 AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , NULL                                                                                 AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , 'KR'                                                                                 AS CNTY_CD                         /*  국가코드               */
        , NULL                                                                                 AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */
        , '902472'                                                                             AS DEPT_CD                         /*  부서코드               */
        , 'KRBH104BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
	, A.OBGT_PSSN_ESTE_YN                                                  AS OBGT_PSSN_ESTE_YN            /* 의무보유부동산여부 */          	
       FROM IKRUSH_PRPT A                              -- 부동산_수기 
        , ACCO_MST B
    WHERE 1=1
      AND A.BASE_DATE = '$$STD_Y4MD'
      AND A.ACCO_CD    = B.ACCO_CD(+)
)

SELECT * FROM T_ACCO

UNION ALL
SELECT * FROM T_CASH

UNION ALL
SELECT * FROM T_PRPT