/* 수정사항 
- 계정코드별 예외처리 : ACCO_EXCPTN
- ACCO_MST 계정코드 마스터 (신규추가) : 계정코드별 계정코드명 

2024-04-12 발행채권 및 차입금 (비용처리대상) 금리부자산 처리 : IS_IRATE_ASSET , INST_TPCD 
2024-04-29 계정과목별 예외 처리 
    1. MATR_DATE 조정 : 만기6개월 처리 대상 (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M') 
    2. CRGR_KICS 조정 : 신용등급 4등급 처리  (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M')
    3. IRATE 조정     : MMF 요건 : 공사채(AAA)등급 6월물 적용할인율 (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M' )

2024-05-02 : 위험경감불인정 통화선도 추가 
     - 730 : 통화스왑 CRS
     - 731 : FX스왑 
     - 733 : 통화스왑 CRS (위험경감불인정) => 신규추가 
     - 734 : FX스왑 (위험경감불인정 => 신규추가)

2024-05-09 : 간접보유 변동금리채권 => 임시 고정금리 타도록 수정  (아래 코드 주석처리 )
    --WHEN B.BOND_INRS_PAY_MTCD = '08' THEN '5'     08: FRN(5)  2024.05.09 간접보유 변동금리채권 : 고정이표채 임시수정 
- 일수계산방식 DCB_CD ='1'

2024-05-24 : KICS 상품코드 분류 수정 : LT 세부상품분류코드를 우선하여 분류 (레버리지 펀드의 경우 세부 속성기준으로 분류)  
2024-05-28 : IKRUSH_PROD_SECR 기준월에 해당되는 수기_상품(유가증권)정보만 사용하도록 수정  
*/

WITH /* SQL-ID : KRBH109BM */ 
ACCO_EXCPTN AS ( /*계정코드 예외처리 대상*/
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRBH109BM'
)
--SELECT * FROM ACCO_EXCPTN;
 , ACCO_MST AS 
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
, LT AS /*유가증권 (수익증권) */
(
         SELECT /*+ USE_HASH(A1 C1) FULL(A1) */
                A1.*
-- 20230421 김재우 수정 기존정보 미사용(수기 테이블만 사용)
                ,  DECODE(A1.VLT_CORP_TPCD, '1' , A1.CRGR_DMST) AS CRGR_KIS
                ,  DECODE(A1.VLT_CORP_TPCD, '2' , A1.CRGR_DMST) AS CRGR_NICE
                ,  DECODE(A1.VLT_CORP_TPCD, '3' , A1.CRGR_DMST) AS CRGR_KR
                ,  DECODE(A1.VLT_CORP_TPCD, '4' , A1.CRGR_OVSE) AS CRGR_SNP
                ,  DECODE(A1.VLT_CORP_TPCD, '5' , A1.CRGR_OVSE) AS CRGR_MOODYS
                ,  DECODE(A1.VLT_CORP_TPCD, '6' , A1.CRGR_OVSE) AS CRGR_FITCH                    
--                  , C1.ITMS_NM
--                  , C1.ISSU_DT
--                  , C1.EXPR_DT
--                  , C1.INRS_PAY_YYCT
--                  , C1.INRS_PAY_FREQ
--                  , C1.INRS_CLCL_MTH_CLCD
--                  , C1.CAP_ROIT
--                  , C1.FLOR_ROIT
--                  , C1.FLT_ITRT_CD
--                  , C1.FLT_ROIT_APLZ_YN
--                  , C1.ADD_ITRT
--                  , C1.ISSU_ROIT
--                  , C1.OPTN_EVNT_FR_DT
--                  , C1.OPTN_EVNT_CLOS_DT
--                  , CASE WHEN A1.ACCO_CD IN ('11170300', '11231000') THEN '08'                                     -- 구조화채권 계정 및 내재옵션이 포함된 상품
--                         ELSE C1.BOND_OPTN_CLCD END                                            AS BOND_OPTN_CLCD
--                  , C1.POCP_DAPY_CLCD
--                  , C1.SHRE_KNCD
--                  , C1.SUOR_BOND_CLCD
--                  , C1.LSTN_CLCD
--                  , C1.ISO_NATN_SYM_CD
--                  , C1.INRS_PAY_CLCD
--                 , C1.ISSU_INT_CUST_NO
--                  , C1.CUST_NM
--                 , C1.CORP_REG_NO
--                  , C1.CRGR_KIS
--                 , C1.CRGR_NICE
--                  , C1.CRGR_KR
--                  , NVL(A1.CRGR_KICS, C1.CRGR_KICS)                                            AS CRGR_KICS2      -- 수기입력된 KICS등급이 존재하면 준용, 그렇지 않으면 매핑정보를 통해 2차 시도 진행
              --, NVL(C1.PRNT_FNDS_ITMS_CD, A1.ISIN_CD)                                      AS SECR_ITMS_CD    -- 분리이전 원시KR코드(ISIN_CD) 생성
              , CASE WHEN A1.LT_TPCD IN ('2') THEN '1'                                                        -- 채권
                     WHEN A1.LT_TPCD IN ('1') THEN CASE WHEN A1.LT_DTLS_TPCD IN (  '131'                      -- 구조화예금
                                                                                 , '150', '151', '152'        -- CMA, CP
                                                                                 , '160', '161')              -- RP, 콜론
                                                        THEN '1'                                              -- 예금(금리부)
                                                        ELSE '9' END                                        -- 예금(비금리부)
                     WHEN A1.LT_TPCD IN ('4') THEN '5'                                          -- 주식
                     WHEN A1.LT_TPCD IN ('6') THEN '9'                                          -- 파생상품인데 우선 속성이 거의 없는 상황
                     WHEN A1.LT_TPCD IN ('8') THEN '9'                                          -- 수익증권
                     WHEN A1.LT_DTLS_TPCD IN ('910','911','920','921') THEN '1'  -- 발행채권 및 차입금 (후순위포함) 이면 금리부자산 처리 --20240412 김재우 추가 
                     ELSE '9' END                                                                             -- 기타
                                                                                           AS INST_TPCD       -- 인스트루먼트유형코드(상품타입)
              , CASE WHEN TRUNC(A1.LT_BS_AMT) = 0 THEN '0'
                     WHEN A1.LT_TPCD IN ('2')     THEN '1'
                     WHEN A1.LT_TPCD IN ('1') AND A1.LT_DTLS_TPCD IN (  '131'                                 -- 구조화예금
                                                                      , '150', '151', '152'                   -- CMA, CP
                                                                      , '160', '161')                         -- RP, 콜론
                                                  THEN '1'                                                    -- 예금(금리부)
-- 20230421 김재우 수정. 수익증권은 금리리스크 미측정으로 수정
--                         WHEN A1.LT_TPCD IN ('8') AND A1.LT_DTLS_TPCD IN ('820')                                  -- 수익증권(채권형)
--                                                      THEN '1'
                     WHEN A1.LT_DTLS_TPCD IN ('910','911','920','921') THEN '1'  -- 발행채권 및 차입금 (후순위포함) 이면 금리부자산 처리 --20240412 김재우 추가 
                     ELSE '0' END
                                                                                           AS IS_IRATE_ASSET  -- 금리부자산(1: 금리부자산)
--20230425 김재우 수정 펀드코드 로직 변경 
--                  , DECODE(FUND.FUND_CD, NULL, 2, 1)                                           AS ACCO_CLCD
              , DECODE(A1.FUND_CD,'G000','1','2')                                           AS ACCO_CLCD  
              , PROD.CD_NM                                                                 AS PROD_TPNM
              , NVL(FX.FX_RATE, 1)                                                         AS FX_RATE

           FROM IKRUSH_LT_TOTAL A1                       -- 수익증권
--                  , (
--                       SELECT /*+ USE_HASH(CC FF) FULL(CC) */
--                              CC.*
--                            , FF.CORP_REG_NO
--                            , FF.CUST_NM
--                            , FF.CRGR_KIS
--                            , FF.CRGR_NICE
--                            , FF.CRGR_KR
--                            , FF.CRGR_KICS
--                         FROM IKRASH_ITEM_INFO CC           -- 유가증권 종목정보
--                            , (
--                                 SELECT DD.CUST_NO                                                  AS CUST_NO
--                                      , MAX(DD.CUST_NM)                                             AS CUST_NM
--                                      , MAX(DD.CORP_REG_NO)                                         AS CORP_REG_NO
--                                      , MAX(EE.CRGR_KIS)                                            AS CRGR_KIS
--                                      , MAX(EE.CRGR_NICE)                                           AS CRGR_NICE
--                                      , MAX(EE.CRGR_KR)                                             AS CRGR_KR
--                                      , MAX(DECODE(EE.CRGR_KICS, 0, NULL, EE.CRGR_KICS))            AS CRGR_KICS
--                                   FROM IKRASH_CUST_BASE DD -- 고객기본
--                                      , (
--                                           SELECT CORP_NO
--                                                , KS_CRGR_DMST                                      AS CRGR_KIS
--                                                , NC_CRGR_DMST                                      AS CRGR_NICE
--                                                , KR_CRGR_DMST                                      AS CRGR_KR
--                                                --, KS_CRGR_KICS, NC_CRGR_KICS, KR_CRGR_KICS
--                                                , GREATEST(  DECODE(LEAST(NVL(KS_CRGR_KICS, '999'), NVL(NC_CRGR_KICS, '999')), '999', '0', LEAST(NVL(KS_CRGR_KICS, '999'), NVL(NC_CRGR_KICS, '999')))
--                                                           , DECODE(LEAST(NVL(KS_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')), '999', '0', LEAST(NVL(KS_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')))
--                                                           , DECODE(LEAST(NVL(NC_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999')), '999', '0', LEAST(NVL(NC_CRGR_KICS, '999'), NVL(KR_CRGR_KICS, '999'))) )
--                                                                                                    AS CRGR_KICS
--                                             FROM (
--                                                     SELECT AA.CORP_NO                              AS CORP_NO
--                                                          , AA.VLT_CD                               AS VLT_CD
--                                                          , NVL(BB.CRGR_DMST, '999')                AS CRGR_DMST
--                                                          , NVL(BB.CRGR_KICS, '999')                AS CRGR_KICS
--                                                       FROM (
--                                                               SELECT T1.SCUR_EDPS_CD
--                                                                    , T1.ICNO                                                                         AS CORP_NO
--                                                                    , ROW_NUMBER() OVER(PARTITION BY T1.ICNO, T1.VLT_CO_CD ORDER BY T1.SCUR_EDPS_CD)  AS RN
--                                                                    , T1.VLT_CO_CD                                                                    AS VLT_CD
--                                                                    , T1.TRSN_OTR_CRED_GRDE_CD                                                        AS CD
--                                                                    , NVL(T2.CD_NM, '999')                                                            AS CD_NM
--                                                                 FROM IDHKRH_ISSUER_GRADE T1 --거래상대방신용등급일별정보
--                                                                    , (
--                                                                         SELECT *
--                                                                           FROM IKRASM_CO_CD
--                                                                          WHERE GRP_CD = 'CRGR_EXT'
--                                                                      ) T2                   -- 외부신용등급 코드와 등급간 매핑정보
--                                                                WHERE 1=1
--                                                                  AND T1.TRSN_OTR_CRED_GRDE_CD = T2.CD(+)
--                                                                  AND T1.STND_DT IN (
--                                                                                       SELECT MAX(STND_DT)
--                                                                                         FROM IDHKRH_ISSUER_GRADE
--                                                                                        WHERE 1=1
--                                                                                          --AND STND_DT <= TO_CHAR(ADD_MONTHS(TO_DATE('$$STD_Y4MD', 'YYYYMMDD'), +12 ), 'YYYYMMDD')
--                                                                                          --AND STND_DT >= TO_CHAR(ADD_MONTHS(TO_DATE('$$STD_Y4MD', 'YYYYMMDD'), -24 ), 'YYYYMMDD')
--                                                                                          AND ICNO = T1.ICNO
--                                                                                          AND SCUR_EDPS_CD = T1.SCUR_EDPS_CD
--                                                                                          AND VLT_CO_CD = T1.VLT_CO_CD
--                                                                                    )
--                                                            ) AA
--                                                          , (
--                                                               SELECT CRGR_DMST                     AS CRGR_DMST
--                                                                    , MIN(CRGR_KICS)                AS CRGR_KICS
--                                                                 FROM IKRUSH_MAP_CRGR
--                                                                GROUP BY CRGR_DMST
--                                                            ) BB                               -- 평가사 신용등급과 KICS등급간 매핑정보
--                                                      WHERE 1=1
--                                                        AND AA.RN = 1
--                                                        AND AA.CD_NM = BB.CRGR_DMST(+)
--                                                  )
--                                            PIVOT ( MIN(CRGR_DMST) AS CRGR_DMST, MIN(CRGR_KICS) AS CRGR_KICS FOR VLT_CD IN ('1' AS KR, '2' AS KS, '3' AS NC) )
--                                        ) EE                -- 법인등록번호기반 신용등급 매핑
--                                  WHERE 1=1
--                                    AND DD.CORP_REG_NO = EE.CORP_NO(+)
--                                    AND DD.STND_DT = (
--                                                        SELECT MAX(STND_DT)
--                                                          FROM IKRASH_CUST_BASE
--                                                         WHERE STND_DT <= '$$STD_Y4MD'
--                                                           AND CUST_NO = DD.CUST_NO
--                                                     )
--                                  GROUP BY DD.CUST_NO
--                              ) FF
--                        WHERE 1=1
--                          AND CC.ISSU_INT_CUST_NO = FF.CUST_NO(+)
--                    ) C1
--                  , (
--                       SELECT FUND_CD
--                         FROM IKRUSH_FUND_CD
--                        WHERE FUND_CLCD = '1'
--                    ) FUND
              , (
                   SELECT CD
                        , CD_NM
                     FROM IKRASM_CO_CD
                    WHERE GRP_CD = 'LT_TPCD'
                ) PROD
              , (
                   SELECT STND_DT
                        , SUBSTR(FOEX_CD, 6, 3)                                             AS CRNC_CD
                        , CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL) AS FX_RATE
                    FROM IDHKRH_FX
                   WHERE 1=1
                     AND STND_DT = (
                                      SELECT MAX(STND_DT)
                                        FROM IDHKRH_FX  -- 외화환율데이터
                                       WHERE STND_DT <= '$$STD_Y4MD'
                                   )
                ) FX
          WHERE 1=1
--                AND A1.BASE_DATE = C1.STND_DT(+)
--                AND A1.ISIN_CD   = C1.STAN_ITMS_CD(+)
--                AND A1.FUND_CD   = FUND.FUND_CD(+)
            AND NVL(A1.LT_TPCD,  '99') = PROD.CD(+)
            AND NVL(A1.CRNY_CD, 'KRW') = FX.CRNC_CD(+)
            AND A1.BASE_DATE = '$$STD_Y4MD'
      
)
, BD_INFO AS /*채권발행정보*/
(
     SELECT *
       FROM IDHKRH_BND_INFO                         -- 채권발행정보
      WHERE 1=1
        AND STND_DT = (
                         SELECT MAX(STND_DT)
                           FROM IDHKRH_BND_INFO
                          WHERE STND_DT <= '$$STD_Y4MD'
                      )          
)
, BOND AS /* 국내채권 */
(
         SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      AS SECR_ITMS_CD
              , TRUNC( (  NVL(T1.UPRC, T2.UPRC) + NVL(T2.UPRC, T1.UPRC)   ) * 0.5, 3)      AS UPRC_DIRTY
              , TRUNC( (  NVL(DECODE(T1.UPRC, 0, NULL, T1.UPRC), T2.UPRC)
                        + NVL(DECODE(T2.UPRC, 0, NULL, T2.UPRC), T1.UPRC) ) * 0.5, 3)      AS UPRC_DIRTY2
              , TRUNC( (  NVL(T1.UPRC, T2.UPRC) + NVL(T2.UPRC, T1.UPRC)   ) * 0.5
                        - NVL(T2.UPRC - NVL(T2.CLE_UPRC, T2.UPRC), 0)            , 2)      AS UPRC_CLEAN
              , TRUNC( (  NVL(DECODE(T1.EXCG_BOND_VALU_AMT, 0, NULL, T1.EXCG_BOND_VALU_AMT), T2.EXCG_BOND_VALU_AMT)
                        + NVL(DECODE(T2.EXCG_BOND_VALU_AMT, 0, NULL, T2.EXCG_BOND_VALU_AMT), T1.EXCG_BOND_VALU_AMT) ) * 0.5
                                                                                 , 3)      AS UPRC_EXCG_BOND
              , TRUNC( (  NVL(DECODE(T1.EXCG_OPTN_VALU_AMT, 0, NULL, T1.EXCG_OPTN_VALU_AMT), T2.EXCG_OPTN_VALU_AMT)
                        + NVL(DECODE(T2.EXCG_OPTN_VALU_AMT, 0, NULL, T2.EXCG_OPTN_VALU_AMT), T1.EXCG_OPTN_VALU_AMT) ) * 0.5
                                                                                 , 3)      AS UPRC_EXCG_OPTN
              , TRUNC( (  NVL(DECODE(T1.MACT_BOND_VALU_AMT, 0, NULL, T1.MACT_BOND_VALU_AMT), T2.MACT_BOND_VALU_AMT)
                        + NVL(DECODE(T2.MACT_BOND_VALU_AMT, 0, NULL, T2.MACT_BOND_VALU_AMT), T1.MACT_BOND_VALU_AMT) ) * 0.5
                                                                                 , 3)      AS UPRC_MACT_BOND
              , TRUNC( (  NVL(DECODE(T1.MACT_OPTN_VALU_AMT, 0, NULL, T1.MACT_OPTN_VALU_AMT), T2.MACT_OPTN_VALU_AMT)
                        + NVL(DECODE(T2.MACT_OPTN_VALU_AMT, 0, NULL, T2.MACT_OPTN_VALU_AMT), T1.MACT_OPTN_VALU_AMT) ) * 0.5
                                                                                 , 3)      AS UPRC_MACT_OPTN
              , TRUNC( (  NVL(DECODE(T1.UPD_DURA          , 0, NULL, T1.UPD_DURA          ), T2.UPD_DURA          )
                        + NVL(DECODE(T2.UPD_DURA          , 0, NULL, T2.UPD_DURA          ), T1.UPD_DURA          ) ) * 0.5
                                                                                 , 4)      AS DURA_MODI
              , TRUNC( (  NVL(DECODE(T1.BPDR100           , 0, NULL, T1.BPDR100           ), T2.BPDR100           )
                        + NVL(DECODE(T2.BPDR100           , 0, NULL, T2.BPDR100           ), T1.BPDR100           ) ) * 0.5
                                                                                 , 4)      AS DURA_EFFE
              , DECODE(T1.UPRC   , 0, NULL, T1.UPRC   )                                    AS UPRC_1
              , DECODE(T2.UPRC   , 0, NULL, T2.UPRC   )                                    AS UPRC_2
              , DECODE(T1.BPDR100, 0, NULL, T1.BPDR100)                                    AS DURA_1
              , DECODE(T2.BPDR100, 0, NULL, T2.BPDR100)                                    AS DURA_2
              --, T1.ISSU_CO_CRED_GRDE_CD                                                    AS CRGR_FNGD
              , NVL( T2.ISSU_CO_CRED_GRDE_CD
                   , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)  -- 외부신용등급 코드와 등급간 매핑정보(eg. '0110' -> 'AAA')
                        , NVL( T1.ISSU_CO_CRED_GRDE_CD
                             , (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD) ) ) )
                                                                                           AS CRGR_NICE
              , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR
                  WHERE CRGR_DMST IN NVL( T2.ISSU_CO_CRED_GRDE_CD
                                        , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)
                                             , NVL( T1.ISSU_CO_CRED_GRDE_CD
                                                  , (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD) ) ) ) )
                                                                                           AS CRGR_KICS
           FROM (
                   SELECT *
                     FROM IDHKRH_FN_BDEV    -- 국내채권 FN
                    WHERE STND_DT = (
                                       SELECT MAX(STND_DT)
                                         FROM IDHKRH_FN_BDEV
                                        WHERE STND_DT <= '$$STD_Y4MD'
                                    )
                ) T1
                     FULL OUTER JOIN
                (
                   SELECT *
                     FROM IDHKRH_NICE_BDEV  -- 국내채권 NICE
                    WHERE STND_DT = (
                                       SELECT MAX(STND_DT)
                                         FROM IDHKRH_NICE_BDEV
                                        WHERE STND_DT <= '$$STD_Y4MD'
                                    )
                ) T2
             ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
          WHERE 1=1
)

, FB  AS /*해외채권*/
(
     SELECT DD.*
          , GREATEST (  DECODE(LEAST(KS_CRGR_KICS, NC_CRGR_KICS), '999', '0', LEAST(KS_CRGR_KICS, NC_CRGR_KICS))
                      , DECODE(LEAST(KS_CRGR_KICS, KR_CRGR_KICS), '999', '0', LEAST(KS_CRGR_KICS, KR_CRGR_KICS))
                      , DECODE(LEAST(NC_CRGR_KICS, KR_CRGR_KICS), '999', '0', LEAST(NC_CRGR_KICS, KR_CRGR_KICS)) )
                                                                                                 AS CRGR_KICS
       FROM (
               SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      AS SECR_ITMS_CD
                    , TRUNC( (  NVL(T1.CLE_UPRC, T2.CLE_UPRC) + NVL(T2.CLE_UPRC, T1.CLE_UPRC)                             ) * 0.5
                                                                                       , 5)      AS UPRC_CLEAN
                    , TRUNC( (  NVL(DECODE(T1.MACT_BOND_VALU_AMT, 0, NULL, T1.MACT_BOND_VALU_AMT), T2.MACT_BOND_VALU_AMT)
                              + NVL(DECODE(T2.MACT_BOND_VALU_AMT, 0, NULL, T2.MACT_BOND_VALU_AMT), T1.MACT_BOND_VALU_AMT) ) * 0.5
                                                                                       , 5)      AS UPRC_MACT_BOND
                    , TRUNC( (  NVL(DECODE(T1.IMCE_DRVT_GDS_PRC,  0, NULL, T1.IMCE_DRVT_GDS_PRC ), T2.IMCE_DRVT_GDS_PRC )
                              + NVL(DECODE(T2.IMCE_DRVT_GDS_PRC,  0, NULL, T2.IMCE_DRVT_GDS_PRC ), T1.IMCE_DRVT_GDS_PRC ) ) * 0.5
                                                                                       , 3)      AS UPRC_MACT_OPTN
                    , TRUNC( (  NVL(DECODE(T1.UPD_DURA,           0, NULL, T1.UPD_DURA          ), T2.UPD_DURA          )
                              + NVL(DECODE(T2.UPD_DURA,           0, NULL, T2.UPD_DURA          ), T1.UPD_DURA          ) ) * 0.5
                                                                                       , 4)      AS DURA_MODI
                    , TRUNC(        DECODE(T2.BPDR100,            0, NULL, T2.BPDR100) , 4)      AS DURA_EFFE
                    , DECODE(T1.CLE_UPRC, 0, NULL, T1.CLE_UPRC)                                  AS UPRC_1
                    , DECODE(T2.CLE_UPRC, 0, NULL, T2.CLE_UPRC)                                  AS UPRC_2
                    , DECODE(T1.UPD_DURA, 0, NULL, T1.UPD_DURA)                                  AS DURA_1
                    , DECODE(T2.BPDR100 , 0, NULL, T2.BPDR100 )                                  AS DURA_2
                    , T2.KSRT_BOND_GRDE                                                          AS CRGR_KIS
                    , T2.NICE_BOND_GRDE                                                          AS CRGR_NICE
                    , T2.KR_BOND_GRDE                                                            AS CRGR_KR
                    , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.KSRT_BOND_GRDE), '999')
                                                                                                 AS KS_CRGR_KICS
                    , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.NICE_BOND_GRDE), '999')
                                                                                                 AS NC_CRGR_KICS
                    , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.KR_BOND_GRDE  ), '999')
                                                                                                 AS KR_CRGR_KICS
                 FROM (
                         SELECT *
                           FROM IDHKRH_FN_FBEV    -- 해외채권 FN
                          WHERE STND_DT = (
                                             SELECT MAX(STND_DT)
                                               FROM IDHKRH_FN_FBEV
                                              WHERE STND_DT <= '$$STD_Y4MD'
                                          )
                      ) T1
                           FULL OUTER JOIN
                      (
                         SELECT *
                           FROM IDHKRH_NICE_FBEV  -- 해외채권 NICE
                          WHERE STND_DT = (
                                             SELECT MAX(STND_DT)
                                               FROM IDHKRH_NICE_FBEV
                                              WHERE STND_DT <= '$$STD_Y4MD'
                                          )
                      ) T2
                   ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
                WHERE 1=1
            ) DD 
)

, CP AS 
(
     SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      AS SECR_ITMS_CD
          , NVL(T2.AST_CLCD, T1.AST_CLCD)                                              AS ASSET_LIQ_CLCD
          , TRUNC(
            CASE WHEN NVL(T2.TXTN_CLCD, T1.TXTN_CLCD) = '2'                            -- 세후단가인 경우
                 THEN (  NVL(           DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
                            , NVL(      DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC )
                                 , NVL( DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
                                      , DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ) ) ) )
                       + NVL(           DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
                            , NVL(      DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC )
                                 , NVL( DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
                                      , DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ) ) ) ) ) * 0.5
                 ELSE (  NVL( DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ), T2.VLT_UPRC)
                       + NVL( DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ), T1.VLT_UPRC)   ) * 0.5
                 END
            , 2)                                                                       AS UPRC_DIRTY
          , TRUNC(
            CASE WHEN T1.TXTN_CLCD = '2'                                               -- 세후단가인 경우
                 THEN    NVL(           DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
                            ,           DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ) )
                 ELSE                   DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC )
                 END
            , 2)                                                                       AS UPRC_1
          , TRUNC(
            CASE WHEN T1.TXTN_CLCD = '2'                                               -- 세후단가인 경우
                 THEN    NVL(           DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
                            ,           DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ) )
                 ELSE                   DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC )
                 END
            , 2)                                                                       AS UPRC_2
          , TRUNC( (  NVL(DECODE(T1.UPD_DURA,  0, NULL, T1.UPD_DURA),  T2.UPD_DURA  )
                    + NVL(DECODE(T2.UPD_DURA,  0, NULL, T2.UPD_DURA),  T1.UPD_DURA  ) ) * 0.5
                                                                             , 4)      AS DURA_MODI
          , DECODE(T1.UPD_DURA,  0, NULL, T1.UPD_DURA)                                 AS DURA_1
          , DECODE(T2.UPD_DURA,  0, NULL, T2.UPD_DURA)                                 AS DURA_2
          , NVL(T2.APLZ_GRDE_NM, NVL(T2.ISSU_INT_GRDE_NM, NVL(T1.APLZ_GRDE_NM, T1.ISSU_INT_GRDE_NM)))
                                                                                       AS CRGR_NICE
          , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR
              WHERE CRGR_DMST = NVL(T2.APLZ_GRDE_NM, NVL(T2.ISSU_INT_GRDE_NM, NVL(T1.APLZ_GRDE_NM, T1.ISSU_INT_GRDE_NM))))
                                                                                       AS CRGR_KICS
       FROM (
               SELECT *
                 FROM IDHKRH_FN_CDEV    -- CP FN
                WHERE STND_DT = (
                                   SELECT MAX(STND_DT)
                                     FROM IDHKRH_FN_CDEV
                                    WHERE STND_DT <= '$$STD_Y4MD'
                                )
            ) T1
                 FULL OUTER JOIN
            (
               SELECT *
                 FROM IDHKRH_NICE_CDEV  -- CP NICE
                WHERE STND_DT = (
                                   SELECT MAX(STND_DT)
                                     FROM IDHKRH_NICE_CDEV
                                    WHERE STND_DT <= '$$STD_Y4MD'
                                )
            ) T2
         ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
      WHERE 1=1
)

, STOCK AS 
(
     SELECT STND_DT
          , SECR_ITMS_CD
          , ITMS_NM                                                                     AS CNPT_NM
          , MKT_CLCD
          , CLPC
          , CASE WHEN LENGTHB(SECR_CRED_GRDE_CD) <= 4                                            -- '취소'등 신용등급이 아닌 값은 제외
                 THEN SECR_CRED_GRDE_CD
                 ELSE NULL END                                                          AS CRGR_NICE
          , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T1.SECR_CRED_GRDE_CD)  -- 매핑_신용등급
                                                                                        AS CRGR_KICS
          , STOC_STND_PRC
          , DVD_RAT
          , STOC_CLSF_CD
          , CRNC_CD
       FROM IDHKRH_STOCK T1             -- KOSPI/KOSDAQ 주식데이터
      WHERE 1=1
        AND STND_DT = (
                         SELECT MAX(STND_DT)
                           FROM IDHKRH_STOCK
                          WHERE STND_DT <= '$$STD_Y4MD'
                      )
)

, T_MAIN AS
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , 'SECR_L_'||A.FUND_CD||A.PRNT_ISIN_CD||'_'||A.ISIN_CD||'_'||LPAD(A.LT_SEQ, 3, '0')    AS EXPO_ID                         /*  익스포저ID             */
        , A.FUND_CD                                                                            AS FUND_CD                         /*  펀드코드               */
        , NVL(A.LT_TPCD, 'Z')                                                                  AS PROD_TPCD                       /*  상품유형코드           */
        , A.PROD_TPNM                                                                          AS PROD_TPNM                       /*  상품유형명             */
     --    , NVL(K.KICS_TPCD, NVL(A.LT_DTLS_TPCD, 'ZZZ'))                           AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
     --    , NVL(K.KICS_TPNM, KICS.KICS_TPNM)                                       AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , NVL( NVL(A.LT_DTLS_TPCD, 'ZZZ'), K.KICS_TPCD)                             AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */ -- 24.05.25 LT 상품분류코드를 우선하여 분류하도록 수정
        , NVL(KICS.KICS_TPNM, K.KICS_TPNM)                                          AS KICS_PROD_TPNM                  /*  KICS상품유형명         */ -- 24.05.25 LT 상품분류코드를 우선하여 분류하도록 수정
        , A.ISIN_CD                                                                               AS ISIN_CD                         /*  종목코드               */
        , A.ISIN_NM                                                                               AS ISIN_NM                         /*  종목명                 */
        , A.ACCO_CLCD||'_'||A.ACCO_CD                                                     AS ACCO_CD                         /*  계정과목코드           */
        , ACCO_MST.ACCO_NM                                                                    AS ACCO_NM                         /*  계정과목명             */
        , CASE WHEN LENGTHB(TO_CHAR(A.PRNT_ISIN_NM)) < 100
               THEN TO_CHAR(A.PRNT_ISIN_NM)
               ELSE SUBSTRB(TO_CHAR(A.PRNT_ISIN_NM), 1, 96)
               END                                                                             AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , CASE WHEN A.IS_IRATE_ASSET = '1' THEN 'B' ELSE 'E' END                               AS INST_TPCD                       /*  인스트루먼트유형코드   */
        , CASE WHEN A.IS_IRATE_ASSET = '1'
--20230421 김재우 수정 외부채권발행정보만 사용으로 변경		
               THEN CASE WHEN B.BOND_INRS_PAY_MTCD = '01' THEN '1'                                    /*  01: 할인채(1)          */
                         WHEN B.BOND_INRS_PAY_MTCD = '07' THEN '2'                                          /*  07: 단리채(2)          */
                         WHEN B.BOND_INRS_PAY_MTCD = '02' THEN '3'                                          /*  02: 복리채(3)          */
                         WHEN B.BOND_INRS_PAY_MTCD = '03' THEN '4'                                          /*  03: 이표채(4)          */
--                         WHEN B.BOND_INRS_PAY_MTCD = '08' THEN '5'                                          /*  08: FRN(5)  2024.05.09 간접보유 변동금리채권 : 고정이표채 임시수정   */
                         WHEN B.BOND_INRS_PAY_MTCD = '04' THEN '6'                                          /*  04: 분할상환(거치변동) */
                         WHEN B.BOND_INRS_PAY_MTCD = '10' THEN '8'                                          /*  10: 만기일시(복+단, 8) */
                         ELSE '4' END        
--               THEN CASE WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '01' THEN '1'                                          /*  01: 할인채(1)          */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '07' THEN '2'                                          /*  07: 단리채(2)          */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '02' THEN '3'                                          /*  02: 복리채(3)          */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '03' THEN '4'                                          /*  03: 이표채(4)          */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '08' THEN '5'                                          /*  08: FRN(5)             */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '04' THEN '6'                                          /*  04: 분할상환(거치변동) */
--                         WHEN NVL(A.INRS_PAY_CLCD, B.BOND_INRS_PAY_MTCD) = '10' THEN '8'                                          /*  10: 만기일시(복+단, 8) */
--                         ELSE '4' END
               ELSE 'Z' END                                                                                                       /*  Z : 비금리             */
                                                                                               AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */
        /* 20210728 modified */                                                                                               
        , NVL(  NVL(A.ISSU_DATE, B.ISSU_DT)                                                                                       -- A.ISSU_DATE(수기내역 내 발행일)미사용(현재 채권발행정보로 모든내용이 매핑됨)                                                                                               
        --, NVL(  B.ISSU_DT                                                                                                         -- A.ISSU_DATE(수기내역 내 발행일)미사용(현재 채권발행정보로 모든내용이 매핑됨)
              , NVL ( CASE WHEN A.INST_TPCD IN ('1', '2', '3', '4') THEN B.ISSU_DT                                                -- 금리부자산 관련한 부문이므로 발행일이 크게 중요하지는 않음
                           WHEN A.INST_TPCD IN ('5', '6', '7'     ) THEN A.BASE_DATE
                           WHEN A.INST_TPCD IN ('8'               ) THEN F.ISSU_DT
                           ELSE A.BASE_DATE END , A.BASE_DATE) )
                                                                                               AS ISSU_DATE                       /*  발행일자               */
        , NVL(  
--                 CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN ('11230300', '11230500', '12600300')
--                CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN ('11230300', '11230500', '1Q070150','1R080300') -- 20230421 김재우 수정 IFRS9 계정변경(MMF)
                  CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M' ) -- 20240429 이선영 수정  : (MMF) 만기 6개월처리대상
                     THEN TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_DATE, 'YYYYMMDD'), 6) - 1, 'YYYYMMDD')
                     WHEN A.IS_IRATE_ASSET = '1' AND A.MATR_DATE IN ('20991231')
                     THEN NULL
                     /* 20210728 modified */
                     ELSE NVL(A.MATR_DATE, B.EXPR_DT) END                                                                         -- A.MATR_DATE가 존재하면 활용, NULL이면 발행정보에서 매핑함
                     --ELSE B.EXPR_DT END                                                                                           -- A.MATR_DATE가 존재하면 활용, NULL이면 발행정보에서 매핑함
              , CASE WHEN A.INST_TPCD IN ('1', '2', '3', '4')
                     THEN NVL(B.EXPR_DT, TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_DATE, 'YYYYMMDD'), 12) - 2, 'YYYYMMDD') )
                     WHEN A.INST_TPCD IN ('8'               )
                     THEN NVL(F.EXPR_DT, TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_DATE, 'YYYYMMDD'), 12) - 2, 'YYYYMMDD') )
                     ELSE TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_DATE, 'YYYYMMDD'), 12) - 2, 'YYYYMMDD') END )
                                                                                               AS MATR_DATE                       /*  만기일자               */
        , NVL(A.CRNY_CD, 'KRW')                                                                AS CRNY_CD                         /*  통화코드               */
        -- 20230421 김재우 수정 기존에는 액면이 NOT NULL 일 경우만 환율 적용.NULL인 경우 외화자산의 환율이 1로 들어옴
        -- 통화코드에 따라 실제 환율로 변경                   
        , NVL(A.FX_RATE, 1)                                                                    AS CRNY_FXRT                       /*  통화환율(기준일)       */
        /* 20210728 modified */
--        , CASE WHEN NVL(A.CRNY_CD, 'KRW') <> 'KRW' 
--               THEN CASE WHEN A.NOTL_AMT IS NOT NULL
--                         THEN NVL(A.FX_RATE, 1)
--                         ELSE 1 END
--               ELSE 1 END                                                                      AS CRNY_FXRT                       /*  통화환율(기준일)       */
        --, NVL(A.FX_RATE, 1)                                                                    AS CRNY_FXRT                       /*  통화환율(기준일)       */
-- 20230421 김재우 수정 액면금액이 전체적으로 이상하게 들어옴. KAP 계약후 정상적으로 입수 예정이므로, 액면이 존재하면 액면사용
-- 액면이 없을 경우 , 공정가치 금액을 환율로 나누어 통화액면으로 적용. 
-- 공정가치 금액을 액면으로 사용할 경우, 만기시 상환금액이 현재 공정가치 금액이므로, 내재스프레드가 실제 내재스프레드보다 크거나 작은 차이가 발생가능.
        ,  CASE WHEN A.NOTL_AMT IS NOT NULL THEN A.NOTL_AMT 
		            ELSE A.LT_BS_AMT / NVL(A.FX_RATE, 1) 
			           END                                                                             AS NOTL_AMT                        /*  이자계산기준원금       */                                 
        /* 20210728 modified */
--        , NVL(A.NOTL_AMT, A.LT_BS_AMT)                                                         AS NOTL_AMT                        /*  이자계산기준원금       */                                                                                                       
        --, A.LT_BS_AMT                                                                          AS NOTL_AMT                        /*  이자계산기준원금       */
-- 20230421 김재우 수정 액면금액이 전체적으로 이상하게 들어옴. KAP 계약후 정상적으로 입수 예정이므로, 액면이 존재하면 액면사용
-- 액면이 없을 경우 , 공정가치 금액을 환율로 나누어 통화액면으로 적용. 
-- 공정가치 금액을 액면으로 사용할 경우, 만기시 상환금액이 현재 공정가치 금액이므로, 내재스프레드가 실제 내재스프레드보다 크거나 작은 차이가 발생가능.
        ,  CASE WHEN A.NOTL_AMT IS NOT NULL THEN A.NOTL_AMT 
		            ELSE A.LT_BS_AMT / NVL(A.FX_RATE, 1) 
			           END                                                                           AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */                                 
--        , NVL(A.NOTL_AMT, A.LT_BS_AMT)                                                         AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */
        , A.LT_BS_AMT                                                                          AS BS_AMT                          /*  BS금액                 */
        , A.LT_BS_AMT                                                                          AS VLT_AMT                         /*  평가금액               */
        , TRUNC(
          CASE WHEN NVL(A.CRNY_CD, 'KRW') = 'KRW'
               THEN CASE WHEN A.LT_TPCD IN ('20', '__') THEN A.LT_BS_AMT  -- NVL(C.UPRC_DIRTY * A.NOTL_AMT / 10000, A.LT_BS_AMT)  -- 일반채권
                         WHEN A.LT_TPCD IN ('40')       THEN A.LT_BS_AMT                                                          -- 주식계열
                         ELSE A.LT_BS_AMT END
               ELSE A.LT_BS_AMT
               END
          , 3)                                                                                 AS FAIR_BS_AMT                     /*  공정가치B/S금액(외부)  */
        , NULL                                                                                 AS ACCR_AMT                        /*  미수수익               */
        , NULL                                                                                 AS UERN_AMT                        /*  선수수익               */
--        , CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN ('12600300')
--        , CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN ('1Q070150','1R080300') --20230421 김재우 수정 IFRS9 기준으로 변경(MMF)
          , CASE WHEN A.IS_IRATE_ASSET = '1' AND A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M' ) --2024.04.29 
               THEN L.IRATE
               ELSE NVL(B.FACE_ITRT, A.IRATE / 100) END                                        AS IRATE                           /*  금리                   */
        , NVL(B.INRS_PAY_CYCL_VAL * DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1), A.INT_PAY_CYC)   AS INT_PAY_CYC                     /*  이자지급/계산주기(월)  */
        , NULL                                                                                 AS INT_PROR_PAY_YN                 /*  이자선지급여부(1: 후취)*/
        , '1'                                                                                  AS IRATE_TPCD                      /*  금리유형코드(1:고정)   */
        , '1111111'                                                                            AS IRATE_CURVE_ID                  /*  금리커브ID             */
        , NULL                                                                                 AS IRATE_DTLS_TPCD                 /*  변동금리유형코드       */
        , NULL                                                                                 AS ADD_SPRD                        /*  가산스프레드           */
        , NULL                                                                                 AS IRATE_RPC_CYC                   /*  금리개정주기(월)       */
     --    , CASE WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('01', '05'      ) THEN  '1'                                              /*  ACT/365                */
     --           WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('02', '10', '12') THEN  '2'                                              /*  A30/360                */
     --           WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('04', '06', '08') THEN  '3'                                              /*  E30/360                */
     --           WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('00', '09'      ) THEN  '4'                                              /*  ACT/ACT                */
     --           WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('03'            ) THEN  '5'                                              /*  ACT/360                */
     --           ELSE '1' END                                                                                                       /*  DEFAULT: ACT/365       */
        , '1'                                                                                  AS DCB_CD                          /*  일수계산코드     2024.05.09 요건 수정      */
        , CASE WHEN A.IS_IRATE_ASSET = '1' THEN B.INRS_RCDT_DD_CLCD ELSE NULL END              AS CF_GEN_TPCD                     /*  현금흐름일생성구분     */
        , CASE WHEN A.IS_IRATE_ASSET = '1'
                    AND MOD( MONTHS_BETWEEN(LAST_DAY(TO_DATE(B.EXPR_DT, 'YYYYMMDD')),
                                            LAST_DAY(TO_DATE(B.FST_CUPN_CLCL_DT, 'YYYYMMDD')))
                             , TO_NUMBER(B.INRS_PAY_CYCL_VAL * DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1))) = 0
               THEN SUBSTR(B.FST_CUPN_CLCL_DT, 1, 8)
               ELSE NULL END
                                                                                               AS FRST_INT_DATE                   /*  최초이자기일           */
        , NULL                                                                                 AS GRACE_END_DATE                  /*  거치기간종료일자       */
        , TO_NUMBER(SUBSTR(B.PRCP_GRPE_TERM_CD, 1, 2)) * 12 + TO_NUMBER(SUBSTR(B.PRCP_GRPE_TERM_CD, 3, 2))
                                                                                               AS GRACE_TERM                      /*  거치기간(월)           */
        , NULL                                                                                 AS IRATE_CAP                       /*  적용금리상한           */
        , NULL                                                                                 AS IRATE_FLO                       /*  적용금리하한           */
        , NULL                                                                                 AS AMORT_TERM                      /*  원금분할상환주기(월)   */
        , NULL                                                                                 AS AMORT_AMT                       /*  분할상환금액           */
        , B.EXPR_RPAY_RAT                                                                      AS MATR_PRMU                       /*  만기상환율             */
        , B.ALDY_SALE_DT                                                                       AS PSLE_DATE                       /*  선매출일자             */
        , B.ALDY_SALE_INRS_CLCD                                                                AS PSLE_INT_TPCD                   /*  선매출이자유형코드     */
        -- 20230421 김재우 수정. 수기 입수 되는 데이터 사용
        , A.CORP_NO                                                                            AS CNPT_ID                         /*  거래상대방ID           */
--        , A.ISSU_INT_CUST_NO                                                                   AS CNPT_ID                         /*  거래상대방ID           */
        -- 20230421 김재우 수정. 수기 입수 되는 데이터 사용
        , NVL(A.CNPT_NM, NVL(B.ISSU_CO_NM, G.CNPT_NM))                                         AS CNPT_NM                         /*  거래상대방명           */
--        , NVL(A.CUST_NM, NVL(B.ISSU_CO_NM, G.CNPT_NM))                                         AS CNPT_NM                         /*  거래상대방명           */
        -- 20230421 김재우 수정. 수기 입수 되는 데이터 사용
        , NVL(A.CORP_NO, B.ISSU_CO_CORP_CD)                                                AS CORP_NO                         /*  법인등록번호           */
--        , NVL(A.CORP_REG_NO, B.ISSU_CO_CORP_CD)                                                AS CORP_NO                         /*  법인등록번호           */
--20230421 김재우 수정 신용등급 NULL 처리변경
        , DECODE(B.ITMS_CLCD, '1', A.CRGR_KIS
--        , DECODE(B.ITMS_CLCD, '1', NULL
                            , '2', NVL(D.CRGR_KIS , A.CRGR_KIS)    , A.CRGR_KIS)               AS CRGR_KIS                        /*  신용등급(한신평)       */
        , DECODE(B.ITMS_CLCD, '1', NVL(C.CRGR_NICE, A.CRGR_NICE)
                            , '2', NVL(D.CRGR_NICE, A.CRGR_NICE)
                                 , NVL(E.CRGR_NICE, NVL(G.CRGR_NICE, A.CRGR_NICE)))             AS CRGR_NICE                       /*  신용등급(한신정)       */
--20230421 김재우 수정 신용등급 NULL 처리변경
        , DECODE(B.ITMS_CLCD, '1', A.CRGR_KR
--        , DECODE(B.ITMS_CLCD, '1', NULL
                            , '2', NVL(D.CRGR_KR  , A.CRGR_KR)     , A.CRGR_KR)                AS CRGR_KR                         /*  신용등급(한기평)       */
--20230421 김재우 수정 평가등급으로 변경(해외신용등급칼럼 추가)
        , A.CRGR_VLT                                                                             AS CRGR_VLT                        /*  신용등급(평가)         */  
--        , NVL(A.CRGR_DMST, A.CRGR_OVSE)                                                        AS CRGR_VLT                        /*  신용등급(평가)         */  -- 정보성으로  해외등급 매핑하였음
        , NVL(K.CRGR_KICS,
          DECODE(B.ITMS_CLCD, '1', NVL(C.CRGR_KICS, A.CRGR_KICS)
                            , '2', NVL(DECODE(D.CRGR_KICS, '0', NULL, D.CRGR_KICS), A.CRGR_KICS)
--                            , CASE WHEN A.ACCO_CD IN ('11230300', '11230500', '12600300') THEN '4'                                -- CMA/MMF/MMDA 계열은 4등급처리(2.0기준서 3.3.3.3.5)
--                            , CASE WHEN A.ACCO_CD IN ('11230300', '11230500','1Q070150','1R080300') THEN '4' --20230421 김재우 수정 IFRS9 기준으로 변경(MMF) CMA/MMF/MMDA 계열은 4등급처리(2.0기준서 3.3.3.3.5)
                              , CASE WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'MAT006M' ) THEN '4'-- 20240205 이선영  수정 만기 6개월처리대상 CMA/MMF/MMDA 계열은 4등급처
                                   ELSE NVL(E.CRGR_KICS, NVL(G.CRGR_KICS, A.CRGR_KICS)) END ) )                                  -- CRGR_KICS2는 수기입력된 CRGR_KICS가 NULL인 경우, 보유계좌와 동일한 로직으로 매핑한 결과임(K: 최우선 속성(PROD_SECR), E: CP, G: 주식)
                                                                                               AS CRGR_KICS                       /*  신용등급(KICS)         */  -- ITMS_CLCD: ISIN(KR코드)기반 매핑, 그외: 법인등록번호 기반 매핑
        , NULL                                                                                 AS CONT_PRC                        /*  체결가격(환율/지수 등) */
        , NULL                                                                                 AS UNDL_EXEC_PRC                   /*  기초자산행사가격       */
        , NULL                                                                                 AS UNDL_SPOT_PRC                   /*  기초자산현재가격       */
        , '0'                                                                                  AS OPT_EMB_TPCD                    /*  내재옵션구분           */
        , NULL                                                                                 AS OPT_STR_DATE                    /*  옵션행사시작일자       */
        , NULL                                                                                 AS OPT_END_DATE                    /*  옵션행사시작일자       */
        , DECODE(B.ITMS_CLCD, '1', C.UPRC_DIRTY, '2', D.UPRC_CLEAN, E.UPRC_DIRTY)              AS EXT_UPRC                        /*  외부평가단가           */
        , CASE WHEN A.LT_TPCD IN ('20', '10') THEN B.STND_PRCP
               ELSE 1 END                                                                      AS EXT_UPRC_UNIT                   /*  외부평가단가단위       */
        , DECODE(B.ITMS_CLCD, '1', C.DURA_EFFE, '2', D.DURA_EFFE, E.DURA_MODI)                 AS EXT_DURA                        /*  외부평가듀레이션       */  -- ITMS_CLCD = 1(원화), 2(외화)
        , DECODE(B.ITMS_CLCD, '1', C.UPRC_1, '2', D.UPRC_1, E.UPRC_1)                          AS EXT_UPRC_1                      /*  외부평가단가1          */
        , DECODE(B.ITMS_CLCD, '1', C.UPRC_2, '2', D.UPRC_2, E.UPRC_2)                          AS EXT_UPRC_2                      /*  외부평가단가2          */
        , DECODE(B.ITMS_CLCD, '1', C.DURA_1, '2', D.DURA_1, E.DURA_1)                          AS EXT_DURA_1                      /*  외부평가듀레이션1      */
        , DECODE(B.ITMS_CLCD, '1', C.DURA_2, '2', D.DURA_2, E.DURA_2)                          AS EXT_DURA_2                      /*  외부평가듀레이션2      */
        , NULL                                                                                 AS STOC_TPCD                       /*  주식유형코드           */
        , NVL(  DECODE(A.PRF_STOC_TPCD, 'Y', 'Y', 'N', 'N', NULL)
              , CASE WHEN SUBSTR(G.SECR_ITMS_CD, 1, 3) IN ('KR7', 'KRY', 'KRU') THEN DECODE(SUBSTR(G.SECR_ITMS_CD, 9, 1), '0', 'N', 'Y')
                     WHEN SUBSTR(A.ISIN_CD     , 1, 3) IN ('KR7', 'KRY', 'KRU') THEN DECODE(SUBSTR(A.ISIN_CD     , 9, 1), '0', 'N', 'Y')
                     ELSE 'N' END )                                                            AS PRF_STOC_YN                     /*  우선주여부             */
        , NVL(A.BOND_RANK_CLCD, '01_N')                                                        AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , DECODE(G.MKT_CLCD, '1', 'Y', '2', 'Y', 'N')                                          AS STOC_LIST_CLCD                  /*  주식상장구분코드       */ -- Y/N으로 처리(19.10.21)
        , NVL(A.DMST_OVSE_CLCD, 'KR')                                                          AS CNTY_CD                         /*  국가코드               */
        , NVL(B.AST_LQTN_CLCD, E.ASSET_LIQ_CLCD)                                               AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */ -- 1: ABS, 2: MBS
        , DECODE(SUBSTR(A.FUND_CD, 1, 1), 'G', '902472', '902476')                             AS DEPT_CD                         /*  부서코드               */
        , 'KRBH109BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
        , A.CRGR_SNP AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
        , A.CRGR_MOODYS AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
        , A.CRGR_FITCH AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
        , NULL        AS IND_L_CLSF_CD                        /*산업대분류코드*/
        , NULL        AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
        , NULL        AS IND_CLSF_CD                          /* 산업분류코드*/
        , A.LVG_RATIO
     FROM LT A
        , BD_INFO B
        , BOND C -- 국내채권 평가결과
        , FB   D -- 해외채권 평가결과
        , CP   E -- CP 평가결과
        , (
             SELECT SECR_GDS_CD                                                                AS SECR_ITMS_CD
                  , SECR_PDNM
                  , ISSU_DT
                  , EXPR_DT
                  , NVL(BNFC_STND_PRC, 0)                                                      AS UPRC
               FROM IKRASH_BNFC_ISSU_INFO T1    -- 수익증권발행정보 (LOOK THROUGH 대상이 온전하게 들어온다면 논리상 참조할 필요는 없음)
              WHERE 1=1
                AND STND_DT = (
                                 SELECT MAX(STND_DT)
                                   FROM IKRASH_BNFC_ISSU_INFO
                                  WHERE STND_DT <= '$$STD_Y4MD'
                              )
                AND CCLT_CLCD = '2'
          ) F
        , STOCK G
--        , (
--             SELECT STND_ITEM_C                AS SECR_ITMS_CD
--                  , ROUND(APLY_UPR, 3)         AS APLY_UPR
--               FROM IDHKRH_WRNTPR T1            -- 워런트시세
--              WHERE 1=1
--                AND BASE_DATE = (
--                                   SELECT MAX(BASE_DATE)
--                                     FROM IDHKRH_WRNTPR
--                                    WHERE BASE_DATE <= '$$STD_Y4MD'
--                                )
--                AND OPE_CD = 'NIC'              -- 2019년10월 현재 'NIC'이외의 데이터는 존재치 않음
--          ) H
        , (
             SELECT PROD_KEY                                          AS SECR_ITMS_CD
                  , KICS_PROD_TPCD                                    AS KICS_TPCD
                  , KICS_PROD_TPNM                                    AS KICS_TPNM
                  , CRGR_KICS                                         AS CRGR_KICS
               FROM IKRUSH_PROD_SECR T1         -- 수기_상품(유가증권)
              WHERE BASE_DATE = (
                                   SELECT MAX(BASE_DATE)
                                     FROM IKRUSH_PROD_SECR
                                    --WHERE BASE_DATE <= '$$STD_Y4MD'
                                    WHERE BASE_DATE = '$$STD_Y4MD' -- 20240528 IKRUSH_PROD_SECR 기준월에 해당되는 수기_상품(유가증권)정보만 사용하도록 수정  
                                      AND PROD_KEY = T1.PROD_KEY
                                )
          ) K
        , (
             SELECT NVL(MAX(SCEN_VAL), 0)                             AS IRATE
               FROM Q_CM_SCEN_RSLT              -- QCM 시나리오 OUTPUT(MMF 요건을 위한 공사채(AAA) 등급 적용할인율 확보목적)
              WHERE 1=1                         -- PB_AAA 6M YIELD:0.0199363876 (cf. 3M: 0.0194246524, 12M: 0.0205973105 @ 2018-12-31)
                AND TO_CHAR(AS_OF_DATE, 'YYYYMMDD') = '$$STD_Y4MD'
                AND VOL_FACTOR_ID = 'PB_AAA'
                AND SCEN_NO  = '1'
                AND MAT_TERM = '6'
                AND MAT_TERM_UNIT = 'M'
          ) L
        , (
             SELECT CD                                                AS CLSF1
                  , CD2                                               AS CLSF2
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_CD'     -- KICS상품분류코드
          ) KICS
        , ACCO_MST

    WHERE 1=1
      AND A.ISIN_CD  = B.SECR_ITMS_CD(+)
      AND A.ISIN_CD  = C.SECR_ITMS_CD(+)
      AND A.ISIN_CD  = D.SECR_ITMS_CD(+)
      AND A.ISIN_CD  = E.SECR_ITMS_CD(+)
      AND A.ISIN_CD  = F.SECR_ITMS_CD(+)
      --AND A.ISIN_CD  = G.SECR_ITMS_CD(+)
      AND CASE WHEN SUBSTR(A.ISIN_CD, 1, 3) IN ('KRY', 'KRU')                   -- KRY, KRU 등으로 시작하는 주식 KR코드를 매칭하기 위하여 좌측과 같은 조건을 설정함
               THEN 'KR7'
               ELSE SUBSTR(A.ISIN_CD, 1, 3) END
          ||SUBSTR(A.ISIN_CD, 4, 8) = SUBSTR(G.SECR_ITMS_CD(+), 1, 11)          -- KR코드의 12째자리는 검증KEY이므로 1~11째자리가 동일하면 같아야 정상이므로 DUP가능성은 없음
--      AND A.ISIN_CD  = H.SECR_ITMS_CD(+)
      AND A.ISIN_CD  = K.SECR_ITMS_CD(+)
      AND A.LT_TPCD  = KICS.CLSF1(+)
      AND A.LT_DTLS_TPCD = KICS.CLSF2(+)
      AND A.ACCO_CD = ACCO_MST.ACCO_CD(+)
      AND NVL(A.LT_TPCD, 'Z') NOT IN ('3')    -- 대출채권계열제외
      AND A.LT_DTLS_TPCD NOT IN ('730','731','733','734') --별도 프로그램에서 처리 

)
SELECT * FROM T_MAIN