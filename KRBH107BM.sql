/* 수정사항
1. IBD_ZTCFMPEBAPST : GL_ACCOUNT 8자리 -> 10자리 변경되는 경우 확인 20240430 
2. 기업대출 공정가치 외부 입수 대상 : 2025-01-31 추가 
      EXPO_ID : 'LOAN_O2'||A.PRNT_ISIN_CD (IKRUSH_LT_GRP1 에 LT_SEQ =0 조건으로 적재한 대상)
      ACCO_CD : A.FUND_CD,'G000','1','2')||'_'||A.ACCO_CD
      ACCO_NM : (SELECT ACCO_NM FROM ACCO_MST WHERE  ACCO_CD  =A.ACCO_CD) 
*/

--INSERT INTO Q_IC_ASSET_SECR 
WITH ACCO_MST AS 
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
 , T AS (
SELECT /* SQL-ID : KRBH107BM */ A.BASE_DATE                  AS BASE_DATE                /*  기준일자           */
   --   , 'LOAN_L_'||A.FUND_CD||A.PRNT_ISIN_CD||'_'||A.ISIN_CD||'_'||LPAD(A.LT_SEQ, 3, '0')
     , CASE WHEN LT_SEQ =0 THEN 'LOAN_O2'||A.PRNT_ISIN_CD
                           ELSE 'LOAN_L_'||A.FUND_CD||A.PRNT_ISIN_CD||'_'||A.ISIN_CD||'_'||LPAD(A.LT_SEQ, 3, '0')
                           END 
                                    AS EXPO_ID                  /*  익스포저ID         */
     , A.FUND_CD                    AS FUND_CD                  /*  펀드코드           */
     , A.LT_TPCD                    AS LT_TPCD                  /*  상품유형코드       */
     , A.LT_TPNM                    AS LT_TPNM                  /*  상품유형명         */
     , A.LT_DTLS_TPCD               AS KICS_PROD_TPCD           /*  KICS상품유형코드    */
     , A.LT_DTLS_TPNM               AS KICS_PROD_TPNM           /*  KICS상품유형명     */
--     , A.PRNT_ISIN_CD               AS ISIN_CD                  /*  종목코드           */
     , CASE WHEN A.LT_SEQ = 0 THEN A.ISIN_CD  ELSE A.PRNT_ISIN_CD  AS ISIN_CD /*  종목코드 2025-02-06 수정          */
     , A.PRNT_ISIN_NM               AS ISIN_NM                  /*  종목명             */
   --   , B.ACCO_CD                    AS ACCO_CD                  /*  계정과목코드   펀드구분 ||계정코드    */
   --   , B.ACCO_NM                    AS ACCO_NM                  /*  계정과목명         */
     , NVL(B.ACCO_CD,  DECODE(A.FUND_CD,'G000','1','2')||'_'||A.ACCO_CD)                    AS ACCO_CD                  /*  계정과목코드   펀드구분 ||계정코드    */
     , NVL(B.ACCO_NM, (SELECT   ACCO_NM FROM ACCO_MST WHERE  ACCO_CD  =A.ACCO_CD))          AS ACCO_NM                  /*  계정과목명         */

     , NULL                         AS CONT_ID                  /*  계약ID             */
     , 'B'                          AS INST_TPCD                /* 인스트루먼트유형코드  */
     , '4'                          AS INST_DTLS_TPCD           /* 인스트루먼트유형상세유형코드 */
     , DECODE(A.ISSU_DATE,NULL,A.BASE_DATE,A.ISSU_DATE) AS ISSU_DATE    /*  발행일자   */ -- 발행일이 없는 경우
     , CASE WHEN A.MATR_DATE = '99991231'   THEN TO_CHAR(TO_DATE(A.BASE_DATE, 'YYYYMMDD') + 364 ,'YYYYMMDD')
            WHEN A.MATR_DATE <= A.BASE_DATE THEN TO_CHAR(TO_DATE(A.BASE_DATE ,'YYYYMMDD') + 364 ,'YYYYMMDD')
            WHEN A.MATR_DATE = '20991231'   THEN TO_CHAR(TO_DATE(A.BASE_DATE, 'YYYYMMDD') + 364 ,'YYYYMMDD')
            ELSE NVL(A.MATR_DATE, TO_CHAR(TO_DATE(A.BASE_DATE, 'YYYYMMDD') + 364 ,'YYYYMMDD')) END
                                    AS MATR_DATE                /*  만기일자           */
     , NVL(A.CRNY_CD,'KRW')         AS CRNY_CD                  /*  통화코드           */
      , 1                            AS CRNY_FXRT                /*  통화환율           */ --수익증권 테이블에 통화환율 추가필요
   --  , NVL(FX.FX_RATE, 1)              AS CRNY_FXRT                /*  통화환율           */ --2024.05.23 이선영 수정 환율정보 가져오기 
     , A.NOTL_AMT                   AS NOTL_AMT                 /*  이자계산기준원금   */
     , NULL                         AS NOTL_AMT_ORG             /*  최초이자계산기준원금  */
     , A.LT_BS_AMT                  AS BS_AMT                   /*  BS금액             */
     , A.LT_BS_AMT                         AS VLT_AMT                  /*  평가금액           */
     , A.LT_BS_AMT                         AS FAIR_BS_AMT              /*  공정가치BS금액     */
     , NULL                         AS ACCR_AMT                 /*  미수수익금액       */
     , NULL                         AS UERN_AMT                 /*  선수수익금액       */
     , NVL(A.IRATE, 0) / 100        AS IRATE                    /*  금리               */
     , NVL(A.INT_PAY_CYC, '12')     AS INT_PAY_CYC              /*  이자지급주기       */
     , 'N'                          AS INT_PROR_PAY_YN          /*  이자선지급여부     */  --후취
--     , A.IRATE_TPCD                 AS IRATE_TPCD               /*  금리유형코드       */
     , '1'                          AS IRATE_TPCD                /* 금리유형코드*/  
     , '1111111'                         AS IRATE_CURVE_ID           /*  금리커브ID         */
     , NULL                         AS IRATE_DTLS_TPCD          /*  금리상세유형코드   */
     , A.ADD_SPRD / 100             AS ADD_SPRD                 /*  가산금리           */
     , A.IRATE_RPC_CYC              AS IRATE_RPC_CYC            /*  금리개정주기       */
     , '1'                          AS DCB_CD  -- ACT/365       /*  일수계산코드       */
     , NULL                         AS CF_GEN_TPCD              /*  현금흐름일생성구분 */
     , NULL                         AS FRST_INT_DATE            /*  최초이자기일       */
        , NULL                                                                                 AS GRACE_END_DATE                  /*  거치기간종료일자       */
        , NULL                                                                                 AS GRACE_TERM                      /*  거치기간(월)           */
        , NULL                                                                                 AS IRATE_CAP                       /*  적용금리상한           */
        , NULL                                                                                 AS IRATE_FLO                       /*  적용금리하한           */
        , NULL                                                                                 AS AMORT_TERM                      /*  원금분할상환주기(월)   */
        , NULL                                                                                 AS AMORT_AMT                       /*  분할상환금액           */
        , 1                                                                      AS MATR_PRMU                       /*  만기상환율             */  --만기상환율 1처리(만기일시)
        , NULL AS PSLE_DATE                       /*  선매출일자             */   --NULL 처리
        , NULL                                                                 AS PSLE_INT_TPCD                   /*  선매출이자유형코드     */  --NULL처리
     , A.CORP_NO                    AS CNPT_ID                  /*  거래상대방ID       */
     , A.CNPT_NM                    AS CNPT_NM                  /*  거래상대방명       */
     , A.CORP_NO                    AS CORP_NO                  /*  법인번호           */
     , DECODE(A.VLT_CORP_TPCD,'1',A.CRGR_DMST,NULL)                         AS CRGR_KIS                 /*  신용등급(한신평)   */
     , DECODE(A.VLT_CORP_TPCD,'2',A.CRGR_DMST,NULL)                         AS CRGR_NICE                /*  신용등급(한신정)   */
     , DECODE(A.VLT_CORP_TPCD,'3',A.CRGR_DMST,NULL)                         AS CRGR_KR                  /*  신용등급(한기평)   */
     , A.CRGR_VLT                   AS CRGR_VLT                 /*  신용등급(평가)     */
     , A.CRGR_KICS                  AS CRGR_KICS                /*  신용등급(KICS)     */
        , NULL                                                                                 AS CONT_PRC                        /*  체결가격(환율/지수 등) */
        , NULL                                                                                 AS UNDL_EXEC_PRC                   /*  기초자산행사가격       */
        , NULL                                                                                 AS UNDL_SPOT_PRC                   /*  기초자산현재가격       */
        , '0'                                                                                  AS OPT_EMB_TPCD                    /*  내재옵션구분           */
        , NULL                                                                                 AS OPT_STR_DATE                    /*  옵션행사시작일자       */
        , NULL                                                                                 AS OPT_END_DATE                    /*  옵션행사시작일자       */
        , NULL            AS EXT_UPRC                        /*  외부평가단가           */   --NULL처리
        , NULL                                                                     AS EXT_UPRC_UNIT                   /*  외부평가단가단위       */    --NULL처리 
        , NULL                 AS EXT_DURA                        /*  외부평가듀레이션       */   --NULL처리 
        , NULL                          AS EXT_UPRC_1                      /*  외부평가단가1          */   --NULL처리 
        , NULL                          AS EXT_UPRC_2                      /*  외부평가단가2          */   --NULL처리 
        , NULL                          AS EXT_DURA_1                      /*  외부평가듀레이션1      */   --NULL처리 
        , NULL                          AS EXT_DURA_2                      /*  외부평가듀레이션2      */   --NULL처리 
        , NULL                                                                                 AS STOC_TPCD                       /*  주식유형코드           */
        , 'N'                                                                                       AS PRF_STOC_YN                     /*  우선주여부             */
        , '01_N'                                                                                  AS BOND_RANK_CLCD                  /*  채권순위구분코드       */
        , 'N'                                          AS STOC_LIST_CLCD                  /*  주식상장구분코드       */ -- Y/N으로 처리(19.10.21)
        , NVL(SUBSTR(A.CRNY_CD,1,2), 'KR')                                                          AS CNTY_CD                         /*  국가코드               */
        , NULL                                               AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */ -- 1: ABS, 2: MBS
        , NULL                             AS DEPT_CD                         /*  부서코드               */  --NULL처리 
        , 'KRBH107BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
        , DECODE(A.VLT_CORP_TPCD,'4',A.CRGR_OVSE,NULL) AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
        , DECODE(A.VLT_CORP_TPCD,'5',A.CRGR_OVSE,NULL) AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
        , DECODE(A.VLT_CORP_TPCD,'6',A.CRGR_OVSE,NULL) AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
        , NULL          AS IND_L_CLSF_CD                        /*산업대분류코드*/
        , NULL          AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
        , NULL          AS IND_CLSF_CD                          /* 산업분류코드*/
        , A.LVG_RATIO
    
  FROM IKRUSH_LT_TOTAL A
     , (
          SELECT B1.FUND_CD
                   , B1.ISIN_CD
                   , B1.ACCO_CD --펀드구분 ||계정코드 
                   , B2.ACCO_NM
                FROM (
                    SELECT PORTFOLIO                           AS FUND_CD
                         , ISIN                                         AS ISIN_CD
                         , MAX(CASE WHEN PORTFOLIO = 'G000' THEN '1' ELSE '2' END||'_'||GL_ACCOUNT) AS ACCO_CD  -- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수
                         , MAX(GL_ACCOUNT) AS ACCO_CD_ORI  -- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수
                      FROM  IBD_ZTCFMPEBAPST
                     WHERE GRDAT  = '$$STD_Y4MD'
                     GROUP BY PORTFOLIO, ISIN
                  ) B1
                , ACCO_MST B2
                  WHERE 1=1
                  AND B1.ACCO_CD_ORI = B2.ACCO_CD(+)
       ) B
--     , (
--          SELECT FUND_CD
--            FROM IKRUSH_FUND_CD
--           WHERE FUND_CLCD = '1'
--       ) C
/*               ,(
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
*/
 WHERE 1=1
   AND A.BASE_DATE = '$$STD_Y4MD'
   AND A.LT_TPCD = '3'                                      -- 대출채권 한정
--   AND A.FUND_CD = B.FUND_CD(+)
   AND DECODE(SUBSTR(A.FUND_CD,1,1),'G','G000',A.FUND_CD)= B.FUND_CD(+)
--   AND A.FUND_CD = C.FUND_CD(+)
   --AND A.PRNT_ISIN_CD = B.ISIN_CD(+)
   and  (CASE WHEN A.LT_SEQ = 0 THEN A.ISIN_CD ELSE A.PRNT_ISIN_CD END) = B.ISIN_CD (+) -- 2025-02-06 수정
--    AND NVL(A.CRNY_CD, 'KRW') = FX.CRNC_CD(+)
   )
 SELECT * 
 FROM T