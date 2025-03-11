/*
수정사항 
 - 계정코드를 매핑하는 SUB쿼리 결과를  WITH 문 처리  
 
 20240503 
 - 위험경감 불인정대상 처리 (직접보유자산 중 위험회피 회계 항목이 아닌 당기손익 공정가치 인식 금융자산으로 입수된 외환파생상품의 KICS상품코드 매핑 예외처리)
 - ACCO_EXCPTN  : 734 외환파생 (위험경감불인정) 1125230020 당기손익공정가치측정금융자산-파생상품_통화관련

20240509 
- REC_DCB_CD = '1'
- PAY_DCB_CD = '1'
*/

WITH /* KRBH103BM */   ACCO_MST AS 
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
 , ACCO_EXCPTN AS ( /*계정코드 예외처리 대상*/
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRBH103BM'
)
--SELECT * FROM ACCO_EXCPTN ;
, T_CUST AS 
(
    SELECT CORP_NO
            , KS_CRGR_DMST                                      AS CRGR_KIS
            , NC_CRGR_DMST                                      AS CRGR_NICE
            , KR_CRGR_DMST                                      AS CRGR_KR
            --, KS_CRGR_KICS, NC_CRGR_KICS, KR_CRGR_KICS
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
,FIDE AS (
SELECT /* KRBH103 */
          A.GRDAT                                                                            AS BASE_DATE                       /*  기준일자               */
        , 'FIDE_O_'||A.POSITION_ID                                                    AS EXPO_ID                         /*  익스포저ID             */
        , A.PORTFOLIO                                                                      AS FUND_CD                         /*  펀드코드               */
        , A.PRODUCT_TYPE                                                                AS PROD_TPCD                       /*  상품유형코드           */
        , KICS.PROD_TPNM                                                                AS PROD_TPNM                       /*  상품유형명             */  -- 우선 NULL 처리
        
        -- 20240503 이선영 K-ICS 상품코드 매핑기준 수정 (위험경감불인정대상) => TODO : 예외처리 대상 계정코드 확인 필요 
        , CASE WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = '734')  
                  THEN '734' 
                  ELSE NVL(KICS.KICS_TPCD, 'ZZZ')  END                           AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
        , KICS.KICS_TPNM                                                                 AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , A.ISIN                                                                               AS ISIN_CD                         /*  종목코드               */
        , A.SECURITY_ID_T                                                                AS ISIN_NM                         /*  종목명                 */
        , A.FNDS_ACCO_CLCD||'_'||A.ACCO_CD                                     AS ACCO_CD                         /*  계정과목코드           */  
        , ACCO_MST.ACCO_NM                                                           AS ACCO_NM                         /*  계정과목명             */
        , A.DEAL_NUMBER                                                                  AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , 'D'                                                                                  AS INST_TPCD       
        , CASE WHEN A.ASSETCODE = 'FX' THEN  '1'  -- FX                                                                  /*  FX      FORWARD        */
               WHEN A.ASSETCODE = 'SW' THEN  '2'  -- 스왑                                                                  /*  CRS, IRS         */
               WHEN A.ASSETCODE = 'ET' THEN  '7'  -- 채권선도                                                              /*  BOND FORWARD        */
               ELSE 'Z' END                                                                  AS INST_DTLS_TPCD 
        , CASE WHEN A.ASSETCODE = 'ET' THEN 'L'
               ELSE NULL
                END                                                                            AS POTN_TPCD                       /*  포지션구분코드(콜/풋)  */
        , CASE WHEN A.DVTRAB <> '00000000' THEN A.DVTRAB
		     WHEN A.BUY_DATE <> '00000000' THEN A.BUY_DATE
		     ELSE NULL 
			END AS ISSU_DATE                       /*  발행일자               */
        , A.EDDT                                                                            AS MATR_DATE                       /*  만기일자               */
        , CASE WHEN A.ASSETCODE IN ('SW', 'ET')
                    AND MOD( MONTHS_BETWEEN(LAST_DAY(TO_DATE(F.CLOS_DT, 'YYYYMMDD')),
                                            LAST_DAY(TO_DATE(F.FST_CUPN_RCDT_DT, 'YYYYMMDD')))
                             , TO_NUMBER(F.INRS_PAY_CYCL_VAL * DECODE(F.INRS_PAY_CYCL_CLCD, '3', 12, 1))) = 0
               THEN SUBSTR(F.FST_CUPN_RCDT_DT, 1, 8)
               ELSE NULL END
                                                                                               AS FRST_INT_DATE                   /*  최초이자기일           */
        , CASE WHEN D.CLCD = 'IRS' THEN 'N' ELSE 'Y' END                                       AS PRIN_EXCG_YN                    /*  원금교환여부           */
        , CASE WHEN A.ASSETCODE IN ('ET')
                  THEN ROUND((TO_DATE(F.CLOS_DT,'YYYYMMDD') - TO_DATE('$$STD_Y4MD','YYYYMMDD') ) / 365)

               ELSE NULL 
                END                                                                            AS KTB_MAT_YEAR                    /*  KTB선물약정만기(년)    */
		-- 계약금액(수취), 계약금액(지급) 사용할지 검토 필요, 통화코드도 통화키_수취, 통화키_지급 사용할지 
        , CASE WHEN A.ASSETCODE IN ('FX') THEN E.RCVG_CRNC_CD
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN F.CRNC_CD
               ELSE A.WAERS_IN END                                                                  AS REC_CRNY_CD                     /*  수취LEG통화코드        */
        , CASE WHEN A.ASSETCODE IN ('FX')THEN E.RCVG_FX_RATE
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN F.RCVG_FX_RATE
               ELSE 1 END                                                                      AS REC_CRNY_FXRT                   /*  수취LEG통화환율        */
        , CASE WHEN A.ASSETCODE IN ('FX') THEN NVL(E.RCVG_OTBL_PRCP, 0)
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN NVL(F.EXPR_PRCP_EXCG_AMT, NVL(F.OTBL_PRCP, 0))
               ELSE 0 END
                                                                                               AS REC_NOTL_AMT                    /*  수취LEG기준원금        */
        , CASE WHEN A.ASSETCODE IN ('FX') THEN E.PAY_CRNC_CD
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN NVL(G.CRNC_CD, NVL(A.WAERS_OUT, 'KRW'))
               ELSE 'KRW' END                                                                  AS PAY_CRNY_CD                     /*  지급LEG통화코드        */
        , CASE WHEN A.ASSETCODE IN ('FX') THEN E.PAY_FX_RATE
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN G.PAY_FX_RATE
               ELSE 1 END                                                                      AS PAY_CRNY_FXRT  	
        , CASE WHEN A.ASSETCODE IN ('FX') THEN NVL(E.PAY_OTBL_PRCP, 0)
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN NVL(G.EXPR_PRCP_EXCG_AMT, NVL(G.OTBL_PRCP, 0))
               ELSE 0 END
                                                                                               AS PAY_NOTL_AMT      			
        , A.BOOK_VAL_AMT                                                                           AS BS_AMT                          /*  장부금액               */
        , A.BOOK_VAL_AMT                                                                           AS VLT_AMT                         /*  BS금액                 */
        , D.CVA_APLZ_VLT_AMT                                                                   AS FAIR_BS_AMT                     /*  공정가치평가결과(외부) */ -- [REC_D - PAY_D] + CVA(대부분 음수) = CVA_APLZ_VLT_AMT --> CLEAN이 아님...
        , D.CVA_ALLO_BOTH_RISK_AMT                                                             AS CVA                             /*  CVA                    */
        , A.ACC_INT_VAL_AMT                                 AS ACCR_AMT                        /*  미수수익               */
        , A.PRPF_DMST                                                  AS UERN_AMT                        /*  선수수익               */
        , NVL(DECODE(F.FIX_FLT_CLCD, 2, F.FIX_FLT_ROIT, F.FIX_FLT_ROIT), 0)                    AS REC_IRATE                       /*  수취LEG금리            */
        , F.INRS_PAY_CYCL_VAL * DECODE(F.INRS_PAY_CYCL_CLCD, '3', 12, 1)                       AS REC_INT_PAY_CYC                 /*  수취LEG이자교환주기(월)*/
        , F.FIX_FLT_CLCD                                                                       AS REC_IRATE_TPCD                  /*  수취LEG금리유형코드(1:고정)*/
        , CASE WHEN A.ASSETCODE IN ('FX') THEN E.RCVG_DC_CRVE_CD
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN F.DC_ITRT_CRVE_CD
               ELSE NULL END                                                                   AS REC_IRATE_CURVE_ID              /*  수취LEG금리커브ID      */
        , CASE WHEN A.ASSETCODE IN ('FX')THEN NULL
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN F.BNCH_ITRT_CD
               ELSE NULL END                                                                   AS REC_IRATE_DTLS_TPCD             /*  수취LEG변동금리유형코드*/
        , NVL(F.ALL_SCTN_APLZ_SPRD_VAL, 0) + NVL(F.PATL_SCTN_APLZ_SPRD_VAL, 0)                 AS REC_ADD_SPRD                    /*  수취LEG가산스프레드    */
        , F.FLT_ITRT_INX_TNR_VAL * DECODE(F.FLT_ITRT_INX_TNR_UNIT_PTCD, '3', 12, 1)            AS REC_IRATE_RPC_CYC               /*  수취LEG금리개정주기(월)*/
       --  , CASE WHEN TO_NUMBER(F.INRS_DDCT_CLCL_MTCD) IN ('01', '05'      ) THEN  '1'                                              /*  ACT/365                */
       --         WHEN TO_NUMBER(F.INRS_DDCT_CLCL_MTCD) IN ('02', '10', '12') THEN  '2'                                              /*  A30/360                */
       --         WHEN TO_NUMBER(F.INRS_DDCT_CLCL_MTCD) IN ('04', '06', '08') THEN  '3'                                              /*  E30/360                */
       --         WHEN TO_NUMBER(F.INRS_DDCT_CLCL_MTCD) IN ('00', '09'      ) THEN  '4'                                              /*  ACT/ACT                */
       --         WHEN TO_NUMBER(F.INRS_DDCT_CLCL_MTCD) IN ('03'            ) THEN  '5'                                              /*  ACT/360                */
       --         ELSE '2' END                                                                                                       /*  DEFAULT: A30/360       */
        , '1'                                                                                  AS REC_DCB_CD                      /*  수취LEG일수계산코드   2024.05.09 요건 수정  */
        , NVL(DECODE(G.FIX_FLT_CLCD, 2, G.FIX_FLT_ROIT, G.FIX_FLT_ROIT), 0)                    AS PAY_IRATE                       /*  지급LEG금리            */
        , G.INRS_PAY_CYCL_VAL * DECODE(G.INRS_PAY_CYCL_CLCD, '3', 12, 1)                       AS PAY_INT_PAY_CYC                 /*  지급LEG이자교환주기(월)*/
        , G.FIX_FLT_CLCD                                                                       AS PAY_IRATE_TPCD                  /*  지급LEG금리유형코드(1:고정)*/
        , CASE WHEN A.ASSETCODE IN ('FX') THEN E.PAY_DC_CRVE_CD
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN G.DC_ITRT_CRVE_CD
               ELSE NULL END                                                                   AS PAY_IRATE_CURVE_ID              /*  지급LEG금리커브ID      */
        , CASE WHEN A.ASSETCODE IN ('FX') THEN NULL
               WHEN A.ASSETCODE IN ('SW', 'ET') THEN G.BNCH_ITRT_CD
               ELSE NULL END                                                                   AS PAY_IRATE_DTLS_TPCD             /*  지급LEG변동금리유형코드*/
        , NVL(G.ALL_SCTN_APLZ_SPRD_VAL, 0) + NVL(G.PATL_SCTN_APLZ_SPRD_VAL, 0)                 AS PAY_ADD_SPRD                    /*  지급LEG가산스프레드    */
        , G.FLT_ITRT_INX_TNR_VAL * DECODE(G.FLT_ITRT_INX_TNR_UNIT_PTCD, '3', 12, 1)            AS PAY_IRATE_RPC_CYC               /*  지급LEG금리개정주기(월)*/
       --  , CASE WHEN TO_NUMBER(G.INRS_DDCT_CLCL_MTCD) IN ('01', '05'      ) THEN  '1'                                              /*  ACT/365                */
       --         WHEN TO_NUMBER(G.INRS_DDCT_CLCL_MTCD) IN ('02', '10', '12') THEN  '2'                                              /*  A30/360                */
       --         WHEN TO_NUMBER(G.INRS_DDCT_CLCL_MTCD) IN ('04', '06', '08') THEN  '3'                                              /*  E30/360                */
       --         WHEN TO_NUMBER(G.INRS_DDCT_CLCL_MTCD) IN ('00', '09'      ) THEN  '4'                                              /*  ACT/ACT                */
       --         WHEN TO_NUMBER(G.INRS_DDCT_CLCL_MTCD) IN ('03'            ) THEN  '5'                                              /*  ACT/360                */
       --         ELSE '2' END                                                                                                       /*  DEFAULT: A30/360       */
        ,'1'                                                                                AS PAY_DCB_CD                      /*  지급LEG일수계산코드  2024.05.09 요건 수정   */
        , A.UNITS                                                                           AS CONT_QNTY                       /*  거래수량               */
        , NVL(LEAST(F.MLTI_VAL, G.MLTI_VAL), 1)                                                AS CONT_MULT                       /*  거래승수               */
        , NVL(A.COTR_KKURS, 0)                                                                  AS CONT_PRC                        /*  체결가격(환율/지수 등) */  -- null 12건, 오류(x10) 1건
        , CASE WHEN  A.ASSETCODE IN ('FX') 
               THEN DECODE(E.PAY_CRNC_CD, 'KRW', 1, E.PAY_FX_RATE) / DECODE(E.RCVG_CRNC_CD, 'KRW', 1, E.RCVG_FX_RATE)
               WHEN  A.ASSETCODE IN ('SW', 'ET') 
               THEN DECODE(NVL(G.CRNC_CD, NVL(A.WAERS_OUT, 'KRW')), 'KRW', 1, G.PAY_FX_RATE)
                    / DECODE(F.CRNC_CD, 'KRW', 1, F.RCVG_FX_RATE)
               ELSE 1 END                                                                      AS SPOT_PRC                        /*  현재가격(환율/지수 등) */
        , NVL(LEAST(F.COUP_UPLI_VAL, G.COUP_UPLI_VAL), 1)                                      AS IRATE_CAP                       /*  적용금리상한           */
        , NVL(GREATEST(F.COUP_LLMT_VAL, G.COUP_LLMT_VAL), -1)                                  AS IRATE_FLO                       /*  적용금리하한           */
        , NULL                                                                                 AS OPT_VOLT                        /*  옵션변동성             */
        , NULL                                                                                 AS UNDL_EXEC_PRC                   /*  기초자산행사가격(옵션) */
        , NULL                                                                                 AS UNDL_SPOT_PRC                   /*  기초자산현재가격(옵션) */
--        , NVL(HDGE.STAN_ITMS_CD, COBD.ISIN_CD)                                                 AS HDGE_ISIN_CD                    /*  헤지대상종목코드       */
--        , NVL(HDGE.MATR_DATE   , COBD.MATR_DATE)                                               AS HDGE_MATR_DATE                  /*  헤지대상종목만기일자   */                
        , I.ISIN  AS HDGE_ISIN_CD                    /*  헤지대상종목코드       */  --추가정보 입수 필요함 우선 NULL 처리 
        , I.EDDT AS HDGE_MATR_DATE                  /*  헤지대상종목만기일자   */ --추가정보 입수 필요함  우선 NULL 처리                		
        , A.BP_BUP                                                                        AS CNPT_ID                         /*  거래상대방ID           */   -- 거래상대방 법인번호 뿐임. 거래상대방 발행인번호 필요함
                                 -- IKRASH_ITEM_INFO 테이블의 ISSU_INT_CUST_NO는 파생상품은 모두 NULL임
        , A.BPARTNER_NAME                                                                         AS CNPT_NM                         /*  거래상대방명           */
        , A.BP_BUP                                                                     AS CORP_NO                         /*  법인등록번호           */
        , CUST.CRGR_KIS                                                                        AS CRGR_KIS                        /*  신용등급(한신평)       */
        , CUST.CRGR_NICE                                                                       AS CRGR_NICE                       /*  신용등급(한신정)       */
        , CUST.CRGR_KR                                                                         AS CRGR_KR                         /*  신용등급(한기평)       */
        , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
        , CUST.CRGR_KICS                                                                       AS CRGR_KICS                       /*  신용등급(KICS)         */
        , D.CVA_APLZ_VLT_AMT                                                                   AS EXT_UPRC                        /*  외부평가단가           */
        , CASE WHEN A.ASSETCODE IN ('FX')  THEN 1
               WHEN A.ASSETCODE IN ('SW', 'ET')  THEN 1
               ELSE 1 END                                                                      AS EXT_UPRC_UNIT                   /*  외부평가단가           */
        , A.DURATION                                                                           AS EXT_DURA                        /*  외부평가듀레이션       */ -- D테이블에는 NET_DURA개념은 없음
        , D.RCVG_LEG_MKPR_DRTY_BUYG_PRC + 1 * D.CVA_ALLO_OTR_RISK_AMT                          AS REC_EXT_UPRC                    /*  수취LEG외부평가단가    */ -- 수취LEG의 Dirty Price에 거래상대방CVA를 더함
        , D.PAY_LEG_MKPR_DRTY_SLDL_PRC  - 1 * D.CVA_ALLO_OSLF_RISK_AMT                         AS PAY_EXT_UPRC                    /*  지급LEG외부평가단가    */ -- 지급LEG의 Dirty Price에 자신의CVA를 차감(REC-PAY 관점에서 차감하였을뿐임, [NET = REC - PAY + SUM(CVA)])
        , D.RCVG_DURA                                                                          AS REC_EXT_DURA                    /*  수취LEG외부평가듀레이션*/
        , D.PAY_DURA                                                                           AS PAY_EXT_DURA                    /*  지급LEG외부평가듀레이션*/
        , NULL                                                                      AS STOC_TPCD                       /*  주식유형코드           */ -- NULL처리 검토 필요
        , 'N'                                                                           AS PRF_STOC_YN                     /*  우선주여부             */ --N 처리 검토 필요
        , '01_N'                              AS BOND_RANK_CLCD                  /*  채권순위구분코드       */ 
        , 'N'                                                                                  AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , 'KR'                                                         AS CNTY_CD                         /*  국가코드               */  -- 하드코딩, 검토 필요
        , NULL                                                                                 AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */
        , A.OPER_PART                       AS DEPT_CD                         /*  부서코드               */
        , 'KRBH103BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
        , A.RATING5 AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
        , A.RATING6 AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
        , A.RATING7 AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
        , A.IND_SECTOR_L        AS IND_L_CLSF_CD                        /*산업대분류코드*/
        , A.IND_SECTOR_L_T      AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
        , A.IND_SECTOR          AS IND_CLSF_CD                          /* 산업분류코드*/

     FROM 
               ( SELECT AA.* 
--	                     ,  CASE WHEN SUBSTR(AA.PORTFOLIO,1,1) IN ('A','E','G','S') THEN '1'     -- A연금저축연동금리, E지수연동 , G 일반계정은 일반계정 분류 
--				           ELSE '2' 
--					      END AS FNDS_ACCO_CLCD	
                      , DECODE(AA.PORTFOLIO,'G000','1','2') AS FNDS_ACCO_CLCD
		              ,  AA.GL_ACCOUNT   AS ACCO_CD	-- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수
			      , NVL(AB.MAP_ID, AA.POSITION_ID) AS JOIN_KEY		  
		    FROM IBD_ZTCFMPEBAPST AA 
			     ,  ( SELECT POSITION_ID
				          ,  MAX(MAP_ID) AS MAP_ID  
				     FROM IKRUSH_FIDE_MAP 
				   GROUP BY POSITION_ID
				 )   AB
		  WHERE 1=1
		       AND AA.GRDAT = '$$STD_Y4MD'  
                       AND AA.VALUATION_AREA = '400'
                       AND AA.ASSETCODE IN ('FX', 'SW','ET' )-- fx, swap, 선도 
		       AND AA.POSITION_ID = AB.POSITION_ID(+)			   
	      )  A
        , (
             SELECT *
               FROM IDHKRH_NICE_FSNP
              WHERE STND_DT = (
                                 SELECT MAX(STND_DT)
                                   FROM IDHKRH_NICE_FSNP
                                  WHERE STND_DT <= '$$STD_Y4MD'
                              )
          ) D                                    -- FXSWAP NICE (FN은 없음)
        , (
             SELECT T1.*
                  , DECODE(NVL(T2.FX_RATE, 0), 0, 1, T2.FX_RATE)      AS RCVG_FX_RATE
                  , DECODE(NVL(T3.FX_RATE, 0), 0, 1, T3.FX_RATE)      AS PAY_FX_RATE
               FROM IDHKRH_FX_INFO T1
                  , (
                       SELECT STND_DT
                            , SUBSTR(FOEX_CD, 6, 3)                                                 AS CRNC_CD
                            , CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL)     AS FX_RATE
                         FROM IDHKRH_FX          -- 외화환율데이터
                     ) T2
                  , (
                       SELECT STND_DT
                            , SUBSTR(FOEX_CD, 6, 3)                                                 AS CRNC_CD
                            , CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL)     AS FX_RATE
                         FROM IDHKRH_FX          -- 외화환율데이터
                     ) T3
              WHERE 1=1
                AND T1.STND_DT = T2.STND_DT(+)
                AND T1.STND_DT = T3.STND_DT(+)
                AND T1.RCVG_CRNC_CD = T2.CRNC_CD(+)
                AND T1.PAY_CRNC_CD  = T3.CRNC_CD(+)
                AND T1.STND_DT = (
                                    SELECT MAX(STND_DT)
                                      FROM IDHKRH_FX_INFO
                                     WHERE STND_DT <= '$$STD_Y4MD'
                                 )

          ) E                                    -- FX FORWARD 내역정보
        , (
             SELECT T1.*
                  , DECODE(NVL(T2.FX_RATE, 0), 0, 1, T2.FX_RATE) AS RCVG_FX_RATE
               FROM IDHKRH_SWAP_INFO T1
                  , (
                       SELECT STND_DT
                            , SUBSTR(FOEX_CD, 6, 3)                                                 AS CRNC_CD
                            , CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL)     AS FX_RATE
                         FROM IDHKRH_FX          -- 외화환율데이터
                     ) T2
              WHERE 1=1
                AND T1.STND_DT = T2.STND_DT(+)
                AND T1.CRNC_CD = T2.CRNC_CD(+)
                AND T1.POTN_CLCD = 2             -- RECEIVE LEG
                AND T1.STND_DT = (
                                    SELECT MAX(STND_DT)
                                      FROM IDHKRH_SWAP_INFO
                                     WHERE STND_DT <= '$$STD_Y4MD'
                                 )
          ) F                                    -- PLAIN SWAP LEG 별입력정보(수취 LEG)
        , (
             SELECT T1.*
                  , DECODE(NVL(T2.FX_RATE, 0), 0, 1, T2.FX_RATE) AS PAY_FX_RATE
               FROM IDHKRH_SWAP_INFO T1
                  , (
                       SELECT STND_DT
                            , SUBSTR(FOEX_CD, 6, 3)                                                 AS CRNC_CD
                            , CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL)     AS FX_RATE
                         FROM IDHKRH_FX          -- 외화환율데이터
                     ) T2
              WHERE 1=1
                AND T1.STND_DT = T2.STND_DT(+)
                AND T1.CRNC_CD = T2.CRNC_CD(+)
                AND T1.POTN_CLCD = 1             -- PAY LEG
                AND T1.STND_DT = (
                                    SELECT MAX(STND_DT)
                                      FROM IDHKRH_SWAP_INFO
                                     WHERE STND_DT <= '$$STD_Y4MD'
                                 )
          ) G                                    -- PLAIN SWAP LEG 별입력정보(지급 LEG)
        , (
             SELECT STND_DT
                  , SECR_ITMS_CD
                  , CLPC
                  , BASE_AST_CD
                  , CNTA_MLTI
                  , CRNC_CD
                  , TECN_PUT_CLCD
               FROM IDHKRH_STOCK_INDEX_FUTURE T1 -- 주가지수선물옵션
              WHERE STND_DT = (
                                 SELECT MAX(STND_DT)
                                   FROM IDHKRH_STOCK_INDEX_FUTURE
                                  WHERE STND_DT <= '$$STD_Y4MD'
                              )
          ) H
         , ( SELECT   IA.ISIN
                    , IA.HDREL
                    , IA.EDDT
                 FROM IBD_ZTCFMPEBAPST IA 
                   ,  IBD_ZTCFM_HDREL IB
             WHERE IA.GRDAT = '$$STD_Y4MD'
                   AND IA.HDREL = IB.HDREL
                   AND IA.SECURITY_ID = IB.SECURITY_ID
                   AND IA.SECURITY_ACCOUNT = IB.SECURITY_ACCOUNT
                   AND IA.BUY_ID = IB.HDREL_IDNT
                   AND IB.HDREL_TYPE = '01'
	         )	 I          
         , (		  
             SELECT CD                                                AS PROD_TPCD
		  , CD_NM                                         AS PROD_TPNM	 
                  , CD2                                               AS KICS_TPCD
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_MAP_SAP'     -- KICS상품분류매핑
          ) KICS	
         , T_CUST  CUST		  
         , ACCO_MST
  WHERE 1=1
        AND A.JOIN_KEY = D.SECR_ITMS_CD(+)
        AND A.JOIN_KEY = E.STAN_ITMS_CD(+)
        AND A.JOIN_KEY = F.STAN_ITMS_CD(+)
        AND A.JOIN_KEY = G.STAN_ITMS_CD(+)
        AND A.ISIN = H.SECR_ITMS_CD(+)
        AND A.HDREL = I.HDREL(+)        
        AND A.PRODUCT_TYPE = KICS.PROD_TPCD(+)
        AND A.ACCO_CD = ACCO_MST.ACCO_CD (+)
	 AND A.BP_BUP    = CUST.CORP_NO(+)
)
SELECT * 
FROM FIDE