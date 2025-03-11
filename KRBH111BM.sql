/* 수정사항 
2024-05-02 : 위험경감불인정 통화선도 추가 
     - 730 : 통화스왑 CRS
     - 731 : FX스왑 
     - 733 : 통화스왑 CRS (위험경감불인정) => 신규추가 
     - 734 : FX스왑 (위험경감불인정 => 신규추가)

2024-05-09 
- REC_DCB_CD = '1'
- PAY_DCB_CD = '1'
*/

WITH /* SQL-ID : KRBH111BM */ 
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
, T_AAA AS
(
   SELECT A.BASE_DATE AS BASE_DATE                       /*  기준일자               */
        , 'FIDE_L_'||A.FUND_CD||A.PRNT_ISIN_CD||'_'||A.ISIN_CD||'_'||LPAD(A.LT_SEQ, 3, '0')    AS EXPO_ID                         /*  익스포저ID             */
        , A.FUND_CD                                                                            AS FUND_CD                         /*  펀드코드               */
        , NVL(A.LT_TPCD, 'Z')                                                                  AS PROD_TPCD                       /*  상품유형코드           */
        , A.PROD_TPNM                                                                          AS PROD_TPNM                       /*  상품유형명             */
        , NVL(A.LT_DTLS_TPCD, 'ZZZ')                                                      AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
        , KICS.KICS_TPNM                                                                       AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
        , A.ISIN_CD                                                                            AS ISIN_CD                         /*  종목코드               */
        , A.ISIN_NM                                                                            AS ISIN_NM                         /*  종목명                 */		
        , A.ACCO_CLCD||'_'||A.ACCO_CD                                                          AS ACCO_CD                         /*  계정과목코드           */
        , ACCO_MST.ACCO_NM                                                                     AS ACCO_NM                         /*  계정과목명             */
        , CASE WHEN LENGTHB(TO_CHAR(A.PRNT_ISIN_NM)) < 100
               THEN TO_CHAR(A.PRNT_ISIN_NM)
               ELSE SUBSTRB(TO_CHAR(A.PRNT_ISIN_NM), 1, 96)
               END                                                                             AS CONT_ID                         /*  계약번호(계좌일련번호) */
        , 'D'                               AS INST_TPCD                       /*  인스트루먼트유형코드   */
        , CASE WHEN A.LT_DTLS_TPCD IN ('730','733')  THEN '2'  -- (통화스왑 CRS) 이선영 수정 20240502 위험경감불인정 KICS PROD TPCD 추가
		           WHEN A.LT_DTLS_TPCD IN ('731','734')  THEN '1'  -- (FX스왑)       이선영 수정20240502 위험경감불인정 KICS PROD TPCD 추가
               ELSE 'Z'
 		      END 	                                                                               AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */	
        , NULL AS POTN_TPCD			  
        , A.ISSU_DATE
        , A.MATR_DATE	
        , NULL AS FRST_INT_DATE
        , A.PRIN_EXCG_YN
        , NULL AS KTB_MAT_YEAR
        , A.REC_CRNY_CD
        , NVL(A.REC_CRNY_FXRT2,1) AS REC_CRNY_FXRT
        , A.REC_NOTL_AMT
        , A.PAY_CRNY_CD
        , NVL(A.PAY_CRNY_FXRT2,1) AS PAY_CRNY_FXRT
        , A.PAY_NOTL_AMT
        , A.LT_BS_AMT AS BS_AMT
        , A.LT_BS_AMT AS VLT_AMT
        , A.LT_BS_AMT AS FAIR_BS_AMT
        , 0 AS CVA
        , 0 AS ACCR_AMT
        , 0 AS UERN_AMT 
        , NULL AS  REC_IRATE
        , NULL AS REC_INT_PAY_CYC
        , NULL AS REC_IRATE_TPCD
        , 'USDIRA'  AS REC_IRATE_CURVE_ID
        , NULL AS REC_IRATE_DTLS_TPCD
        , NULL AS REC_ADD_SPRD
        , NULL AS REC_IRATE_RPC_CYC
        -- , '2' AS REC_DCB_CD
        ,'1'   AS REC_DCB_CD -- 2024.05.09 요건 수정 
        , NULL AS PAY_IRATE
        , NULL AS PAY_INT_PAY_CYC
        , NULL AS PAY_IRATE_TPCD
        , 'USDIRA' AS PAY_IRATE_CURVE_ID
        , NULL AS PAY_IRATE_DTLS_TPCD
        , NULL AS PAY_ADD_SPRD
        , NULL AS PAY_IRATE_RPC_CYC
        -- , '2' AS PAY_DCB_CD
        , '1' AS PAY_DCB_CD -- 2024.05.09 요건 수정 
        , NULL AS CONT_QNTY
        , NULL AS CONT_MULT
        , NULL AS CONT_PRC
        , CASE WHEN A.PAY_CRNY_CD <> 'KRW' THEN NVL(A.PAY_CRNY_FXRT2,1) 
	             WHEN A.REC_CRNY_CD <> 'KRW' THEN NVL(A.REC_CRNY_FXRT2,1) 
	 	           ELSE NULL 
		      END AS SPOT_PRC  
        , 1 AS IRATE_CAP
        , - 1 AS IRATE_FLO
        , NULL AS OPT_VOLT
        , NULL AS UNDL_EXEC_PRC
        , NULL AS UNDL_SPOT_PRC
        , NULL AS HDGE_ISIN_CD
        , NULL AS HDGE_MATR_DATE
        , A.CORP_NO AS CNPT_ID
        , A.CNPT_NM 
        , A.CORP_NO
      	, A.CRGR_KIS
      	, A.CRGR_NICE
      	, A.CRGR_KR
        , NULL AS CRGR_VLT
        , A.CRGR_KICS
      	, A.LT_BS_AMT AS EXT_UPRC
        , 1 AS EXT_UPRC_UNIT
        , 0  AS EXT_DURA
        , NULL AS REC_EXT_UPRC
        , NULL AS PAY_EXT_UPRC
        , NULL AS REC_EXT_DURA
        , NULL AS PAY_EXT_DURA
        , NULL AS STOC_TPCD
        , 'N' AS PRF_STOC_YN
        , '01_N' AS BOND_RANK_CLCD
        , 'N' AS STOC_LIST_CLCD
        , 'KR' AS CNTY_CD
        , NULL AS ASSET_LIQ_CLCD
        , DECODE(SUBSTR(A.FUND_CD, 1, 1), 'G', '902472', '902476')                             AS DEPT_CD                         /*  부서코드               */
        , 'KRBH111BM' AS  LAST_MODIFIED_BY
        , SYSDATE AS LAST_UPDATE_DATE
      	, A.CRGR_SNP
      	, A.CRGR_MOODYS
        , A.CRGR_FITCH
        , NULL AS IND_L_CLSF_CD
        , NULL AS IND_L_CLSF_NM
        , NULL AS IND_CLSF_CD
     FROM (
             SELECT /*+ USE_HASH(A1 C1) FULL(A1) */
                    A1.*
-- 20230421 김재우 수정 기존정보 미사용(수기 테이블만 사용)
                   , DECODE(A1.VLT_CORP_TPCD, '1' , A1.CRGR_DMST) AS CRGR_KIS
                   , DECODE(A1.VLT_CORP_TPCD, '2' , A1.CRGR_DMST) AS CRGR_NICE
                   , DECODE(A1.VLT_CORP_TPCD, '3' , A1.CRGR_DMST) AS CRGR_KR
                   , DECODE(A1.VLT_CORP_TPCD, '4' , A1.CRGR_OVSE) AS CRGR_SNP
                   , DECODE(A1.VLT_CORP_TPCD, '5' , A1.CRGR_OVSE) AS CRGR_MOODYS
                   , DECODE(A1.VLT_CORP_TPCD, '6' , A1.CRGR_OVSE) AS CRGR_FITCH                    
--20230425 김재우 수정 펀드코드 로직 변경 
--                 , DECODE(FUND.FUND_CD, NULL, 2, 1)                                         AS ACCO_CLCD
                   , DECODE(A1.FUND_CD,'G000','1','2')                                          AS ACCO_CLCD
                   , PROD.CD_NM                                                                 AS PROD_TPNM
                   , NVL(FX.FX_RATE, 1)                                                         AS FX_RATE
            		   , REC_FX.FX_RATE AS  REC_CRNY_FXRT2		  
                   , PAY_FX.FX_RATE  AS  PAY_CRNY_FXRT2
                FROM IKRUSH_LT_TOTAL A1                       -- 수익증권
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
                     ) REC_FX
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
                    ) PAY_FX					
              WHERE 1=1
                AND NVL(A1.LT_TPCD,  '99') = PROD.CD(+)
                AND NVL(A1.CRNY_CD, 'KRW') = FX.CRNC_CD(+)
             		AND NVL(A1.REC_CRNY_CD,'KRW') = REC_FX.CRNC_CD(+)
            		AND NVL(A1.PAY_CRNY_CD,'KRW') =  PAY_FX.CRNC_CD(+)
                AND A1.BASE_DATE = '$$STD_Y4MD'
          ) A
         , (
             SELECT CD                                                AS CLSF1
                  , CD2                                               AS CLSF2
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_CD'     -- KICS상품분류코드
          ) KICS
        ,  ACCO_MST
    WHERE 1=1
      AND A.LT_DTLS_TPCD IN ('730','731','733','734') -- 이선영 수정 20240502 위험경감불인정 KICS PROD TPCD 추가
      AND A.LT_TPCD  = KICS.CLSF1(+)
      AND A.LT_DTLS_TPCD = KICS.CLSF2(+)
      AND NVL(A.ACCO_CD, '99999999')  = ACCO_MST.ACCO_CD  (+)
)
SELECT * FROM T_AAA