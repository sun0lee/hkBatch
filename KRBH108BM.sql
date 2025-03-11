SELECT /* SQL-ID : KRBH108BM */
       BASE_DATE
     , LPAD(NVL(FUND_CD, 0), 4, '0')                        AS FUND_CD
     , ISIN_CD                                              AS PRNT_ISIN_CD
     , '999999999999'                                       AS ISIN_CD          -- 특수금융의 경우 상세내역의 ISIN_CD는 미해당(편입자산이 표준코드가 없음)
     , LT_SEQ
     , ISIN_NM                                              AS PRNT_ISIN_NM
     , NULL                                                 AS ISIN_NM          -- 특수금융의 경우 ISIN_NM은 미해당
     , ACCO_CD
     , NVL(LT_TPCD, 'Z')                                    AS LT_TPCD
     , LT_TPNM
     , NVL(LT_DTLS_TPCD, CASE WHEN LT_TPCD IS NOT NULL THEN LT_TPCD||'99' ELSE 'ZZZ' END)
                                                            AS LT_DTLS_TPCD     -- LT_DTLS_TPCD -> KICS_PROD_TPCD로 그대로 연결되며, NULL인경우 LT_TPCD + '99' (즉, 주식의 경우 499)로 변환됨
     , LT_DTLS_TPNM
     , ISSU_DATE
     , MATR_DATE
     , CRNY_CD
     , NULL                                                 AS CRNY_FXRT
     , BS_AMT
     , NVL(LT_RTO, 1)                                       AS LT_RTO
     , LT_BS_AMT
     , NOTL_AMT
     , IRATE
     , INT_PAY_CYC
     , IRATE_TPCD
     , ADD_SPRD
     , IRATE_RPC_DATE
     , IRATE_RPC_CYC
     , '1'                                                  AS DCB_CD
     , AMORT_TPCD
     , CNPT_NM
     , CORP_NO
     , CNTY_CD
     , NULL                                                 AS VLT_CORP_TPCD
     , NULL                                                 AS CRGR_DMST
     , NULL                                                 AS CRGR_OVSE
     , CRGR_VLT                                             AS CRGR_VLT
     , CRGR_KICS                                            AS CRGR_KICS
     , NULL                                                 AS REC_NOTL_AMT
     , NULL                                                 AS PAY_NOTL_AMT
     , NULL                                                 AS REC_CRNY_CD
     , NULL                                                 AS PAY_CRNY_CD
     , NULL                                                 AS REC_CRNY_FXRT
     , NULL                                                 AS PAY_CRNY_FXRT
     , NULL                                                 AS REC_IRATE
     , NULL                                                 AS PAY_IRATE
     , NULL                                                 AS PRIN_EXCG_YN
     , NULL                                                 AS OPT_TPCD
     , NULL                                                 AS UNDL_ISIN_CD
     , NULL                                                 AS FIDE_UNDL_CLCD
     , NULL                                                 AS CONT_QNTY
     , NULL                                                 AS EXT_UPRC
     , NULL                                                 AS UNDL_SPOT_PRC
     , NULL                                                 AS UNDL_EXEC_PRC
     , NULL                                                 AS OPT_VOLT
     , NULL                                                 AS CONT_MULT
     , BOND_RANK_CLCD
     , PRF_STOC_TPCD
     , DMST_OVSE_CLCD
     , PRPT_OCPY_TPCD
     , LTV
     , DSCR
     , RMK
     , 'KRBH108BM'
     , SYSDATE
     , LVG_RATIO	 
     
  FROM IKRUSH_LT_GRP1 A
 WHERE 1=1
   AND BASE_DATE = '$$STD_Y4MD'
   AND  NOT EXISTS ( SELECT 1 FROM IKRUSH_LT_GRP2 M 
					WHERE 1=1
					AND M.BASE_DATE =  '$$STD_Y4MD'
					AND M.BASE_DATE = A.BASE_DATE
					AND  M.PRNT_ISIN_CD = A.ISIN_CD
					AND  M.LT_SEQ = A.LT_SEQ
				)
UNION ALL
SELECT BASE_DATE
     , LPAD(NVL(FUND_CD, 0), 4, '0')                        AS FUND_CD
     , NVL(PRNT_ISIN_CD, '999999999999')                    AS PRNT_ISIN_CD
     , NVL(ISIN_CD, '999999999999')                         AS ISIN_CD
     , NVL(LT_SEQ, ROW_NUMBER() OVER (PARTITION BY ISIN_CD ORDER BY LT_DTLS_TPCD, BS_AMT)) AS LT_SEQ
     , PRNT_ISIN_NM                                         AS PRNT_ISIN_NM
     , ISIN_NM                                              AS ISIN_NM
     , ACCO_CD
     , NVL(LT_TPCD, 'Z')                                    AS LT_TPCD
     , LT_TPNM
     , NVL(LT_DTLS_TPCD, CASE WHEN LT_TPCD IS NOT NULL THEN LT_TPCD||'99' ELSE 'ZZZ' END)
                                                            AS LT_DTLS_TPCD    -- LT_DTLS_TPCD -> KICS_PROD_TPCD로 그대로 연결되며, NULL인경우 LT_TPCD + '99' (즉, 주식의 경우 499)로 변환됨
     , LT_DTLS_TPNM
     , ISSU_DATE
     , MATR_DATE
     , CRNY_CD
     , 1.0                                                  AS CRNY_FXRT
     , BS_AMT
     , NVL(LT_RTO, 1.0)                                     AS LT_RTO
     , LT_BS_AMT                                            AS LT_BS_AMT
     , NOTL_AMT                                             AS NOTL_AMT
     , IRATE
     , INT_PAY_CYC
     , IRATE_TPCD
     , ADD_SPRD
     , IRATE_RPC_DATE
     , NULL                                                 AS IRATE_RPC_CYC
     , DCB_CD
     , AMORT_TPCD
     , CNPT_NM
     , CORP_NO
     , CNTY_CD
     , VLT_CORP_TPCD
     , CRGR_DMST
     , CRGR_OVSE
     , NULL                                                 AS CRGR_VLT
     , CRGR_KICS
     , REC_NOTL_AMT
     , PAY_NOTL_AMT
     , REC_CRNY_CD
     , PAY_CRNY_CD
     , NULL                                                 AS REC_CRNY_FXRT
     , NULL                                                 AS PAY_CRNY_FXRT
     , REC_IRATE
     , PAY_IRATE
     , PRIN_EXCG_YN
     , OPT_TPCD
     , UNDL_ISIN_CD
     , FIDE_UNDL_CLCD
     , CONT_QNTY
     , EXT_UPRC
     , UNDL_SPOT_PRC
     , UNDL_EXEC_PRC
     , OPT_VOLT
     , CONT_MULT
     , BOND_RANK_CLCD
     , PRF_STOC_TPCD
     , DMST_OVSE_CLCD
     , PRPT_OCPY_TPCD
     , LTV
     , DSCR
     , NULL                                                 AS RMK
     , 'KRBH108BM'
     , SYSDATE
     , LVG_RATIO	     
  FROM IKRUSH_LT_GRP2
 WHERE 1=1
   AND BASE_DATE = '$$STD_Y4MD'