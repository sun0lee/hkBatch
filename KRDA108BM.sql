SELECT /* SQL ID : KRDA108BM */
       A.BASE_DATE
     , A.EXPO_ID
     , A.LEG_TYPE
     , A.ASSET_TPCD
     , A.FUND_CD
     , A.PROD_TPCD
     , A.PROD_TPNM
     , NVL(B.KICS_PROD_TPCD, A.KICS_PROD_TPCD)                                       AS KICS_PROD_TPCD
     , NVL(B.KICS_PROD_TPNM, NVL(C.KICS_PROD_TPNM, A.KICS_PROD_TPNM))                AS KICS_PROD_TPNM
     , A.ISIN_CD
     , A.ISIN_NM
     , A.LT_TP
     , A.PRNT_ISIN_CD
     , A.PRNT_ISIN_NM
     , A.ACCO_CLCD
     , A.ACCO_CD
     , A.ACCO_NM
     , A.INST_CLCD
     , A.ISSU_DATE
     , A.MATR_DATE
     , A.IRATE
     , A.IRATE_TPCD
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
     , A.DEDT_ACCO_AMT
     , A.CNPT_CRNY_CD
     , A.CNPT_CRNY_FXRT
     , A.CNPT_FAIR_BS_AMT
     , A.FV_CLSF_CD
     , NVL(B.CR_CLSF_CD, A.CR_CLSF_CD)                                               AS CR_CLSF_CD
     , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD
         WHERE GRP_CD = 'CR_CLSF_CD'
           AND CD = NVL(B.CR_CLSF_CD, A.CR_CLSF_CD))                                 AS CR_CLSF_NM
     , NVL(B.CR_ACCO_TPCD, A.CR_ACCO_TPCD)                                           AS CR_ACCO_TPCD
     , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD
         WHERE GRP_CD = 'CR_ACCO_TPCD'
           AND CD = NVL(B.CR_ACCO_TPCD, A.CR_ACCO_TPCD))                             AS CR_ACCO_TPNM
     , A.CR_EXPO_AMT                                                                 AS CR_EXPO_AMT
     , NVL(B.IR_CLSF_CD, A.IR_CLSF_CD)                                               AS IR_CLSF_CD
     , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD
         WHERE GRP_CD = 'IR_CLSF_CD'
           AND CD = NVL(B.IR_CLSF_CD, A.IR_CLSF_CD))                                 AS IR_CLSF_NM
     , GREATEST(LEAST(NVL(B.IR_SHCK_WGT, A.IR_SHCK_WGT), 1.0), 0.0)                  AS IR_SHCK_WGT
     , A.IR_EXPO_AMT                                                                 AS IR_EXPO_AMT
     , NVL(B.SR_CLSF_CD, A.SR_CLSF_CD)                                               AS SR_CLSF_CD
     , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD
         WHERE GRP_CD = 'SR_CLSF_CD'
           AND CD = NVL(B.SR_CLSF_CD, A.SR_CLSF_CD))                                 AS SR_CLSF_NM
     , A.SR_EXPO_AMT                                                                 AS SR_EXPO_AMT
     , A.FR_CLSF_CD                                                                  AS FR_CLSF_CD
     , A.FR_CLSF_NM                                                                  AS FR_CLSF_NM
     , A.FR_EXPO_AMT                                                                 AS FR_EXPO_AMT
     , A.CNPT_SUM_AMT
     , A.CNPT_ID
     , NVL(B.CNPT_NM, A.CNPT_NM)                                                     AS CNPT_NM
     , NVL(B.CORP_NO, A.CORP_NO)                                                     AS CORP_NO
     , NVL(B.CRGR_KIS , DECODE(SUBSTR(A.CR_CLSF_CD, 1, 2), 'SS', 'RF', A.CRGR_KIS) ) AS CRGR_KIS
     , NVL(B.CRGR_NICE, DECODE(SUBSTR(A.CR_CLSF_CD, 1, 2), 'SS', 'RF', A.CRGR_NICE)) AS CRGR_NICE
     , NVL(B.CRGR_KR  , DECODE(SUBSTR(A.CR_CLSF_CD, 1, 2), 'SS', 'RF', A.CRGR_KR)  ) AS CRGR_KR
     , NVL(B.CRGR_KICS, DECODE(SUBSTR(A.CR_CLSF_CD, 1, 2), 'SS', 'RF', A.CRGR_KICS)) AS CRGR_KICS
     , A.CRGR_CB_NICE
     , A.CRGR_CB_KCB
     , NVL(B.GRP_CD, A.GRP_CD)                                                       AS GRP_CD
     , NVL(B.GRP_NM, A.GRP_NM)                                                       AS GRP_NM
     , A.LOAN_CONT_TPCD
     , A.COLL_ID
     , A.COLL_KND
     , A.COLL_SET_AMT
     , A.PRPT_DTLS_TPCD
     , A.LTV
     , A.PRPT_OCPY_TPCD
     , A.PRPT_CNTY_TPCD
     , A.CONT_QNTY
     , A.CONT_MULT
     , A.CONT_PRC
     , A.STOC_TPCD
     , A.PRF_STOC_YN
     , A.STOC_LIST_CLCD
     , A.CPTC_YN
     , A.BOND_RANK_CLCD
     , A.ASSET_LIQ_CLCD
     , A.HDGE_ISIN_CD
     , A.HDGE_MATR_DATE
     , A.UUSE_LMT_AMT
     , A.CCF
     , A.FIDE_UNDL_CLCD
     , A.ARR_DAYS
     , A.FLC_CD
     , A.DFLT_YN
     , A.IR_EXPO_SCEN01 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN02 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN03 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN04 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN05 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN06 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN07 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN08 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN09 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN10 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN11 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN12 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN13 + NVL(A.IMP_SPRD_AMT, 0)
     , A.IR_EXPO_SCEN14 + NVL(A.IMP_SPRD_AMT, 0)
     , A.DEPT_CD
     , 'KRDA108BM'
     , SYSDATE
-- 20230518 김재우 수정
-- 칼럼추가
     , A.CRGR_SNP	                          /*  신용등급(S&P) */
     , A.CRGR_MOODYS	          /* 신용등급(무디스) */
     , A.CRGR_FITCH	                  /*  신용등급(피치) */
     , A.IND_L_CLSF_CD	          /* 산업대분류코드 */
     , A.IND_L_CLSF_NM	          /* 산업대분류명 */
     , A.IND_CLSF_CD	                  /* 산업분류코드 */
     , A.OBGT_PSSN_ESTE_YN	  /* 의무보유부동산여부 */
     , A.LVG_RATIO	                          /* 레버리지비율(%) */
     , A.FV_RPT_COA	                  /*  공정가치보고서COA */
     , A.FV_HIER_NODE_ID	          /* 공정가치계층노드ID */
     , A.SNR_LTV	                          /* 선순위LTV(%)) */
     , A.FAIR_VAL_SNR	                  /* 선순위대출금액 */
     , A.DSCR	                                  /* DSCR */	      
  FROM (
          SELECT A1.*
               , CASE WHEN A1.LEG_TYPE IN ('STD', 'NET') AND A1.ASSET_TPCD IN ('SECR', 'FIDE')
                           AND ABS(NVL(A1.FAIR_BS_AMT, 0) - NVL(A1.IR_EXPO_SCEN01, 0)) > 1000 AND ABS(A1.IMP_SPRD) = 0
                           AND A1.RMN_MATR < 0.1
                           THEN NVL(A1.FAIR_BS_AMT, 0) - NVL(A1.IR_EXPO_SCEN01, 0)
                      ELSE NULL END AS IMP_SPRD_AMT         -- 잔존만기가 극단적으로 짧으면(e.g. 1일~2일) 내재스프레드 계산이 실패하는 경우가 존재함
            FROM Q_IC_PRE_EXPO A1                           -- 실패하면 내재스프레드를 0으로 도출되지만, 공정가치와 기준시나리오의 가치가 차이가 나는 경우가 발생하므로 이를 보정함(유가증권, 파생상품 한정)
       ) A
     , (
          SELECT *
            FROM IKRUSH_USER_ADJ T1
           WHERE 1=1
             AND BASE_DATE = (
                                SELECT MAX(BASE_DATE)
                                  FROM IKRUSH_USER_ADJ
                                 WHERE 1=1
                                   AND BASE_DATE <= '$$STD_Y4MD'
                                   and EXPO_ID = T1.EXPO_ID
                             )
       ) B
     , (
          SELECT KICS_PROD_TPCD                             AS KICS_PROD_TPCD
               , MAX(KICS_PROD_TPNM)                        AS KICS_PROD_TPNM
               , MAX(CR_CLSF_CD)                            AS CR_CLSF_CD
               , MAX(CR_CLSF_NM)                            AS CR_CLSF_NM
               , MAX(CR_ACCO_TPCD)                          AS CR_ACCO_TPCD
               , MAX(CR_ACCO_TPNM)                          AS CR_ACCO_TPNM
               , MAX(IR_CLSF_CD)                            AS IR_CLSF_CD
               , MAX(IR_CLSF_NM)                            AS IR_CLSF_NM
               , MAX(IR_SHCK_WGT)                           AS IR_SHCK_WGT
               , MAX(SR_CLSF_CD)                            AS SR_CLSF_CD
               , MAX(SR_CLSF_NM)                            AS SR_CLSF_NM
               , MAX(FR_CLSF_CD)                            AS FR_CLSF_CD
               , MAX(FR_CLSF_NM)                            AS FR_CLSF_NM
            FROM IKRUSH_KICS_RISK_MAP
           WHERE 1=1
             AND '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
           GROUP BY KICS_PROD_TPCD
       ) C

 WHERE 1=1
   AND A.EXPO_ID        = B.EXPO_ID(+)
   AND A.KICS_PROD_TPCD = C.KICS_PROD_TPCD(+)
   AND A.BASE_DATE      = '$$STD_Y4MD'