WITH /* SQL-ID : KRDA107BM */ T_AAA AS
(
   SELECT A.BASE_DATE                                                                          AS BASE_DATE                       /*  기준일자               */
        , A.PROD_CD||'_'||A.SEQ||'_'||A.OFB_TPCD||NVL(A.CNBT_TPCD, '0')                        AS EXPO_ID                         /*  익스포저ID             */  -- SUBSTR(EXPO_ID, -1, 2) = '21' --> 주식N블라인드
        , 'STD'                                                                                AS LEG_TYPE
        , 'OFFB'                                                                               AS ASSET_TPCD
        , '9999'                                                                               AS FUND_CD
        , A.OFB_PROD_TPCD                                                                      AS PROD_TPCD
        , A.OFB_PROD_TPNM                                                                      AS PROD_TPNM
        , A.OFB_PROD_DTLS_TPCD                                                                 AS KICS_PROD_TPCD
        , A.OFB_PROD_DTLS_TPNM                                                                 AS KICS_PROD_TPNM
        , A.PROD_CD                                                                            AS ISIN_CD
        , A.PROD_NM                                                                            AS ISIN_NM
        , 'N'                                                                                  AS LT_TP
        , NULL                                                                                 AS PRNT_ISIN_CD
        , NULL                                                                                 AS PRNT_ISIN_NM                                            
        , '9'                                                                                  AS ACCO_CLCD
        , '99999999'                                                                           AS ACCO_CD
        , '난외약정'                                                                           AS ACCO_NM
        , '99'                                                                                 AS INST_CLCD
        , A.ISSU_DATE                                                                          AS ISSU_DATE
        , A.MATR_DATE                                                                          AS MATR_DATE
        , NULL                                                                                 AS IRATE                           /*  금리                   */
        , NULL                                                                                 AS IRATE_TPCD                      /*  금리유형코드(1:고정)   */
        , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(ISSU_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3)
                                                                                               AS CONT_MATR
        , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3)
                                                                                               AS RMN_MATR
        , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3)
                                                                                               AS EFFE_MATR
        , ROUND(GREATEST(NVL(MONTHS_BETWEEN(TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD')), 0), 0.03) / 12, 3)
                                                                                               AS EFFE_DURA
        , NULL                                                                                 AS IMP_SPRD                        /*  내재스프레드(채권)     */
        , A.CRNY_CD                                                                            AS CRNY_CD
        , A.CRNY_FXRT                                                                          AS CRNY_FXRT
        , A.CNTY_CD                                                                            AS CNTY_CD
        , 0.0                                                                                  AS NOTL_AMT
        , 0.0                                                                                  AS BS_AMT
        , 0.0                                                                                  AS FAIR_BS_AMT
        , 0.0                                                                                  AS ACCR_AMT
        , 0.0                                                                                  AS UERN_AMT
        , NULL                                                                                 AS DEDT_ACCO_AMT                   /*  차감계정합계금액       */
        , NULL                                                                                 AS CNPT_CRNY_CD
        , NULL                                                                                 AS CNPT_CRNY_FXRT
        , NULL                                                                                 AS CNPT_FAIR_BS_AMT
        , NULL                                                                                 AS FV_CLSF_CD
        , CASE WHEN C.ASSET_CLSF_CD IS NOT NULL AND A.OFB_PROD_TPCD = '3'                                                         -- 3: 대출채권, 4:출자약정(주식)
               THEN C.ASSET_CLSF_CD
               ELSE R.CR_CLSF_CD END                                                           AS CR_CLSF_CD                      -- 난외약정은 오직 대출약정과 출자약정으로 구성
        , NVL(C.ASSET_CLSF_NM, NVL(R.CR_CLSF_NM, (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_CLSF_CD' AND CD = 'NON')))
                                                                                               AS CR_CLSF_NM                      /*  신용리스크분류명       */
        , '5'                                                                                  AS CR_ACCO_TPCD                    -- 신용리스크 난외약정은 구분자 '5'로 설정
        , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'CR_ACCO_TPCD' AND CD = '5')     AS CR_ACCO_TPNM                    /*  신용리스크계정분류명   */
        , DECODE(A.OFB_TPCD, '1', NVL(GREATEST(A.OFB_AMT, 0), 0) * A.CCF, NULL)                             AS CR_EXPO_AMT
        , 'NON'                                                                                AS IR_CLSF_CD                      /*  금리리스크분류코드     */
        , (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'IR_CLSF_CD' AND CD = 'NON')     AS IR_CLSF_NM                      /*  금리리스크분류명       */
        , 0.0                                                                                  AS IR_SHCK_WGT                     /*  금리리스크적용비율     */
        , NULL                                                                                 AS IR_EXPO_AMT
        , NVL(R.SR_CLSF_CD, 'NON')                                                             AS SR_CLSF_CD                      /*  주식리스크분류코드     */
        , NVL(R.SR_CLSF_NM,
          (SELECT MAX(CD_NM) FROM IKRASM_CO_CD WHERE GRP_CD = 'SR_CLSF_CD' AND CD = 'NON'))    AS SR_CLSF_NM                      /*  주식리스크분류명       */
        , CASE WHEN A.OFB_TPCD IN ('1')
               THEN NULL                                                                                                          -- 대출약정은 NULL(난외약정구분코드 기준)
               WHEN A.OFB_TPCD IN ('2')
               THEN CASE WHEN A.CNBT_TPCD = '1' THEN NVL(GREATEST(A.OFB_AMT, 0), 0) / GREATEST(NVL(A.RMN_IVSM_TERM, 0), 1)                     -- 출자구분코드 기준: 블라인드펀드(약정금액 / 잔존투자기간)
                         WHEN A.CNBT_TPCD = '2' THEN NVL(GREATEST(A.OFB_AMT, 0), 0)                                                            -- 출자구분코드 기준: 프로젝트펀드(향후 1년이내에 인출될 금액)
                         ELSE NULL END * DECODE(A.CALL_END_YN, 'Y', 0, 1)
               ELSE NULL END                                                                   AS SR_EXPO_AMT
        , 'N'                                                                                  AS FR_CLSF_CD
        , '비대상'                                                                             AS FR_CLSF_NM                      /*  외환리스크분류명       */
        , NULL                                                                                 AS FR_EXPO_AMT
        , NULL                                                                                 AS CNPT_SUM_AMT
        , A.CNPT_ID                                                                            AS CNPT_ID
        , A.CNPT_NM                                                                            AS CNPT_NM
        , A.CORP_NO                                                                            AS CORP_NO
        , NULL                                                                                 AS CRGR_KIS
        , NULL                                                                                 AS CRGR_NICE
        , NULL                                                                                 AS CRGR_KR
        , NVL(A.CRGR_KICS, '99')                                                               AS CRGR_KICS
        , NULL                                                                                 AS CRGR_CB_NICE
        , NULL                                                                                 AS CRGR_CB_KCB
        , B.GOCO_CD                                                                            AS GRP_CD
        , B.GOCO_NM                                                                            AS GRP_NM
        , NULL                                                                                 AS LOAN_CONT_TPCD
        , NULL                                                                                 AS COLL_ID
        , NULL                                                                                 AS COLL_KND
        , NULL                                                                                 AS COLL_SET_AMT
        , NULL                                                                                 AS PRPT_DTLS_TPCD
        , A.LTV                                                                                AS LTV
        , NULL                                                                                 AS PRPT_OCPY_TPCD
        , NULL                                                                                 AS PRPT_CNTY_TPCD
        , NULL                                                                                 AS CONT_QNTY
        , NULL                                                                                 AS CONT_MULT
        , NULL                                                                                 AS CONT_PRC
        , NULL                                                                                 AS STOC_TPCD
        , NULL                                                                                 AS PRF_STOC_YN
        , NULL                                                                                 AS STOC_LIST_CLCD
        , 'Y'                                                                                  AS CPTC_YN
        , NULL                                                                                 AS BOND_RANK_CLCD
        , NULL                                                                                 AS ASSET_LIQ_CLCD
        , NULL                                                                                 AS HDGE_ISIN_CD
        , NULL                                                                                 AS HDGE_MATR_DATE
        , GREATEST(A.OFB_AMT, 0)                                                                            AS UUSE_LMT_AMT
        , A.CCF                                                                                AS CCF
        , NULL                                                                                 AS FIDE_UNDL_CLCD
        , NULL                                                                                 AS ARR_DAYS
        , NULL                                                                                 AS FLC_CD
        , NULL                                                                                 AS DFLT_YN
        , 0.0                                                                                  AS IR_EXPO_SCEN01
        , 0.0                                                                                  AS IR_EXPO_SCEN02
        , 0.0                                                                                  AS IR_EXPO_SCEN03
        , 0.0                                                                                  AS IR_EXPO_SCEN04
        , 0.0                                                                                  AS IR_EXPO_SCEN05
        , 0.0                                                                                  AS IR_EXPO_SCEN06
        , 0.0                                                                                  AS IR_EXPO_SCEN07
        , 0.0                                                                                  AS IR_EXPO_SCEN08
        , 0.0                                                                                  AS IR_EXPO_SCEN09
        , 0.0                                                                                  AS IR_EXPO_SCEN10
        , 0.0                                                                                  AS IR_EXPO_SCEN11
        , 0.0                                                                                  AS IR_EXPO_SCEN12
        , 0.0                                                                                  AS IR_EXPO_SCEN13
        , 0.0                                                                                  AS IR_EXPO_SCEN14
        , '999999'                                                                             AS DEPT_CD
        , 'KRDA107BM'                                                                          AS LAST_MODIFIED_BY
        , SYSDATE                                                                              AS LAST_UPDATE_DATE
-- 20230518 김재우 수정
-- 칼럼 추가
        , NULL AS CRGR_SNP	                          /*  신용등급(S&P) */
        , NULL AS CRGR_MOODYS	          /* 신용등급(무디스) */
        , NULL AS CRGR_FITCH	                  /*  신용등급(피치) */
        , NULL AS IND_L_CLSF_CD	          /* 산업대분류코드 */
        , NULL AS IND_L_CLSF_NM	          /* 산업대분류명 */
        , NULL AS IND_CLSF_CD	                  /* 산업분류코드 */
        , NULL AS OBGT_PSSN_ESTE_YN	  /* 의무보유부동산여부 */
        , NULL AS LVG_RATIO	                          /* 레버리지비율(%) */
        , NULL AS FV_RPT_COA	                  /*  공정가치보고서COA */
        , NULL AS FV_HIER_NODE_ID	          /* 공정가치계층노드ID */
        , NULL AS SNR_LTV	                          /* 선순위LTV(%)) */
        , NULL AS FAIR_VAL_SNR	                  /* 선순위대출금액 */
        , NULL AS DSCR	                                  /* DSCR */

     FROM IKRUSH_OFB A
        , (
             SELECT *
               FROM IDHKRH_COMPANY_INFO
              WHERE STND_DT = (
                                 SELECT MAX(STND_DT)
                                   FROM IDHKRH_COMPANY_INFO
                                  WHERE 1=1
                                    AND STND_DT <= '$$STD_Y4MD'
                              )
          ) B
        , (
             SELECT CORP_NO
                  , ASSET_CLSF_CD
                  , ASSET_CLSF_NM
               FROM IKRUSH_PUBI
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
          ) C
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
    WHERE 1=1
      AND A.BASE_DATE          = '$$STD_Y4MD'
      AND A.CORP_NO            = B.ICNO(+)
      AND A.CORP_NO            = C.CORP_NO(+)
      AND A.OFB_PROD_DTLS_TPCD = R.KICS_PROD_TPCD(+)
)
SELECT * FROM T_AAA