/* 수정사항
- 조기상환 키 :  ASIS 8자리 계정과목코드 -> 아래와 같이 SEG 특성에 따라 부여함.
    [LN_CONT_GB, COLL_KND]
    DECODE (A.LOAN_CONT_TPCD,  '10', 'PP_INDI', 'PP_CORP') ||
            DECODE (A.COLL_TPCD, '30', '_PROP'
                      , '40', '_CRDT'
                      , '50', '_GUNT'
                                          )   				 AS PREPAY_KEY
- 2024.04.15 후순위채권 : 자기신용위험스프레드 (IKRUSH_CR_SPREAD)
- 개인대출 : 잔여스프레드 (금감원제공; IKRUSH_SPREAD_RESIDUAL)
*/
SELECT /* SQL_ID : KRBH301BM */
       TO_DATE(A.BASE_DATE, 'YYYYMMDD')                 AS AS_OF_DATE            /*  기준일자              */
     , A.EXPO_ID                                   AS FCONT_ID              /*  금융계약ID            */
     , '1'                                              AS UNIQ_KEY              /*  유일 KEY              */
     , 'KICS'                                           AS EVENT_ID              /*  이벤트ID              */
     , A.PROD_TPCD                               AS F_PROD_ID             /*  금융상품ID            */
     , A.PROD_TPNM                               AS F_PROD_NM             /*  금융상품명            */
     , 0                                                AS SCEN_NUM              /*  시나리오번호          */
     , 'N'                                              AS VIRTUAL_CONT_YN       /*  가상계약여부          */
     , A.INST_TPCD                                      AS INST_TYP              /*  인스트루먼트 유형     */
     , '1'                                              AS RSLT_TYP_CD           /*  결과유형코드          */      -- 보유계약
     , '999'                                            AS FUND_TYP_CD           /*  펀드구분코드          */      -- 특별계정 관련 수정필요
     , A.DEPT_CD                                        AS ORG_UNIT              /*  조직코드              */
     , NULL                                             AS CHN_CD                /*  채널코드              */


/* COA별 할인율 구분적용 : Q_CB_DSCNT_COA  24.04.12
  - 개인대출 A.INST_TPCD = '1' : 무위험 (99999) + Residual spread 금감원 제시 ; IKRUSH_SPREAD_RESIDUAL
  - 기업대출 A.INST_TPCD = '2' : 무위험 + 위험스프레드 (RM_COA_ID)=>  할인율 커브에 위험스프레드 (국고채와 회사채 수익률간 차이)를 직접 반영함.   Q_CB_DSCNT_COA
  - 금융부채 SUBSTR(A.ACCO_CD,3,1)= '2' AND A.PROD_TPCD IN ('901', '903') :  무위험 (99999) + 잔여스프레드 (= 위험스프레드 - 신용위험스프레드 (금감원 제시 ; IKRUSH_CR_SPREAD_TOBE) )
*/
     , CASE WHEN  SUBSTR(A.ACCO_CD,3,1)= '2' AND A.PROD_TPCD IN ('901', '903')
                THEN '99999' -- 후순위 채권 :  할인율 : 무위험 할인율 + 잔여스프레드
                WHEN A.INST_TPCD = '2' -- 기업대출의 경우 위험스프레드를 반영한 할인율
                THEN CASE WHEN A.COLL_TPCD IN ('30')  THEN 'PB_'||NVL(A.CRGR_VLT, 'A0')            -- 담보대출은 K-ICS요건에 따라 NULL인 경우 A0 적용(4.0기준서 p.16)
                                  WHEN C.CRGR_VLT IS NOT NULL THEN 'PB_'||NVL(C.CRGR_VLT, 'BBB0')    -- AND C.CRGR_NUM_ORG > C.CRGR_NUM_ADJ
                        ELSE 'PB_'||NVL(A.CRGR_VLT, 'BBB0') END                                                        -- 그외대출은 K-ICS요건에 따라 NULL인 경우 BBB0 적용(4.0기준서 p.16)
              ELSE '99999' END                            AS RM_COA_ID             /*  RM_COA_ID             */

     , A.ACCO_CD                                        AS IFRS_COA_ID           /*  IFRS_COA_ID           */
     , A.INST_DTLS_TPCD                                 AS BOND_TYP              /*  채권유형코드          */
     , CASE WHEN A.COLL_TPCD IN ('20','30','80') THEN '1'
            ELSE '2' END                                AS LOAN_TYP              /*  대출유형코드          */
     , CASE WHEN A.INST_TPCD = '1' AND A.COLL_TPCD = '30'  AND A.PRPT_DTLS_TPCD = '11' THEN '1'                    -- 아파트
            WHEN A.INST_TPCD = '1' AND A.COLL_TPCD = '30' THEN '2'                                                 -- 아파트 외
            WHEN A.INST_TPCD = '1' AND A.COLL_TPCD IN ('10', '50') THEN '3'                                        -- 지급보증
            WHEN A.INST_TPCD = '1' AND A.COLL_TPCD IN ('20', '80') THEN '4'                                        -- 기타담보
            WHEN A.INST_TPCD = '1' AND A.COLL_TPCD IN ('40', '41') THEN '5'                                        -- 신용
            ELSE NULL END                               AS COLL_TYP              /*  신용보강유형          */      -- LGD 세그먼트 적재
     , NULL                                             AS COLL_DET_TYP          /*  신용보강상세유형      */
     , CASE WHEN A.INST_TPCD = '1' AND A.CRGR_CB_NICE IS NULL     AND A.CRGR_CB_KCB IS     NULL THEN 'NICE_13'     -- 99 무등급 처리에서 10등급 처리로 우선 수정
            WHEN A.INST_TPCD = '1' AND A.CRGR_CB_NICE IS NULL     AND A.CRGR_CB_KCB IS NOT NULL THEN 'KCB_' ||LPAD(CRGR_CB_KCB , 2,'0')
            WHEN A.INST_TPCD = '1' AND A.CRGR_CB_NICE IS NOT NULL AND A.CRGR_CB_KCB IS     NULL THEN 'NICE_'||LPAD(CRGR_CB_NICE, 2,'0')
            WHEN A.INST_TPCD = '1' AND A.CRGR_CB_NICE  >= A.CRGR_CB_KCB                         THEN 'NICE_'||LPAD(CRGR_CB_NICE, 2,'0')
            WHEN A.INST_TPCD = '1' AND A.CRGR_CB_NICE  <  A.CRGR_CB_KCB                         THEN 'KCB_' ||LPAD(CRGR_CB_KCB , 2,'0')
            ELSE NULL END                               AS CNTR_PTY_ID           /*  거래상대방ID          */      -- 신용등급 적재
     , NULL                                             AS CPTY_CRD_GRD_CD       /*  거래상대방신용등급코드*/
     , SUBSTR(A.FLC_CD,1,1)                             AS FLC_CD                /*  자산건전성분류코드    */
     , NULL                                             AS GUARANTY_ID           /*  보증기관ID            */
     , NULL                                             AS CPTY_GUAR_CRD_GRD_CD  /*  거래상대방 보증기관 신용등급코드 */
     , 1                                                AS AMT_SIGN              /*  금액부호              */
     , NULL                                             AS PRE_SELL_INT_TYP      /*  선매출처리구분코드    */
     , NULL                                             AS PRE_SELL_DATE         /*  선매출일자            */
     , TO_DATE(A.ISSU_DATE, 'YYYYMMDD')                 AS ISSUE_DATE            /*  발행일자              */
     , NULL                                             AS ORG_DATE              /*  매입일자              */
     , NULL                                             AS SETTL_DATE            /*  결제일자              */
     , NULL                                             AS ORG_SEQ               /*  매입차수              */
     , TO_DATE(A.MATR_DATE, 'YYYYMMDD')                 AS MATURITY_DATE         /*  만기일자              */
     , TO_DATE(A.MATR_DATE, 'YYYYMMDD')                 AS LIQ_MATURITY_DATE     /*  유동성만기일자        */
     , A.NOTL_AMT                                       AS NOTIONAL              /*  액면금액              */
     , A.LMT_AMT                                        AS LIMIT_AMT             /*  한도금액              */
     , NULL                                             AS SELL_RATIO            /*  매각비율              */
     , 'KRW'                                            AS CUR_CD                /*  통화코드              */
     , 'QCM_DEF'                                        AS FX_SEG                /*  환율 세그             */
-- 20230601
-- 김재우수정
-- 기업대출 변동금리 처리
--     , CASE WHEN A.INST_TPCD = '1' THEN A.IRATE_TPCD
--            ELSE '1' END                                AS IR_TYP_CD             /*  금리유형코드          */      -- 기업대출 고정금리 처리
     , CASE WHEN A.INST_TPCD = '1' THEN A.IRATE_TPCD
            ELSE '1' END                                AS IR_TYP_CD             /*  금리유형코드          */      -- 기업대출 고정금리 처리
     , NULL                                             AS STRUC_TYP             /*  구조화유형            */
     , A.IRATE                                          AS COUPON_RATE           /*  표면금리              */
     , A.IRATE                                          AS LAST_RESET_RATE       /*  최종금리              */
     , NULL                                             AS NEXT_RESET_RATE       /*  차기금리              */
--     , IRATE                                            AS NEXT_RESET_RATE       /*  차기금리              */
     , NULL                                             AS LAST_RESET_DATE       /*  직전금리개정일        */
     , NULL                                             AS NEXT_RESET_DATE       /*  차기금리개정일        */
     , NULL                                             AS ORG_RATE              /*  매입수익률            */
     , NULL                                             AS G_YTM                 /*  만기보장수익률        */
     , NULL                                             AS PRMTM                 /*  만기상환율            */
     , 1                                                AS SPREAD_TYP_CD         /*  스프레드 방법코드     */
     , A.ADD_SPRD                                       AS ACCRUAL_SPREAD        /*  부리SPREAD            */

     , NULL                                             AS CREDIT_SPREAD         /*  CREDIT SPREAD    (기업대출 : 위험스프레드를 할인율에 TERM STURCTURE 형태로 반영함 => RM_COA_ID별 적용 )     */

/* (잔여스프레드)  24.04.12
  - 개인대출 A.INST_TPCD = '1' : 금감원 제시 ; IKRUSH_SPREAD_RESIDUAL
  - 기업대출 A.INST_TPCD = '2' : 위험스프레드 적용하며, 할인율 커브에 위험스프레드 (국고채와 회사채 수익률간 차이)를 직접 반영함.  => RM_COA_ID 별로 구분하여 적용 ; Q_CB_DSCNT_COA
  - 금융부채 SUBSTR(A.ACCO_CD,3,1)= '2' AND A.PROD_TPCD IN ('901', '903') :  잔여스프레드 = 위험스프레드 - 신용위험스프레드 (금감원 제시 ; IKRUSH_CR_SPREAD_TOBE)
*/
     , CASE WHEN A.INST_TPCD = '1'  THEN NVL(D.SPREAD, 0) / 100  /*개인대출*/
		  WHEN SUBSTR(A.ACCO_CD,3,1)= '2' AND A.PROD_TPCD IN ('901', '903') THEN /*금융부채 */
                                        ( F.PREV_SPRD + (F.SPRD -F.PREV_SPRD) * ( A.RES_MON - F.PREV_NUM) / (F.MAT_NUM - F.PREV_NUM)) /100   -- 위험스프레드 (회사채수익률 - 국고채수익률)
				-	( E.PREV_SPRD + (E.SPRD -E.PREV_SPRD) * ( A.RES_MON - E.PREV_NUM) / (E.MAT_NUM - E.PREV_NUM)) /100       -- 신용위험스프레드 (감독원장 제공)
                  ELSE NULL
             END                                        AS RESIDUAL_SPREAD       /*  RESIDUAL SPREAD       */
        /* CHECK
                                ,   A.CRGR_VLT
                                ,        ( F.PREV_SPRD + (F.SPRD -F.PREV_SPRD) * ( A.RES_MON - F.PREV_NUM) / (F.MAT_NUM - F.PREV_NUM)) /100   AS RISK_SP-- 위험스프레드 (회사채수익률 - 국고채수익률)
				,	( E.PREV_SPRD + (E.SPRD -E.PREV_SPRD) * ( A.RES_MON - E.PREV_NUM) / (E.MAT_NUM - E.PREV_NUM))        AS CR_SP-- 신용위험스프레드 (감독원장 제공)
*/

     , NULL                                             AS MARGIN_SPREAD         /*  MARGIN SPREAD         */
     , NULL                                             AS DELAY_RATE            /*  연체이율              */
     , NULL                                             AS LAPS_RATE             /*  중도해지이율          */
     , NULL                                             AS AFT_MAT_RATE          /*  만기후이율            */
     , A.IRATE_CAP                                      AS RATE_CAP              /*  RATE CAP              */
     , A.IRATE_FLO                                      AS RATE_FLOOR            /*  RATE FLOOR            */
     , 'KDSP1000'                                       AS IRC_ID                /*  IRC_ID                */
     , NULL                                             AS MAT_CD                /*  만기코드              */
     , NULL                                             AS VOL_FACTOR_ID         /*  변동성요인ID          */
     , NULL                                             AS VOL_FACTOR_BETA       /*  변동성요인 베타       */
     , NULL                                             AS VOL_FACTOR_ORG_IDX    /*  변동성요인 매입시점 지수 */
     , A.IRATE_RPC_CYC                                  AS REPRICING_TERM        /*  금리개정주기          */
     , 'M'                                              AS REPRICING_TERM_UNIT   /*  금리개정타임유닛코드  */
     , 'CALKOR'                                         AS CLD_ID                /*  달력ID                */
     , 0                                                AS NAT_BIZ_DAY_RULE_CD   /*  법정휴일 영업일 규칙  */
     , 0                                                AS NON_NAT_BIZ_DAY_RULE_CD  /*  비법정휴일 영업일 규칙  */
     , NULL                                             AS BIZ_DAY_NUM           /*  BIZ DAY NUM           */
     , 'Y'                                              AS EOM_RULE_APL_YN       /*  말일규칙 적용여부     */
     , '1'                                              AS ACCRUAL_BASIS         /*  이자계산코드          */      -- 대출 단리채 처리
     , 3                                                AS CPN_RND_RULE_CD       /*  이자 라운딩 규칙코드  */      -- ROUND 처리
     , 0                                                AS CPN_DECIMAL_PLACE     /*  이자 소숫점 자리수    */      -- 소수점 0으로 처리
     , 'F'                                              AS COUPON_GEN_METHOD     /*  이표생성방식          */
     , A.DCB_CD                                         AS DCB_CD                /*  일수계산방식코드      */
     , 'Y'                                              AS IS_COUPON_PRORATED    /*  이자 날짜비례배분여부 */
     , A.INT_PAY_CYC                                    AS PAYMENT_TERM          /*  이자지급주기          */
     , 'M'                                              AS PAYMENT_TERM_UNIT     /*  이자지급주기 타임유닛코드 */
     , A.INT_PROR_PAY_YN                                AS IS_COUPON_FIRST       /*  이자선취여부          */
     , NULL                                             AS LAST_INT_PAY_DATE     /*  직전이자지급일        */
     , NULL                                             AS LAST_PAY_DATE         /*  직전 지급일           */
     , NULL                                             AS NEXT_PAY_DATE         /*  차기 지급일           */
     , NULL                                             AS REG_COUPON_STR_DATE   /*  정규이표구간시작일    */
     , 'S'                                              AS ODD_COUPON_TYP        /*  ODD 쿠폰속성          */
     , NULL                                             AS PRIN_RND_RULE_CD      /*  원금 라운딩규칙코드   */
     , NULL                                             AS PRIN_DECIMAL_PLACE    /*  원금 소숫점 자리수    */
     , A.AMORT_TPCD                                     AS AMORT_TYP             /*  분할상환유형코드      */
     , NULL                                             AS NOTIONAL_AT_END       /*  만기원금교환여부      */
     , NULL                                             AS NOTIONAL_AT_START     /*  최초원금교환여부      */
     , A.GRACE_TERM                                     AS PRIN_DEFERRED_TERM    /*  원금거치기간          */      -- 확인 후 처리
     , 'M'                                              AS PRIN_DEFERRED_TERM_UNIT /*  원금거치기간 타임유닛코드 */
     , 2                                                AS PRIN_DFRD_DCB_CD      /*  원금거치기간 일수계산방식 */  -- ACT/365
     , TO_DATE(A.MATR_DATE, 'YYYYMMDD')                 AS AMORT_MAT_DATE        /*  상환만기일자          */      -- 만기일자와 동일하게 처리
     , 1                                                AS AMORT_TERM_MULT       /*  원금지급주기          */
     , NULL                                             AS PRIN_GUAR_RATIO       /*  원금보장비율          */
     , NULL                                             AS INSTL_TERM            /*  적금불입주기          */
     , NULL                                             AS INSTL_TERM_UNIT       /*  적금불입주기 타임유닛코드  */
     , NULL                                             AS INSTL_TOT_CNT         /*  적금불입총횟수        */
     , NULL                                             AS INSTL_LAPS_CUM        /*  적금경과적수          */
     , NULL                                             AS INSTL_TOT_CUM         /*  적금총적수            */
     , NULL                                             AS ANNU_DEFERRED_TERM    /*  연금거치기간          */
     , NULL                                             AS ANNU_DEFERRED_TERM_UNIT /*  연금거치기간 타임유닛코드 */
     , NULL                                             AS ANNU_TERM             /*  연금지급주기          */
     , NULL                                             AS ANNU_TERM_UNIT        /*  연금지급주기 타임유닛코드 */
     , 'R'                                              AS PI_RESCH_METH         /*  원리금 RESCHEDULING 방법 */
--     , CASE WHEN AMORT_TPCD IN ('0', '1') THEN 'P'
--            ELSE 'A' END                                AS PI_RESCH_METH         /*  원리금 RESCHEDULING 방법 */
     , 'Y'                                              AS EQ_PAYMENT_CALC_METH  /*  원리금 균등액 산출방법 */
     --, DECODE(A.AMORT_AMT, 0, NULL, A.AMORT_AMT)        AS PRIN_PAYMENT          /*  상환액 */
     , NULL                                             AS PRIN_PAYMENT          /*  상환액 */
     , NULL                                             AS AT_MAT_PRIN_PAYMENT   /*  만기상환액            */
     , A.BS_AMT                                         AS CUR_BOOK_BAL          /*  장부금액              */
     , NULL                                             AS EXT_FV                /*  외부 공정가치         */
     , NULL                                             AS EXT_DURATION          /*  외부 듀레이션         */
     , A.BS_AMT                                         AS CUR_PAR_BAL           /*  현재잔액              */      -- 계산기준원금
     , NULL                                             AS AVG_BAL               /*  평균잔액              */
     , NULL                                             AS LLP                   /*  대손충당금            */
     , NULL                                             AS UNHOLD_AI             /*  미보유경과이자        */
     , A.UERN_AMT                                       AS INT_ADVANCED          /*  선수이자              */
     , A.ACCR_AMT                                       AS INT_RECEIVABLE        /*  미수이자              */
     , NULL                                             AS UNAMORT_BAL_DISC_PREM /*  미상각잔액            */
     , NULL                                             AS UNAMORT_BAL_LOC_F     /*  미상각잔액            */
     , NULL                                             AS UNAMORT_BAL_PPAID_COST /*  미상각잔액 선급비용  */
     , NULL                                             AS PSD_PAY_AMT           /*  가지급금              */
     , NULL                                             AS RECEIVABLE            /*  미수금                */
     , NULL                                             AS RECEIVABLE_VAL        /*  미수잔가              */
     , NULL                                             AS PP_FEE_FREE_YN          /*  조기상환 수수료 면제 대상 여부   */ -- 찾아보고 입력 Y: 차주가 근저당 설정비 등 대출관련 부대비용을 부담하여 중도 상환 수수료가 면제 된 경우
     , NULL                                             AS PP_FEE_FREE_NOT_APL_YN  /*  조기상환 수수료 면제 미적용 여부 */ -- Y: 담보인정비율(LTV)을 40% 이상 적용받은 고객은 3년이 지나더라도 만기 전에 갚으면 상환금액의 0.5%를 수수료로 내는 등 일반적인 조기상환 수수료 면제 구조를 따르지 않을 경우
     , NULL                                             AS UNDERLYING_STOCK_ID   /*  기초자산 주식ID */
     , NULL                                             AS STAGE                 /*  STAGE */
     , NULL                                             AS IFRS_IMP_YN           /*  IFRS 손상여부         */
     , NULL                                             AS IFRS_IMP_DATE         /*  손상일자              */
     , NULL                                             AS DELAY_STRT_DATE       /*  연체기산일자          */
     , NULL                                             AS DELAY_CNT             /*  연체회차              */
     , A.ARR_DAYS                                       AS DELAY_TOT_DAY_CNT     /*  연체총일수            */
     , NULL                                             AS PRIN_DELAY            /*  연체원금              */
     , NULL                                             AS INT_DELAY             /*  연체이자              */
     , NULL                                             AS HEDGE_F_PROD_ID       /*  헤지대상 금융상품 ID  */
     , NULL                                             AS HEDGE_ORG_DATE        /*  헤지대상 매입일자     */
     , NULL                                             AS HEDGE_ORG_SEQ         /*  헤지대상 매입차수     */
     , NULL                                             AS FULL_HEDGE_YN         /*  완전헤지 여부         */
     , NULL                                             AS WORK_UNIT             /*  작업 단위             */
     , CASE WHEN B.PREPAY_KEY IS NOT NULL THEN B.PREPAY_KEY
            WHEN A.LOAN_CONT_TPCD = '10'  THEN 'PP_INDI'
            WHEN A.LOAN_CONT_TPCD = '20'  THEN 'PP_CORP'
            WHEN A.LOAN_CONT_TPCD = '30'  THEN 'PP_BOND'
            WHEN A.LOAN_CONT_TPCD = '40'  THEN 'PP_FUND'
            ELSE NULL END                               AS PREPAY_KEY            /*  조기상환 KEY          */      -- 계정과목별 조기상환율 SEG 매핑
     , NULL                                             AS INSTLS_RAT_KEY        /*  적금불입률 KEY        */
     , NULL                                             AS MAT_EXT_RAT_KEY       /*  만기연장률 KEY        */
     , NULL                                             AS CORED_RAT_KEY         /*  핵심잔고비율 KEY      */
     , NULL                                             AS RBC_COA_IR            /*  RBC COA 금리          */
     , NULL                                             AS RBC_COA_MR            /*  RBC COA 시장          */
     , NULL                                             AS RBC_COA_CR            /*  RBC COA 신용          */
     , NULL                                             AS LCR_HQLA_KEY          /*  LCR_고유동성자산 KEY  */
     , NULL                                             AS LCR_CF_OUT_KEY        /*  LCR_현금유출 KEY      */
     , NULL                                             AS LCR_CF_IN_KEY         /*  LCR_현금유입 KEY      */
     , A.INST_TPCD                                      AS AGGR_KEY_1            /*  집계 KEY 1            */
     , A.FUND_CD||'_'||SUBSTR(A.ACCO_CD, 3, 10) AS AGGR_KEY_2            /*  집계 KEY 2            */
     , 'KRBH301BM'                                      AS LAST_MODIFIED_BY
     , SYSDATE                                          AS LAST_UPDATE_DATE
--	 , ( PREV_SPRD + (SPRD -PREV_SPRD) * ( RES_MON - PREV_NUM) / (MAT_NUM - PREV_NUM)) /1000   AS CALC_SPRD
--	 , A.RES_MON
--	 , A.CRGR_VLT
--	 , E.PREV_SPRD
--		, E.SPRD
--		, E.PREV_NUM
--		, E.MAT_NUM
  FROM (
              SELECT AA.*
              , TO_NUMBER(GREATEST (
                           LEAST(
                                  CEIL((TO_DATE(AA.MATR_DATE,'YYYYMMDD') - TO_DATE('$$STD_Y4MD','YYYYMMDD')) /365)
                                  , 31
                                )
                          , 1
                         ))            AS RMN_MATR_CLCD
               , CASE WHEN LAST_MODIFIED_BY='KRBH101BM' THEN  /*융자데이터 입수의 경우에만 대출세부속성에 따라 조기상환 KEY 매핑 */
                 DECODE (LOAN_CONT_TPCD,  '10', 'PP_INDI', 'PP_CORP') ||
				 DECODE (COLL_TPCD, '30', '_PROP'
								  , '40', '_CRDT'
								  , '50', '_GUNT'
--                                      , 'ETC'
                                      )
                  ELSE NULL  END 				 AS PREPAY_KEY
		, CEIL(MONTHS_BETWEEN ( TO_DATE(MATR_DATE, 'YYYYMMDD'), TO_DATE(BASE_DATE, 'YYYYMMDD') )) AS RES_MON

           FROM Q_IC_ASSET_LOAN AA
          WHERE 1=1
            AND AA.BASE_DATE = '$$STD_Y4MD'
       )  A
     , ( SELECT *
            FROM Q_CB_PREPAY_CONST
           WHERE APPLY_START_DATE = (
                                      SELECT MAX(APPLY_START_DATE)
                                        FROM Q_CB_PREPAY_CONST
                                       WHERE APPLY_START_DATE <= TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
                           )
       )B
     , (
          SELECT /*+ USE_HASH(C1 C2) */
                 C1.EXPO_ID
               , C1.ACCO_CD
               , C1.IRATE
               , C1.MAT_TERM
               , C2.STR_IRATE
               , C2.END_IRATE
               , C1.BS_AMT
               , C1.CRGR_VLT AS CRGR_VLT_ORG
               , C2.CRGR_VLT AS CRGR_VLT
               , (SELECT NVL(CD, '9999')
                   FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD_NM = C1.CRGR_VLT)                                 AS CRGR_NUM_ORG
               , (SELECT NVL(CD, '9999')
                   FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD_NM = C2.CRGR_VLT)                                 AS CRGR_NUM_ADJ

            FROM (
                    SELECT LEAST(GREATEST(
                           TRUNC(NVL(MONTHS_BETWEEN(TO_DATE(T1.MATR_DATE, 'YYYYMMDD'), TO_DATE(T1.BASE_DATE, 'YYYYMMDD')), 36))
                           , 1), 1200)                                                                                  AS MAT_TERM
                         , T1.*
                      FROM Q_IC_ASSET_LOAN T1
                     WHERE 1=1
                       AND BASE_DATE = '$$STD_Y4MD'
                       AND INST_TPCD = '2'
                       AND NVL(COLL_TPCD, '99') NOT IN ('30')
                       AND SUBSTR(EXPO_ID, 6, 1) IN ('L')
                 ) C1
               , (
                    SELECT SUBSTR(VOL_FACTOR_ID, 4)                                                                     AS CRGR_VLT
                         , MAT_TERM                                                                                     AS MAT_TERM
                         , 0.5 * (NVL(LAG (SCEN_VAL) OVER (PARTITION BY MAT_TERM ORDER BY SCEN_VAL), -1) + SCEN_VAL)    AS STR_IRATE
                         , 0.5 * (NVL(LEAD(SCEN_VAL) OVER (PARTITION BY MAT_TERM ORDER BY SCEN_VAL),  1) + SCEN_VAL)    AS END_IRATE
                      FROM Q_CM_SCEN_RSLT
                     WHERE 1=1
                       AND SUBSTR(VOL_FACTOR_ID, 1, 2) = 'PB'
                       AND SCEN_NO = 1
                       AND AS_OF_DATE = TO_DATE('$$STD_Y4MD', 'YYYYMMDD')
                 ) C2
            WHERE 1=1
              AND C1.MAT_TERM =  C2.MAT_TERM(+)
              AND C1.IRATE    >= C2.STR_IRATE
              AND C1.IRATE    <  C2.END_IRATE
       ) C
     , (
          SELECT SPREAD
            FROM IKRUSH_SPREAD_RESIDUAL
           WHERE BASE_YM = (
                              SELECT MAX(BASE_YM)
                                FROM IKRUSH_SPREAD_RESIDUAL
                               WHERE BASE_YM <= SUBSTR('$$STD_Y4MD', 1, 6)
                           )
       ) D
--     , (
--     	  SELECT 'LOAN_O_G110KR6095082AB20142020113000001' AS EXPO_ID, 0.01 AS SPREAD FROM DUAL UNION ALL
--     	  SELECT 'LOAN_O_G110KR6095081AB40142020111600003' AS EXPO_ID, 0.01 AS SPREAD FROM DUAL UNION ALL
--     	  SELECT 'LOAN_O_G110KR6095081AB40142020111600002' AS EXPO_ID, 0.01 AS SPREAD FROM DUAL UNION ALL
--     	  SELECT 'LOAN_O_G110KR6095081AB40142020111600001' AS EXPO_ID, 0.01 AS SPREAD FROM DUAL UNION ALL
--     	  SELECT 'LOAN_O_G110KRK1113100010142013101000001' AS EXPO_ID, 0.00 AS SPREAD FROM DUAL UNION ALL
--     	  SELECT 'LOAN_O_G110KR60950817350142017033100002' AS EXPO_ID, 0.00 AS SPREAD FROM DUAL
--     	 ) E  -- 금융부채 자기신용위험스프레드
--     	 , (
--           SELECT *
--             FROM IKRUSH_CR_SPREAD
--            WHERE BASE_DATE = '$$STD_Y4MD'
--     	 ) E
,
            ( /*금감원 제공 : 신용위험스프레드 => 자기신용위험 제거 목적 (;회사의 신용등급에 해당하는 신용위험스프레드 적재) */
                SELECT A.*
                  , '9' AS PROD_DV
                    , CASE WHEN PREV_NUM IS NULL  THEN SPRD / 3
                            ELSE DIFF_SPRD / DIFF_MON
                            END AS FACTOR
                FROM (
                    SELECT  AA.*
                        , LAG(MAT_NUM,1,0) OVER (ORDER BY MAT_NUM) +1                   AS PREV_NUM -- 2024.10.29 GoF 수정 (경계값 중복 오류수정)
                        , LAG(SPRD,1,0) OVER (ORDER BY MAT_NUM)                     AS PREV_SPRD -- 2024.10.29 GoF 수정
                        , MAT_NUM - LAG(MAT_NUM,1,0) OVER (ORDER BY MAT_NUM)            AS DIFF_MON -- 2024.10.29 GoF 수정
                        , SPRD - LAG(SPRD,1,0) OVER (ORDER BY MAT_NUM)              AS DIFF_SPRD -- 2024.10.29 GoF 수정
                    FROM (
                              SELECT  RMN_MATR_CLCD AS MAT_CD
                                    , REPLACE(RMN_MATR_CLCD,31,100)*12 AS MAT_NUM
                                    , CR_SPREAD AS SPRD
                             FROM IKRUSH_CR_SPREAD
                             WHERE BASE_DATE='$$STD_Y4MD'
                            ) AA
                    )	A
            ) E
	, ( /* 위험스프레드 */
      SELECT A.*
        , '9' AS PROD_DV
			, CASE WHEN PREV_NUM IS NULL  THEN SPRD / 3
					ELSE DIFF_SPRD / DIFF_MON
					END AS FACTOR
		FROM (
			SELECT  AA.*
        , REPLACE(REPLACE(REPLACE(CRD_GRD,'+',''),'-',''),'0','') AS CRD_GRD_MAP
        , LAG(MAT_NUM,1,0) OVER (PARTITION BY CRD_GRD ORDER BY MAT_NUM) +1                 AS PREV_NUM -- 2024.10.29 GoF 수정 (경계값 중복 오류수정)
        , LAG(SPRD,1,0) OVER (PARTITION BY CRD_GRD ORDER BY MAT_NUM)                    AS PREV_SPRD -- 2024.10.29 GoF 수정
        , MAT_NUM - LAG(MAT_NUM,1,0) OVER (PARTITION BY CRD_GRD ORDER BY MAT_NUM)   AS DIFF_MON -- 2024.10.29 GoF 수정
        , SPRD - LAG(SPRD,1,0) OVER (PARTITION BY CRD_GRD ORDER BY MAT_NUM)             AS DIFF_SPRD -- 2024.10.29 GoF 수정 
			FROM (

					SELECT  CRD_GRD
						, MAT_CD
						, TO_NUMBER(SUBSTR(MAT_CD,2,3)) AS MAT_NUM
						, SPRD
					FROM (
						SELECT A.*
						FROM IKRUSH_SPREAD_BD A
						WHERE 1=1
						AND BASE_YM = SUBSTR('$$STD_Y4MD', 1,6)
						AND BOND_TYP LIKE '%금융기관채%'
						)
					UNPIVOT ( SPRD  FOR MAT_CD IN ( M003, M006, M009, M012, M018,M024, M030, M036, M048, M060, M084, M120, M180, M240, M360, M600))
					) AA
			)	A
	 )  F
 WHERE 1=1
   AND A.PREPAY_KEY = B.PREPAY_KEY(+)
   AND A.EXPO_ID = C.EXPO_ID(+)
   AND SUBSTR(A.PROD_TPCD,1,1) = E.PROD_DV (+)
   AND SUBSTR(A.PROD_TPCD,1,1) = F.PROD_DV (+)
   AND A.CRGR_VLT = F.CRD_GRD(+)
   AND A.RES_MON BETWEEN F.PREV_NUM(+)  AND F.MAT_NUM(+)
   AND A.RES_MON BETWEEN E.PREV_NUM(+)  AND E.MAT_NUM(+)
