/* 수정사항 
- 계정코드별 예외처리 : ACCO_EXCPTN
- ACCO_MST 계정코드 마스터 (신규추가) : 계정코드별 계정코드명 

1. ACCO_EXCPTN 기존 프로그램내 계정과목별 예외 처리 
   A.ACCO_CD IN ( SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'BV2FV' ) => '1111090260' 기타예금_수익증권익일매수 : 수익증권 익일매수는 공정가치 입수되나, 장부가 처리함. 
2. INST_DTLS_TPCD 인스트루먼트상세코드 : WHEN A.SEC_CLASS IN ('O05') THEN '4' -- MMF 금리부 자산
3. IS_IRATE_ASSET : AA.SEC_CLASS IN ('O05') THEN '1'	-- 20231213 김재우 추가 MMF 금리부자산 처리 20240412 코드 오타 수정 ; 005 --> O05 
4. 2024.05.09 일수계산방식 변경 DCB_CD ='1'
5. 2024.05.28 IKRUSH_PROD_SECR 기준월에 해당되는 수기_상품(유가증권)정보만 사용하도록 수정  
*/
WITH /* SQL-ID : KRBH102BM */
ACCO_EXCPTN AS (
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRBH102BM'
)  
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
, SECR AS (
        SELECT A.GRDAT AS BASE_DATE 
             , 'SECR_O_'||A.POSITION_ID                                                             AS EXPO_ID                         /*  익스포저ID             */
             , A.PORTFOLIO                                                                          AS FUND_CD                         /*  펀드코드               */
             , KICS.PROD_TPCD                                                                       AS PROD_TPCD                       /*  상품유형코드           */
             , KICS.PROD_TPNM                                                                       AS PROD_TPNM                       /*  상품유형명             */  
             , COALESCE(A.KICS_PROD_TPCD,K.KICS_TPCD, NVL(KICS.KICS_TPCD, 'ZZZ'))                   AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
             , COALESCE(A.KICS_PROD_TPCD,K.KICS_TPNM, NVL(KICS.KICS_TPNM, '기타자산'))                 AS KICS_PROD_TPNM      	
             , A.ISIN                                                                               AS ISIN_CD                         /*  종목코드               */
             , A.SECURITY_ID_T                                                                      AS ISIN_NM                         /*  종목명                 */
             , A.FNDS_ACCO_CLCD||'_'||GL_ACCOUNT2                                    AS ACCO_CD                         /*  계정과목코드      */  
             , ACCO_MST.ACCO_NM                                                                AS ACCO_NM                         /*  계정과목명             */
             ,  A.POSITION_ID                                                                       AS CONT_ID                         /*  계약번호(계좌일련번호) */
	     ,  CASE WHEN  A.IS_IRATE_ASSET = '1' THEN 'B'
		            ELSE 'E'
		              END  AS INST_TPCD
	    , CASE WHEN A.ASSETCODE = 'BD'
		         THEN  CASE WHEN M.IRATE_TPCD = '1' THEN '4' --이표채
				                WHEN A.LATO_DVSN = '02'  AND M.POSITION_ID IS NULL THEN '4'  --변동금리 정보를 미입력시 이표채 처리
				                WHEN A.PRODUCT_TYPE =  '41A' THEN '1' --할인채
				                WHEN A.PRODUCT_TYPE =  '4AA' THEN '2' --단리채
				                WHEN A.PRODUCT_TYPE =   '42A' THEN '3' --복리채                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
					        WHEN A.PRODUCT_TYPE =   '43A' THEN '4' -- 이표채
					        WHEN A.PRODUCT_TYPE IN ( '04H','46A', '55A', '55B')  THEN '5' -- FRN ( 04H변동이자채권, 46A 변동금리 55A 정기예금(변동금리), 55B 외화정기예금(변동금리)
					        WHEN A.PRODUCT_TYPE =   '43B' THEN '6' -- 분할상환
					        WHEN A.PRODUCT_TYPE IN ( '45A', '45B') THEN '8' -- 만기일시(  복+단, 45A 5복2단, 45B 5복4단 )
					        ELSE '3' 
						   END	
			 WHEN A.SEC_CLASS IN ('O05') THEN '4' -- MMF 는 이표채 처리	ASIS에 비금리 처리됨 => 20240412 주석처리 해제 : 금리부 자산으로 처리 
                          ELSE  'Z'      /*  Z : 비금리             */
			    END AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */		  
	 --1 할인채 2 복리채 3 단리채 4 고정이표채 5 변동이표채 6 분할상환채 7 분할상환변동채 8 적금 연금				
	     ,  CASE WHEN A.ISSUE_DATE = '00000000' THEN 
		                       CASE WHEN ASSETCODE IN ('MM') THEN TO_CHAR(TO_DATE('$$STD_Y4MD','YYYYMMDD') , 'YYYYMMDD')
	                                           WHEN ASSETCODE IN ('ST','OS') THEN TO_CHAR(TO_DATE('19000101','YYYYMMDD'),'YYYYMMDD')
	                                           ELSE NULL 
	                                             END
			   ELSE A.ISSUE_DATE  
			     END  AS ISSU_DATE                       /*  발행일자               */
	     ,  CASE WHEN A.SEC_CLASS = 'O05' THEN TO_CHAR(ADD_MONTHS(TO_DATE('$$STD_Y4MD','YYYYMMDD'), 6) ,'YYYYMMDD') --MMF는 요건정의에 따라 6개월 처리( 듀레이션 미입수됨)
	             WHEN A.EDDT = '00000000' THEN 
		                       CASE WHEN ASSETCODE IN ('MM') THEN TO_CHAR(TO_DATE('$$STD_Y4MD','YYYYMMDD')+1 , 'YYYYMMDD') --단기금융중 만기가 들어오지 않는 건은 1일 처리 
	                                           WHEN ASSETCODE IN ('ST','OS') THEN TO_CHAR(TO_DATE('20991231','YYYYMMDD'),'YYYYMMDD') -- 만기없는 주식, 수익증권 은 20991231 처리
	                                           ELSE NULL 
	                                             END
			   ELSE A.EDDT  
			     END                                                          AS MATR_DATE                       /*  만기일자               */                      /*  만기일자               */
	     ,  A.POSITION_CURR                                            AS  CRNY_CD                         /*  통화코드               */
       ,  A.KURSF                                                              AS CRNY_FXRT                       /*  통화환율(기준일)       */
	     ,  CASE WHEN A.ASSETCODE = 'MM' THEN A.BOOK_POS_AMT  --단기금융은 장부금액(통화)
	             WHEN A.SEC_CLASS = 'O05' THEN A.BOOK_VAL_AMT --MMF는 원화이며, 장부금액(통화)금액이 1로 입수되어, 원화장부금액 처리 
	             ELSE A.NOMINAL_AMT                                                
	              END                                                          AS NOTL_AMT                        /*  이자계산기준원금       */    
       ,  CASE WHEN A.ASSETCODE = 'MM' THEN A.BOOK_POS_AMT --단기금융은 장부금액(통화)
               WHEN A.SEC_CLASS = 'O05' THEN A.BOOK_VAL_AMT --MMF는 원화이며, 장부금액(통화)금액이 1로 입수되어, 원화장부금액 처리
	             ELSE A.NOMINAL_AMT                                                
	              END                                                          AS NOTL_AMT_ORG                    /*  최초이자계산기준원금   */     
	     ,  A.BOOK_VAL_AMT                                             AS BS_AMT                          /*  BS금액                 */
	     ,	A.FAIR_VALUE_VAL_AMT                                  AS VLT_AMT                         /*  평가금액               */    
             , CASE WHEN NVL(J.UPRC_CLEAN, 0) <> 0
--                          THEN J.UPRC_CLEAN / J.UPRC_UNIT * A.NOMINAL_AMT * A.KURSF + (A.ACC_INT_VAL_AMT - A.PRPF_DMST)                                                          -- 수기 CLEAN단가 테이블(IKRUSH_UPRC)에 포함된 채권의 공정가치 반영
                          THEN J.UPRC_CLEAN / J.UPRC_UNIT * A.NOMINAL_AMT * A.KURSF  -- 수기 CLEAN단가 테이블(IKRUSH_UPRC)에 포함된 채권의 공정가치 반영
                          WHEN A.ASSETCODE = 'MM' THEN A.BOOK_VAL_AMT -- 단기금융상품은 CMA , 예치금 등이며, 모두 장부가 처리(만기X), 구조화 정기예금은 채권으로 분류되어 있음
--			  WHEN A.GL_ACCOUNT = '0011231600' THEN A.BOOK_VAL_AMT --수익증권 익일매수는 공정가치가 입수되나, 장부가 처리함
                          WHEN A.GL_ACCOUNT IN ( SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'BV2FV' ) THEN A.BOOK_VAL_AMT 
			  WHEN A.FAIR_VALUE_VAL_AMT = 0 THEN A.BOOK_VAL_AMT
			  ELSE A.FAIR_VALUE_VAL_AMT
			    END AS FAIR_BS_AMT  /*  공정가치B/S금액(외부)  */
            ,  A.ACC_INT_VAL_AMT                                                                 AS ACCR_AMT                        /*  미수수익               */
            ,  A.PRPF_DMST                                                                            AS UERN_AMT                        /*  선수수익               */			
            , CASE WHEN A.SEC_CLASS IN ('O05') THEN L.IRATE  --MMF는 AAA 회사채 만기 6개월 금리정보 가져옴                                                                      -- MMF 요건처리(12600300), C42 및 C44(MMDA/CMA 등)은 일반 단기예금처리
                           ELSE NVL(  B.FACE_ITRT
                                               , NVL(DECODE(A.CUPR, 0, NULL, A.CUPR / 100)
--                                               , NVL(DECODE(A.APPYLD, 0, NULL, A.APPYLD / 100)
                                               , NVL(A.APCN_LATO / 100, 0) )) 
                            END                                                                             AS IRATE                           /*  금리                   */
            , NVL(  B.INRS_PAY_CYCL_VAL * DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1)
                         , 12 / NVL(DECODE(A.ITTM, 0, NULL, A.ITTM), 1) )               AS INT_PAY_CYC                     /*  이자지급/계산주기(월)  */
--            , CASE WHEN A.IS_IRATE_ASSET = '1' AND A.POCP_DAPY_CLCD = '01'
--                          THEN 'Y' ELSE NULL END                                                          AS INT_PROR_PAY_YN                 /*  이자선지급여부(Y: 후취)*/
            , NULL 			          AS INT_PROR_PAY_YN                 /*  이자선지급여부(Y: 후취)*/ 
            , CASE WHEN  A.ASSETCODE IN ('BD','MM')  
                          THEN CASE WHEN M.IRATE_TPCD = '1' THEN '1'
						 WHEN  A.LATO_DVSN = '02'  AND M.POSITION_ID IS NULL THEN '1'  --변동금리 정보를 미입력시 고정금리 처리
						 WHEN A.LATO_DVSN = '02' THEN '2' 
						   ELSE '1' 
			                            END
			   WHEN A.SEC_CLASS IN ('O05')  THEN '1'  -- mmf는 해당코드가 없어 고정금리 처리
			   ELSE '3' END                                                                    AS IRATE_TPCD                      /*  금리유형코드(1:고정)   */			
            , '1111111'                                                                        AS IRATE_CURVE_ID                  /*  금리커브ID(기본RF설정) */
--            , A.FLT_ITRT_CD                                                                        AS IRATE_DTLS_TPCD                 /*  변동금리유형코드       */
--            , A.LATO_DVSN                                                                   AS IRATE_DTLS_TPCD                 /*  변동금리유형코드       */  -- 뒤에서 사용하는지 , 정보성인지 추가 확인 필요
--            , A.REFERENV                                                                            AS IRATE_DTLS_TPCD                 /*  변동금리유형코드       */ 
            , M.BASE_IRATE_CD  AS IRATE_DTLS_TPCD                 /*  변동금리유형코드       */ 
            , A.GARI  AS ADD_SPRD                        /*  가산스프레드           */ 
            , DECODE(A.LATO_DVSN, '02', DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1), NULL)      AS IRATE_RPC_CYC                   /*  금리개정주기(월)       */

          --   , CASE WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('01', '05'      ) THEN  '1'                                              /*  ACT/365                */
          --                WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('02', '10', '12') THEN  '2'                                              /*  A30/360                */
          --                WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('04', '06', '08') THEN  '3'                                              /*  E30/360                */ 
          --                WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('00', '09'      ) THEN  '4'                                              /*  ACT/ACT                */
          --                WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('03'            ) THEN  '5'                                              /*  ACT/360                */
          --                  ELSE '1' 
			    -- END                                                                                                       /*  DEFAULT: ACT/365       */
          --                      AS DCB_CD                          /*  일수계산코드           */	  
       , '1'                   AS DCB_CD                          /*  일수계산코드    2024.05.09 요건 수정   */	  
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
        , DECODE(A.IS_IRATE_ASSET, '1', A.SRCL, NULL)                                     AS IRATE_CAP                       /*  적용금리상한           */
        , DECODE(A.IS_IRATE_ASSET, '1', A.SRFL, NULL)                                     AS IRATE_FLO                       /*  적용금리하한           */
        , B.DIV_RPAY_UNIT_TERM_MMCT                                                            AS AMORT_TERM                      /*  원금분할상환주기(월)   */
        , NULL                                                                                 AS AMORT_AMT                       /*  분할상환금액           */
        , NVL(B.EXPR_RPAY_RAT,A.SAPR/100)                                                          AS MATR_PRMU                       /*  만기상환율             */
        , B.ALDY_SALE_DT                                                                       AS PSLE_DATE                       /*  선매출일자             */
        , B.ALDY_SALE_INRS_CLCD                                                                AS PSLE_INT_TPCD                   /*  선매출이자유형코드     */
        , CASE WHEN A.ASSETCODE = 'MM' THEN A.BP_BUP -- MM상품은 거래상대방
               ELSE A.ISSUER   -- 그외는 발행인
                END                                                                            AS CNPT_ID                         /*  거래상대방ID           */
        , CASE WHEN A.ASSETCODE = 'MM' THEN A.BPARTNER_NAME
               ELSE NVL(A.ISSUER_NAME, NVL(B.ISSU_CO_NM, G.CNPT_NM)) 
                END                                                                            AS CNPT_NM                         /*  거래상대방명           */
        , CASE WHEN A.ASSETCODE = 'MM' THEN A.BP_BUP
               ELSE NVL(A.ISSUER_BUP, B.ISSU_CO_CORP_CD)
                END                                                                            AS CORP_NO                         /*  법인등록번호           */
--        , DECODE(B.ITMS_CLCD, '1', NULL
--                            , '2', NVL(D.CRGR_KIS , A.CRGR_KIS)    , A.CRGR_KIS)               AS CRGR_KIS                        /*  신용등급(한신평)       */
--        , DECODE(B.ITMS_CLCD, '1', NVL(C.CRGR_NICE, A.CRGR_NICE)
--                            , '2', NVL(D.CRGR_NICE, A.CRGR_NICE)
--                                 , NVL(E.CRGR_NICE, NVL(G.CRGR_NICE, A.CRGR_NICE)))            AS CRGR_NICE                       /*  신용등급(한신정)       */
--        , DECODE(B.ITMS_CLCD, '1', NULL
--                            , '2', NVL(D.CRGR_KR  , A.CRGR_KR)     , A.CRGR_KR)                AS CRGR_KR                         /*  신용등급(한기평)       */
         , A.RATING1 AS CRGR_KIS                        /*  신용등급(한신평)       */
    	   , A.RATING3 AS CRGR_NICE                       /*  신용등급(한신정)       */	
         , A.RATING4 AS CRGR_KR                         /*  신용등급(한기평)       */  
         , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
         , NVL(K.CRGR_KICS,DECODE(A.SEC_CLASS,'O05','4', DECODE(A.CRGR_KICS,'0',NULL,A.CRGR_KICS))) AS CRGR_KICS -- MMF 4등급 적용, KICS 신용등급은 서브쿼리에서 작업후 처리
--       , NVL(A.BUYG_EXCR, 0)                                                                  AS CONT_PRC                        /*  체결가격(환율/지수 등) */
         , A.RATE_MAT  AS CONT_PRC                        /*  체결가격(환율/지수 등) */ 
	       , NULL                                                                                 AS UNDL_EXEC_PRC                   /*  기초자산행사가격       */
         , I.LAST_INDX_VAL                                                                      AS UNDL_SPOT_PRC                   /*  기초자산현재가격       */
--        , CASE WHEN A.BOND_OPTN_CLCD IN ('08', '09', '10') THEN 1                                                                 -- 옵션구분: 08콜 09풋 10콜풋
--               ELSE 0 END                                                                      AS OPT_EMB_TPCD                    /*  내재옵션구분           */
--        , A.OPTN_EVNT_FR_DT                                                                    AS OPT_STR_DATE                    /*  옵션행사시작일자       */
--        , A.OPTN_EVNT_CLOS_DT                                                                  AS OPT_END_DATE                    /*  옵션행사시작일자       */
         , A.OPCD AS OPT_EMB_TPCD                    /*  내재옵션구분           */  --도메인 확인 필요
	       , A.EXSD AS OPT_STR_DATE                    /*  옵션행사시작일자       */ 
         , A.EXND AS OPT_END_DATE                    /*  옵션행사시작일자       */ 
         , CASE WHEN NVL(J.UPRC_CLEAN, 0) <> 0 THEN J.UPRC_CLEAN
                ELSE DECODE(B.ITMS_CLCD, '1', DECODE(A.IS_CLEAN_UPRC, 0, C.UPRC_DIRTY, C.UPRC_CLEAN), '2', D.UPRC_CLEAN, NVL(E.UPRC_DIRTY, NVL(J.UPRC_CLEAN, A.CLEAN_PRICE)))
                 END                                                                             AS EXT_UPRC                        /*  외부평가단가           */
        , CASE WHEN A.ASSETCODE IN ('BD') THEN B.STND_PRCP
               ELSE 1 END                                                                      AS EXT_UPRC_UNIT                   /*  외부평가단가단위       */
        , DECODE(B.ITMS_CLCD, '1', NVL(C.DURA_EFFE, A.DURATION), '2', NVL(D.DURA_EFFE, A.DURATION), NVL(E.DURA_MODI, A.DURATION))
                                                                                               AS EXT_DURA                        /*  외부평가듀레이션       */  -- ITMS_CLCD = 1(원화), 2(외화), DURA1&2의 평균이 아니라 NICE의 유효듀레이션으로 설정
        , DECODE(B.ITMS_CLCD, '1', C.UPRC_1, '2', D.UPRC_1, NVL(E.UPRC_1, J.UPRC_1))           AS EXT_UPRC_1                      /*  외부평가단가1          */
        , DECODE(B.ITMS_CLCD, '1', C.UPRC_2, '2', D.UPRC_2, NVL(E.UPRC_2, J.UPRC_2))           AS EXT_UPRC_2                      /*  외부평가단가2          */
        , DECODE(B.ITMS_CLCD, '1', C.DURA_1, '2', D.DURA_1, E.DURA_1)                          AS EXT_DURA_1                      /*  외부평가듀레이션1      */
        , DECODE(B.ITMS_CLCD, '1', C.DURA_2, '2', D.DURA_2, E.DURA_2)                          AS EXT_DURA_2                      /*  외부평가듀레이션2      */
        , CASE WHEN A.STOCK_CATEGORY = '1' THEN '10'
               WHEN A.STOCK_CATEGORY = '2' THEN '20'
               WHEN A.STOCK_CATEGORY = '3' THEN '30'  --> 전환우선주
               ELSE NULL END                                                                   AS STOC_TPCD                       /*  주식유형코드           */
        , CASE WHEN A.STOCK_CATEGORY      IN ('2', '3')     THEN 'Y'                                                                                                -- 10:보통, 20:우선주(15는 DUMMY), 25: 상환우선주 -- SELECT * FROM IDHKRC_TCO_CO_C_INFO WHERE C_ID = '0507' ORDER BY 3;
                 WHEN SUBSTR(G.SECR_ITMS_CD, 1, 3) = 'KR7' THEN DECODE(SUBSTR(G.SECR_ITMS_CD, 9, 1), '0', 'N', 'Y')
                 WHEN SUBSTR(A.ISIN, 1, 3) = 'KR7' THEN DECODE(SUBSTR(A.ISIN, 9, 1), '0', 'N', 'Y')
                 ELSE 'N' END                                                                    AS PRF_STOC_YN                     /*  우선주여부             */  -- '02_N': 231(후순위), '02_Y': 232(조건부후순위), '03_N': 233(신종)  , '03_Y': 234(조건부신종)
        , CASE WHEN A.PRCR_DVSN = '1' THEN '01' 
               WHEN A.PRCR_DVSN IN ('2','3','5') THEN '02'
               WHEN A.PRCR_DVSN IN ('4') THEN '03'
               ELSE '01' END 
               ||'_'||'N'  AS BOND_RANK_CLCD                  /*  채권순위구분코드       */  -- DET_RPMT_RANK_CLCD IN IDHKRH_BND_INFO --> 3, 4는 후순위/신종에 해당하는 듯함(2는 유동화SPC)
                 
        , CASE WHEN G.MKT_CLCD IN ('1', '2')             THEN DECODE(G.MKT_CLCD,   '1', 'Y',  '2', 'Y', 'N')
               WHEN A.SEC_CLASS IN ('S01','S02', 'S06' ) THEN 'Y'
               ELSE 'N' END                                                                    AS STOC_LIST_CLCD                  /*  주식상장구분코드       */ -- KOSPI/KOSDAQ 모두 선진시장이므로 우선 Y/N으로 처리(19.10.21)
        , NVL(A.COUNTRY, NVL(B.NATN_CD, 'KR'))                                                 AS CNTY_CD                         /*  국가코드               */
        , A.ASKB                                                                               AS ASSET_LIQ_CLCD                  /*  자산유동화구분코드     */ -- 1: ABS, 2: MBS
        , A.OPER_PART                                                                          AS DEPT_CD                         /*  부서코드               */
        , 'KRBH102BM'                                                                          AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
        , A.RATING5 AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
        , A.RATING6 AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
        , A.RATING7 AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
        , A.IND_SECTOR_L        AS IND_L_CLSF_CD                        /*산업대분류코드*/
        , A.IND_SECTOR_L_T      AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
        , A.IND_SECTOR          AS IND_CLSF_CD                          /* 산업분류코드*/
        , NULL AS LVG_RATIO

   FROM 
               ( SELECT AA.* 
             , CASE WHEN AA.GL_ACCOUNT = '0011600200' THEN '1P010802' /*원천에서 기존 계정코드 매핑의 오류*/ 
                    ELSE AA.GL_ACCOUNT -- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수
                     END AS GL_ACCOUNT2  
             , CASE WHEN AA.BDCL = '05' THEN '240' -- CLN 
                    ELSE NULL
                     END AS KICS_PROD_TPCD   
             , CASE WHEN AA.BDCL = '05' THEN '신용연계채권' -- CLN 
                    ELSE NULL
                     END AS KICS_PROD_TPNM                        
			       ,  CASE WHEN AA.ASSETCODE = 'BD' THEN TO_CHAR(AA.BOND_CLASS)
				             ELSE TO_CHAR(AA.SEC_CLASS)
					       END AS  PROD_MAP_CD		
			      ,  CASE WHEN AA.ASSETCODE = 'BD' THEN '1' 
				  WHEN AA.SEC_CLASS IN ('O05') THEN '1'	-- 20231213 김재우 추가 MMF 금리부자산 처리 --20240412 코드 오타 수정 ; 005 --> O05
			              ELSE '0'
					     END AS IS_IRATE_ASSET
			      , CASE WHEN AA.ASSETCODE = 'BD' THEN '0'  -- 채권 금리부자산
				           ELSE '0'
				               END AS IS_CLEAN_UPRC			 
--	                     ,  CASE WHEN SUBSTR(AA.PORTFOLIO,1,1) IN ('A','E','G','S') THEN '1'     -- A연금저축연동금리, E지수연동 , G 일반계정은 일반계정 분류 
--				           ELSE '2' 
--					      END AS FNDS_ACCO_CLCD	
           , DECODE(AA.PORTFOLIO,'G000','1','2') AS FNDS_ACCO_CLCD  
			     , CASE WHEN AA.RATING1 IS NOT NULL OR AA.RATING3 IS NOT NULL OR AA.RATING4 IS NOT NULL			  
				                      THEN
				                      -- LEAST(NVL(AB.CRGR_KICS,'99'), NVL(AC.CRGR_KICS,'99'))  -- 국내등급은 2개 들어옴. 2개 들어올 경우 WORST 등급 사용
						      GREATEST (  DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999'))))
				          ELSE  
						      GREATEST (  DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999'))))
                                            END AS CRGR_KICS	
                              , AH.SRCL
			      , AH.SRFL
			      , DECODE(AH.GARC,'+',AH.GARI, -AH.GARI) AS GARI 
			      , AG.SAPR
			      , NVL(AG.OPCD,AH.OPCD) AS OPCD
			      , AH.EXSD
            , AH.EXND 
		    FROM IBD_ZTCFMPEBAPST AA
			     , IKRUSH_MAP_CRGR AB
			     , IKRUSH_MAP_CRGR AC
			     , IKRUSH_MAP_CRGR AD
			     , IKRUSH_MAP_CRGR AE
			     , IKRUSH_MAP_CRGR AF
			     , IKRUSH_MAP_CRGR AG
			     , (SELECT * 
			          FROM IBD_ZTCFMBDCLASS 
			         WHERE VERS = '000' 
			        )  AG
			     , (SELECT * 
			          FROM IBD_ZTCFMBDSTOCK 
			         WHERE VERS = '000'			        
			       )  AH			        
		  WHERE 1=1
		       AND AA.RATING1 = AB.CRGR_DMST(+)
		       AND AA.RATING3 = AC.CRGR_DMST(+)
		       AND AA.RATING4 = AD.CRGR_DMST(+)
		       AND AA.RATING5 = AE.CRGR_SNP(+)
		       AND AA.RATING6 = AF.CRGR_MOODYS(+)		  
		       AND AA.RATING7 = AG.CRGR_FITCH(+)
  		     AND AA.SECURITY_ID = AG.SECURITY_ID(+)   
           AND AA.SECURITY_ID = AH.SECURITY_ID(+)   			   
		       AND AA.GRDAT = '$$STD_Y4MD'  
                       AND AA.VALUATION_AREA = '400'
                       AND AA.ASSETCODE IN ('BD', 'ST','MM','OS') -- 채권,주식, 금융상품;수익증권
--		       AND AA.SEC_CLASS <> 'B08' --사모사채 제외 KRBH106 에서 처리
--           AND AA.BOND_CLASS <> '401' -- 사모사채 제외 ( B08로 처리하면 채권도 함께 들어와서 조건변경)
           AND AA.SEC_CLASS <> 'O08' -- 변액초기자금 제외
	      )  A
        , (
             SELECT *
               FROM IDHKRH_BND_INFO                         -- 채권발행정보
              WHERE 1=1
                AND STND_DT = (
                                 SELECT MAX(STND_DT)
                                   FROM IDHKRH_BND_INFO
                                  WHERE STND_DT <= '$$STD_Y4MD'
                              )
          ) B	
        , (
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
                  , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)  -- 외부신용등급 코드와 등급간 매핑정보(eg. '0110' -> 'AAA')
                       , NVL( T2.ISSU_CO_CRED_GRDE_CD
                            , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD)
                                 , T1.ISSU_CO_CRED_GRDE_CD ) ) )                               AS CRGR_NICE
                  , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR
                      WHERE CRGR_DMST IN NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)
                                            , NVL( T2.ISSU_CO_CRED_GRDE_CD
                                                 , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD)
                                                      , T1.ISSU_CO_CRED_GRDE_CD ) ) ) )        AS CRGR_KICS
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
          ) C		
       , (
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
          ) D		  
        , (
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
          ) E		  
        , (
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
          ) G		  
       , (
             SELECT INDX_DSCM_ID
                  , INDX_DSCM_ID_NM
                  , LAST_INDX_VAL
               FROM IDHKRH_INDEX T1             -- 국내해외지수데이터
              WHERE 1=1
                AND T1.STND_DT = (
                                    SELECT MAX(STND_DT)
                                      FROM IDHKRH_INDEX
                                     WHERE STND_DT <= '$$STD_Y4MD'
                                 )
          ) I		  
       , (
             SELECT ISIN_CD                                           AS SECR_ITMS_CD
                  , TRUNC( (  NVL(DECODE(CLE_UPRC1, 0, NULL, CLE_UPRC1), CLE_UPRC2)
                            + NVL(DECODE(CLE_UPRC2, 0, NULL, CLE_UPRC2), CLE_UPRC1) ) * 0.5, 3)
                                                                      AS UPRC_CLEAN
                  , DECODE(CLE_UPRC1, 0, NULL, CLE_UPRC1)             AS UPRC_1
                  , DECODE(CLE_UPRC2, 0, NULL, CLE_UPRC2)             AS UPRC_2
                  , NVL(DECODE(UPRC_UNIT, 0, NULL, UPRC_UNIT), 10000) AS UPRC_UNIT
               FROM IKRUSH_UPRC                   -- 수기_단가(구조화예금 등 CLEAN단가 적용대상)
              WHERE BASE_DATE = '$$STD_Y4MD'
          ) J		  
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
         , ( SELECT * 
               FROM IKRUSH_FLOATINGIRATE_MAP
              WHERE BASE_DATE = '$$STD_Y4MD'      
            ) M  
           , (
             SELECT CD                                                AS PROD_TPCD
                  , CD_NM                                             AS PROD_TPNM 
                  , CD2                                               AS KICS_TPCD
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_MAP_SAP'    -- KICS상품분류매핑(기존분류코드를 KICS상품분류코드로 매핑)
          ) KICS
          , ACCO_MST 
  WHERE 1=1
      AND A.ISIN  = B.SECR_ITMS_CD(+)
      AND A.ISIN  = C.SECR_ITMS_CD(+)
      AND A.ISIN  = D.SECR_ITMS_CD(+)
      AND A.ISIN  = E.SECR_ITMS_CD(+)
      AND A.ISIN  = G.SECR_ITMS_CD(+)
      AND A.ISIN  = I.INDX_DSCM_ID(+)
      AND NVL(A.ISIN,A.POSITION_ID) =  K.SECR_ITMS_CD(+)
  	  AND A.PROD_MAP_CD = KICS.PROD_TPCD(+)	
      AND A.GL_ACCOUNT2 = ACCO_MST.ACCO_CD (+) 
	    AND A.ISIN = J.SECR_ITMS_CD(+)
	    AND A.POSITION_ID = M.POSITION_ID(+)
	    AND not EXISTS
          (
             SELECT *
               FROM IKRUSH_LT_TOTAL --중복을 방지하기 위해 수기 수익증권에 존재하는 대상은 제외함
              WHERE 1=1
                AND BASE_DATE = '$$STD_Y4MD'
                AND FUND_CD   = A.PORTFOLIO
--                AND ACCO_CD   = GL_ACCOUNT2 -- 24.02.05 주석처리 check : 이 조건이 반드시 필요한가 ??
                AND PRNT_ISIN_CD = A.ISIN

          )
)
SELECT * 
FROM SECR