/* 수정사항 
- 계정코드를 매핑하는 SUB쿼리 결과를  WITH 문 처리  
- IBD_ZTCFMPEBAPST 원천의 계정과목코드가 Null로 들어오므로 매핑테이블과 무관한 결과가 생성됨. 
- null 처리를 어디에서 하는지 확인 필요 !!
--국채 선물 : pay, leg 액면금액 수정 
*/

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
, CUST AS /*거래상대방 신용등급*/
( 
     SELECT /*+ USE_HASH(DD EE) FULL(DD) */
            DD.CUST_NO                                                  AS CUST_NO
          , MAX(DD.CUST_NM)                                             AS CUST_NM
          , MAX(DD.CORP_REG_NO)                                         AS CORP_REG_NO
          , MAX(EE.CRGR_KIS)                                            AS CRGR_KIS
          , MAX(EE.CRGR_NICE)                                           AS CRGR_NICE
          , MAX(EE.CRGR_KR)                                             AS CRGR_KR
          , MAX(DECODE(EE.CRGR_KICS, 0, NULL, EE.CRGR_KICS))            AS CRGR_KICS
       FROM IKRASH_CUST_BASE DD          -- 고객기본
          , (
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
            ) EE                                                   -- 법인등록번호기반 신용등급 매핑
      WHERE 1=1
        AND DD.CORP_REG_NO = EE.CORP_NO(+)
        AND DD.STND_DT = (
                            SELECT MAX(STND_DT)
                              FROM IKRASH_CUST_BASE
                             WHERE STND_DT <= '$$STD_Y4MD'
                               AND CUST_NO = DD.CUST_NO
                         )
      GROUP BY DD.CUST_NO
)
, KICS AS
(
         SELECT CD                                                AS PROD_TPCD
                , CD_NM                                             AS PROD_TPNM	 
              , CD2                                               AS KICS_TPCD
              , CD2_NM                                            AS KICS_TPNM
           FROM IKRUSH_CODE_MAP
          WHERE GRP_CD = 'KICS_PROD_MAP_SAP'     -- KICS상품분류매핑
)
, T_IDX AS (
	SELECT T1.STND_DT
	  , T1.SECR_ITMS_CD
	  , T1.CLPC
	  , T1.BASE_AST_CD
	  , T1.CNTA_MLTI
	  , T1.CRNC_CD
	  , T1.TECN_PUT_CLCD
	  , T1.EVNT_PRC
	  , T2.LAST_INDX_VAL
	FROM IDHKRH_STOCK_INDEX_FUTURE T1 -- 주가지수선물옵션
	  , IDHKRH_INDEX T2              				-- 국내해외지수데이터
	WHERE 1=1
	AND T1.BASE_AST_CD = T2.INDX_DSCM_ID(+)
	AND T1.STND_DT = T2.STND_DT(+)
	AND T1.STND_DT = (
						SELECT MAX(STND_DT)
						  FROM IDHKRH_STOCK_INDEX_FUTURE
						 WHERE STND_DT <= '$$STD_Y4MD'
					 )
)
, T_KTB_PRC AS (
	SELECT STND_DT
		, FURS_CD  AS SECR_ITMS_CD
		, THDD_LAST_VAL
		, EXPR_DT
		, CRNC_CD
	FROM IDHKRH_FUTURES T1            -- 국채선물데이터
	WHERE STND_DT = (
				 SELECT MAX(STND_DT)
				   FROM IDHKRH_FUTURES
				  WHERE STND_DT <= '$$STD_Y4MD'
			  )
)
, T_FIDE_MST AS (				
	SELECT JA.* 
	FROM IBD_ZTCFMDECLASS JA					--장내파생종목정보
		 , ( SELECT SECURITY_ID, MAX(VERS) AS VERS 
				 FROM IBD_ZTCFMDECLASS
				GROUP BY SECURITY_ID
			) JB		
	WHERE JA.SECURITY_ID = JB.SECURITY_ID
	   AND JA.VERS = JB.VERS
)
, T_BASE AS (
	SELECT A.* 
--	                     ,  CASE WHEN SUBSTR(AA.PORTFOLIO,1,1) IN ('A','E','G','S') THEN '1'     -- A연금저축연동금리, E지수연동 , G 일반계정은 일반계정 분류 
--				           ELSE '2' 
--					      END AS FNDS_ACCO_CLCD				   
                      , DECODE(A.PORTFOLIO,'G000','1','2') AS FNDS_ACCO_CLCD
        /* 만기일자가 NULL 일경우 처리(한국거래소 코드체계 준용)
           선물옵션의 만기년은 표준코드 체계에 따라  2010 E 부터 년도를 알파벳 순으로 2025년 W까지 부여하며, 2026년부터 6, 7(2027), 8(2028), ..., 5(2035) ,A(2036년) 순서로 부여함
           6~W는 30년마다 반복됨 (6 7 8 9 0 1 2 3 4 5 A B C ... W까지)
           국채선물은 만기월 셋째주 화요일, 주식/지수의 선물/옵션은 만기월 둘째주 목요일
        */					  
		      , NVL(DECODE(A.EDDT,'00000000',NULL,A.EDDT)			  
			,TO_CHAR(NEXT_DAY(
                               TO_DATE( CASE WHEN SUBSTR(A.ISIN, 7, 1) = 'G' THEN '2012'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'H' THEN '2013'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'J' THEN '2014'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'K' THEN '2015'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'L' THEN '2016'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'M' THEN '2017'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'N' THEN '2018'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'P' THEN '2019'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'Q' THEN '2020'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'R' THEN '2021'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'S' THEN '2022'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'T' THEN '2023'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'V' THEN '2024'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'W' THEN '2025'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '6' THEN '2026'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '7' THEN '2027'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '8' THEN '2028'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '9' THEN '2029'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '0' THEN '2030'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '1' THEN '2031'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '2' THEN '2032'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '3' THEN '2033'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '4' THEN '2034'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = '5' THEN '2035'
                                             WHEN SUBSTR(A.ISIN, 7, 1) = 'A' THEN '2036'
                                             ELSE TO_CHAR(TO_DATE(A.GRDAT, 'YYYYMMDD') + 1, 'YYYY')
                                             END
                                        ||
                                        CASE WHEN SUBSTR(A.ISIN, 8, 1) = 'A' THEN '10'
                                             WHEN SUBSTR(A.ISIN, 8, 1) = 'B' THEN '11'
                                             WHEN SUBSTR(A.ISIN, 8, 1) = 'C' THEN '12'
                                             ELSE LPAD(SUBSTR(A.ISIN, 8, 1), 2, 0)
                                             END
                                        || '01', 'YYYYMMDD'
                                      ) - 1
                               , DECODE(A.PRODUCT_TYPE, '70E', 3, 5)  --미국국채는 제외함
                              )
                      + DECODE(A.PRODUCT_TYPE, '70E', 14, 7), 'YYYYMMDD') --미국국채는 제외함
			)		  
                                                                                              AS MATR_DATE          
		  FROM IBD_ZTCFMPEBAPST A
		  WHERE 1=1
		       AND A.GRDAT = '$$STD_Y4MD'  
                       AND A.VALUATION_AREA = '400'
                       AND A.ASSETCODE IN ('FU' )-- 선물
)
--SELECT * FROM T_BASE
--WHERE PRODUCT_TYPE = '70D' ;

SELECT /* KRBH105BM */
               A.GRDAT                                                         AS BASE_DATE                       /*  기준일자               */
             , 'FIDE_O_'||A.LOT_ID                              		AS EXPO_ID                         /*  익스포저ID             */
             , A.PORTFOLIO                                                   AS FUND_CD                         /*  펀드코드               */
             , A.PRODUCT_TYPE                                             AS PROD_TPCD                       /*  상품유형코드           */
             , KICS.PROD_TPNM                                             AS PROD_TPNM                       /*  상품유형명             */  -- 우선 NULL 처리
             ,  NVL(KICS.KICS_TPCD, 'ZZZ')                   AS KICS_PROD_TPCD                  /*  KICS상품유형코드       */
             , KICS.KICS_TPNM                                              AS KICS_PROD_TPNM                  /*  KICS상품유형명         */
             , A.ISIN                                                            AS ISIN_CD                         /*  종목코드               */
             , A.SECURITY_ID_T                                             AS ISIN_NM                         /*  종목명                 */
			 
             , A.FNDS_ACCO_CLCD||'_'||NVL(A.GL_ACCOUNT,CASE WHEN A.LONGSHORT = 'L' THEN '1'||SUBSTR(A.FU_ACCT, -4)
														ELSE '2'||SUBSTR(A.FU_ACCT, -4) 
														END ) 				    AS ACCO_CD                         /*  계정과목코드           */  --선물계저은 예치금 계정이므로, 선물계정가져옴. 자리수 문제로 잘라서 사용
             , NVL(ACCO_MST.ACCO_NM, '선물계정')                    AS ACCO_NM                         /*  계정과목명             */
            , A.POSITION_ID                                                 AS CONT_ID                         /*  계약번호(계좌일련번호) */
           , 'D'                                                                   AS INST_TPCD                       /*  인스트루먼트유형코드   */
           
           , CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G') THEN '7'                                                   /*  3: KTB FUTURES  */  -- 70E 국채선물 , 70G 미국채선물 FORWARD 평가를위해 설정, EXPO 에서 원복
                  WHEN A.PRODUCT_TYPE IN ('70A', '70D') THEN '4'                                                   /*  EQUITY FUTURES  */  --70A 개별주식선물 70D 주가지수선물
		          WHEN A.PRODUCT_TYPE  IN ('75A','75D') THEN DECODE(SUBSTR(A.ISIN, 4, 1), '2', '5', '3', '6', 'Z') /*  EQUITY OPTION(콜:5,풋:6)*/   --75A주식옵션 75D 주가지수옵션			
                          ELSE 'Z' END                                                                    AS INST_DTLS_TPCD                  /*  인스트루먼트상세코드   */
                          
         , A.LONGSHORT                                                         AS POTN_TPCD                       /*  포지션구분(매수:L,매도:S)*/
         , A.BUY_DATE                                                           AS ISSU_DATE                       /*  발행일자               */
	     , A.MATR_DATE     
              ,CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G') 	THEN TO_CHAR( ADD_MONTHS(TO_DATE(MATR_DATE, 'YYYYMMDD'), 6),'YYYYMMDD')	
			  ELSE NULL	END 						  AS FRST_INT_DATE                   /*  최초이자기일           */
              , 'Y'	                                                                                 AS PRIN_EXCG_YN                    /*  원금교환여부           */
              , CASE WHEN A.PRODUCT_TYPE IN ('70E')
                                       THEN CASE WHEN SUBSTR(A.ISIN, 4, 3) =  '165' THEN '3'
                                                             WHEN SUBSTR(A.ISIN, 4, 3) =  '166' THEN '5'
                                                             WHEN SUBSTR(A.ISIN, 4, 3) =  '167' THEN '10'
                                                               ELSE NULL END
                          ELSE NULL END                                                                  AS KTB_MAT_YEAR  		
        , NVL(H.CRNC_CD, A.POSITION_CURR)                                                                AS REC_CRNY_CD                     /*  수취LEG통화코드        */
        , '1'                                                                                  			 AS REC_CRNY_FXRT                   /*  수취LEG통화환율        */
	-- 국채선물의 경우 FST_BUYG_AMT가 현재가격과 동일한 상황...계약가격이 필요함
	-- 202402 수정 LONG 인경우 평가결과 = ( 평가단가 - 매수시 가격) * 승수 (1,000,000) * 보유수량 >   선도가격 = 매수시가격 * 승수* 보유수량 = 평가단가*승수*수량 - 평가결과  ( LONG이면 PAY)
	-- 202402 수정 Short  인경우 평가결과 = ( 매도시 가격-평가단가   ) * 승수 (1,000,000) * 보유수량 >   선도가격 = 매도시가격* 승수* 보유수량 = 평가단가*승수*수량 + 평가결과  ( short이면 rec)
        , CASE 	WHEN A.PRODUCT_TYPE IN ('70E')
				THEN DECODE(A.LONGSHORT, 'L',  A.UNITS * A.AVR_RATE_BOOK* 1000000,  	A.UNITS * A.AVR_RATE_BOOK * 1000000 + VAL_VAL_AMT )
--				THEN DECODE(A.LONGSHORT, 'L',  A.UNITS * 100* 1000000,  					A.UNITS * A.AVR_RATE_BOOK * 1000000 + VAL_VAL_AMT )
			WHEN SUBSTR(A.PRODUCT_TYPE, 1, 1) IN ('7')
				THEN DECODE(A.LONGSHORT, 'L', NVL(A.UNITS * H.CLPC * H.CNTA_MLTI, 0), A.UNITS * A.AVR_RATE_BOOK * H.CNTA_MLTI) 
			ELSE 0 END                                                      AS REC_NOTL_AMT                    /*  수취LEG기준원금        */
        , NVL(H.CRNC_CD, A.POSITION_CURR)                                   AS PAY_CRNY_CD                     /*  지급LEG통화코드        */
        , '1'                                                               AS PAY_CRNY_FXRT                   /*  지급LEG통화환율        */
	--202402 수정 : LONG 인경우 평가결과 = ( 평가단가 - 매수시 가격) * 승수 (1,000,000) * 보유수량 >   선도가격 = 매수시가격 * 승수* 보유수량 = 평가단가*승수*수량 - 평가결과  ( LONG이면 PAY)
	--202402 수정 : Short  인경우 평가결과 = ( 매도시 가격-평가단가   ) * 승수 (1,000,000) * 보유수량 >   선도가격 = 매도시가격* 승수* 보유수량 = 평가단가*승수*수량 + 평가결과  ( short이면 rec)	
        , CASE WHEN A.PRODUCT_TYPE IN ('70E')
			THEN DECODE(A.LONGSHORT, 'L',  A.UNITS * A.AVR_RATE_BOOK * 1000000 -  VAL_VAL_AMT,  A.UNITS * A.AVR_RATE_BOOK* 1000000)
--			THEN DECODE(A.LONGSHORT, 'L',  A.UNITS * A.AVR_RATE_BOOK * 1000000 -  VAL_VAL_AMT,  A.UNITS * 100* 1000000)
               WHEN SUBSTR(A.PRODUCT_TYPE, 1, 1) IN ('7')
			THEN DECODE(A.LONGSHORT, 'S', NVL(A.UNITS * H.CLPC * H.CNTA_MLTI, 0), A.UNITS * A.AVR_RATE_BOOK * H.CNTA_MLTI)
               ELSE 0 END                                                   AS PAY_NOTL_AMT                    /*  지급LEG기준원금        */
			   
        -- 장내상품은 현예금에서 처리되므로 BOKA_AMT사용하지 않아야 함
        , NVL(A.BOOK_VAL_AMT,0)                                             AS BS_AMT                          /*  장부금액               */

        -- , NVL(A.FAIR_VALUE_VAL_AMT,0)                                    AS VLT_AMT                      /*  BS금액                 */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E') THEN NVL(A.VAL_VAL_AMT,0) 
               ELSE NVL(A.FAIR_VALUE_VAL_AMT,0)  END                               AS VLT_AMT    /*  BS금액                 */
	, CASE 	WHEN A.PRODUCT_TYPE IN ('70E')		THEN NVL(A.VAL_VAL_AMT,0)       
			 ELSE NVL(A.FAIR_VALUE_VAL_AMT,0)    END                        AS FAIR_BS_AMT                     /*  공정가치평가결과(외부) */
--	, NVL(A.FAIR_VALUE_VAL_AMT,0)                                                     AS FAIR_BS_AMT                     /*  공정가치평가결과(외부) */			 
        , NULL                                                                                       AS CVA                             /*  CVA                    */
        , NVL(A.ACC_INT_VAL_AMT,0)                                                      AS ACCR_AMT                        /*  미수수익               */
        , NVL(A.PRPF_DMST,0)                                                                 AS UERN_AMT    /*  선수수익               */			
        , CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G')  THEN DECODE(A.LONGSHORT, 'L', 0.05, 0)
               ELSE NULL END                                                                   AS REC_IRATE                       /*  수취LEG금리            */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G')  THEN DECODE(A.LONGSHORT, 'L', '6', NULL)
               ELSE NULL END                                                                   AS REC_INT_PAY_CYC                 /*  수취LEG이자교환주기(월)*/
        , NULL                                                                                      AS REC_IRATE_TPCD                  /*  수취LEG금리유형코드(1:고정)*/
        , '1111111'                                                                                AS REC_IRATE_CURVE_ID              /*  수취LEG금리커브ID      */
        , NULL                                                                                 	AS REC_IRATE_DTLS_TPCD             /*  수취LEG변동금리유형코드*/
        , 0                                                                                    	AS REC_ADD_SPRD                    /*  수취LEG가산스프레드    */
        , NULL                                                                                      AS REC_IRATE_RPC_CYC               /*  수취LEG금리개정주기(월)*/
        , '1'                                                                                          AS REC_DCB_CD                      /*  수취LEG일수계산코드    */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G')  THEN DECODE(A.LONGSHORT, 'S', 0.05, 0)
               ELSE NULL END                                                                   AS PAY_IRATE                       /*  지급LEG금리            */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E', '70G')  THEN DECODE(A.LONGSHORT, 'S', '6', NULL)
               ELSE NULL END                                                          	AS PAY_INT_PAY_CYC                 /*  지급LEG이자교환주기(월)*/
        , NULL                                                                                 	AS PAY_IRATE_TPCD                  /*  지급LEG금리유형코드(1:고정)*/
        , '1111111'                                                                            	AS PAY_IRATE_CURVE_ID              /*  지급LEG금리커브ID      */
        , NULL                                                                                 	AS PAY_IRATE_DTLS_TPCD             /*  지급LEG변동금리유형코드*/
        , 0		                                                                             	AS PAY_ADD_SPRD                    /*  지급LEG가산스프레드    */
        , NULL                                                                                 	AS PAY_IRATE_RPC_CYC               /*  지급LEG금리개정주기(월)*/
        , '1'                                                                                  	       AS PAY_DCB_CD                      /*  지급LEG일수계산코드    */
        , A.UNITS                                                                           	AS CONT_QNTY                       /*  거래수량               */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E')        THEN 1000000
               WHEN A.PRODUCT_TYPE IN ('70A', '70D') THEN H.CNTA_MLTI
               WHEN A.PRODUCT_TYPE IN  ('75A','75D') THEN H.CNTA_MLTI
               ELSE 1.0 END
                                                                                               AS CONT_MULT                       /*  거래승수               */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E')
               THEN NVL(A.PURCH_VAL_AMT / DECODE(NVL(A.UNITS, 0), 0, 1, A.UNITS) / 1000000, 1.0)    --매입금액 0임 최초매입금액 가져와야함
               WHEN SUBSTR(A.PRODUCT_TYPE, 1, 1) IN ('7')
               THEN NVL(A.PURCH_VAL_AMT / DECODE(NVL(A.UNITS * H.CNTA_MLTI, 0), 0, 1, A.UNITS * H.CNTA_MLTI), 1.0) 
               ELSE 1.0 END
                                                                                               AS CONT_PRC                        /*  체결가격(환율/지수 등) */
        , CASE WHEN A.PRODUCT_TYPE IN ('70E')      THEN I.THDD_LAST_VAL
               WHEN A.PRODUCT_TYPE IN ('70A', '70D')  THEN H.CLPC
               WHEN A.PRODUCT_TYPE IN  ('75A','75D') THEN H.CLPC
               ELSE 0.0 END
                                                                                              AS SPOT_PRC                        /*  현재가격(환율/지수 등) */
        , NULL                                                                              AS IRATE_CAP                       /*  적용금리상한           */
        , NULL                                                                              AS IRATE_FLO                       /*  적용금리하한           */
        , NULL                                                                              AS OPT_VOLT                        /*  옵션변동성             */
        , NVL(H.EVNT_PRC, 0)                                                         AS UNDL_EXEC_PRC                   /*  기초자산행사가격(옵션) */
        , NVL(H.LAST_INDX_VAL, 0)                                                 AS UNDL_SPOT_PRC                   /*  기초자산현재가격(옵션) */
        , NULL                                                                              AS HDGE_ISIN_CD                    /*  헤지대상종목코드       */
        , NULL                                                                              AS HDGE_EXPR_DT                    /*  헤지대상종목만기일자   */
        , A.ISSUER                                                            			AS CNPT_ID                         /*  거래상대방ID           */ -- 장내파생은 거래소이므로, 중개회사ID 사용안함
        , A.ISSUER_NAME                                                       		AS CNPT_NM                         /*  거래상대방명           */
        , A.ISSUER_BUP                                                        		AS CORP_NO                         /*  법인등록번호           */
        , NULL                                                                                 AS CRGR_KIS                        /*  신용등급(한신평)       */
        , NULL                                                                                 AS CRGR_NICE                       /*  신용등급(한신정)       */
        , NULL                                                                                 AS CRGR_KR                         /*  신용등급(한기평)       */
        , NULL                                                                                 AS CRGR_VLT                        /*  신용등급(평가)         */
        , 'RF'                                                                                AS CRGR_KICS                       /*  신용등급(KICS)         */
        , 0                                                                                    AS EXT_UPRC                        /*  외부평가단가           */
        , 0                                                                                    AS EXT_UPRC_UNIT                   /*  외부평가단가           */
        , 0                                                                                    AS EXT_DURA                        /*  외부평가듀레이션       */
        , 0                                                                                    AS REC_EXT_UPRC                    /*  수취LEG외부평가단가    */
        , 0                                                                                    AS PAY_EXT_UPRC                    /*  지급LEG외부평가단가    */
        , 0                                                                                    AS REC_EXT_DURA                    /*  수취LEG외부평가듀레이션*/
        , 0                                                                                    AS PAY_EXT_DURA                    /*  지급LEG외부평가듀레이션*/
        , A.STOCK_CATEGORY                                                AS STOC_TPCD                       /*  주식유형코드           */    -- 코드값 AS-IS로 유지할지 검토
        , CASE WHEN A.STOCK_CATEGORY IN ('2') THEN 'Y' ELSE 'N' END                           AS PRF_STOC_YN                     /*  우선주여부             */
        , A.PRCR_DVSN                                                                  AS BOND_RANK_CLCD                  /*  채권순위구분코드       */ -- 코드값 AS-IS로 유지할지 검토
        , 'Y'                                                                                  AS STOC_LIST_CLCD                  /*  주식상장구분코드       */
        , SUBSTR(A.POSITION_CURR,1,2)                                          AS CNTY_CD                         /*  국가코드               */
        , NULL                                                                              AS ASSET_LIQ_CLCD     
        , A.OPER_PART                       AS DEPT_CD                         /*  부서코드               */
        , 'KRBH105BM'                                                                    AS LAST_MODIFIED_BY                /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                        AS LAST_UPDATE_DATE                /*  LAST_UPDATE_DATE       */
        , A.RATING5 											AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
        , A.RATING6 											AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
        , A.RATING7 											AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
        , A.IND_SECTOR_L        								AS IND_L_CLSF_CD                        /*산업대분류코드*/
        , A.IND_SECTOR_L_T     								AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
        , A.IND_SECTOR          									AS IND_CLSF_CD                          /* 산업분류코드*/
     FROM  T_BASE        A
	      , T_IDX			H
            , T_KTB_PRC   	I
           ,  T_FIDE_MST	J 	         
            , KICS	
            , CUST		  
           , ACCO_MST		  
  WHERE 1=1
        AND A.ISIN = H.SECR_ITMS_CD(+)
        AND A.ISIN  = I.SECR_ITMS_CD(+)
        AND A.SECURITY_ID = J.SECURITY_ID(+)
      	AND A.PRODUCT_TYPE = KICS.PROD_TPCD(+)
        AND A.GL_ACCOUNT = ACCO_MST.ACCO_CD (+)
      	AND A.ISSUER_BUP    = CUST.CUST_NO(+)  -- 거래상대방 번호 확인 필요, CUST 테이블도 확인 필요