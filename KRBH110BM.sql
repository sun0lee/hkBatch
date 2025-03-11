/* 수정사항 
1. 계정체계 변경 (8자리 ->10자리)
2. 검증용 컬럼 매핑 : NOMINAL_VAL_AMT
3. 검증용 컬럼 매핑 : 미상각 이연대출부대손익금액 NODEP_LOCF_DMST 
4. DSCR 0인 경우 Null 처리 : DECODE(S.DSCR, 0, NULL, S.DSCR) : 신용위험보고서COA 분류 
5. 2025-01-31 : 기업대출 공정가치 외부 입수 대상은 자동입수 대상에서 제외 
    (IKRUSH_LT_TOTAL (LT_SEQ =0 ) 으로 입수되는 대상은 자동입수 대상에서 제외처리Q_IC_ASSET_LOAN에는 생성 / Q_CB_INST_BOND_LOAN에는 생성제외 )
*/

WITH  /* SQL-ID :  KRBH110BM */ 
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
,T_AAA AS (
	SELECT AA.* 
		--	                     ,  CASE WHEN SUBSTR(AA.PORTFOLIO,1,1) IN ('A','E','G','S') THEN '1'     -- A연금저축연동금리, E지수연동 , G 일반계정은 일반계정 분류 
		--				           ELSE '2' 
		--					      END AS FNDS_ACCO_CLCD			
		, DECODE(AA.PORTFOLIO,'G000','1','2') 										AS FNDS_ACCO_CLCD
		, CASE WHEN AA.RATING1 IS NOT NULL OR AA.RATING3 IS NOT NULL OR AA.RATING4 IS NOT NULL
		--				                     THEN LEAST(NVL(AB.CRGR_KICS,'99'), NVL(AC.CRGR_KICS,'99'))  -- 국내등급은 2개 들어옴. 2개 들어올 경우 WORST 등급 사용
				   THEN GREATEST (  DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')))
					, DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')))
					, DECODE(LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999'))))
			 ELSE  
					  GREATEST (  DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')), '999', '0', 	
						LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')))
					, DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')))
					, DECODE(LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999'))))
			  END AS CRGR_KICS			
		,  NVL(AB.CRGR_KICS, '999')					AS KICS_RATING1		
		,  NVL(AC.CRGR_KICS, '999')					AS KICS_RATING3
		,  NVL(AD.CRGR_KICS, '999')					AS KICS_RATING4
		,  NVL(AE.CRGR_KICS, '999')					AS KICS_RATING5
		,  NVL(AF.CRGR_KICS, '999')						AS KICS_RATING6
		,  NVL(AG.CRGR_KICS, '999')					AS KICS_RATING7		
		, AA.GL_ACCOUNT 		AS ACCO_CD	-- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수										
	FROM IBD_ZTCFMPEBAPST AA
		, IKRUSH_MAP_CRGR AB
		, IKRUSH_MAP_CRGR AC
		, IKRUSH_MAP_CRGR AD
		, IKRUSH_MAP_CRGR AE
		, IKRUSH_MAP_CRGR AF	
		, IKRUSH_MAP_CRGR AG				     		
	WHERE 1=1
		AND AA.RATING1 = AB.CRGR_DMST(+)
		AND AA.RATING3 = AC.CRGR_DMST(+)
		AND AA.RATING4 = AD.CRGR_DMST(+)
		AND AA.RATING5 = AE.CRGR_SNP(+)
		AND AA.RATING6 = AF.CRGR_MOODYS(+)
		AND AA.RATING7 = AG.CRGR_FITCH(+)		  
		AND AA.GRDAT = '$$STD_Y4MD'  
		AND AA.VALUATION_AREA = '400'  --IFRS9
		AND AA.ASSETCODE IN ('LO') -- 기업대출
)
, T_BBB AS (
	SELECT A.* 
           FROM IBD_ZTCFMELT0020 A
              , ( SELECT MANDT,PJECT_ID,AGRE_ID,TRCH_ID,REPAY_INFO,VERS
			                 , MAX(FDATE) AS FDATE
		                FROM IBD_ZTCFMELT0020 
		               WHERE VERS = '000'
		                 AND REPAY_INFO = '01'
		                 AND FDATE <= '$$STD_Y4MD'
	                 GROUP BY MANDT,PJECT_ID,AGRE_ID,TRCH_ID,REPAY_INFO,VERS		   
                ) B		   
          WHERE 1=1
            AND A.MANDT = B.MANDT
            AND A.PJECT_ID = B.PJECT_ID	 
            AND A.AGRE_ID = B.AGRE_ID  
            AND A.TRCH_ID = B.TRCH_ID
            AND A.REPAY_INFO = B.REPAY_INFO	 
            AND A.VERS = B.VERS	 
            AND A.FDATE = B.FDATE
)
, T_CCC AS
(
    SELECT  /* KRBH110BM */
       A.GRDAT AS BASE_DATE	        	     			/* 기준일자                 */
      , 'LOAN_O2'||A.POSITION_ID AS EXPO_ID	          	/* 익스포저ID               */
    -- , 'LOAN_O2'||A.SECURITY_ID AS EXPO_ID	          	/* 익스포저ID               */
     , A.PORTFOLIO AS FUND_CD	          		   	/* 펀드코드                 */ 
     , A.LOAN_SUBJ_DVSN AS PROD_TPCD	        	/* 상품유형코드             */
     , KICS.CD_NM AS PROD_TPNM	        		   	/* 상품유형명               */ -- 우선 NULL 처리
      , CASE WHEN S.KICS_PROD_TPCD IS NOT NULL 	THEN S.KICS_PROD_TPCD
		WHEN KICS.CD2 IS NOT NULL THEN  KICS.CD2		 
             ELSE '320' 
	       END                                 AS KICS_PROD_TPCD              /*  상품구분코드     */
     , CASE WHEN S.KICS_PROD_TPCD IS NOT NULL THEN S.KICS_PROD_TPNM
	          WHEN KICS.CD2 IS NOT NULL THEN  KICS.CD2_NM	
             ELSE '기업신용대출'  
	        END AS KICS_PROD_TPNM	  		   /* KICS상품유형명           */
      , CASE WHEN A.ISIN IS NOT NULL THEN A.ISIN
	           ELSE S.PROD_KEY                                          
		    END AS ISIN_CD                      							/*  종목코드              */
      , A.SECURITY_ID_T                                           AS ISIN_NM                      /*  종목명                */
      , A.FNDS_ACCO_CLCD||'_'||A.ACCO_CD 	AS ACCO_CD	          		   /* 계정과목코드             */  --계정구분은 추후 수정
      , ACCO_MST.ACCO_NM                              AS ACCO_NM                         /*  계정과목명             */
     , A.SECURITY_ID  AS CONT_ID	          	     /* 계약ID                   */
     , '2' AS INST_TPCD	        	     /* 인스트루먼트유형코드     */  --1 개인대출 2 기업대출
	 -- 상환방법(RFMD) 1만기상환 2원금균등 3 원리금균등 4 불균등상환  , 금리구분(LATO_DVSN) 01 고정 02 변동
         --인스트루먼트상세유형코드 1 할인채 2 복리채 3 단리채 4 고정이표채 5 변동이표채 6 분할상환채 7 분할상환변동채 8 적금 연금
     , CASE WHEN A.RFMD = '1'  AND A.LATO_DVSN = '01' THEN '4'   --고정이표채 .
	          WHEN A.RFMD = '1'  AND A.LATO_DVSN = '02' THEN '5'  -- 변동이표채
	          WHEN A.RFMD = '2'  AND A.LATO_DVSN = '01' THEN '6' --분할상환채
	          WHEN A.RFMD = '2'  AND A.LATO_DVSN = '02' THEN '7' --분할상환변동채
	          WHEN A.RFMD = '3'  AND A.LATO_DVSN = '01' THEN '6' --분할상환채
	          WHEN A.RFMD = '3'  AND A.LATO_DVSN = '02' THEN '7' --분할상환변동채	 		  
	          WHEN A.RFMD = '4'  AND A.LATO_DVSN = '01' THEN '4'   --고정이표채 .
	          WHEN A.RFMD = '4'  AND A.LATO_DVSN = '02' THEN '5'  -- 변동이표채
		  ELSE '4' END AS INST_DTLS_TPCD               /*  인스트루먼트상세유형코드*/
     , A.BUY_DATE 					AS ISSU_DATE	        	     /* 발행일자                 */
     , A.EDDT 						AS MATR_DATE	        	     /* 만기일자                 */
     , A.POSITION_CURR 			AS CRNY_CD	          	     /* 통화코드                 */
     , A.KURSF                                         AS CRNY_FXRT                       /*  통화환율(기준일)       */
--     , A.BOOK_VAL_AMT                 	AS NOTL_AMT	        		   /* 이자계산기준원금         */
     , A.NOMINAL_VAL_AMT                 	AS NOTL_AMT	        		   /* 이자계산기준원금 : 탁기돈 20240215        */	 
     , A.TOT_LOAN_AMT_DMST            AS NOTL_AMT_ORG	    		   /* 최초이자계산기준원금     */
     , A.BOOK_VAL_AMT                 	AS BS_AMT	          		   /* BS금액                   */
     , A.RF_BK_VAL_AMT 						AS VLT_AMT	          		   /* 평가금액     : 상각후 원가  : 액면 - 누적상각금액            */
     , NULL 						AS FAIR_BS_AMT	      		   /* 공정가치B/S금액          */
     , A.ACC_INT_VAL_AMT              	AS ACCR_AMT                        /*  미수수익               */
     , A.PRPF_DMST                    		AS UERN_AMT                        /*  선수수익               */		
     , A.APCN_LATO /100			 AS IRATE	            		   /* 금리                     */
     , CASE WHEN A.ITTM = 0 THEN 12
	          ELSE NVL(TO_NUMBER(A.ITTM), 12) 
		         END  				AS INT_PAY_CYC	      		   /* 이자지급주기             */
     , 'N' 							AS INT_PROR_PAY_YN	  	     /* 이자선지급여부           */  --확인 필요
     ,  CASE WHEN A.LATO_DVSN = '01' THEN '1'
	           WHEN A.LATO_DVSN = '02' THEN '2'
	           ELSE '1' 
		     END AS IRATE_TPCD	      	     /* 금리유형코드             */
     , T.REFERENZ AS IRATE_CURVE_ID	  		   /* 금리커브ID               */
     , T.REFERENZ AS IRATE_DTLS_TPCD	  		   /* 금리상세유형코드         */   --기준금리코드 안들어옴
     , T.SPRD_LATO / 100 AS ADD_SPRD	        		   /* 가산스프레드             */  -- 추가입수 필요
     , TO_NUMBER(A.LATO_CHNG_CYCL) AS IRATE_RPC_CYC	           /* 금리개정주기             */
     ,  '1' AS DCB_CD	          	     /* 일수계산코드             */  
     ,  '1' AS CF_GEN_TPCD	      	     /* 현금흐름일생성구분       */
     , CASE WHEN A.HIDT = '00000000' THEN NULL
	          ELSE A.HIDT
		    END AS FRST_INT_DATE	    	     /* 최초이자기일             */  -- 값이 없는것 같음
     , CASE WHEN A.NEXT_RELV_DT = '00000000' THEN NULL
	          ELSE A.NEXT_RELV_DT
		    END AS NEXT_INT_DATE	    	     /* 차기이자기일             */
     , NULL AS GRACE_END_DATE	  	     /* 거치종료일자             */ -- 관련칼럼 없음. 확인 필요
     , T.DFMT_TERM AS GRACE_TERM	             /* 거치기간                 */   -- 모두 NULL 값임. 매핑우선함
     , T.CAP AS IRATE_CAP	        		   /* 적용금리상한             */
     , T.FLOOR AS IRATE_FLO	        		   /* 적용금리하한             */
	 -- 상환방법(RFMD) 1만기상환 2원금균등 3 원리금균등 4 불균등상환  , 금리구분(LATO_DVSN) 01 고정 02 변동
     , CASE WHEN A.RFMD = '1' THEN '10' --일시상환
	          WHEN A.RFMD = '2' THEN '30' -- 원금균등
	          WHEN A.RFMD = '3' THEN '70'  --원리균등
		        WHEN A.RFMD = '4' THEN '10' --일시상환
	          ELSE  '10'  --일시상환
  	         END AS RPAY_TPCD	        	     /* 상환유형코드             */  -- 정보성칼럼
     , T.DIV_RPMT_CYCL AS AMORT_TERM	             /* 원금분할상환주기         */
     , NULL AS AMORT_AMT	        		   /* 분할상환금액             */
      , CASE WHEN A.RFMD = '1' THEN 0  -- 만기일시상환
             WHEN A.RFMD = '2' THEN 1  -- 원금균등 
             WHEN A.RFMD = '3' THEN 2  -- 원리금균등
             WHEN A.RFMD = '4' THEN 0 -- 불균등상환은 만기일사상환 처리
             ELSE 0 END                                     AS AMORT_TPCD                   /*  분할상환유형코드      */	 
     , NULL AS AMORT_DTLS_TPCD	  	     /* 분할상환상세유형코드     */ --참조값 NULL 처리
     , NULL AS AMORT_END_YMD	    	     /* 분할상환종료일자         */ -- NULL 처리
     , NULL AS AMORT_DAYS	             /* 분할상환일수             */  -- NULL처리
     , NULL AS IRATE_FIX_TERM	  		   /* 금리고정기간             */  
     , A.ISSUER AS CNPT_ID	          		   /* 거래상대방ID             */
     , A.ISSUER_NAME AS CNPT_NM	          		   /* 거래상대방명             */
     , A.ISSUER_BUP AS CORP_NO	          		   /* 법인등록번호             */
     , A.RATING1 AS CRGR_KIS	        	     /* 신용등급(한신평)         */
     , A.RATING3 AS CRGR_NICE	        	     /* 신용등급(한신정)         */	 
     , A.RATING4 AS CRGR_KR	          	     /* 신용등급(한기평)         */
     , S.CRGR_VLT                                          AS CRGR_VLT                     /*  신용등급(평가)        */
     , NVL(S.CRGR_KICS , DECODE(A.CRGR_KICS,'0','99',A.CRGR_KICS))   AS CRGR_KICS                    /*  신용등급(KICS)        */
     , '20' AS CUST_GB	          	     /* 고객구분                 */
     , '11' AS CUST_TPCD	        	     /* 고객유형코드             */ --영리법인본사 처리
     , CASE WHEN A.BP_SCALE = '1'  THEN '01' --대기업 
	          WHEN A.BP_SCALE = '2'  THEN  '02' --중소기업 
	          ELSE NULL
		    END AS CORP_TPCD	        	     /* 법인유형코드             */
     , '20' AS LOAN_CONT_TPCD	  	     /* 대출계약유형코드         */ --기업대출
     , NULL AS CRGR_CB_NICE	           /* 개인신용등급(한신정)     */
     , NULL AS CRGR_CB_KCB	             /* 개인신용등급(KCB)        */
     , NULL AS COLL_ID	          	     /* 담보ID                   */
     , CASE WHEN A.LOAN_SUBJ_DVSN = 'L01' THEN '30'  --부동산
	          WHEN A.LOAN_SUBJ_DVSN = 'L02' THEN '40'  --신용
	          WHEN A.LOAN_SUBJ_DVSN = 'L03' THEN '20'  --유가증권
	          WHEN A.LOAN_SUBJ_DVSN = 'L05' THEN '40'  --기타대출은 신용처리
                  ELSE NULL 
		   END AS COLL_TPCD	        	     /* 담보유형코드             */
     , NULL AS COLL_DTLS_TPCD	  	     /* 담보상세유형코드         */ --NULL 처리 개인대출에 사용
     , NULL AS PRPT_DTLS_TPCD	  	     /* 부동산상세유형코드       */  --NULL 처리 개인대출에 사용
     , NULL AS RENT_GUNT_AMT	    	     /* 임대보증금               */ --NULL 처리 개인대출에 사용
     , NULL AS PRPT_OCPY_TPCD	  	     /* 부동산점유유형코드       */ --NULL 처리 개인대출에 사용
     , NULL AS LAND_APPR_AMT	           /* 토지감정금액             */ --NULL 처리 개인대출에 사용
     , NULL AS BULD_APPR_AMT	           /* 건물감정금액             */ --NULL 처리 개인대출에 사용
     , NULL AS COLL_AMT	               /* 담보금액                 */ --NULL 처리 개인대출에 사용
     , NULL AS REDC_AMT	               /* 감액금액                 */ --NULL 처리 개인대출에 사용
     , S.FAIR_VAL_SNR AS PRIM_AMT	               /* 선순위금액               */
--     , A.LTV_LATO AS LTV	              	     /* LTV                      */    
     , S.LTV AS LTV	              	     /* LTV - 수기 업로드 값으로 수정 - 2023.12.04 */    
     , S.COLL_SET_AMT AS COLL_SET_AMT	    	     /* 담보설정금액             */
     , NULL AS COLL_SET_RTO	    	     /* 담보설정비율             */
     , A.TRCH_AGRE_AMT AS LMT_AMT	          	     /* 한도금액                 */ 
     , NULL AS REG_RPAY_AMT	    	     /* 정기상환금액             */  -- NULL처리 개인대출 조기상환수수료 산출하는 값. 기업대출은 조기상환율 수기 입수
     , NULL AS PRE_RPAY_AMT	    	     /* 조기상환금액             */ -- NULL처리 개인대출 조기상환수수료 산출하는 값. 기업대출은 조기상환율 수기 입수
     , CASE WHEN A.STAGE_DVSN = '03'  THEN 91  -- STAGE3인경우 손상요건으로 부도처리
            ELSE TO_NUMBER(A.INTE_ARRE_DAYS)
             END AS ARR_DAYS	               /* 연체일수                 */-- 만기일시상환을 고려하여 이자연체일수로 처리
     , CASE WHEN A.FLCCD  = '01' THEN '10'
	          WHEN A.FLCCD  = '02' THEN '20'
	          WHEN A.FLCCD  = '03' THEN '30'
	          WHEN A.FLCCD  = '04' THEN '40'
	          WHEN A.FLCCD  = '05' THEN '50'
		  ELSE NULL	  
	            END AS FLC_CD	          	     /* 자산건전성코드           */
     , A.PRCT_BDAM_IFRS_DMST 		AS ABD_AMT	          	     /* 대손충당금금액           */  --100을 곱할지 검토 필요
     , A.UCPF_BDAM_IFRS_DMST 		AS ACCR_ABD_AMT	    	     /* 미수수익대손충당금금액   */ --100을 곱할지 검토 필요
--     , A.LOCF_DMST 				AS LOCF_AMT	        	     /* 이연대출부대손익금액     */ --100을 곱할지 검토 필요
     , A.NODEP_LOCF_DMST 			AS LOCF_AMT	        	     /* 탁기돈 수정 : 20240215  미상각 이연대출부대손익금액    액면, 장부금액 검증용*/
     , A.ARCR_KAMT 				AS PV_DISC_AMT	      	     /* 현재가치할인차금금액     */ --100을 곱할지 검토 필요
     , A.OPER_PART 				AS DEPT_CD	          	     /* 부서코드                 */
      , 'KRBH110BM'                                	    AS LAST_MODIFIED_BY             /*  최종수정자            */
      , SYSDATE                                       	AS LAST_MODIFIED_DATE           /*  최종수정일자          */
     , A.RATING5 					AS CRGR_SNP	        	     /* 신용등급(S&P)         */ 
     , A.RATING6 					AS CRGR_MOODYS	        	     /* 신용등급(무디스)         */ 
     , A.RATING7 					AS CRGR_FITCH	        	     /* 신용등급(피치)         */ 
     , A.IND_SECTOR_L        			AS IND_L_CLSF_CD                        /*산업대분류코드*/
     , A.IND_SECTOR_L_T      		AS IND_L_CLSF_NM                 /* 산업대분류명*/ 
     , A.IND_SECTOR          			AS IND_CLSF_CD                          /* 산업분류코드*/
     , S.LTV_SNR                                              /* 선순위LTV(%)) */
     , S.FAIR_VAL_SNR                                         /* 선순위대출금액 */
    -- , S.DSCR                                                 /* DSCR */ 
 , DECODE(S.DSCR, 0, NULL, S.DSCR)                       /* DSCR */ 
   FROM   T_AAA			    A
        , IKRUSH_PROD_LOAN 	S
        , T_BBB 			T 
        , ACCO_MST 
        , ( SELECT * FROM IKRUSH_CODE_MAP
            WHERE GRP_CD = 'KICS_PROD_MAP_SAP'
	        )  KICS	  
  WHERE 1=1 
    AND A.POSITION_ID = S.PROD_KEY(+)
    AND S.BASE_DATE(+) = '$$STD_Y4MD'
	  AND A.ACCO_CD = ACCO_MST.ACCO_CD(+) 
	  AND A.LOAN_SUBJ_DVSN = KICS.CD(+)
	  AND A.PJECT_ID = T.PJECT_ID(+)
	  AND A.AGRE_ID = T.AGRE_ID(+)
	  AND A.TRCH_ID= T.TRCH_ID(+)
 )
 SELECT * 
 FROM T_CCC
 WHERE 1=1
--  AND CONT_ID NOT IN 
--       (SELECT PRNT_ISIN_CD 
--        FROM IKRUSH_LT_TOTAL 
--        WHERE BASE_DATE = :BASE_DATE 
--        AND LT_SEQ = 0
--        )