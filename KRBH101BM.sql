/* 수정사항 
TO-BE 계정과목코드 : 대출상품코드별 계정과목 코드 매핑 (IKRUSH_CODE_MAP WHERE GRP_CD ='LN_ACCO_ASET' ) 
*/

WITH  /* SQL-ID : KRBH101BM */ 
T_ACCO AS ( /*대출상품유형별 계정코드 매핑 => 수기 (10자리 계정과목코드)*/
       SELECT CD         AS LN_CD 
                , CD_NM   AS LN_NM
                , CD2       AS ACCO_CD  
                , CD2_NM  AS ACCO_NM
        FROM IKRUSH_CODE_MAP
        WHERE GRP_CD ='LN_ACCO_ASET' 
)
,T_LNDP AS (
	SELECT *
	  FROM (
		  SELECT L1.*
			, RANK() OVER ( PARTITION BY  STD_YMD, LN_NO ORDER BY SND_NTS DESC) AS RN
		FROM IDHLNH_IFRS_LNDP_ACCT L1  /* IFRS 여수신계좌 */
		WHERE L1.STD_YMD	= '$$STD_Y4MD'
		AND L1.WK_CYL  		= 'M'					--월별 처리 
		)
	WHERE RN =1
)		
,T_CUST AS (
	SELECT CUST_ID
		     , SUM(LN_AMT) 			AS	 SUM_LN_AMT		-- 고객별 대출금액
          FROM IDHLNH_IFRS_AST_SOUN				  /*  IFRS자산건전성월마감(FNTBFSMC) */
         WHERE FIN_YM = SUBSTR('$$STD_Y4MD',1,6)
           AND LN_RAMT 		> 0    --잔액이 0이상
           AND LN_CONT_GB <> '30'
         GROUP BY CUST_ID
)
, T_BASE AS (
	SELECT A. *
    	, CASE WHEN A.NICE_CRED_GD = 0 THEN NULL
    	          WHEN A.NICE_CRED_GD < 400 THEN '13'  
    	          WHEN A.NICE_CRED_GD < 450 THEN '12'
    	          WHEN A.NICE_CRED_GD < 500 THEN '11'
    	          WHEN A.NICE_CRED_GD < 550 THEN '10'
    	          WHEN A.NICE_CRED_GD < 600 THEN '9'
    	          WHEN A.NICE_CRED_GD < 650 THEN '8'
    	          WHEN A.NICE_CRED_GD < 700 THEN '7'
    	          WHEN A.NICE_CRED_GD < 750 THEN '6'
    	          WHEN A.NICE_CRED_GD < 800 THEN '5'
    	          WHEN A.NICE_CRED_GD < 850 THEN '4'
    	          WHEN A.NICE_CRED_GD < 900 THEN '3'
    	          WHEN A.NICE_CRED_GD < 950 THEN '2'
    	          WHEN A.NICE_CRED_GD >= 950  THEN '1'
    		   END AS CB_CRGR_NICE					/*  CB신용점수제(NICE)      */  -- 0일 경우 무등급. NULL 처리
    	, CASE WHEN A.KCB_CRED_GD = 0 THEN NULL
    	          WHEN A.KCB_CRED_GD < 400 THEN '13'  
    	          WHEN A.KCB_CRED_GD < 450 THEN '12'
    	          WHEN A.KCB_CRED_GD < 500 THEN '11'
    	          WHEN A.KCB_CRED_GD < 550 THEN '10'
    	          WHEN A.KCB_CRED_GD < 600 THEN '9'
    	          WHEN A.KCB_CRED_GD < 650 THEN '8'
    	          WHEN A.KCB_CRED_GD < 700 THEN '7'
    	          WHEN A.KCB_CRED_GD < 750 THEN '6'
    	          WHEN A.KCB_CRED_GD < 800 THEN '5'
    	          WHEN A.KCB_CRED_GD < 850 THEN '4'
    	          WHEN A.KCB_CRED_GD < 900 THEN '3'
    	          WHEN A.KCB_CRED_GD < 950 THEN '2'
    	          WHEN A.KCB_CRED_GD >= 950  THEN '1'
    		   END AS CB_CRGR_KCB					/*  CB신용점수제(KCB)      */  -- 0일 경우 무등급. NULL 처리		
		   
    	, B.LN_CONT_GB		AS B_LN_CONT_GB
    	, B.COLL_KND		AS B_COLL_KND
    	, B.COLL_ATTR		AS B_COLL_ATTR
    	, B.LN_PITEM		AS B_LN_PITEM
    	, B.RADPT_METH
    	, B.CUST_GB
    	, B.RET_DEPT
    	, B.CMM_ARR_DDS
    	, B.LN_LMT_AMT  	           /*  한도금액              */
    	, B.EXCH_AMT_2              /*  정기상환금액          */
    	, B.EXCH_AMT_3                /*  조기상환금액          */
    	, B.G_GB
    	, B.DIVI_EXCH_GB                            /*  분할상환상세유형코드  */
    	, B.INTP_END_YMD                            /*  분할상환종료일자      */
    	, B.INTP_DDS                                      /*  분할상환일수          */
    	, B.LEXCH_METH
    	, B.MPAY_AMT                              /*  분할상환금액          */
    	, B.DEF_PRD                                /*  거치기간              */
    	, B.PM_CYL
    	, B.DEFR_EXPD_YMD
    	, B.RATE_FIX_GB
    	, B.LINT_CMP_METH
    	, B.ADPT_RATE
    	, B.RECT_RATE
    	, S.DSCR                                                            /* DSCR */   
    	, S.FAIR_VAL_SNR
    	, S.LTV_SNR
    	, S.COLL_SET_AMT
    	, S.LTV
    	, S.CRGR_VLT                                              /*  신용등급(평가)        */
        , S.CRGR_KICS                                         /*  신용등급(KICS)        */
    	, S.PROD_KEY                                          /*  종목코드              */
    	, S.PROD_NM                                           /*  종목명                */		
    --	, S.KICS_PROD_TPCD
    --	, S.KICS_PROD_TPNM
        , CASE WHEN A.EXPD_YMD IS NULL 		THEN TO_CHAR(TO_DATE('$$STD_Y4MD','YYYYMMDD') + 364, 'YYYYMMDD')
                     WHEN A.EXPD_YMD <= '$$STD_Y4MD' 	THEN TO_CHAR(TO_DATE('$$STD_Y4MD','YYYYMMDD') + 364, 'YYYYMMDD')
                     ELSE A.EXPD_YMD				END										                                AS MATR_DATE                    /*  만기일자              */	    
        , NVL(DECODE(NVL(B.ADPT_RATE,0),0, B.RECT_RATE, B.ADPT_RATE), 0) / 100              AS IRATE                        /*  금리                  */
        , CASE 	WHEN B.LINT_CMP_METH IN ('11','21') THEN 'Y'
			WHEN B.LINT_CMP_METH IN ('12','22') THEN 'N'
			ELSE NULL END                                  AS INT_PROR_PAY_YN              /*  이자선지급여부        */
        , CASE WHEN B.LEXCH_METH = '10' THEN 0  -- 만기일시상환
             WHEN B.LEXCH_METH = '20' THEN 1  -- 분할상환 (값없음) 원금균등으로 우선 처리
             WHEN B.LEXCH_METH = '30' THEN 1  -- 원금균등
             WHEN B.LEXCH_METH = '50' THEN 2  -- 거치후원리균등
             WHEN B.LEXCH_METH = '60' THEN 0  -- 고객임의상환
             WHEN B.LEXCH_METH = '70' THEN 2  -- 원리균등
             WHEN B.LEXCH_METH = '80' THEN 2  -- 원금잔존원리균등
             WHEN B.LEXCH_METH = '81' THEN 2  -- 거치후 원금잔존원리균등
             ELSE 0 END                                           				AS AMORT_TPCD                   /*  분할상환유형코드      */	
        , L.FST_INT_GET                                     		  		AS FRST_INT_DATE               /*  최초이자기일          */
        , CASE 	WHEN L.NEXT_INT_YMD  >=  A.EXPD_YMD THEN NULL                                         --만기일자보다 크면 NULL 처리
			        ELSE L.NEXT_INT_YMD END                      AS NEXT_INT_DATE               /*  차기이자기일          */
        , L.DEF_TRAN_YMD                                   		   	  	AS GRACE_END_DATE              /*  거치종료일자          */	
        , CASE 	WHEN S.KICS_PROD_TPCD IS NOT NULL								THEN S.KICS_PROD_TPCD
			WHEN A.COLL_KND = '30' AND 	A.BLDG_USE_GB BETWEEN '11' AND '19'  	THEN '350'		/*주거용 부동산 담보(독립형) :  연계형은 추가 식별함*/
			WHEN A.COLL_KND = '30' 										    	  	THEN '352'		/*상업용 부동산 담보(독립형) : 연계형은 추가 식별함 */
			WHEN A.LN_CONT_GB = '10'             									THEN '310'		/*개인신용대출	*/
			WHEN A.LN_CONT_GB = '20'	AND T.SUM_LN_AMT <= 1000000000          	THEN '321'      	/*거래상대방합계 10억원 이하이면 중소기업신용대출	*/ 
			WHEN A.LN_CONT_GB = '20'	AND T.SUM_LN_AMT >= 1000000000          	THEN '320'		/*거래상대방합계 10억원 이상이면 기업신용대출	*/ 
			ELSE '399' END                                 			AS KICS_PROD_TPCD             /*  상품구분코드   */ 
			
    	, CASE 	WHEN S.KICS_PROD_TPCD IS NOT NULL								THEN S.KICS_PROD_TPNM
			WHEN A.COLL_KND = '30' AND 	A.BLDG_USE_GB BETWEEN '11' AND '19'  	THEN '부동산담보대출(주거용/독립형)'		
			WHEN A.COLL_KND = '30' 										    	  	THEN '부동산담보대출(상업용/독립형)'		
			WHEN A.LN_CONT_GB = '10'             									THEN '개인신용대출'		
			WHEN A.LN_CONT_GB = '20'	AND T.SUM_LN_AMT <= 1000000000          	THEN '기업신용대출(중소기업)'     
			WHEN A.LN_CONT_GB = '20'	AND T.SUM_LN_AMT >= 1000000000          	THEN '기업신용대출'		  
			ELSE '기타미분류대출' END                                 			AS KICS_PROD_TPNM            					 /*  KICS상품구분명   */ 		 
     FROM IDHLNH_IFRS_AST_SOUN 	A                               /*  IFRS자산건전성월마감(FNTBFSMC) */
		, IDHLNH_LONG_MM_CLSN 	B                               /*  융자월마감 */
		, IKRUSH_PROD_LOAN 	 	S								/* 사용자 입력 정보*/	
		, T_LNDP					L							/* IFRS 여수신계죄 */
		, T_CUST					T							/* 고객별 잔액  */
    WHERE A.FIN_YM 		= SUBSTR('$$STD_Y4MD',1,6)
	AND A.EVAL_GD_02 	IS NOT NULL 			-- 20230412 김재우 수정 NULL인 건은 SAP 보유원장에서 관리 : 별도 배치 처리
	AND A.LN_CONT_GB 	<> '30'   					-- 사모사채 대출채권 제외 : 별도 배치 처리
	AND A.LN_RAMT 		> 0          -- 잔액이 0이상
	AND A.FIN_YM 		= B.FIN_YM(+)
	AND A.LN_NO  		= B.LN_NO(+)
	AND B.LN_FIN_GB(+)	= '99'  -- 마감인건만
	AND B.LN_STS(+) 	= '01'     -- 정상인건만
	AND B.LN_RAMT(+) 	> 0       -- 잔액이 0이상
	AND A.LN_NO 		= S.PROD_KEY(+)
	AND S.BASE_DATE(+) = '$$STD_Y4MD'
	AND A.LN_NO 		= L.LN_NO(+)
	AND A.CUST_ID 		= T.CUST_ID(+)
)
--SELECT * FROM T_BASE WHERE LN_NO = '9987205200018';
, T_APPR AS (
	SELECT LN_COLL_ID
	   , SUM(DECODE (UNDW_ITEM_GB,'01',UNDW_ITEM_AMT,0)) 	AS GAM_AMT
	   , SUM(DECODE (UNDW_ITEM_GB,'02',UNDW_ITEM_AMT,0)) 	AS SUN_AMT
	FROM IDHLNI_APPR_ICDC		 /*감정가감액*/
	WHERE RAL_UNDW_ITEM NOT IN ('51','52','53')
	 AND LN_COLL_ID||AVL_END_YMD IN ( SELECT LN_COLL_ID||MIN(AVL_END_YMD)
										FROM IDHLNI_APPR_ICDC /*감정가감액*/
									   WHERE AVL_END_YMD >= '$$STD_Y4MD'
									   GROUP BY LN_COLL_ID
									)
	GROUP BY LN_COLL_ID
)
, T_PROP_COLL AS (
	SELECT F.LN_NO
		, F.LN_COLL_ID
		, F.MN_COLL_YN
		, F.TRTR_ENO
		, NVL(G.LND_AVAL,0)                                     AS LND_AVAL                /*  토지감정금액          */
		, NVL(G.BLDG_AVAL,0)                                    AS BLDG_AVAL              /*  건물감정금액          */
		, NVL(H.GAM_AMT,0)                                      AS GAM_AMT                 /*  감액금액              */
		, NVL(H.SUN_AMT,0)					                            AS SUN_AMT			            /* 선순위금액               */
	FROM  IDHLNM_ESTE_VLDT_MRTG 		F                                                   /*부동산 유효담보 및 주담보*/
			, IDHLNI_APVL 					    G                                                   /*감정가*/
			, T_APPR       				 		  H								                                    /*감정가감액*/		
	WHERE 1=1		
	AND F.LN_COLL_ID 		= G.LN_COLL_ID(+)
	AND G.AVL_STR_YMD(+) <= '$$STD_Y4MD'
	AND G.AVL_END_YMD(+) >= '$$STD_Y4MD'
	AND F.LN_COLL_ID 		= H.LN_COLL_ID(+)			-- OUTER ???		
)
, T_SPRD AS (
	SELECT LN_NO
		, SUM( DECODE(SUBSTR(NVL(CTRL_RATE_CD, 'C000' ),1,1),'C',1,-1) * TO_NUMBER(SUBSTR(NVL(CTRL_RATE_CD, 'C000' ),2,4))/1000) / 100  AS ADT_RATE
	FROM IDHLNI_ADD_ATTR_ITRT_DESC /*가산속성금리내역*/
	WHERE '$$STD_Y4MD' BETWEEN ADPT_SYMD AND ADPT_EYMD
	GROUP BY LN_NO
)
, T_USR_COLL AS (
	SELECT * 	
	  FROM IKRUSH_COLL_SET_RTO
	  WHERE BASE_DATE = (SELECT MAX(BASE_DATE)  -- 업로드 미수행시 가장 최근값 가져옴
						 FROM IKRUSH_COLL_SET_RTO  
						WHERE BASE_DATE <= '$$STD_Y4MD' 
					  ) 
)
, T_REFIX AS (
	SELECT SUBSTR(EID,4,5) 										AS LN_PITEM 
  --		,EID
    		, MAX( DECODE(DM_ALIAS, 'FN_RCH_HEST_CYL', TO_NUMBER(DECODE(NVAL_1,0,NULL,REPLACE(NVAL_1,'*',NULL))), NULL))	AS IRATE_RPC_CYC_1
    		, MAX( DECODE(DM_ALIAS, 'FN_EXT_RCH_CYL' , TO_NUMBER(DECODE(NVAL_1,0,NULL,REPLACE(NVAL_1,'*',NULL))), NULL))	AS IRATE_RPC_CYC_2
    		, MAX( DECODE(DM_ALIAS, 'FN_RATE_FIX_PRD', DECODE(TVAL_1, '*', NULL, TVAL_1), NULL))  AS RATE_FIX_PRD
	FROM IDHLFM_GDS_ITEM_VAL -- 상품항목값
	WHERE DM_ALIAS IN ('FN_RATE_FIX_PRD', 'FN_RCH_HEST_CYL', 'FN_EXT_RCH_CYL')
	GROUP BY SUBSTR(EID,4,5)
)
--SELECT * FROM T_REFIX;
, T_AAA AS (
SELECT /* SQL_ID : KRBH101BM */
        '$$STD_Y4MD'                                            AS BASE_DATE                   /*  기준일자              */
      , 'LOAN_O1'||A.LN_NO                                  AS EXPO_ID                    /*  익스포저ID            */
      , DECODE(A.EVAL_GD_02,'0000','G000',A.EVAL_GD_02)     AS FUND_CD                    /*  펀드코드              */
      , C.LN_CD||'_'||LPAD(A.LN_PITEM,5,'0')           AS PROD_TPCD                  /*  상품유형코드          */
      , C.LN_NM||'_'||D.MTVAL_1                           AS PROD_TPNM                  /*  상품유형명            */
      , CASE WHEN A.KICS_PROD_TPCD ='350' AND E.UNDW_AMT_01 >0 THEN '351' 			  /*  연계형 구분            */
      		   WHEN A.KICS_PROD_TPCD ='352' AND E.UNDW_AMT_01 >0 THEN '353' 			  /*  연계형 구분            */
	  	       ELSE KICS_PROD_TPCD 		  END			                              AS	KICS_PROD_TPCD
		  
    	, CASE WHEN  A.KICS_PROD_TPCD ='350' AND E.UNDW_AMT_01 >0 THEN '부동산담보대출(주거용/연계형)' 
      		   WHEN  A.KICS_PROD_TPCD ='352' AND E.UNDW_AMT_01 >0 THEN '부동산담보대출(상업용/연계형)' 
      	  	 ELSE KICS_PROD_TPNM 		  END			                              AS	KICS_PROD_TPNM		  
      , A.PROD_KEY                                          AS ISIN_CD                      /*  종목코드              */
      , A.PROD_NM                                           AS ISIN_NM                      /*  종목명                */
      , CASE WHEN TRIM(A.EVAL_GD_02) IN ('0000') THEN '1_' ELSE '2_' END
        ||C.ACCO_CD                                         AS ACCO_CD                      /*  계정과목코드          */ -- RDM과 동일하게 처리 (실제 계정과목 존재함)
      , C.ACCO_NM                                           AS ACCO_NM                      /*  계정과목명            */ -- 사모사채 없음
      , A.LN_NO                                             AS CONT_ID                      /*  계약ID                */
-- 20230602 김재우 수정 
-- 개인대출로 수정
--      , CASE WHEN A.LN_CONT_GB = '10' OR B.G_GB = '40' THEN '1'                                                          -- 개인대출이거나 ,개인사업자
--             ELSE '2' END                                   AS INST_TPCD                    /*  인스트루먼트유형코드  */ -- 개인대출은 1 기업대출은 2
      , '1' 				AS INST_TPCD   
      , CASE WHEN A.LEXCH_METH IN ('10','60')
             THEN CASE WHEN A.RATE_FIX_GB IN ('10')      THEN '4'
                       WHEN A.RATE_FIX_GB IN ('20','40') THEN '5'
                       WHEN A.RATE_FIX_GB ='30'
                       THEN CASE WHEN R.RATE_FIX_PRD IS NULL THEN '5' -- 20230414 김재우 추가 금리고정기간이 NULL이면 변동처리
                                 WHEN ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD)  < TO_DATE('$$STD_Y4MD','YYYYMMDD')
                                                         THEN '5' ELSE '4' END
                       ELSE '4' END
             WHEN A.LEXCH_METH IN ('20', '30', '50', '70', '80', '81')
             THEN CASE WHEN A.RATE_FIX_GB IN ('10')      THEN '6'
                       WHEN A.RATE_FIX_GB IN ('20','40') THEN '7'
                       WHEN A.RATE_FIX_GB IN ('30') 
                       THEN CASE WHEN R.RATE_FIX_PRD IS NULL THEN '7' -- 20230414 김재우 추가 금리고정기간이 NULL이면 변동처리
                                 WHEN ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD) >= TO_DATE('$$STD_Y4MD','YYYYMMDD') THEN '6'
                                 WHEN ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD)  < TO_DATE('$$STD_Y4MD','YYYYMMDD') THEN '7'
                                 ELSE '4' END
                       ELSE '4' END
             ELSE '4' END                                   AS INST_DTLS_TPCD               /*  인스트루먼트상세유형코드*/
      , A.LN_YMD                                            AS ISSU_DATE                    /*  발행일자              */
	, A.MATR_DATE		
      , 'KRW'                                               AS CRNY_CD                      /*  통화코드              */
      , 1                                                   AS CRNY_FXRT                    /*  통화환율              */
      , A.LN_RAMT                                           AS NOTL_AMT                     /*  이자계산기준원금      */  -->분할상환의 경우 현재잔액이 원금
      , A.LN_AMT                                            AS NOTL_AMT_ORG                 /*  최초이자계산기준원금  */  -->대출금액
      , A.LN_RAMT                                           AS BS_AMT                       /*  BS금액                */
      , NULL                                                AS VLT_AMT                      /*  평가금액              */
      , NULL                                                AS FAIR_BS_AMT                  /*  공정가치BS금액        */
      , A.UC_AMT                                            AS ACCR_AMT                     /*  미수수익금액          */
      , NULL                                                AS UERN_AMT                     /*  선수수익              */
--      , NVL(DECODE(NVL(A.ADPT_RATE,0),0, A.RECT_RATE, A.ADPT_RATE), 0) / 100                           AS IRATE                        /*  금리                  */
    	, A.IRATE
      , NVL(TO_NUMBER(A.PM_CYL), '12')                      AS INT_PAY_CYC                  /*  이자지급주기          */
      , CASE WHEN A.LINT_CMP_METH IN ('11','21') THEN 'Y'
             WHEN A.LINT_CMP_METH IN ('12','22') THEN 'N'
             ELSE NULL END                                  AS INT_PROR_PAY_YN              /*  이자선지급여부        */
      , CASE WHEN A.RATE_FIX_GB = '10' THEN '1'          -- 고정
             WHEN A.RATE_FIX_GB IN ('20','40') THEN '2'  -- 변동 및 ARM은 변동
	           WHEN A.RATE_FIX_GB = '30' AND R.RATE_FIX_PRD IS NULL THEN '2' --금리고정기간이 없는 경우 변동처리		 
             WHEN A.RATE_FIX_GB = '30' AND ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD) >= TO_DATE('$$STD_Y4MD','YYYYMMDD') THEN '1' -- 금리고정기간이 기준일보다 크거나 같으면 고정
             WHEN A.RATE_FIX_GB = '30' AND ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD)  < TO_DATE('$$STD_Y4MD','YYYYMMDD') THEN '2' -- 금리고정기간이 기준일보다 작으면 변동
             ELSE '9' END                                   AS IRATE_TPCD                   /*  금리유형코드          */               -- 10고정 20변동 30고정후변동 40ARM , 이외는 비금리처리(없음)
      , NULL                                                AS IRATE_CURVE_ID               /*  금리커브ID            */
      , A.RADPT_METH                                        AS IRATE_DTLS_TPCD              /*  금리상세유형코드      */
      , O.ADT_RATE                                          AS ADD_SPRD                     /*  가산스프레드          */
      , CASE WHEN A.RATE_FIX_GB = '10' THEN NULL  --고정금리 NULL
             WHEN A.RATE_FIX_GB IN ('20', '40') AND TRIM(A.DEFR_EXPD_YMD) IS NULL                                 THEN NVL(R.IRATE_RPC_CYC_1, TO_NUMBER(A.PM_CYL))      -- 변동 및 ARM, 만기미연장건이면 연장전 변동금리
             WHEN A.RATE_FIX_GB IN ('20', '40') AND TRIM(A.DEFR_EXPD_YMD) IS NOT NULL THEN NVL(R.IRATE_RPC_CYC_2, TO_NUMBER(A.PM_CYL))                                  -- 변동 및 ARM, 만기연장건이면  연장후 변동금리
             WHEN A.RATE_FIX_GB = '30'          AND R.RATE_FIX_PRD IS NULL AND TRIM(A.DEFR_EXPD_YMD) IS NULL      THEN NVL(R.IRATE_RPC_CYC_1, TO_NUMBER(A.PM_CYL))      --20230414 김재우 추가 금리기간 고정이 NULL인 경우고정후변동, 만기미연장건이면 연장전 변동금리
             WHEN A.RATE_FIX_GB = '30'          AND R.RATE_FIX_PRD IS NULL AND TRIM(A.DEFR_EXPD_YMD) IS NOT NULL  THEN NVL(R.IRATE_RPC_CYC_2, TO_NUMBER(A.PM_CYL))      --20230414 김재우 추가 금리기간 고정이 NULL인 경우고정후변동, 만기연장건이면  연장후 변동금리
             WHEN A.RATE_FIX_GB = '30'          AND ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD) >= TO_DATE('$$STD_Y4MD','YYYYMMDD') THEN NULL                     -- 고정후변동, 고정금리기간이 지나지 않은 건 NULL
             WHEN A.RATE_FIX_GB = '30'          AND TRIM(A.DEFR_EXPD_YMD) IS NULL                                 THEN NVL(R.IRATE_RPC_CYC_1, TO_NUMBER(A.PM_CYL))      -- 고정후변동, 만기미연장건이면 연장전 변동금리
             WHEN A.RATE_FIX_GB = '30'          AND TRIM(A.DEFR_EXPD_YMD) IS NOT NULL                             THEN NVL(R.IRATE_RPC_CYC_2, TO_NUMBER(A.PM_CYL))      -- 고정후변동, 만기연장건이면  연장후 변동금리
             ELSE NULL END                                  AS IRATE_RPC_CYC               /*  금리개정주기          */
      , '1'                                                 AS DCB_CD                      /*  일수계산코드          */                 -- 대출은 ACT/365로 처리
      , '1'                                                 AS CF_GEN_TPCD                 /*  현금흐름일생성구분    */                 -- 1금리부 자산
      , A. FRST_INT_DATE               /*  최초이자기일          */
      , A.NEXT_INT_DATE               /*  차기이자기일          */
      , A.GRACE_END_DATE              /*  거치종료일자          */	  
      , A.DEF_PRD                                          AS GRACE_TERM                  /*  거치기간              */
      , NULL                                                AS IRATE_CAP                   /*  적용금리상한          */
      , NULL                                                AS IRATE_FLO                   /*  적용금리하한          */
      , A.LEXCH_METH                                        AS RPAY_TPCD                   /*  상환유형코드          */
      , NULL                                                AS AMORT_TERM                  /*  원금분할상환주기      */
      , A.MPAY_AMT                                          AS AMORT_AMT                   /*  분할상환금액          */
    	, A.AMORT_TPCD		                                                                    /*  분할상환코드         */
      , A.DIVI_EXCH_GB                                      AS AMORT_DTLS_TPCD              /*  분할상환상세유형코드  */
      , A.INTP_END_YMD                                      AS AMORT_END_YMD                /*  분할상환종료일자      */
      , A.INTP_DDS                                          AS AMORT_DAYS                   /*  분할상환일수          */
      , TO_CHAR(ADD_MONTHS(TO_DATE(A.LN_YMD,'YYYYMMDD'), R.RATE_FIX_PRD),'YYYYMMDD') AS IRATE_FIX_TERM /*금리고정기간*/
      , A.CUST_ID                                           AS CNPT_ID                      /*  거래상대방ID          */
      , A.CORP_NM                                           AS CNPT_NM                      /*  거래상대방명          */
      , A.CORP_NO                                           AS CORP_NO                      /*  법인번호              */
      , NULL                                                AS CRGR_KIS                     /*  신용등급(한신평)      */
      , NULL                                                AS CRGR_NICE                    /*  신용등급(한신정)      */
      , NULL                                                AS CRGR_KR                      /*  신용등급(한기평)      */
      , A.CRGR_VLT                                          AS CRGR_VLT                     /*  신용등급(평가)        */
      , A.CRGR_KICS                                         AS CRGR_KICS                    /*  신용등급(KICS)        */
      , NVL(A.CUST_GB,'30')                                 AS CUST_GB                      /*  고객구분              */ -- 사모사채 30
      , A.G_GB                                              AS CUST_TPCD                    /*  고객유형코드          */
      , A.ENTP_SCLE_CD                                      AS CORP_TPCD                    /*  법인유형코드          */
      , A.LN_CONT_GB                                        AS LOAN_CONT_TPCD               /*  대출계약유형코드      */
--    	, A.NICE_CRED_GD
        , A.CB_CRGR_NICE
    	, A.CB_CRGR_KCB
      , F.LN_COLL_ID                                        AS COLL_ID                      /*  담보ID                */
      , A.COLL_KND                                          AS COLL_TPCD                    /*  담보유형코드          */
      , A.COLL_ATTR                                         AS COLL_DTLS_TPCD               /*  담보상세유형코드      */
      , A.BLDG_USE_GB                                       AS PRPT_DTLS_TPCD               /*  부동산상세유형코드    */
      , E.UNDW_AMT_01                                       AS RENT_GUNT_AMT                /*  임대보증금            */
      , CASE WHEN A.COLL_KND = '30' AND E.UNDW_AMT_01 > 0 THEN '2'
             WHEN A.COLL_KND = '30'                       THEN '1'
             ELSE NULL END                                  AS PRPT_OCPY_TPCD               /*  부동산점유유형코드    */ -- 1.자가 2.임대
      , NVL(F.LND_AVAL,0)                                   AS LAND_APPR_AMT                /*  토지감정금액          */
      , NVL(F.BLDG_AVAL,0)                                  AS BULD_APPR_AMT                /*  건물감정금액          */
      , NVL(F.LND_AVAL,0) + NVL(F.BLDG_AVAL,0)              AS COLL_AMT                     /*  담보금액              */
      , F.GAM_AMT                                    AS REDC_AMT                            /*  감액금액              */
      , NVL(A.FAIR_VAL_SNR, DECODE(A.COLL_KND, 30,NVL(F.SUN_AMT,0), NULL)) 
                                                            AS PRIM_AMT                     /*  선순위금액            */
      , NVL(A.LTV,
        DECODE(A.COLL_KND, 30, DECODE( F.LND_AVAL + F.BLDG_AVAL - F.GAM_AMT, 0, 0
                                      , ROUND((NVL(A.LN_RAMT,0) + F.SUN_AMT) / (F.LND_AVAL + F.BLDG_AVAL - F.GAM_AMT), 2) ) * 100
               , NULL) )                                    AS LTV                          /*  LTV                   */
      , CASE WHEN A.COLL_SET_AMT IS NOT NULL  THEN A.COLL_SET_AMT
             WHEN A.COLL_KND = '30'           THEN A.LN_AMT * P.COLL_SET_RTO
             ELSE NULL END                                  AS COLL_SET_AMT                 /*  담보설정금액          */           
      , DECODE(A.COLL_KND, '30', P.COLL_SET_RTO, NULL)      AS COLL_SET_RTO                 /*  담보설정비율          */           
      , A.LN_LMT_AMT                                        AS LMT_AMT                      /*  한도금액              */
      , A.EXCH_AMT_2                                        AS REG_RPAY_AMT                 /*  정기상환금액          */
      , A.EXCH_AMT_3                                        AS PRE_RPAY_AMT                 /*  조기상환금액          */
      , A.CMM_ARR_DDS                                           AS ARR_DAYS                     /*  연체일수              */	  
      , A.ASET_SOND_CD                                      AS FLC_CD                       /*  자산건전성코드        */
      , A.AP_AMT_TOT                                        AS ABD_AMT                     /*  대손충당금금액        */
      , A.UC_AP_AMT                                         AS ACCR_ABD_AMT                 /*  미수수익대손충당금금액*/
      , A.CUR_PRIC                                          AS LOCF_AMT                     /*  이연대출부대손익금액  ???? 시세*/
      , A.PRNK_AMT                                          AS PV_DISC_AMT                  /*  현재가치할인차금금액  ??? 선순위 */
      , A.RET_DEPT  	                                       AS DEPT_CD                      /*  부서코드              */
      , 'KRBH101BM'                                         AS LAST_MODIFIED_BY             /*  최종수정자            */
      , SYSDATE                                             AS LAST_MODIFIED_DATE           /*  최종수정일자          */
--20230510 김재우 추가
-- KICS 고도화 관련 칼럼추가      
      , NULL                                                AS CRGR_SNP                     /* 신용등급(S&P)          */ 
      , NULL                                                AS CRGR_MOODYS                  /* 신용등급(무디스)       */
      , NULL                                                AS CRGR_FITCH                   /* 신용등급(피치)         */
      , NULL                                                AS IND_L_CLSF_CD                /* 산업대분류코드         */ 
      , NULL                                                AS IND_L_CLSF_NM                /* 산업대분류명           */ 
      , NULL                                                AS IND_CLSF_CD                  /* 산업분류코드           */ 
      , NVL(A.LTV_SNR,
        DECODE(A.COLL_KND, 30, DECODE(  F.LND_AVAL+ F.BLDG_AVAL - F.GAM_AMT, 0, 0
                                      , ROUND(F.SUN_AMT / (F.LND_AVAL + F.BLDG_AVAL - F.GAM_AMT), 2) ) * 100
               , NULL) )                                           								AS SNR_LTV	                    			/* 선순위LTV(%)) */
      , NVL(A.FAIR_VAL_SNR, DECODE(A.COLL_KND, 30, F.SUN_AMT, NULL)) 	AS FAIR_VAL_SNR               	/* 선순위대출금액 */
	, A.DSCR                                              AS DSCR                         /* DSCR */   	  
                                                            
  FROM T_BASE						        A
      , T_ACCO        				 		C
      , IDHLFM_GDS_ITEM_VAL 				D                                                            /*상품항목값 : 상품 유형명 상세*/
      , IDHLNH_ESTE_MRTG_LONS_DTL 		    E	                                                         /*부동산담보대출상세내역: 임대보증금 */
      , T_PROP_COLL			 		      	F								/* 부동산 담보 */	
      , T_SPRD							    O								/* 가산 스프레드 */
      , T_USR_COLL						    P            					 /*사용자입력 부동산 담보	*/
      , T_REFIX 							R						  /*금리변경주기	*/
WHERE 1=1	 
   AND A.B_LN_CONT_GB||A.B_COLL_KND||A.B_COLL_ATTR 	= C.LN_CD(+)
--   AND CASE WHEN TRIM(A.EVAL_GD_02) IN ('0000') THEN 'ACCO_G' ELSE 'ACCO_S' END = C.GRP_CD 
   AND A.LN_PITEM 				= SUBSTR(D.EID(+),4,5)
   AND D.DM_ALIAS(+)			='FN_ABBR_PNM'                --상품명
   AND A.FIN_YM 				= SUBSTR(E.TRT_YMD(+),1,6)
   AND A.LN_NO  				= E.LN_NO(+)
   AND E.PRI_GUBN(+) 			= '1'
   AND A.LN_NO 				    = F.LN_NO(+)
   AND F.MN_COLL_YN(+) 		    = '1' -- 주담보만
   AND A.LN_NO 	 			    = O.LN_NO(+)
   AND A.CUST_GB 			    = P.CUST_GB(+)														-- B TABLE 은 이미  OUTER JOIN 된 테이블임.
   AND A.LN_PITEM 			    = R.LN_PITEM(+)
  )
  SELECT * 
  FROM T_AAA 
--  WHERE PROD_TPCD = '203030_61103'