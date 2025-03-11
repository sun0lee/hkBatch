/* 수정사항 
- 계정코드별 예외처리 : ACCO_EXCPTN
- SUB TABLE WITH 문 처리

기존 프로그램내 계정과목별 예외 처리 --24.04.29 계정코드별 신용리스크 보고서 COA 매핑
    1. 신용리스크 분류코드 매핑 : SUBSTR(A.ACCO_CD,1,4) ='1800' => (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC4')  신용리스크 보고서 COA : 보험미수금
    2. 신용리스크 분류코드 매핑 : SUBSTR(A.ACCO_CD,1,4) ='1830' => (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC5')  신용리스크 보고서 COA : 미수수익
    3. 신용리스크 분류코드 매핑 : SUBSTR(A.ACCO_CD,1,4) ='1890' => (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC6) 신용리스크 보고서 COA : 본지점계정차
*/
WITH /* SQL-ID : KRDB101BM */  
ACCO_EXCPTN AS (
        SELECT CD AS EXCPTN_TYP , CD_NM, CD2 AS ACCO_CD , CD2_NM 
        FROM IKRUSH_CODE_MAP
        WHERE grp_cd ='ACCO_EXCPTN'
        AND RMK='KRDB101BM'
)
--SELECT DISTINCT EXCPTN_TYP FROM ACCO_EXCPTN ;
 ,T_COLL AS (			
	SELECT COLL_ID
			, FUND_CD
			, SUM(COLL_APPR_AMT)                                               AS COLL_APPR_AMT
			, SUM(WTD_APPR_RTO)                                                AS COLL_APPR_RTO                   -- 금액가중평균 담보인정비율
		 FROM (
				 SELECT A.*
					, A.COLL_VLT_AMT / SUM(A.COLL_VLT_AMT) OVER (PARTITION BY A.COLL_ID,A.FUND_CD) * A.COLL_APPR_RTO AS WTD_APPR_RTO
				   FROM IKRUSH_COLL A							/*적격금융담보*/
				  WHERE BASE_DATE = '$$STD_Y4MD'
			  )
	  GROUP BY COLL_ID, FUND_CD
)
--SELECT * FROM T_COLL;
, T_FX AS (
	SELECT STND_DT
		, SUBSTR(FOEX_CD, 6, 3)                                             AS CRNC_CD
		, CLPC / DECODE(NVL(PRC_DISP_UNIT_VAL, 0), 0, 1, PRC_DISP_UNIT_VAL) AS FX_RATE
	FROM IDHKRH_FX
	WHERE 1=1
	AND STND_DT = ( SELECT MAX(STND_DT) FROM IDHKRH_FX  -- 외화환율데이터
					WHERE STND_DT <= '$$STD_Y4MD' )
)
--SELECT * FROM T_FX  ;
, T_PUBI AS (
	SELECT A.*
	FROM (
			SELECT A.* 
				, ROW_NUMBER() OVER (PARTITION BY CORP_NO ORDER BY BASE_DATE DESC) AS RN
			 FROM IKRUSH_PUBI 		A
			WHERE BASE_DATE <= '$$STD_Y4MD'
		 )	A
	WHERE RN = 1                                                                                   -- BASE_DATE 역순으로 정렬하여 최신값 적용
)
--SELECT * FROM T_PUBI ;
, T_GUNT AS (
	SELECT IBA.BASE_DATE
		, IBA.GUNT_ID
		, IBA.GUNT_AGNC_CRGR
		, DECODE(IBA.GUNT_AGNC_CRGR, 'RF', 'RF', IBB.CRGR_KICS)            AS CRGR_GUNT
		, IBA.GUNT_AGNC_NM
		, IBA.GUNT_AGNC_CORP_NO
		, IBA.GUNT_RTO / 100                                               						AS GUNT_COEF
		, IBB.CRGR_KICS
		, CASE WHEN GUNT_AGNC_CORP_NO IN ('1356710033355') AND GUNT_AGNC_CRGR = 'RF'   	THEN 'SS1'       -- 토지주택공사 보증이 있는 익스포저 분해함(정부보증, 공공보증)
			   WHEN GUNT_AGNC_CORP_NO IN ('1356710033355') AND GUNT_AGNC_CRGR <> 'RF'	THEN 'SP1'      -- 정부보증이외의 비율에 대해서는 공공기관 처리(정부 2/3, 공공 1/3)			   
			   ELSE NVL(IBC.ASSET_CLSF_CD, 'SC1') END                      		AS GUNT_ASSET_CLSF_CD
		, COUNT(*) OVER ( PARTITION BY GUNT_ID) 						AS CNT					                                      
	 FROM IKRUSH_GUNT 		IBA
		, IKRUSH_MAP_CRGR 	IBB
		, T_PUBI 				IBC
	WHERE 1=1
	  AND IBA.BASE_DATE         		= '$$STD_Y4MD'
	  AND IBA.GUNT_AGNC_CRGR    	= IBB.CRGR_DMST(+)
	  AND IBA.GUNT_AGNC_CORP_NO 	= IBC.CORP_NO(+)
)			
--SELECT * FROM T_GUNT ;
, T_EXPO AS (
	SELECT A.BASE_DATE
		, EXPO_ID
		, FUND_CD
		, ASSET_TPCD
		, KICS_PROD_TPCD
		, KICS_PROD_TPNM
		, ISIN_CD
		, ISIN_NM
		, LT_TP
		, PRNT_ISIN_CD
		, PRNT_ISIN_NM
		, ACCO_CLCD
		, DECODE(A.ACCO_CLCD, '1', '1', '2')                                        AS IFRS_ACCO_CLCD
		, ACCO_CD
		, ACCO_NM
		, EFFE_MATR
		, CR_CLSF_CD
		, CR_CLSF_NM
		, CR_ACCO_TPCD
		, CR_ACCO_TPNM
		, CNPT_ID
		, CNPT_NM
		, CORP_NO
		, CRGR_KICS
		, LTV
		, SNR_LTV
		, FAIR_VAL_SNR
		, DSCR
		, CR_EXPO_AMT
		, FAIR_BS_AMT
		, ACCR_AMT
		
		, CASE 	WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) 	IN ('SS')  	THEN 'RF'                                                    -- 무위험대상은 RF처리
				 WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) 	IN ('SF')  		THEN '99'                                                    -- 특수금융은 무등급처리
				 WHEN A.CR_CLSF_CD              		IN ('SD2') 	THEN '4'                                                     -- MMF는 만기 1년미만 4등급 처리
				 ELSE NVL(A.CRGR_KICS, '99') END                                      AS CRGR_KICS_ADJ
				 
		, CASE WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SD')		THEN LEAST(GREATEST(NVL(A.EFFE_MATR, 0), 0.002), 0.997)
				WHEN NVL(A.EFFE_MATR, 0) < 0.002			THEN NVL(A.RMN_MATR , 0.002)
				 ELSE NVL(A.EFFE_MATR, 0.002) END 						AS EFFE_MATR2

		, DECODE(SUBSTR(A.CR_CLSF_CD, 1, 2), 'SL', NVL(A.DSCR, 999),NULL)        AS DSCR_ADJ
		
		, CASE WHEN A.COLL_KND = '50'                  THEN '50'                                            -- 지급보증 대출 (담보코드로 JOIN)
			   WHEN SUBSTR(A.EXPO_ID, 1, 6) = 'LOAN_O' THEN SUBSTR(A.EXPO_ID, 8)
			   WHEN SUBSTR(A.EXPO_ID, 1, 6) = 'SECR_O' THEN SUBSTR(A.EXPO_ID, 8)
			   ELSE NULL END                                               AS GUNT_KEY
			   
		, CASE WHEN SUBSTR(A.EXPO_ID, 1, 6) = 'LOAN_O' 	THEN SUBSTR(A.EXPO_ID, 8,13)
			    WHEN SUBSTR(A.EXPO_ID, 1, 6) = 'SECR_O' 	THEN A.ISIN_CD
			   ELSE NULL END                                               AS COLL_KEY                        -- EXPO_ID 8자리 이후부터의 ID, LT_TP = 'Y'이면 보증/담보KEY가 없음
			   
	FROM 	Q_IC_EXPO 	A
	WHERE 1=1
	  AND A.BASE_DATE = '$$STD_Y4MD'
	  AND A.LEG_TYPE       IN ('STD', 'NET')
	  AND A.ACCO_CLCD  NOT IN ('4', '5')                                                                    -- 4:퇴직연금(실적), 5:변액보험(5) 제외(1:일반, 2:퇴직보험, 3:퇴직연금, 9:난외약정으로 한정)
	  AND A.CR_CLSF_CD NOT IN ('NON')  	-- 신용리스크 미해당 제외
	  AND NVL(A.DFLT_YN,'N') = 'N' 			-- 부도여부가 N인것            
)	  
--SELECT * FROM T_EXPO ;
, T_ACCR_EXPO AS (
	SELECT A.* 
		,  'ACCR'							AS DIV
		,  A.EXPO_ID 						AS EXPO_ID_ADJ		
		,  A.ACCR_AMT  					AS FAIR_BS_AMT2
		,  A.ACCR_AMT  					AS CR_EXPO_AMT2
		,  A.LTV							AS LTV2
		, NVL(B.CRGR_GUNT, '00')			        AS GUNT_KEY_ADJ
		,  'Y'             						AS ACCR_YN	  
		, '4'								AS CR_ACCO_TPCD_ADJ
		, '비운용자산(파생상품 제외)' 			        AS CR_ACCO_TPNM_ADJ
                  , DECODE(SUBSTR(A.CR_CLSF_CD,1,2),'SR',	B.CRGR_GUNT,NULL)	                          AS CRGR_GUNT                       -- 미수수익은 보증처리X
                  , DECODE(SUBSTR(A.CR_CLSF_CD,1,2),'SR',	B.GUNT_AGNC_NM,NULL)                      AS GUNT_AGNC_NM
                  , DECODE(SUBSTR(A.CR_CLSF_CD,1,2),'SR',	B.GUNT_AGNC_CORP_NO,NULL)            AS GUNT_AGNC_CORP_NO
                  , DECODE(SUBSTR(A.CR_CLSF_CD,1,2),'SR',	DECODE(B.CNT, 2, 1, B.GUNT_COEF * 1),NULL)	AS GUNT_COEF                       -- 토지주택공사보증은 보증비율에 따라 익스포저 분리하였으므로 1(100%)로 처리
                  , DECODE(SUBSTR(A.CR_CLSF_CD,1,2),'SR',	B.GUNT_ASSET_CLSF_CD,NULL)		                AS GUNT_ASSET_CLSF_CD
                  , 0	                          	AS COLL_APPR_AMT
                  , 0                           	AS COLL_APPR_RTO
		 , 0.99				AS EFFE_MATR_ADJ					  
                  , '1'                              	AS MATR_CLCD

		, 0                                  AS CRM_COLL_AMT
		  , A.ACCR_AMT * NVL( B.GUNT_COEF, 1) 											AS  FAIR_BS_AMT_ADJ						
		  , A.ACCR_AMT * NVL( B.GUNT_COEF, 1)  											AS CR_EXPO_AMT_ADJ		
		 ,  A.ACCR_AMT * NVL( B.GUNT_COEF, 1) 											AS CRM_AFT_EXPO_AMT    
		 , CASE WHEN A.CR_CLSF_CD = 'SL4' 				AND A.LTV IS NULL 				THEN 9999
			     WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) ='SL'  	AND A.LTV IS NULL 				THEN 999   	-- LTV가 NULL인 경우 '999' 처리(상업용담보대출 임대수익연계 중 LTV, DSCR 모두 산출 불가로 분류)				 
			     WHEN A.LTV <= 0.0 														THEN 0.1	-- LTV가 0이하로 입력되는 경우 미매핑방지를 위해 처리		 
			     ELSE A.LTV END							AS LTV_ADJ
	 FROM 	 T_EXPO	A
			, T_GUNT 	B
			, T_COLL	C
   WHERE 1=1
	  AND A.ACCR_AMT > 0  				-- 미수수익이 0보다 큰건
	  AND A.GUNT_KEY = B.GUNT_ID(+)
	  AND A.COLL_KEY = C.COLL_ID(+)
	  AND A.FUND_CD  = C.FUND_CD(+)
)
, T_BASE_EXPO AS (
	SELECT A.*
		   , NVL(B.CRGR_GUNT, '00')					 AS GUNT_KEY_ADJ
		  ,  'N'             						 AS ACCR_YN
		  ,  A.CR_ACCO_TPCD						     AS CR_ACCO_TPCD_ADJ
		  ,  A.CR_ACCO_TPNM						     AS CR_ACCO_TPNM_ADJ
 		  ,  B.CRGR_GUNT                             AS CRGR_GUNT      
          , B.GUNT_AGNC_NM                           AS GUNT_AGNC_NM
          , B.GUNT_AGNC_CORP_NO                 	 AS GUNT_AGNC_CORP_NO
          , DECODE(B.CNT, 2, 1, B.GUNT_COEF * 1)	 AS GUNT_COEF                       -- 토지주택공사보증은 보증비율에 따라 익스포저 분리하였으므로 1(100%)로 처리
          , B.GUNT_ASSET_CLSF_CD                     AS GUNT_ASSET_CLSF_CD
          , C.COLL_APPR_AMT		                     AS COLL_APPR_AMT
          , C.COLL_APPR_RTO                          AS COLL_APPR_RTO
		  , EFFE_MATR2							     AS EFFE_MATR_ADJ		  
          , CASE WHEN A.EFFE_MATR2 >  14.0 THEN '14+'
                 WHEN A.EFFE_MATR2	 <=  0.0 THEN '1'
                 ELSE TO_CHAR( CASE WHEN CEIL(EFFE_MATR2) = FLOOR(EFFE_MATR2) THEN TRUNC(CEIL(EFFE_MATR2))                -- 초과~이하로 처리 (2.00년이면 1~2년 만기코드)
                                    ELSE CEIL(EFFE_MATR2) END )
                 END                                                                   								AS MATR_CLCD
						 
          , NVL( B.GUNT_COEF, 1) * NVL(LEAST(A.CR_EXPO_AMT2  , C.COLL_APPR_AMT), 0)			AS CRM_COLL_AMT
		  , A.FAIR_BS_AMT2 * NVL( B.GUNT_COEF, 1) 									AS  FAIR_BS_AMT_ADJ						
		  , A.CR_EXPO_AMT2 * NVL( B.GUNT_COEF, 1)  									AS CR_EXPO_AMT_ADJ		
--		 , CASE WHEN EXPO_ID_ADJ LIKE '%SNR'  THEN A.CR_EXPO_AMT2 
--			     ELSE NVL( B.GUNT_COEF, 1) * GREATEST(A.CR_EXPO_AMT2 - NVL(C.COLL_APPR_AMT, 0), 0)	
--			END 																			AS CRM_AFT_EXPO_AMT                -- 보증비율이 100%임을 전제로 함(IKRUSH_GUNT의 보증ID별로 100%이 안되면 익스포저가 축소됨!)   		  
		, CASE WHEN NVL(C.COLL_APPR_AMT, 0) > 0  THEN   NVL( B.GUNT_COEF, 1) * GREATEST(A.CR_EXPO_AMT2 - NVL(C.COLL_APPR_AMT, 0), 0)
			     ELSE CR_EXPO_AMT2 * NVL( B.GUNT_COEF, 1) 
			END 																			AS CRM_AFT_EXPO_AMT                -- 보증비율이 100%임을 전제로 함(IKRUSH_GUNT의 보증ID별로 100%이 안되면 익스포저가 축소됨!)   		  			
		 , CASE WHEN A.CR_CLSF_CD = 'SL4' 				AND A.LTV2 IS NULL 				THEN 9999
			     WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) ='SL'  	AND A.LTV2 IS NULL 				THEN 999   	-- LTV가 NULL인 경우 '999' 처리(상업용담보대출 임대수익연계 중 LTV, DSCR 모두 산출 불가로 분류)				 
			     WHEN A.LTV2 <= 0.0 														THEN 0.1	-- LTV가 0이하로 입력되는 경우 미매핑방지를 위해 처리		 
			     ELSE A.LTV2 END							AS LTV_ADJ		 
	FROM (
				SELECT A.*
					, 'BASE'		 								AS DIV
					,  A.EXPO_ID 									AS EXPO_ID_ADJ
				  	, A.FAIR_BS_AMT  							AS  FAIR_BS_AMT2
					, A.CR_EXPO_AMT + NVL( A.FAIR_VAL_SNR, 0)  	AS  CR_EXPO_AMT2
					,  CASE WHEN A.LTV <= 0.0 THEN 0.1 ELSE A.LTV END  AS LTV2
				FROM T_EXPO 		A
				WHERE A.CR_EXPO_AMT <> 0 			  
				UNION ALL			
				 SELECT A.* 
					, 'SNR'		 			AS DIV
					,  EXPO_ID||'_SNR' 		AS EXPO_ID_ADJ
					, 0                 	AS FAIR_BS_AMT2
					, -1 * A.FAIR_VAL_SNR  	AS  CR_EXPO_AMT_ADJ
					,  CASE WHEN A.SNR_LTV <= 0.0 THEN 0.1 ELSE A.SNR_LTV END  AS LTV2
				   FROM T_EXPO			 A				/* 선수위 차감   EXPO . 리스크 차감용*/	
				   WHERE A.FAIR_VAL_SNR > 0 			  --선순위 금액이 0보다 큰건
			)					 A
			  , T_GUNT 			 B
			  , T_COLL			 C
                  WHERE 1=1
                  AND A.GUNT_KEY =B.GUNT_ID(+)
                  AND A.COLL_KEY = C.COLL_ID(+)
                  AND A.FUND_CD  = C.FUND_CD(+)
)
--SELECT * FROM T_ACCR_EXPO ;
, T_CR_EXPO AS (
	SELECT A.BASE_DATE                                                                                                               /* 기준일자 */
			, A.EXPO_ID_ADJ										                            AS EXPO_ID                               /* 익스포저ID */
			, A.GUNT_KEY_ADJ                                                     			AS GUNT_KEY		                         /* 보증KEY */
			, A.ACCR_YN                                                                                                               /* 미수수익여부 */
			, A.FUND_CD                                                                                                               /* 펀드코드 */
			, A.ASSET_TPCD                                                                                                            /* 자산유형코드*/
			, A.KICS_PROD_TPCD                                                                                                        /* KICS상품유형코드 */
			, A.KICS_PROD_TPNM                                                                                                        /* KICS상품유형명*/
			, A.ISIN_CD                                                                                                               /* 종목코드 */
			, A.ISIN_NM                                                                                                               /* 종목명 */
			, A.LT_TP                                                                                                                 /* LOOK_THROUGH구분 */
			, A.ACCO_CLCD
			, A.ACCO_CD
			, A.ACCO_NM
			, A.CR_CLSF_CD                                                                         	AS ASSET_CLSF_CD                   /* 신용리스크분류코드 */
			, A.CR_CLSF_NM 										                                    AS ASSET_CLSF_NM                   /* 신용리스크분류명*/
			, A.CR_ACCO_TPCD_ADJ                                                                    AS CR_ACCO_TPCD                    /* 신용리스크계정분류코드 */
			, A.CR_ACCO_TPNM_ADJ								                                    AS CR_ACCO_TPNM  			/* 신용리스크계정분류명 */
			, A.EFFE_MATR_ADJ                                                                   	AS EFFE_MATR     /* 유효만기 */
			, A.MATR_CLCD                                                                                                             /* 만기구분코드 */
			, A.CNPT_ID                                                                                                               /* 거래상대방ID */
			, A.CNPT_NM                                                                                                               /* 거래상대방명 */
			, A.CORP_NO                                                                                                               /* 법인번호 */
			, A.CRGR_KICS_ADJ 									AS CRGR_KICS                                                                                                     /* 신용등급(KICS) */
			, A.LTV_ADJ                                                                                	AS LTV                             /* LTV */
--			, A.DSCR_ADJ			                                                       AS DSCR                            /* DSCR */
			, DECODE(A.DSCR_ADJ, 999, NULL, A.DSCR_ADJ)               AS DSCR			
			, DECODE( DIV , 'BASE', A.SNR_LTV , NULL) 			   	AS LTV_SNR 			
			, A.FAIR_VAL_SNR									AS FAIR_VAL_SNR                         /* 선순위대출금액 */
			, A.COLL_APPR_AMT                                                                                                         /* 담보인정금액 */
			, A.COLL_APPR_RTO                                                                                                         /* 담보인정비율 */
			, A.GUNT_AGNC_NM                                                                                                          /* 보증기관명 */
			, A.GUNT_AGNC_CORP_NO                                                                                                     /* 보증기관법인번호 */
			, A.GUNT_COEF * 100                                                                     AS GUNT_RTO                       /* 보증비율(%)*/
			, A.FAIR_BS_AMT_ADJ                                                                    AS FAIR_BS_AMT                     /* 공정가치B/S금액 */
			, A.CR_EXPO_AMT_ADJ                                                                 AS EXPO_AMT                        /* 익스포져금액*/

			, CASE WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SS')                         		THEN 0                                           -- 무위험
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SP', 'SC', 'SA', 'SF', 'SD') 	THEN B.RISK_COEF                                 -- 신용등급/만기      -- 단기예금(SD1)의 경우 테이블내에 LEAST(0.4%, 해당계수)값이 입력되어 있어 단순 매칭으로 처리됨
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SL')                                     THEN C.RISK_COEF                                 -- 담보부
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SR')                         		THEN D.RISK_COEF                                 -- 개인/중소기업
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SO')                         		THEN NVL(E.RISK_COEF, D.RISK_COEF)                                 -- 기타자산
				   ELSE NULL	 END                                                                   AS RISK_COEF                       /* 위험계수 */
				   
			, CASE WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SS')                         		THEN 0
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SP', 'SC', 'SA', 'SF', 'SD') 	THEN NVL(B.RISK_COEF, 1) * A.CR_EXPO_AMT_ADJ
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SL')					THEN NVL(C.RISK_COEF, 1) * A.CR_EXPO_AMT_ADJ
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SR')                         		THEN NVL(D.RISK_COEF, 1) * A.CR_EXPO_AMT_ADJ
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SO')                         		THEN NVL(NVL(E.RISK_COEF, D.RISK_COEF), 1) * A.CR_EXPO_AMT_ADJ
				   ELSE NULL END                                                                   	AS CRM_BEF_SCR                     /* 신용위험경감전요구자본 */
			, A.CRM_COLL_AMT                                                                       	AS CRM_COLL_AMT                    /* 신용위험경감담보금액 */
			, A.CRM_AFT_EXPO_AMT              							AS CRM_AFT_EXPO_AMT                /* 신용위험경감후익스포저금액 */
			, A.GUNT_ASSET_CLSF_CD                                                                                                    /* 보증자산분류코드 */
			
			, DECODE(SUBSTR(A.GUNT_ASSET_CLSF_CD, 1, 2), 'SS', 0, F.RISK_COEF)                     AS GUNT_RISK_COEF                  /* 보증위험계수 */
			
			, CASE WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SS')					   THEN 0        -- 차주가 무위험이면 보증보다 우선처리
				WHEN SUBSTR(A.GUNT_ASSET_CLSF_CD, 1, 2) IN ('SS')         					-- 보증이 무위험이면 보증비율 0원 처리 & 보증비율 이외의 부분은  차주위험 적용(담보경감후)  , B: 신용, C: 담보, D: 소매   
					  THEN NVL(B.RISK_COEF, NVL(C.RISK_COEF, NVL(D.RISK_COEF, 1))) * (1 - A.GUNT_COEF) * A.CRM_AFT_EXPO_AMT
				WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SP', 'SC', 'SA', 'SF', 'SD')
					   THEN CASE WHEN NVL(F.RISK_COEF, 1) < NVL(B.RISK_COEF, 1)
								 THEN (A.GUNT_COEF * NVL(F.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT) + (1 - A.GUNT_COEF) * NVL(B.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT
								 ELSE NVL(B.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT END
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SL')
					   THEN CASE WHEN NVL(F.RISK_COEF, 1) <  NVL(C.RISK_COEF, 1)
								 THEN (A.GUNT_COEF * NVL(F.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT) + (1 - A.GUNT_COEF) * NVL(C.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT
								 ELSE NVL(C.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT
								 END
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SR')
					   THEN CASE WHEN NVL(F.RISK_COEF, 1) < NVL(D.RISK_COEF, 1)
								 THEN (A.GUNT_COEF * NVL(F.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT) + (1 - A.GUNT_COEF) * NVL(D.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT
								 ELSE NVL(D.RISK_COEF, 1) * A.CRM_AFT_EXPO_AMT END
				   WHEN SUBSTR(A.CR_CLSF_CD, 1, 2) IN ('SO')
						THEN NVL(NVL(E.RISK_COEF, D.RISK_COEF), 1) * A.CRM_AFT_EXPO_AMT        	-- 계정처리한 기타자산은 보증 없음
				   ELSE NULL END                                                                   AS CRM_AFT_SCR                     /* 신용위험경감후요구자본 */
			, 'KRDB101BM'                                                                          AS LAST_MODIFIED_BY                /* 최종수정자 */
			, SYSDATE                                                                              AS LAST_UPDATE_DATE                /* 최종수정일자 */
			, A.PRNT_ISIN_CD
			, A.PRNT_ISIN_NM
--		 FROM  T_EXPO      A	
		FROM 	 ( SELECT * FROM T_BASE_EXPO 		UNION ALL
				   SELECT * FROM T_ACCR_EXPO
				)								A
			 , (
				 SELECT *
				   FROM IKRUSH_COEF_CRGR_MATR         -- 위험계수_신용등급/만기별(A.CR_CLSF_CD 와 매칭)
				  WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
			  ) B
			, (
				 SELECT *
				   FROM IKRUSH_COEF_COLL              -- 위험계수_담보(LTV with DSCR)
				  WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
			  ) C
			, (
				 SELECT *
				   FROM IKRUSH_COEF_RETAIL            -- 위험계수_소매
				  WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
			  ) D
			, (
				 SELECT *
				   FROM (
						   SELECT E1.*
								, ROW_NUMBER() OVER (PARTITION BY IFRS_ACCO_CLCD, ACCO_CD ORDER BY BASE_DATE DESC) AS RN
							 FROM IKRUSH_ACCO E1      -- 수기_계정과목
							WHERE 1=1
							  AND E1.BASE_DATE <= '$$STD_Y4MD'
							  AND E1.CR_ACCO_YN = 'Y'
						)
				  WHERE RN = 1                        -- BASE_DATE 역순으로 정렬하여 최신값 적용
			  ) E
			, (
				 SELECT *
				   FROM IKRUSH_COEF_CRGR_MATR         -- 위험계수_신용등급/만기별(A.GUNT_ASSET_CLSF_CD 와 매칭)
				  WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE AND APLY_END_DATE
			  ) F
		WHERE 1=1
		  AND A.CR_CLSF_CD         = B.ASSET_CLSF_CD(+)
		  AND A.MATR_CLCD          = B.MATR_CLCD(+)
		  AND A.CRGR_KICS_ADJ      = B.CRGR_KICS(+)

		  AND A.CR_CLSF_CD             = C.ASSET_CLSF_CD(+)
		  AND A.LTV_ADJ               >  C.LTV_STRT_VAL(+)
		  AND A.LTV_ADJ               <= C.LTV_END_VAL(+)
		  AND A.DSCR_ADJ              >  C.DSCR_STRT_VAL(+)
		  AND A.DSCR_ADJ              <= C.DSCR_END_VAL(+)
		  AND A.CR_CLSF_CD         	= D.ASSET_CLSF_CD(+)
		  AND A.IFRS_ACCO_CLCD     	= E.IFRS_ACCO_CLCD(+)
		  AND A.ACCO_CD            			= E.ACCO_CD(+)
		  AND A.GUNT_ASSET_CLSF_CD 	= F.ASSET_CLSF_CD(+)
		  AND A.MATR_CLCD          			= F.MATR_CLCD(+)
		  AND A.CRGR_GUNT          		= F.CRGR_KICS(+)
		
)
--SELECT * FROM T_CR_EXPO WHERE EXPO_ID ='FIDE_O_1000000004144';
,T_USR_FIDE AS (
	SELECT AA.BASE_DATE
		, 'CF'||'_'||AA.POSITION_ID                       	                AS EXPO_ID_ADJ	               /* 익스포저ID                    */
		 , '00'                                                           	AS GUNT_KEY	             /* 보증KEY                       */
		 , 'N'                                                            	AS ACCR_YN	               /* 미수수익여부                  */
		, NVL(AB.FUND_CD, 'G000') 			                                AS FUND_CD
		, NVL(AB.ACCO_CLCD,'1')     			                            AS ACCO_CLCD			/* 계정구분코드                  */	
		  , AB.ACCO_CD	               									/* 계정과목코드                  */
		  , AB.ACCO_NM	              									/* 계정과목명                    */
		  , 'FIDE'                              AS ASSET_TPCD	         /* 자산유형코드                  */
		  , AC.KICS_PROD_TPCD	                                                                 /* KICS상품유형코드              */
		  , AC.KICS_PROD_TPNM	                                                                 /* KICS상품유형명                */
		  , NVL(AB.ISIN_CD,'999999999999') 		AS ISIN_CD		/* 종목코드                      */
		  , NVL(AB.ISIN_NM, '신용파생상품')       	AS ISIN_NM 		/* 종목명                        */
		  , 'N'                                 AS LT_TP	                 /* LOOK_THROUGH구분              */
		  , AA.CR_CLSF_CD
		  , AA.CR_CLSF_NM
		  , AA.CR_CLSF_CD                       AS ASSET_CLSF_CD	         /* 신용리스크분류코드            */
		  , AA.CR_CLSF_NM                       AS ASSET_CLSF_NM	         /* 신용리스크분류명              */
		  , '5'                                 AS CR_ACCO_TPCD	         /* 신용리스크계정분류코드        */
		  , '난외자산(장외파생/신용파생/신용공여)'        	AS CR_ACCO_TPNM	         /* 신용리스크계정분류명          */
 		  , AA.GRTE_TGT_CORP_NO                 AS CNPT_ID	              		 /* 거래상대방ID                  */
		  , AA.GRTE_TGT_CORP_NM                 AS CNPT_NM	               	 /* 거래상대방명                  */
		  , AA.GRTE_TGT_CORP_NO                 AS CORP_NO	               	 /* 법인번호                      */
		  , AA.CRGR_KICS	             										/* 신용등급_KICS                 */
		, NVL(AB.EFFE_MATR,15)                	AS EFFE_MATR	
		, NVL( CASE WHEN AB.EFFE_MATR >  14.0 THEN '14+'
				WHEN AB.EFFE_MATR <=  0.0 THEN '1'
				ELSE TO_CHAR( CASE WHEN CEIL(EFFE_MATR) = FLOOR(EFFE_MATR) THEN TRUNC(CEIL(EFFE_MATR))                -- 초과~이하로 처리 (2.00년이면 1~2년 만기코드)
									  ELSE CEIL(EFFE_MATR) END )
					END                                                                   
			, '14+')							AS MATR_CLCD	
		, GREATEST(NVL(AA.GRTE_CONT_AMT,0) , NVL(AB.NOTL_AMT,0)) * NVL(FX.FX_RATE,1)		AS EXPO_AMT 				

	FROM IKRUSH_CR_FIDE 	AA 
		   , Q_IC_EXPO 		AB
		   , ( SELECT A.* 
				, RANK() OVER (PARTITION BY KICS_PROD_TPCD ORDER BY APLY_END_DATE DESC )AS RN
			FROM IKRUSH_KICS_RISK_MAP A
			WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE	AND APLY_END_DATE
			AND KICS_PROD_TPCD = '754'   -- 내재파생옵션-신용관련
		   )					AC
		, T_FX 				FX		    			 			
		WHERE 1=1 
		AND AA.BASE_DATE 				= '$$STD_Y4MD'
		AND AA.BASE_DATE 				= AB.BASE_DATE(+)
		AND 'SECR_O_'||AA.POSITION_ID 	= AB.EXPO_ID(+)
		AND  'STD'						= AB.LEG_TYPE(+) 
		AND AA.CRNY_CD 				= FX.CRNC_CD(+)		   
		AND 1							= AC.RN(+)
)
--SELECT * FROM T_USR_FIDE  ;
, T_CR_FIDE AS (
-- 20230425 김재우 추가 신용파생상품 추가
	SELECT A.BASE_DATE                                     AS BASE_DATE /* 기준일자                      */
		 ,  EXPO_ID_ADJ					AS EXPO_ID
		  , GUNT_KEY	             /* 보증KEY                       */
		  , ACCR_YN	               /* 미수수익여부                  */
		  , A.FUND_CD                                           AS FUND_CD	               /* 펀드코드                      */
		  , A.ASSET_TPCD	           /* 자산유형코드                  */
		  , A.KICS_PROD_TPCD	                          /* KICS상품유형코드              */
		  , A.KICS_PROD_TPNM	                     /* KICS상품유형명                */
		  , A.ISIN_CD                                                 AS ISIN_CD 	               /* 종목코드                      */
		  , A.ISIN_NM	                                               AS ISIN_NM     /* 종목명                        */
		  , A.LT_TP	                 /* LOOK_THROUGH구분              */
		  , A.ACCO_CLCD                                     /* 계정구분코드                  */
		  , A.ACCO_CD	               /* 계정과목코드                  */
		  , A.ACCO_NM	               /* 계정과목명                    */
		  , A.ASSET_CLSF_CD	         /* 신용리스크분류코드            */
		  , A.ASSET_CLSF_NM	         /* 신용리스크분류명              */
		  , A.CR_ACCO_TPCD	         /* 신용리스크계정분류코드        */
		  , A.CR_ACCO_TPNM	         /* 신용리스크계정분류명          */
		  , A.EFFE_MATR                                         AS EFFE_MATR	             /* 유효만기                      */
		  , A.MATR_CLCD	             /* 만기구분코드                  */
 		  , A.CNPT_ID	               /* 거래상대방ID                  */
		  , A.CNPT_NM	               /* 거래상대방명                  */
		  , A. CORP_NO	               /* 법인번호                      */
		  , A.CRGR_KICS	             /* 신용등급_KICS                 */
		  , NULL								AS LTV	                   /* LTV(%)                        */
		  , NULL                                                         AS  DSCR	                 /* DSCR                          */
		  , NULL                                                         AS  LTV_SNR	               /* 선순위LTV(%)                  */
		  , NULL                                                         AS FAIR_VAL_SNR	         /* 선순위대출금액                */
		  , NULL                                                         AS COLL_APPR_AMT	         /* 담보인정금액                  */
		  , NULL                                                         AS COLL_APPR_RTO	         /* 담보인정비율                  */
		  , NULL                                                         AS GUNT_AGNC_NM	         /* 보증기관명                    */
		  , NULL                                                         AS  GUNT_AGNC_CORP_NO	     /* 보증기관법인번호              */
		  , NULL                                                         AS  GUNT_RTO	             /* 보증비율                      */
		  , 0 									AS FAIR_BS_AMT	           /* 공정가치B/S금액               */  --IFRS9 기준 분리회계를 적용하지 않아, 채권의 금액이 공정가치 이므로, 파생상품의 공정가치는 0원 처리
		  , A.EXPO_AMT                               		AS EXPO_AMT	             /* 익스포저금액                  */
		  , CASE WHEN SUBSTR(A.CR_CLSF_CD,1,2) ='SS'  OR A.CRGR_KICS = 'RF' THEN 0
		               ELSE D.RISK_COEF
				  END AS RISK_COEF	    /* 위험계수                      */
		  , CASE WHEN SUBSTR(A.CR_CLSF_CD,1,2) ='SS'  OR A.CRGR_KICS = 'RF' THEN 0
		               ELSE D.RISK_COEF * A.EXPO_AMT 
				  END                                            AS CRM_BEF_SCR	           /* 신용위험경감전 요구자본       */
		  , NULL								AS CRM_COLL_AMT		  
		  , A.EXPO_AMT 					     AS CRM_AFT_EXPO_AMT	     /* 신용위험경감후익스포저금액    */
		  , NULL								AS GUNT_ASSET_CLSF_CD
		  , NULL								AS GUNT_RISK_COEF
		  , CASE WHEN SUBSTR(A.CR_CLSF_CD,1,2) ='SS'  OR A.CRGR_KICS = 'RF' THEN 0
		               ELSE D.RISK_COEF * A.EXPO_AMT 
				  END AS CRM_AFT_SCR	           /* 신용위험경감후요구자본        */
		  , 'KRDB101BM'                          AS LAST_MODIFIED_BY	     /* 최종수정자                    */
		  , SYSDATE                                AS LAST_UPDATE_DATE	     /* 최종수정일자   	*/
		  , NULL						AS PRNT_ISIN_CD
		  , NULL						AS PRNT_ISIN_NM 
		
         FROM T_USR_FIDE			 A
		  , ( 
                  SELECT * 
		          FROM IKRUSH_COEF_CRGR_MATR
                  WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE	AND APLY_END_DATE
		    ) 	D	
      WHERE 1=1
	     AND A.CR_CLSF_CD 	= D.ASSET_CLSF_CD(+)
	     AND A.CRGR_KICS 	= D.CRGR_KICS(+)
	     AND A.MATR_CLCD 	= D.MATR_CLCD(+)	 

)
--SELECT * FROM T_CR_FIDE WHERE EXPO_ID ='FIDE_O_1000000004144' ;
, T_RST AS (
	SELECT A.*
			,    CASE WHEN A.ACCR_YN = 'Y'  THEN 'ETC5' --미수수익 
			
					WHEN A.ASSET_CLSF_CD = 'SO3'   --   AND SUBSTR(A.ACCO_CD,1,4) ='1830'  THEN 'ETC5'
                                           AND A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC5' ) THEN 'ETC5'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑
                    			WHEN A.ASSET_CLSF_CD = 'SD1' AND A.CRGR_KICS IN ('1','2' ) THEN 'ETC1'  --2등급이하 단기예금
					WHEN A.ASSET_CLSF_CD = 'SD1' THEN 'ETC2'  --3등급이상 단기예금 
					WHEN A.ASSET_CLSF_CD = 'SR2' THEN 'ETC3' -- 중소기업대출 
--					WHEN SUBSTR(A.ACCO_CD,1,4) ='1800' THEN 'ETC4' --24.02.13 계정코드별 신용리스크 보고서 COA 매핑
--					WHEN SUBSTR(A.ACCO_CD,1,4) ='1890' THEN 'ETC6' --24.02.13 계정코드별 신용리스크 보고서 COA 매핑
                                        WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC4' ) THEN 'ETC4'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑 : 보험미수금
                                        WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC6' ) THEN 'ETC6'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑 : 본지점계정차 
					WHEN A.ASSET_CLSF_CD IN ( 'SR1','SO3')  THEN 'ETC7' -- 개인신용대출 및 기타자산 
					--20230703 김재우 추가
					WHEN A.ASSET_CLSF_CD = 'SO1' THEN 'SS1RF'
					WHEN A.KICS_PROD_TPCD = 'ZZ2' THEN 'ETC5' -- 미수수익 
					WHEN A.KICS_PROD_TPCD = 'ZZ3' THEN 'ETC7' --개인신용대출 및 기타자산 
					WHEN A.KICS_PROD_TPCD = 'ZZ4' THEN 'ETC8' -- 부도어음 
					WHEN A.ASSET_CLSF_CD = 'SO4' THEN 'ETC8' --부도어음 
					WHEN A.ASSET_CLSF_CD IN ( 'SC1','SC2','SD2') AND A.CRGR_KICS NOT IN ('1','2' ,'3') THEN 'SC1'||A.CRGR_KICS
					WHEN A.ASSET_CLSF_CD IN ( 'SF5','SF6')  THEN 'SF5'||A.CRGR_KICS
					WHEN A.ASSET_CLSF_CD IN ( 'SF7','SF8', 'SF9','SFA','SFB','SFC')  THEN 'SF7'||A.CRGR_KICS
					ELSE A.ASSET_CLSF_CD || A.CRGR_KICS
					END AS CR_RPT_COA
                    
			,    CASE WHEN A.ACCR_YN = 'Y'  THEN 'ETC5'
					WHEN A.ASSET_CLSF_CD = 'SO3'  -- AND SUBSTR(A.ACCO_CD,1,4) ='1830'  THEN 'ETC5'
                                           AND A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC5' ) THEN 'ETC5'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑
					WHEN A.ASSET_CLSF_CD = 'SD1' AND A.CRGR_KICS IN ('1','2' ) THEN 'ETC1'
					WHEN A.ASSET_CLSF_CD = 'SD1' THEN 'ETC2'
					WHEN A.ASSET_CLSF_CD = 'SR2' THEN 'ETC3'	
--					WHEN SUBSTR(A.ACCO_CD,1,4) ='1800' THEN 'ETC4'--24.02.13 계정코드별 신용리스크 보고서 COA 매핑
--					WHEN SUBSTR(A.ACCO_CD,1,4) ='1890' THEN 'ETC6'--24.02.13 계정코드별 신용리스크 보고서 COA 매핑
                                        WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC4' ) THEN 'ETC4'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑 : 보험미수금
                                        WHEN A.ACCO_CD IN (SELECT ACCO_CD FROM ACCO_EXCPTN WHERE EXCPTN_TYP = 'ETC6' ) THEN 'ETC6'  --24.04.29 계정코드별 신용리스크 보고서 COA 매핑 : 본지점계정차 
					WHEN A.ASSET_CLSF_CD IN ( 'SR1','SO3')  THEN 'ETC7'
					WHEN A.ASSET_CLSF_CD = 'SO4' THEN 'ETC8'		
					--20230703 김재우 추가
					WHEN A.KICS_PROD_TPCD = 'ZZ2' THEN 'ETC5'
					WHEN A.KICS_PROD_TPCD = 'ZZ3' THEN 'ETC7'
					WHEN A.KICS_PROD_TPCD = 'ZZ4' THEN 'ETC8'
					ELSE NULL END AS ETC_ASSET_CLCD				
                    
			, CASE WHEN A.ASSET_CLSF_CD = 'SL1'  THEN --주거(독립) 
						CASE WHEN A.LTV <= 40 THEN '1'
						  WHEN A.LTV <= 60 THEN '2'
						  WHEN A.LTV <= 80 THEN '3'
						  WHEN A.LTV <= 90 THEN '4'
						  WHEN A.LTV <= 100 THEN '5'
						  ELSE  '6' 
											END 							     
					WHEN A.ASSET_CLSF_CD = 'SL2'  THEN --주거(연계)
						CASE WHEN A.LTV <= 50 THEN '1'
						  WHEN A.LTV <= 60 THEN '2'
						  WHEN A.LTV <= 80 THEN '3'
						  WHEN A.LTV <= 90 THEN '4'
						  WHEN A.LTV <= 100 THEN '5'
						  ELSE  '6' 
											END 							     
					WHEN A.ASSET_CLSF_CD = 'SL3' THEN --상업(독립)
						CASE WHEN A.LTV <= 60 THEN '1'
						  WHEN A.LTV <= 80 THEN '2'
						  WHEN A.LTV <= 100 THEN '3'
						  ELSE  '4' 
											END 	
					WHEN A.ASSET_CLSF_CD = 'SL4'  AND NVL(A.LTV,9999) = 9999 AND A.DSCR IS NULL THEN '11'													
					WHEN A.ASSET_CLSF_CD = 'SL4'  AND A.DSCR IS NULL THEN  --상업(연계)
							CASE WHEN A.LTV <= 60 THEN '7'
							  WHEN A.LTV <= 80 THEN '8'
							  WHEN A.LTV <= 1000 THEN '9'
							  ELSE  '10' 
											END 							     
					WHEN A.ASSET_CLSF_CD = 'SL4'  AND A.DSCR IS NOT NULL THEN  --상업(연계)
							CASE WHEN A.LTV <= 60 THEN '1'
							  WHEN A.LTV <= 70 THEN '2'
							  WHEN A.LTV <= 80 THEN '3'
							  WHEN A.LTV <= 90 THEN '4'
							  WHEN A.LTV <= 100 THEN '5'
							  ELSE  '6' 
											END 							     
					ELSE NULL END AS LTV_CLCD
			, CASE WHEN A.ASSET_CLSF_CD IN ('SL1','SL2','SL3') THEN '10'
					WHEN A.ASSET_CLSF_CD = 'SL4'  AND A.DSCR IS NULL THEN '10'
					WHEN A.ASSET_CLSF_CD = 'SL4' THEN  --상업(연계) 			   
							CASE WHEN A.DSCR <= 0.6 THEN  '1'
												 WHEN A.DSCR <= 0.8 THEN  '2'
												 WHEN A.DSCR <= 1 THEN  '3'
												 WHEN A.DSCR <= 1.2 THEN  '4'
												 WHEN A.DSCR <= 1.4 THEN  '5'
												 WHEN A.DSCR <= 1.6 THEN  '6'
												 WHEN A.DSCR <= 1.8 THEN  '7'
												 WHEN A.DSCR <= 2 THEN  '8'
												 ELSE '9'  
												  END 
				ELSE NULL 
				END AS DSCR_CLCD
			, CASE WHEN A.COLL_APPR_AMT > 0  THEN '1'  --담보
				WHEN A.GUNT_ASSET_CLSF_CD IS NOT NULL AND SUBSTR(A.ASSET_CLSF_CD,1,2) <> 'SS'  THEN '3' --보증 
				ELSE NULL  --1.담보 2.상계 3. 보증 4 신용파생. 담보와 보증만 반영함
				END AS CRM_CLCD
			,  CASE WHEN SUBSTR(A.ASSET_CLSF_CD,1,2) IN ('SP') OR A.ASSET_CLSF_CD IN ('SF1','SF2','SF3','SF4') THEN 'SP1'
				    WHEN SUBSTR(A.ASSET_CLSF_CD,1,2) IN ('SC') OR SUBSTR(A.ASSET_CLSF_CD,1,2) IN ('SF' ) THEN 'SC1'
			   	   WHEN A.ASSET_CLSF_CD IN ('SA1' ,'SA2', 'SR1', 'SR2') THEN A.ASSET_CLSF_CD
				    ELSE NULL END AS GUNT_TGT_CLCD
			,  CASE WHEN A.GUNT_ASSET_CLSF_CD IS NOT NULL AND SUBSTR(A.ASSET_CLSF_CD,1,2) <> 'SS' THEN 	
						CASE WHEN  SUBSTR(A.GUNT_ASSET_CLSF_CD,1,2) = 'SS' THEN '1'
								WHEN  SUBSTR(A.GUNT_ASSET_CLSF_CD,1,2) = 'SP' THEN 
										CASE WHEN A.GUNT_KEY = '1' THEN '2'
											WHEN A.GUNT_KEY = '2' THEN '3'
											ELSE '4'
										  END 						
								WHEN  SUBSTR(A.GUNT_ASSET_CLSF_CD,1,2) = 'SC' THEN
										CASE WHEN A.GUNT_KEY = '1' THEN '5'
													WHEN A.GUNT_KEY = '2' THEN '6'
													ELSE '7'
										  END 	
									  ELSE NULL	
						  END								  
				ELSE NULL
				END AS CRM_GUNT_CLCD    	
                FROM ( 
                        SELECT * FROM T_CR_EXPO	
                        UNION ALL 
                        SELECT * FROM T_CR_FIDE
                      )  A
)
--SELECT * FROM T_RST ;

SELECT BASE_DATE	                /* 기준일자                     */
           , EXPO_ID	                  /* 익스포저ID                   */
           , GUNT_KEY	                /* 보증KEY                      */
           , ACCR_YN	                  /* 미수수익여부                 */
           , FUND_CD	                  /* 펀드코드                     */
           , ASSET_TPCD    	          /* 자산유형코드                 */
           , KICS_PROD_TPCD	          /* KICS상품유형코드             */
           , KICS_PROD_TPNM	          /* KICS상품유형명               */
           , ISIN_CD	                  /* 종목코드                     */
           , ISIN_NM	                  /* 종목명                       */
           , LT_TP	                    /* LOOK_THROUGH구분             */
           , ACCO_CLCD	                /* 계정구분코드                 */
           , ACCO_CD	                  /* 계정과목코드                 */
           , ACCO_NM	                  /* 계정과목명                   */
           , ASSET_CLSF_CD	            /* 신용리스크분류코드           */
           , ASSET_CLSF_NM	            /* 신용리스크분류명             */
           , CR_ACCO_TPCD	            /* 신용리스크계정분류코드       */
           , CR_ACCO_TPNM	            /* 신용리스크계정분류명         */
           , EFFE_MATR	                /* 유효만기                     */
--20230428 김재우 추가   
-- 무위험은 ETC로 매핑
-- 기타자산은ETC로 매핑 (위험계수 조인 때문에 여기에서 수정)
--           , CASE WHEN SUBSTR(ASSET_CLSF_CD,1,2) = 'SS' THEN 'ETC'
--                  WHEN ETC_ASSET_CLCD IS NOT NULL THEN 'ETC'
--                  ELSE MATR_CLCD
--                   END AS MATR_CLCD  /* 만기구분코드                 */
           , MATR_CLCD	                /* 만기구분코드                 */
           , CNPT_ID	                  /* 거래상대방ID                 */
           , CNPT_NM	                  /* 거래상대방명                 */
           , CORP_NO	                  /* 법인번호                     */
           , CRGR_KICS	                /* 신용등급_KICS                */
           , LEAST(DECODE(LTV, 0.1, 0, LTV),999)	AS LTV	          /* LTV(%)                       */
           , DSCR	                    /* DSCR                         */
           , LTV_SNR	                  /* 선순위LTV(%)                 */
           , FAIR_VAL_SNR	            /* 선순위대출금액               */
           , COLL_APPR_AMT	            /* 담보인정금액                 */
           , COLL_APPR_RTO	            /* 담보인정비율                 */
           , GUNT_AGNC_NM	            /* 보증기관명                   */
           , GUNT_AGNC_CORP_NO	        /* 보증기관법인번호             */
           , GUNT_RTO	                /* 보증비율                     */
           , FAIR_BS_AMT	              /* 공정가치B/S금액              */
           , EXPO_AMT	                /* 익스포저금액                 */
           , RISK_COEF	                /* 위험계수                     */
           , CRM_BEF_SCR	              /* 신용위험경감전 요구자본      */
           , CRM_COLL_AMT	            /* 신용위험경감담보금액         */
           , CRM_AFT_EXPO_AMT	        /* 신용위험경감후익스포저금액   */
           , GUNT_ASSET_CLSF_CD	      /* 보증자산분류코드             */
           , GUNT_RISK_COEF	          /* 보증위험계수                 */
           , CRM_AFT_SCR	              /* 신용위험경감후요구자본       */
           , LAST_MODIFIED_BY	        /* 최종수정자                   */
           , LAST_UPDATE_DATE	        /* 최종수정일자                 */
           , LTV_CLCD
           , DSCR_CLCD
	       , ETC_ASSET_CLCD
      	   , CASE WHEN ASSET_CLSF_CD IN ('SL1','SL2','SL3','SL4') AND ACCR_YN = 'N' THEN ASSET_CLSF_CD||LTV_CLCD
	                ELSE CR_RPT_COA	   
		               END AS CR_RPT_COA
           , CRM_CLCD
           , GUNT_TGT_CLCD
           , CRM_GUNT_CLCD
           , CASE WHEN CRM_CLCD = '1' THEN 'A'||CRM_CLCD||CR_RPT_COA
                  WHEN CRM_CLCD = '3' THEN 'A'||CRM_CLCD||DECODE(SUBSTR(CR_RPT_COA,1,3), 'ETC', ASSET_CLSF_CD, CR_RPT_COA)
			            ELSE NULL 
			             END AS CRM_TGT_COA  -- 위험경감대상 COA 
           , CASE WHEN CRM_CLCD = '1' THEN  'B'||CRM_CLCD||CR_RPT_COA
		              WHEN CRM_CLCD = '3' THEN   'B'||CRM_CLCD||GUNT_TGT_CLCD||CRM_GUNT_CLCD
                  ELSE NULL 
			             END  AS CRM_MSR_COA -- 위험경감수단 COA 	   	  	   
            , CASE WHEN CRM_CLCD = '3' THEN EXPO_AMT * GUNT_RTO / 100   
                     ELSE NULL 
                     END AS GUNT_APPL_AMT
           , PRNT_ISIN_CD
           , PRNT_ISIN_NM
        FROM  T_RST        
	WHERE 1=1