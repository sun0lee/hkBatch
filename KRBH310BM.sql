/* 수정사항  
조기상환율 KEY : 기존 계정과목코드로 구성된 조기상환율 KEY를 
계정코드체계 변경으로 계정코드별 데이터 집적이 불가능하므로, 대출 속성구분에 따라 SEG를 구분하여 KEY 재구성 
*/
WITH /* SQL ID : KRBH310BM */ T_AAA AS                      -- LEVEL(e.g. 60개월 등) 만큼의 월별데이터 추출
(                                                           -- K-ICS 산출방법서(2017)에 따라 기본적으로 60개월로 설정
   SELECT TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE('$$STD_Y4MD', 'YYYYMMDD'), -1 *(LEVEL-1))), 'YYYYMM') AS  STAT_YM
     FROM DUAL
  CONNECT BY LEVEL <= 60
    ORDER BY 1
)
, T_PP_MST AS (
	SELECT  DECODE ( MOD(LEVEL, 2), 1, 'PP_INDI', 'PP_CORP')   
		       ||  DECODE ( MOD(LEVEL, 5)	, 1, '_PROP'
											, 2, '_CRDT'
											, 3, '_GUNT'
											)   		AS 		PREPAY_KEY
	FROM DUAL	
	CONNECT BY LEVEL <= 8
    UNION ALL
    SELECT 'PP_BOND' FROM DUAL 
    UNION ALL 
    SELECT 'PP_FUND' FROM DUAL 

)
--SELECT * FROM T_PP_MST ;
, T_PREPAY AS (
                /*COLL_KND
                10 : 주택연금보증
                20 : 유가증권
                
                30 : 부동산
                40 : 신용
                50 : 지급보증
                
                41 : 퇴직금신용
                60 : 어음할인
                61 : 상업어음할인 (부동산)
                62 : 무역어음할인 (신용)
                63 : 기타어음할인 (신용)
                70 : 사모사채인수 
                80 : 기타담보 
                90 : 주택보험 
                */
	SELECT   A.FIN_YM
			, A.LN_NO                                                                            AS LONN
			, DECODE(A.LN_CONT_GB, '10', 1, 2)                                                   AS CUST_TPCD
			, COLL_KND						                                                    		-- 10 : 연금  20 : 유가증권 30 : 부동산 40: 신용  50 : 지급보증  60 상업어음, 70 : 후순위, 80 : 기타
			, DECODE (A.LN_CONT_GB,  '10', 'PP_INDI', 'PP_CORP') ||
				DECODE (A.COLL_KND, '30', '_PROP'
								  , '40', '_CRDT'
								  , '50', '_GUNT'
                                      )   				 AS PREPAY_KEY
			, A.PMM_RAMT
			, A.LN_RAMT
			, A.PMM_RAMT                                                                         		 AS BLC
			, A.EXCH_AMT_3                                                                                      AS RPAY_AMT
	FROM IDHLNH_LONG_MM_CLSN 		A
    WHERE 1=1
      AND A.LN_FIN_GB = '99'                                -- 마감
      AND A.PMM_RAMT <> 0                                    -- 잔액 0이상
      AND  EXISTS ( SELECT 1  FROM T_AAA WHERE A.FIN_YM = STAT_YM)
      AND A.COLL_KND IN ('30','40','50')
)
--SELECT *   FROM T_PREPAY ;
--SELECT FIN_YM, PREPAY_KEY,COUNT(*)  FROM T_PREPAY WHERE FIN_YM = '202309' GROUP BY FIN_YM, PREPAY_KEY;
,T_RATIO AS (
	SELECT FIN_YM
		, PREPAY_KEY
		, SUM(PMM_RAMT)						AS PMM_RAMT
		, SUM(RPAY_AMT)						AS RPAY_AMT	
		, SUM(RPAY_AMT) / SUM(PMM_RAMT)		AS PREPAY_RATIO
	FROM    T_PREPAY	P
	WHERE 1=1
	GROUP BY FIN_YM, PREPAY_KEY
)
, T_PP_RATIO AS (
	SELECT '$$STD_Y4MD'									AS BASE_YM
			, M.PREPAY_KEY						AS PREPAY_KEY
			, SUM(PMM_RAMT)						AS PMM_RAMT
			, SUM(RPAY_AMT) 						AS RPAY_AMT
			, SUM(RPAY_AMT) /SUM(PMM_RAMT)		AS  AMT_WGHT_RATIO
			, AVG(PREPAY_RATIO) 					AS AVG_RATIO
	FROM 
        T_PP_MST   M ,  
        T_RATIO      P
	WHERE 1=1
	AND M.PREPAY_KEY = P.PREPAY_KEY(+)
	GROUP BY M.PREPAY_KEY
)
--SELECT * FROM T_PP_RATIO;
, T_EEE AS
(
   SELECT SUBSTR(BASE_YM, 1, 6) 			     AS BASE_YM	
        , 'KICS_A_PREPAY'                                    AS PREPAY_ID
        , PREPAY_KEY
        , PREPAY_RATIO
     FROM IKRUSH_PREPAY_CONST
    WHERE 1=1
      AND SUBSTR(BASE_YM, 1, 6) = SUBSTR('$$STD_Y4MD', 1, 6)
	  
	UNION ALL
	
	SELECT BASE_YM
        , 'KICS_A_PREPAY'                               AS PREPAY_ID
        , PREPAY_KEY						AS PREPAY_KEY
        , NVL(AVG_RATIO, 0.0)				AS PREPAY_RATIO
	FROM T_PP_RATIO A
	WHERE 1=1
	AND NOT EXISTS ( SELECT 1 FROM IKRUSH_PREPAY_CONST H 
							WHERE H.PREPAY_KEY = A.PREPAY_KEY
							AND SUBSTR(BASE_YM, 1, 6) 	 = SUBSTR('$$STD_Y4MD', 1, 6)
						)
)
--SELECT * FROM T_EEE;
SELECT  TO_DATE(BASE_YM,'YYYY-MM-DD')									AS APPLY_START_DATE
        , PREPAY_ID                                                     AS PREPAY_ID
        , PREPAY_KEY                                                    AS PREPAY_KEY
        , 1 - TRUNC(POWER(1 - ROUND(PREPAY_RATIO, 4), 12), 4)           AS PREPAY_RATIO
        , 'KRBH310BM'                                                   AS LAST_MODIFIED_BY
        , SYSDATE                                                       AS LAST_UPDATE_DATE
FROM T_EEE