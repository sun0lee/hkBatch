SELECT /* SQL ID - KRDA104BM */
       A.BASE_DATE
     , A.EXPO_ID
  	 , A.PROD_TPCD
  	 , A.PROD_TPNM
  	 , A.KICS_PROD_TPCD
  	 , A.KICS_PROD_TPNM
  	 , A.FAIR_BS_AMT AS EXPO_AMT
     , B.SHCK_VAL AS RISK_COEF
  	 , ROUND(A.FAIR_BS_AMT * B.SHCK_VAL,3) AS SCR
  	 , ROUND(A.FAIR_BS_AMT * (1-B.SHCK_VAL),3) AS SCAF_FAIR_BS_AMT
  	 , 'KRDA104BM' AS LAST_MODIFIED_BY
     , SYSDATE AS LAST_UPDATE_DATE
     , A.OBGT_PSSN_ESTE_YN	 
     
  FROM Q_IC_EXPO A
     ,(SELECT * 
	     FROM IKRUSH_SHCK_PRPT 
	    WHERE '$$STD_Y4MD' BETWEEN APLY_STRT_DATE	AND APLY_END_DATE
      ) B
 WHERE A.BASE_DATE = '$$STD_Y4MD'
   AND A.KICS_PROD_TPCD = B.PRPT_RISK_TPCD(+)
   AND A.KICS_PROD_TPCD IN ('510' , '520' ,'530', '540','550','560')  
--   AND A.KICS_PROD_TPCD IN ('510' , '520' ,'530', '540','550','560','340','341','346','347')  -- 20240523: 부동산PF 포함
   AND A.LEG_TYPE = 'STD'