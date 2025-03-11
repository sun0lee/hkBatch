SELECT /* SQL-ID : KRDA105BM */
       A.BASE_DATE
     , A.EXPO_ID 
	 , '1' AS ASSET_CNCT_TPCD
	 , A.PROD_TPCD
	 , A.PROD_TPNM
	 , A.KICS_PROD_TPCD
	 , A.KICS_PROD_TPNM
	 , A.ISIN_CD
	 , A.ISIN_NM
     , NVL(A.GUNT_GRP_CD, A.GRP_CD) AS GRP_CD  --보증의 경우 보증인 그룹코드사용
     , NVL(A.GUNT_GRP_NM, A.GRP_NM) AS GRP_NM  --보증의 경우 보증인 그릅명사용
--	 , A.GUNT_GRP_CD
	 , A.CNPT_ID
	 , A.CNPT_NM
	 , A.CORP_NO
	 , CRGR_KICS2 AS CRGR_KICS  --무등급인 경우 99
	 , A.CRGR_KICS_GRP
	 , A.EXPO_AMT
	 , A.GRP_EXPO_AMT
	 , B.VALUE_KICS AS TOT_ASSET_AMT
	 , C.LMT_RTO
	 , B.VALUE_KICS * C.LMT_RTO AS LMT_AMT
	 , GREATEST(GRP_EXPO_AMT - (B.VALUE_KICS * C.LMT_RTO),0) AS LMT_OVER_AMT
	 , C.RISK_COEF
	 , CASE WHEN GREATEST(GRP_EXPO_AMT - (B.VALUE_KICS * C.LMT_RTO),0) > 0 THEN GREATEST(GRP_EXPO_AMT - (B.VALUE_KICS * C.LMT_RTO),0) * A.EXPO_AMT/A.GRP_EXPO_AMT * C.RISK_COEF
	        ELSE 0 END AS SCR
	 , CASE WHEN GREATEST(GRP_EXPO_AMT - (B.VALUE_KICS * C.LMT_RTO),0) > 0 THEN A.EXPO_AMT - (GREATEST(GRP_EXPO_AMT - (B.VALUE_KICS * C.LMT_RTO),0) * A.EXPO_AMT/A.GRP_EXPO_AMT * C.RISK_COEF)
	        ELSE A.EXPO_AMT 
			END AS SCAF_FAIR_BS_AMT
	 , 'KRDA105BM' AS LAST_MODIFIED_BY
	 , SYSDATE AS LAST_UPDATE_DATE
-- 20230427 김재우 수정 칼럼4개 추가	 
	 , A.ACCR_YN
	 , A.GUNT_KEY
	 , A.CNCT_ACCO_TPCD
FROM 
             (
-- 20230427 김재우 수정
-- 기존 로직에서신용리스크 미수수익을 미반영하였으나 미수수익을 반영하기 위해조건수정              
 SELECT IA.BASE_DATE, IA.EXPO_ID, IA.PROD_TPCD,	IA.PROD_TPNM, IA.KICS_PROD_TPCD, IA.KICS_PROD_TPNM, IA.ISIN_CD, IA.ISIN_NM
              , IA.CNPT_ID, IA.CNPT_NM, IA.CORP_NO
	      , IA.CR_CLSF_CD, IA.SR_CLSF_CD
              , NVL(IB.GUNT_GRP_CD, IA.GRP_CD) AS GRP_CD
	      , NVL(IB.GUNT_GRP_NM, IA.GRP_NM) AS GRP_NM
              , NVL(IB.ACCR_YN,'N') AS ACCR_YN
              , NVL(IB.GUNT_KEY,'00') AS GUNT_KEY
	      , NVL(IB.CRM_AFT_EXPO_AMT,IA.SR_EXPO_AMT) AS EXPO_AMT --신용익스포져 + 주식익스포져		  
               , ROUND(AVG(LEAST(CASE WHEN IB.GUNT_ASSET_CLSF_CD IS NOT NULL THEN IB.GUNT_KEY
			                                         WHEN NVL(IA.CRGR_KICS,'99') = '99' THEN '6'
			                                           ELSE IA.CRGR_KICS END
					             ,'7')) OVER (PARTITION BY NVL(IB.GUNT_GRP_CD, IA.GRP_CD)) ,0) AS CRGR_KICS_GRP
				--보증이 있는 경우 보증인의 그룹코드 및 보증인 신용등급 사용
--			  , ROUND(AVG(LEAST(NVL(DECODE(IA.CRGR_KICS,'RF','1',IA.CRGR_KICS),'99'),'7')) OVER (PARTITION BY IA.GRP_CD),0) AS CRGR_KICS_GRP -- 그룹KICS 신용등급
              , SUM(NVL(IB.CRM_AFT_EXPO_AMT,IA.SR_EXPO_AMT)) OVER (PARTITION BY NVL(IB.GUNT_GRP_CD, IA.GRP_CD)) AS GRP_EXPO_AMT  --그룹익스포져		  
              , IB.GUNT_GRP_CD
              , IB.GUNT_GRP_NM
	      , CASE WHEN IB.GUNT_ASSET_CLSF_CD IS NOT NULL THEN IB.GUNT_KEY 
			    ELSE NVL(IA.CRGR_KICS,'99') 
			      END AS CRGR_KICS2  --보증의 경우 보증인 신용등급			  
	      , IB.CR_ACCO_TPCD
              , IB.CR_ACCO_TPNM
-- 20230427 김재우 수정 자산집중리스크계정구분코드,코드명 추가              
              , CASE WHEN IA.SR_CLSF_CD <> 'NON' THEN '2'  --주식 및 채권
			   WHEN IB.ETC_ASSET_CLCD IS NOT NULL THEN '4' --기타미수채권
			   WHEN IB.CR_ACCO_TPCD = '1' THEN '1'  --예치금
			   WHEN IB.CR_ACCO_TPCD = '2' THEN '2' --주식 및 채권
			   WHEN IB.CR_ACCO_TPCD = '3' THEN '3' --신용공여
			   WHEN IB.CR_ACCO_TPCD = '5' THEN '5' --장내 및 장외파생상품
			   WHEN IB.CR_ACCO_TPCD = '9' THEN '6' --난외익스포져
			   ELSE '4' 
			     END AS CNCT_ACCO_TPCD 
--              , CASE WHEN IA.SR_CLSF_CD <> 'NON' THEN '주식 및 채권'
--			   WHEN IB.ETC_ASSET_CLCD IS NOT NULL THEN '기타미수채권' 
--			   WHEN IB.CR_ACCO_TPCD = '1' THEN '예치금'
--			   WHEN IB.CR_ACCO_TPCD = '2' THEN '주식 및 채권'
--			   WHEN IB.CR_ACCO_TPCD = '3' THEN '신용공여'
--			   WHEN IB.CR_ACCO_TPCD = '5' THEN '장내 및 장외파생상품'  
--			   WHEN IB.CR_ACCO_TPCD = '9' THEN '난외익스포져'
--			   ELSE NULL
--			     END AS CNCT_ACCO_TPNM              
    FROM Q_IC_EXPO IA 
	     ,  (
		        SELECT IBA.*
				    , IBB.GUNT_GRP_CD 
				    , IBB.GUNT_GRP_NM
			   FROM Q_IC_CR_RSLT IBA
				      , (
                                           SELECT ICNO                AS CORP_NO
                                                        , NVL(STRC_PTCD, '9') AS LIST_GB
                                                        , GOCO_CD             AS GUNT_GRP_CD
                                                        , GOCO_NM             AS GUNT_GRP_NM
                                               FROM IDHKRH_COMPANY_INFO
                                            WHERE STND_DT = (
                                                                                     SELECT MAX(STND_DT)
                                                                                        FROM IDHKRH_COMPANY_INFO
                                                                                     WHERE STND_DT <= '$$STD_Y4MD'
                                                                                  ) 
				        )IBB
		      WHERE IBA.BASE_DATE = '$$STD_Y4MD'
			     AND IBA.GUNT_AGNC_CORP_NO = IBB.CORP_NO(+)
		  ) IB	  
--	     ,  Q_IC_STOC_RSLT IC 
WHERE IA.BASE_DATE = '$$STD_Y4MD'
      AND IA.BASE_DATE = IB.BASE_DATE(+)
      AND IA.EXPO_ID = IB.EXPO_ID(+)
--      AND IA.BASE_DATE = IC.BASE_DATE(+)
--      AND IA.EXPO_ID = IC.EXPO_ID(+)
      AND IA.LEG_TYPE IN ('STD','NET')  --REC, PAY 제외
      AND IA.GRP_CD IS NOT NULL  -- 그룹코드가 존재하는 건만	  
      AND SUBSTR(IA.CR_CLSF_CD ,1,2) <> 'SS'-- 무위험 거래상대방 제외	  
-- 20230427 김재우 수정
-- 기존에는 발행채권만 제외였으나, 차입/채권부채상품 모두를 제외함      
      AND SUBSTR(IA.KICS_PROD_TPCD,1,1) <> '9' -- 부채상품 제외
      AND IA.EXPO_ID NOT IN ( SELECT EXPO_ID
			                               FROM Q_IC_CR_RSLT
						    WHERE BASE_DATE = '$$STD_Y4MD' 
							   AND SUBSTR(GUNT_ASSET_CLSF_CD,1,2) = 'SS'    -- 무위험 보증 제외
						  )
      AND IA.CORP_NO NOT IN (SELECT CORP_NO 
			                               FROM IKRUSH_PUBI
-- 20230427 김재우 수정
-- 기존에는 공공기관이 제외 있었으나 공공기관 포함시킴
--						    WHERE SUBSTR(ASSET_CLSF_CD,1,2) IN ('SS', 'SP')
						    WHERE SUBSTR(ASSET_CLSF_CD,1,2) IN ('SS')
						    ) --무위험 기관 제외
      AND IA.ACCO_CLCD IN ('1','2','3') -- 실적배당형, 변액 제외
-- 20230427 김재우 수정      
-- 기준서에서 분해되지 않는 수익증권 및 자산재구성 수익증권은 자산집중리스크에서 제외하라고 명시되어 있어 반영함
      AND SUBSTR(IA.KICS_PROD_TPCD,1,1) <> '8'  -- 미분해 수익증권은 제외
      AND IA.KICS_PROD_TPCD <> '270'  -- MMF 는  수익증권 자산재구성이므로 제외함	  
--      AND IA.EXPO_ID = 'SECR_O_4000000000399G000000001'
) A 
	 , (
--	   SELECT SUM(DECODE(RPT_COA_ID,'A',VALUE_KICS,-1*VALUE_KICS)) AS VALUE_KICS 
--	     FROM Q_IC_RPT_FVBS 
--		WHERE BASE_DATE = '$$STD_Y4MD'
--		  AND RPT_COA_ID IN ('A' , 'A3')  --총자산금액  
	     SELECT SUM(FAIR_BS_AMT) AS VALUE_KICS 
           FROM Q_IC_IRATE_RSLT 
          WHERE 1=1
		    AND BASE_DATE = '$$STD_Y4MD'
            AND SUBSTR(ACCO_CD, 1, 1) = '1'
            AND NVL(ACCO_CLCD, '9') NOT IN ('4','5') 
            AND IRATE_RPT_TPCD IN ('1', '2')
	   ) B
	 , (SELECT * 
	      FROM IKRUSH_SHCK_CNCT 
		 WHERE '$$STD_Y4MD'  BETWEEN APLY_STRT_DATE AND APLY_END_DATE
		   AND CNCT_RISK_DTLS_TPCD IN ('C1')  --거래상대방
	   ) C 
 WHERE A.EXPO_AMT >0 
   AND A.CRGR_KICS_GRP = C.CRGR_KICS(+)