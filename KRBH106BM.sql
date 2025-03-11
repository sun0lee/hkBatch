/* 수정사항  
- 2024.04.15 : 신용등급매핑 수정  
- 2024.04.15 : 후순위채 만기 수정 (콜옵션만기)
- IBD_ZTCFMPEBAPST 테이블의 계정과목코드체계는 10자리 입수 가정
- 2024.05.09 : DCB_CD ='1'
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
, T_AAA AS (
	SELECT AA.* 
			  ,  CASE  WHEN AA.ASSETCODE = 'SB'  THEN 4                                       -- 비용상품(사채, 차입금 등)
					WHEN AA.POSITION_CURR =  'KRW'   THEN 1                                       -- 원화채권
					WHEN AA.POSITION_CURR <> 'KRW'   THEN 2                                       -- 외화채권
					 ELSE 9 END                                              AS INST_TPCD       -- 인스트루먼트유형코드(상품타입)			   
			-- ,  SUBSTR(AA.GL_ACCOUNT,3,8) AS  ACCO_CD
      ,  AA.GL_ACCOUNT 		AS ACCO_CD	-- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수
--	                     ,  CASE WHEN SUBSTR(AA.PORTFOLIO,1,1) IN ('A','E','G','S') THEN '1'     -- A연금저축연동금리, E지수연동 , G 일반계정은 일반계정 분류 
--                                 ELSE '2' 
--					      END AS FNDS_ACCO_CLCD
			, DECODE(AA.PORTFOLIO, 'G000','1','2') AS FNDS_ACCO_CLCD
			, CASE WHEN AA.RATING1 IS NOT NULL OR AA.RATING3 IS NOT NULL OR AA.RATING4 IS NOT NULL 			  
--				                      THEN LEAST(NVL(AB.CRGR_KICS,'99'), NVL(AC.CRGR_KICS,'99'))  -- 국내등급은 2개 들어옴. 2개 들어올 경우 WORST 등급 사용
						THEN GREATEST (  DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AC.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AB.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AC.CRGR_KICS,'999'), NVL(AD.CRGR_KICS,'999'))))
				          ELSE  
						      GREATEST (  DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AF.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AE.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')))
                                                                            , DECODE(LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999')), '999', '0', LEAST(NVL(AF.CRGR_KICS,'999'), NVL(AG.CRGR_KICS,'999'))))
                                            END AS CRGR_KICS	
			, AH.SRCL
			, AH.SRFL
			, AH.GARI
			, AG2.SAPR
			, NVL(AG2.OPCD,AH.OPCD) AS OPCD
			, AH.EXSD
			, AH.EXND 
			, CASE WHEN TO_CHAR(ADD_MONTHS( TO_DATE(AA.ISSUE_DATE, 'YYYYMMDD'), 60), 'YYYYMMDD') <=  AA.GRDAT  THEN  AA.EDDT
				     ELSE TO_CHAR(ADD_MONTHS( TO_DATE(AA.ISSUE_DATE, 'YYYYMMDD'), 60), 'YYYYMMDD') END 			AS CALL_OPT_YYMM
		    FROM IBD_ZTCFMPEBAPST AA
			     , IKRUSH_MAP_CRGR AB
			     , IKRUSH_MAP_CRGR AC
			     , IKRUSH_MAP_CRGR AD
			     , IKRUSH_MAP_CRGR AE
			     , IKRUSH_MAP_CRGR AF		
			     , IKRUSH_MAP_CRGR AG		
			     , (SELECT AGA.* 
			          FROM IBD_ZTCFMBDCLASS AGA
			             , (SELECT MANDT, SECURITY_ID, MAX(VERS) AS VERS 
			                  FROM IBD_ZTCFMBDCLASS
			                 GROUP BY MANDT, SECURITY_ID 
			               ) AGB
			         WHERE AGA.MANDT = AGB.MANDT
			           AND AGA.SECURITY_ID = AGB.SECURITY_ID
			           AND AGA.VERS = AGB.VERS       
			        )  AG2
			     , (SELECT AHA.* 
			          FROM IBD_ZTCFMBDSTOCK AHA
			             , (SELECT MANDT, SECURITY_ID, MAX(VERS) AS VERS 
			                  FROM IBD_ZTCFMBDSTOCK
			                 GROUP BY MANDT, SECURITY_ID 
			               ) AHB
			         WHERE AHA.MANDT = AHB.MANDT
			           AND AHA.SECURITY_ID = AHB.SECURITY_ID
			           AND AHA.VERS = AHB.VERS       
			        )  AH			        			     		 
		  WHERE 1=1
			AND AA.RATING1 = AB.CRGR_DMST(+)
			AND AA.RATING3 = AC.CRGR_DMST(+)
			AND AA.RATING4 = AD.CRGR_DMST(+)
			AND AA.RATING5 = AE.CRGR_SNP(+)
			AND AA.RATING6 = AF.CRGR_MOODYS(+)
			AND AA.RATING7 = AG.CRGR_FITCH(+)
			AND AA.SECURITY_ID = AG2.SECURITY_ID(+)   
			AND AA.SECURITY_ID = AH.SECURITY_ID(+)   			   
			AND AA.GRDAT = '$$STD_Y4MD'  
			AND AA.VALUATION_AREA = '400'
			-- AND SUBSTR(AA.GL_ACCOUNT,1,3) IN ('001','002') -- 자산, 부채만 (신종자본증권 제외)      
      AND SUBSTR(AA.GL_ACCOUNT,1,1) IN ('1','2') -- 자산, 부채만 (신종자본증권 제외)            -- 20240430 이선영 TOBE:수정 계정과목 코드 10자리로 입수      
--		       AND (AA.SEC_CLASS = 'B08' --사모사채   ( 만기보유 회사채가 하나 끼어 있음) 
--           AND (AA.BOND_CLASS = '401'   -- 사모사채 조건변경(IFRS9 도입으로 기존사모사채 -> 채권으로 변경된건이 있어 해당건 제외)
--			        OR AA.ASSETCODE = 'SB') -- 발행사채, 차입금 등 
			AND AA.ASSETCODE = 'SB' -- 발행사채, 차입금 등 
	      )  
--SELECT * FROM T_AAA WHERE 1=1 ;
, T_BBB AS (
	SELECT *
	FROM IDHKRH_BND_INFO                         -- 채권발행정보
	WHERE 1=1
	AND STND_DT = ( SELECT MAX(STND_DT)FROM IDHKRH_BND_INFO WHERE STND_DT <= '$$STD_Y4MD' )
)
--,T_CCC AS 
--(
--             SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      	AS SECR_ITMS_CD
--                  , TRUNC( (  NVL(T1.UPRC, T2.UPRC) + NVL(T2.UPRC, T1.UPRC)   ) * 0.5, 3)    AS UPRC_DIRTY
--                  , TRUNC( (  NVL(DECODE(T1.UPRC, 0, NULL, T1.UPRC), T2.UPRC)
--                            + NVL(DECODE(T2.UPRC, 0, NULL, T2.UPRC), T1.UPRC) ) * 0.5, 3)      	AS UPRC_DIRTY2
--                  , TRUNC( (  NVL(T1.UPRC, T2.UPRC) + NVL(T2.UPRC, T1.UPRC)   ) * 0.5
--                            - NVL(T2.UPRC - NVL(T2.CLE_UPRC, T2.UPRC), 0)            , 2)      		AS UPRC_CLEAN
--                  , TRUNC( (  NVL(DECODE(T1.EXCG_BOND_VALU_AMT, 0, NULL, T1.EXCG_BOND_VALU_AMT), T2.EXCG_BOND_VALU_AMT)
--                            + NVL(DECODE(T2.EXCG_BOND_VALU_AMT, 0, NULL, T2.EXCG_BOND_VALU_AMT), T1.EXCG_BOND_VALU_AMT) ) * 0.5
--                                                                                     , 3)      AS UPRC_EXCG_BOND
--                  , TRUNC( (  NVL(DECODE(T1.EXCG_OPTN_VALU_AMT, 0, NULL, T1.EXCG_OPTN_VALU_AMT), T2.EXCG_OPTN_VALU_AMT)
--                            + NVL(DECODE(T2.EXCG_OPTN_VALU_AMT, 0, NULL, T2.EXCG_OPTN_VALU_AMT), T1.EXCG_OPTN_VALU_AMT) ) * 0.5
--                                                                                     , 3)      AS UPRC_EXCG_OPTN
--                  , TRUNC( (  NVL(DECODE(T1.MACT_BOND_VALU_AMT, 0, NULL, T1.MACT_BOND_VALU_AMT), T2.MACT_BOND_VALU_AMT)
--                            + NVL(DECODE(T2.MACT_BOND_VALU_AMT, 0, NULL, T2.MACT_BOND_VALU_AMT), T1.MACT_BOND_VALU_AMT) ) * 0.5
--                                                                                     , 3)      AS UPRC_MACT_BOND
--                  , TRUNC( (  NVL(DECODE(T1.MACT_OPTN_VALU_AMT, 0, NULL, T1.MACT_OPTN_VALU_AMT), T2.MACT_OPTN_VALU_AMT)
--                            + NVL(DECODE(T2.MACT_OPTN_VALU_AMT, 0, NULL, T2.MACT_OPTN_VALU_AMT), T1.MACT_OPTN_VALU_AMT) ) * 0.5
--                                                                                     , 3)      AS UPRC_MACT_OPTN
--                  , TRUNC( (  NVL(DECODE(T1.UPD_DURA          , 0, NULL, T1.UPD_DURA          ), T2.UPD_DURA          )
--                            + NVL(DECODE(T2.UPD_DURA          , 0, NULL, T2.UPD_DURA          ), T1.UPD_DURA          ) ) * 0.5
--                                                                                     , 4)      AS DURA_MODI
--                  , TRUNC( (  NVL(DECODE(T1.BPDR100           , 0, NULL, T1.BPDR100           ), T2.BPDR100           )
--                            + NVL(DECODE(T2.BPDR100           , 0, NULL, T2.BPDR100           ), T1.BPDR100           ) ) * 0.5
--                                                                                     , 4)      AS DURA_EFFE
--                  , DECODE(T1.UPRC   , 0, NULL, T1.UPRC   )                                    AS UPRC_1
--                  , DECODE(T2.UPRC   , 0, NULL, T2.UPRC   )                                    AS UPRC_2
--                  , DECODE(T1.BPDR100, 0, NULL, T1.BPDR100)                                    AS DURA_1
--                  , DECODE(T2.BPDR100, 0, NULL, T2.BPDR100)                                    AS DURA_2
--                  , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)  -- 외부신용등급 코드와 등급간 매핑정보(eg. '0110' -> 'AAA')
--                  , NVL( T2.ISSU_CO_CRED_GRDE_CD
--                  , NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD)
--					, T1.ISSU_CO_CRED_GRDE_CD ) ) )                               AS CRGR_NICE
--                  , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR
--                      WHERE CRGR_DMST IN NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T2.CRED_GRDE_CD)
--									, NVL( T2.ISSU_CO_CRED_GRDE_CD
--											, NVL( (SELECT CD_NM FROM IKRASM_CO_CD WHERE GRP_CD = 'CRGR_EXT' AND CD = T1.CRED_GRDE_CD)
--														, T1.ISSU_CO_CRED_GRDE_CD ) ) ) )        AS CRGR_KICS
--               FROM (
--                       SELECT *
--                         FROM IDHKRH_FN_BDEV    -- 국내채권 FN
--                        WHERE STND_DT = '$$STD_Y4MD'
--                    ) T1
--                         FULL OUTER JOIN
--                    (
--                       SELECT *
--                         FROM IDHKRH_NICE_BDEV  -- 국내채권 NICE
--                        WHERE STND_DT = '$$STD_Y4MD'
--                    ) T2
--                 ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
--              WHERE 1=1
--          )
--, T_DDD AS 
--(
--             SELECT DD.*
--                  , GREATEST (  DECODE(LEAST(KS_CRGR_KICS, NC_CRGR_KICS), '999', '0', LEAST(KS_CRGR_KICS, NC_CRGR_KICS))
--                              , DECODE(LEAST(KS_CRGR_KICS, KR_CRGR_KICS), '999', '0', LEAST(KS_CRGR_KICS, KR_CRGR_KICS))
--                              , DECODE(LEAST(NC_CRGR_KICS, KR_CRGR_KICS), '999', '0', LEAST(NC_CRGR_KICS, KR_CRGR_KICS)) )
--                                                                                                         AS CRGR_KICS
--               FROM (
--                       SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      AS SECR_ITMS_CD
--                            , TRUNC( (  NVL(T1.CLE_UPRC, T2.CLE_UPRC) + NVL(T2.CLE_UPRC, T1.CLE_UPRC)                             ) * 0.5
--                                                                                               , 5)      AS UPRC_CLEAN
--                            , TRUNC( (  NVL(DECODE(T1.MACT_BOND_VALU_AMT, 0, NULL, T1.MACT_BOND_VALU_AMT), T2.MACT_BOND_VALU_AMT)
--                                      + NVL(DECODE(T2.MACT_BOND_VALU_AMT, 0, NULL, T2.MACT_BOND_VALU_AMT), T1.MACT_BOND_VALU_AMT) ) * 0.5
--                                                                                               , 5)      AS UPRC_MACT_BOND
--                            , TRUNC( (  NVL(DECODE(T1.IMCE_DRVT_GDS_PRC,  0, NULL, T1.IMCE_DRVT_GDS_PRC ), T2.IMCE_DRVT_GDS_PRC )
--                                      + NVL(DECODE(T2.IMCE_DRVT_GDS_PRC,  0, NULL, T2.IMCE_DRVT_GDS_PRC ), T1.IMCE_DRVT_GDS_PRC ) ) * 0.5
--                                                                                               , 3)      AS UPRC_MACT_OPTN
--                            , TRUNC( (  NVL(DECODE(T1.UPD_DURA,           0, NULL, T1.UPD_DURA          ), T2.UPD_DURA          )
--                                      + NVL(DECODE(T2.UPD_DURA,           0, NULL, T2.UPD_DURA          ), T1.UPD_DURA          ) ) * 0.5
--                                                                                               , 4)      AS DURA_MODI
--                            , TRUNC(        DECODE(T2.BPDR100,            0, NULL, T2.BPDR100) , 4)      AS DURA_EFFE
--                            , DECODE(T1.CLE_UPRC, 0, NULL, T1.CLE_UPRC)                                  AS UPRC_1
--                            , DECODE(T2.CLE_UPRC, 0, NULL, T2.CLE_UPRC)                                  AS UPRC_2
--                            , DECODE(T1.UPD_DURA, 0, NULL, T1.UPD_DURA)                                  AS DURA_1
--                            , DECODE(T2.BPDR100 , 0, NULL, T2.BPDR100 )                                  AS DURA_2
--                            , T2.KSRT_BOND_GRDE                                                          AS CRGR_KIS
--                            , T2.NICE_BOND_GRDE                                                          AS CRGR_NICE
--                            , T2.KR_BOND_GRDE                                                            AS CRGR_KR
--                            , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.KSRT_BOND_GRDE), '999')
--                                                                                                         AS KS_CRGR_KICS
--                            , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.NICE_BOND_GRDE), '999')
--                                                                                                         AS NC_CRGR_KICS
--                            , NVL((SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR WHERE CRGR_DMST = T2.KR_BOND_GRDE  ), '999')
--                                                                                                         AS KR_CRGR_KICS
--                         FROM (
--                                 SELECT *
--                                   FROM IDHKRH_FN_FBEV    -- 해외채권 FN
--                                  WHERE STND_DT = (
--                                                     SELECT MAX(STND_DT)
--                                                       FROM IDHKRH_FN_FBEV
--                                                      WHERE STND_DT <= '$$STD_Y4MD'
--                                                  )
--                              ) T1
--                                   FULL OUTER JOIN
--                              (
--                                 SELECT *
--                                   FROM IDHKRH_NICE_FBEV  -- 해외채권 NICE
--                                  WHERE STND_DT = (
--                                                     SELECT MAX(STND_DT)
--                                                       FROM IDHKRH_NICE_FBEV
--                                                      WHERE STND_DT <= '$$STD_Y4MD'
--                                                  )
--                              ) T2
--                           ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
--                        WHERE 1=1
--                    ) DD
--          )
--, T_EEE AS 
--(
--             SELECT NVL(T1.SECR_ITMS_CD, T2.SECR_ITMS_CD)                                      AS SECR_ITMS_CD
--                  , NVL(T2.AST_CLCD, T1.AST_CLCD)                                              AS ASSET_LIQ_CLCD
--                  , TRUNC(
--                    CASE WHEN NVL(T2.TXTN_CLCD, T1.TXTN_CLCD) = '2'                            -- 세후단가인 경우
--                         THEN (  NVL(           DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
--                                    , NVL(      DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC )
--                                         , NVL( DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
--                                              , DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ) ) ) )
--                               + NVL(           DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
--                                    , NVL(      DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC )
--                                         , NVL( DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
--                                              , DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ) ) ) ) ) * 0.5
--                         ELSE (  NVL( DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ), T2.VLT_UPRC)
--                               + NVL( DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ), T1.VLT_UPRC)   ) * 0.5
--                         END
--                    , 2)                                                                       AS UPRC_DIRTY
--                  , TRUNC(
--                    CASE WHEN T1.TXTN_CLCD = '2'                                               -- 세후단가인 경우
--                         THEN    NVL(           DECODE(T1.AFTX_UPRC, 0, NULL, T1.AFTX_UPRC)
--                                    ,           DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC ) )
--                         ELSE                   DECODE(T1.VLT_UPRC , 0, NULL, T1.VLT_UPRC )
--                         END
--                    , 2)                                                                       AS UPRC_1
--                  , TRUNC(
--                    CASE WHEN T1.TXTN_CLCD = '2'                                               -- 세후단가인 경우
--                         THEN    NVL(           DECODE(T2.AFTX_UPRC, 0, NULL, T2.AFTX_UPRC)
--                                    ,           DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC ) )
--                         ELSE                   DECODE(T2.VLT_UPRC , 0, NULL, T2.VLT_UPRC )
--                         END
--                    , 2)                                                                       AS UPRC_2
--                  , TRUNC( (  NVL(DECODE(T1.UPD_DURA,  0, NULL, T1.UPD_DURA),  T2.UPD_DURA  )
--                            + NVL(DECODE(T2.UPD_DURA,  0, NULL, T2.UPD_DURA),  T1.UPD_DURA  ) ) * 0.5
--                                                                                     , 4)      AS DURA_MODI
--                  , DECODE(T1.UPD_DURA,  0, NULL, T1.UPD_DURA)                                 AS DURA_1
--                  , DECODE(T2.UPD_DURA,  0, NULL, T2.UPD_DURA)                                 AS DURA_2
--                  , NVL(T2.APLZ_GRDE_NM, NVL(T2.ISSU_INT_GRDE_NM, NVL(T1.APLZ_GRDE_NM, T1.ISSU_INT_GRDE_NM)))
--                                                                                               AS CRGR_NICE
--                  , (SELECT MIN(CRGR_KICS) FROM IKRUSH_MAP_CRGR
--                      WHERE CRGR_DMST = NVL(T2.APLZ_GRDE_NM, NVL(T2.ISSU_INT_GRDE_NM, NVL(T1.APLZ_GRDE_NM, T1.ISSU_INT_GRDE_NM))))
--                                                                                               AS CRGR_KICS
--               FROM (
--                       SELECT *
--                         FROM IDHKRH_FN_CDEV    -- CP FN
--                        WHERE STND_DT = (
--                                           SELECT MAX(STND_DT)
--                                             FROM IDHKRH_FN_CDEV
--                                            WHERE STND_DT <= '$$STD_Y4MD'
--                                        )
--                    ) T1
--                         FULL OUTER JOIN
--                    (
--                       SELECT *
--                         FROM IDHKRH_NICE_CDEV  -- CP NICE
--                        WHERE STND_DT = (
--                                           SELECT MAX(STND_DT)
--                                             FROM IDHKRH_NICE_CDEV
--                                            WHERE STND_DT <= '$$STD_Y4MD'
--                                        )
--                    ) T2
--                 ON T1.SECR_ITMS_CD = T2.SECR_ITMS_CD
--              WHERE 1=1
--          )
SELECT /* SQL-ID : KRBH106BM */
          A.GRDAT                                                                            AS BASE_DATE                      /*  기준일자               */
        , 'LOAN_O_'||A.POSITION_ID                                             AS EXPO_ID                        /*  익스포저ID             */
        , A.PORTFOLIO                                                                    AS FUND_CD                        /*  펀드코드               */
        , A.BOND_CLASS                                                                 AS PROD_TPCD                      /*  상품구분코드           */
        , KICS.PROD_TPNM                                                            AS PROD_TPNM                      /*  상품구분명             */
        , NVL(J.KICS_PROD_TPCD, KICS.KICS_TPCD)               AS KICS_PROD_TPCD                 /*  KICS상품유형코드       */
        , NVL(J.KICS_PROD_TPNM, KICS.KICS_TPNM)              AS KICS_PROD_TPNM                 /*  KICS상품유형명         */
        , A.ISIN                                                                                    AS ISIN_CD                        /*  종목코드               */
        , A.SECURITY_ID_T                                                               AS ISIN_NM                        /*  종목명                 */
        , A.FNDS_ACCO_CLCD||'_'||A.ACCO_CD                          AS ACCO_CD                         /*  계정과목코드           */
        , ACCO_MST.ACCO_NM                                                    AS ACCO_NM                        /*  계정과목명             */
        , A.SECURITY_ID                                                                   AS CONT_ID                        /*  계약번호(계좌일련번호) */
        ,'2'                                                                                   AS INST_TPCD                      /*  인스트루먼트유형코드   */    -- 기업대출: 2(CF. 개인대출: 1)
        ,'4'                                                                                   AS INST_DTLS_TPCD                 /*  인스트루먼트상세코드   */
        , A.ISSUE_DATE AS ISSU_DATE                      /*  발행일자               */
--        , A.EDDT                                                                                               AS MATR_DATE                      /*  만기일자               */
	, CASE WHEN A.BOND_CLASS IN ('901', '903') THEN NVL(B.FST_OPTN_DT, CALL_OPT_YYMM) END AS  		MATR_DATE
		
        , A.POSITION_CURR                                                                 AS CRNY_CD                        /*  통화코드               */
        , A.KURSF                    AS CRNY_FXRT                      /*  통화환율(기준일)       */
        , A.NOMINAL_AMT                                                   AS NOTL_AMT                       /*  이자계산기준원금       */
        , A.NOMINAL_AMT                                                    AS NOTL_AMT_ORG                   /*  최초이자계산기준원금   */
        , DECODE(A.ASSETCODE ,'SB', NVL(A.BOOK_VAL_AMT,0) * -1, NVL(A.BOOK_VAL_AMT,0)) AS BS_AMT                         /*  BS금액 (전기+당기 장부금액)        */
        , DECODE(A.ASSETCODE ,'SB', NULL, A.FAIR_VALUE_VAL_AMT)  AS VLT_AMT                        /*  평가금액               */
        , DECODE(A.ASSETCODE ,'SB', NULL, A.FAIR_VALUE_VAL_AMT)  AS FAIR_BS_AMT                        /*  공정가치B/S금액               */
        , A.ACC_INT_VAL_AMT                                                                 AS ACCR_AMT                        /*  미수수익               */
        , A.PRPF_DMST                                                                            AS UERN_AMT                        /*  선수수익               */		
	, DECODE(A.APCN_LATO,0,A.CUPR ,A.APCN_LATO) / 100        AS IRATE                          /*  금리                   */
        , NVL(  B.INRS_PAY_CYCL_VAL * DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1)
              , 12 / NVL(DECODE(A.ITTM, 0, NULL, A.ITTM), 1) )               AS INT_PAY_CYC                    /*  이자지급/계산주기(월)  */
        , NULL                                                                                                  AS INT_PROR_PAY_YN                /*  이자선지급여부(1: 후취)*/
        , CASE WHEN NVL(A.LATO_DVSN,'01') = '01' THEN '1' 
		     WHEN A.LATO_DVSN = '02' THEN '2' 
		      ELSE '1'
                        END                                                                    AS IRATE_TPCD                     /*  금리유형코드(1:고정)   */
        , 'KDSP1000'                                                                           AS IRATE_CURVE_ID                 /*  금리커브ID             */
        , NULL                                                                              AS IRATE_DTLS_TPCD                /*  변동금리유형코드       */  -- 우선 NULL 처리
        , A.GARI                                                                              AS ADD_SPRD                       /*  가산스프레드           */  --우선 NULL  처리
        , NVL( DECODE(A.LATO_DVSN,'02' ,DECODE(B.INRS_PAY_CYCL_CLCD, '3', 12, 1), NULL)
		     , A.LATO_CHNG_CYCL)   AS IRATE_RPC_CYC                  /*  금리개정주기(월)       */
        -- , CASE WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('01', '05'      ) THEN  '1'                                             /*  ACT/365                */
        --        WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('02', '10', '12') THEN  '2'                                             /*  A30/360                */
        --        WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('04', '06', '08') THEN  '3'                                             /*  E30/360                */
        --        WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('00', '09'      ) THEN  '4'                                             /*  ACT/ACT                */
        --        WHEN TO_NUMBER(B.INRS_DDCT_CLCL_MTCD) IN ('03'            ) THEN  '5'                                             /*  ACT/360                */
        --        ELSE '1' END                                                                                                      /*  DEFAULT: A30/360       */
        , '1'                                                        AS DCB_CD                         /*  일수계산코드    2024.05.09 요건 수정        */
        , B.INRS_RCDT_DD_CLCD                                        AS CF_GEN_TPCD                    /*  현금흐름일생성구분     */
        , A.HIDT                                                                         AS FRST_INT_DATE                  /*  최초이자기일           */
        , A.NEXT_RELV_DT                                                     AS NEXT_INT_DATE                  /*  차기이자기일           */
        , NULL                                                                                 AS GRACE_END_DATE                 /*  거치기간종료일자       */
        , TO_NUMBER(SUBSTR(B.PRCP_GRPE_TERM_CD, 1, 2)) * 12 + TO_NUMBER(SUBSTR(B.PRCP_GRPE_TERM_CD, 3, 2))
                                                                                               AS GRACE_TERM                     /*  거치기간(월)           */
        , NULL                                     AS IRATE_CAP                      /*  적용금리상한           */  --우선 NULL 처리
        , NULL                                    AS IRATE_FLO                      /*  적용금리하한           */   --우선 NULL 처리
        , NULL                                                                                 AS RPAY_TPCD                      /*  상환유형코드           */
        , B.DIV_RPAY_UNIT_TERM_MMCT                                                            AS AMORT_TERM                     /*  원금분할상환주기(월)   */
        , NULL                                                                                 AS AMORT_AMT                      /*  분할상환금액           */
        , NULL                                                                                 AS AMORT_TPCD                     /*  분할상환유형코드       */
        , NULL                                                                                 AS AMORT_DTLS_TPCD                /*  분할상환상세유형코드   */
        , NULL                                                                                 AS AMORT_END_YMD                  /*  분할상환종료일자       */
        , NULL                                                                                 AS AMORT_DAYS                     /*  분할상환일수           */
        , NULL                                                                                 AS IRATE_FIX_TERM                 /*  금리고정기간           */
       , A.ISSUER                                                                   AS CNPT_ID                        /*  거래상대방ID           */
        , A.ISSUER_NAME                                        AS CNPT_NM                        /*  거래상대방명           */
        , NVL(A.ISSUER_BUP, B.ISSU_CO_CORP_CD)                                                AS CORP_NO                        /*  법인등록번호           */
        , A.RATING1               AS CRGR_KIS                       /*  신용등급(한신평)       */
        , A.RATING3               AS CRGR_NICE                      /*  신용등급(한신정)       */
        , A.RATING4               AS CRGR_KR                        /*  신용등급(한기평)       */
        ,NVL( J.CRGR_VLT, DECODE(A.ASSETCODE ,'SB', NVL(RATING1, 'AA-' )))                                                                     AS CRGR_VLT                       /*  신용등급(평가)         */  
        , REPLACE(NVL(J.CRGR_KICS, A.CRGR_KICS),'0','99')                                      AS CRGR_KICS                      /*  신용등급(KICS)         */  
       , '30'                                                                                 AS CUST_GB                        /*  고객구분               */
        , NULL                                                                                 AS CUST_TPCD                      /*  고객유형코드           */
        , NULL                                                                                 AS CORP_TPCD                      /*  법인유형코드           */
        , '30'                                                                                 AS LOAN_CONT_TPCD                 /*  대출계약유형코드       */
        , NULL                                                                                 AS CRGR_CB_NICE                   /*  개인신용등급(한신정)   */
        , NULL                                                                                 AS CRGR_CB_KCB                    /*  개인신용등급(KCB)      */
        , NULL                                                                                 AS COLL_ID                        /*  담보ID                 */
        , NULL                                                                                 AS COLL_TPCD                      /*  담보유형코드           */
        , NULL                                                                                 AS COLL_DTLS_TPCD                 /*  담보상세유형코드       */
        , NULL                                                                                 AS PRPT_DTLS_TPCD                 /*  부동산상세유형코드     */
        , NULL                                                                                 AS RENT_GUNT_AMT                  /*  임대보증금             */
        , NULL                                                                                 AS PRPT_OCPY_TPCD                 /*  부동산점유유형코드     */
        , NULL                                                                                 AS LAND_APPR_AMT                  /*  토지감정금액           */
        , NULL                                                                                 AS BULD_APPR_AMT                  /*  건물감정금액           */
        , NULL                                                                                 AS COLL_AMT                       /*  담보금액               */
        , NULL                                                                                 AS REDC_AMT                       /*  감액금액               */
        , J.FAIR_VAL_SNR                                                                       AS PRIM_AMT                       /*  선순위금액             */
        , J.LTV                                                                                AS LTV                            /*  LTV                    */
        , J.COLL_SET_AMT                                                                       AS COLL_SET_AMT                   /*  담보설정금액           */
        , NULL                                                                                 AS COLL_SET_RTO                   /*  담보설정비율           */
        , NULL                                                                                 AS LMT_AMT                        /*  한도금액               */
        , NULL                                                                                 AS REG_RPAY_AMT                   /*  정기상환금액           */
        , NULL                                                                                 AS PRE_RPAY_AMT                   /*  조기상환금액           */
        , NULL                                                                                 AS ARR_DAYS                       /*  연체일수               */
        , NULL                                                                                 AS FLC_CD                         /*  자산건전성코드         */
        , 0                                                                                    AS ABD_AMT                        /*  대손충당금금액         */
        , 0                                                                                    AS ACCR_ABD_AMT                   /*  미수수익대손충당금금액 */
        , 0                                                                                    AS LOCF_AMT                       /*  이연대출부대손익금액   */
        , 0                                                                                    AS PV_DISC_AMT                    /*  현재가치할인차금금액   */
        , A.OPER_PART                         AS DEPT_CD                        /*  부서코드               */
        , 'KRBH106BM'                                                                          AS LAST_MODIFIED_BY               /*  LAST_MODIFIED_BY       */
        , SYSDATE                                                                              AS LAST_UPDATE_DATE               /*  LAST_UPDATE_DATE       */		
        , A.RATING5                                                                            AS CRGR_SNP	        	/* 신용등급(S&P)         */ 
        , A.RATING6                                                                            AS CRGR_MOODYS	      /* 신용등급(무디스)         */ 
        , A.RATING7                                                                            AS CRGR_FITCH	        /* 신용등급(피치)         */ 
        , A.IND_SECTOR_L                                                                       AS IND_L_CLSF_CD      /*산업대분류코드*/
        , A.IND_SECTOR_L_T                                                                     AS IND_L_CLSF_NM      /* 산업대분류명*/ 
        , A.IND_SECTOR                                                                         AS IND_CLSF_CD        /* 산업분류코드*/
        , J.LTV_SNR                                                                            AS SNR_LTV            /* 선순위LTV(%)) */
        , J.FAIR_VAL_SNR                              /* 선순위대출금액*/
        , J.DSCR                                      /* DSCR */
   FROM   T_AAA     A
        , T_BBB			B		  
--       ,  T_CCC 		CC		  
--       ,  T_DDD 		DD
--        , T_EEE  		E		  
      , ACCO_MST
      , (
             SELECT CD                                                AS PROD_TPCD
	                , CD_NM                                         AS PROD_TPNM		 
                  , CD2                                               AS KICS_TPCD
                  , CD2_NM                                            AS KICS_TPNM
               FROM IKRUSH_CODE_MAP
              WHERE GRP_CD = 'KICS_PROD_MAP_SAP'    -- KICS상품분류매핑(기존분류코드를 KICS상품분류코드로 매핑)
          ) KICS		  
        , (
             SELECT *
               FROM IKRUSH_PROD_LOAN T1         -- 수기_상품(대출채권)
              WHERE BASE_DATE = (
                                   SELECT MAX(BASE_DATE)
                                     FROM IKRUSH_PROD_LOAN
                                    WHERE BASE_DATE <= '$$STD_Y4MD'
                                      AND PROD_KEY = T1.PROD_KEY
                                )
          ) J		  
    WHERE 1=1
          AND A.ISIN  = B.SECR_ITMS_CD(+)
--          AND A.ISIN  = CC.SECR_ITMS_CD(+)
--          AND A.ISIN  = DD.SECR_ITMS_CD(+)
--          AND A.ISIN  = E.SECR_ITMS_CD(+)		  
	  AND TO_CHAR(A.BOND_CLASS) = KICS.PROD_TPCD(+)
          AND A.POSITION_ID  = J.PROD_KEY(+)
          AND A.ACCO_CD  = ACCO_MST.ACCO_CD(+)