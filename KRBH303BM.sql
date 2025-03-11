WITH /* SQL ID : KRBH303BM */ T_AAA AS( 
               SELECT '$$STD_Y4MM'        AS BASE_YM
                    , BOND_TYP||CRD_GRD   AS IRC_NM
                    , MAT_CD
                    , CRD_GRD_SPREAD
                 FROM IKRUSH_SPREAD_BD              
              UNPIVOT( CRD_GRD_SPREAD FOR MAT_CD IN(M003, M006, M009,M012,M018,M024,M030,M036,M048,M060,M084,M120,M180,M240,M360,M600))
                WHERE BASE_YM = (SELECT MAX(BASE_YM)
                                         FROM IKRUSH_SPREAD_BD 
                                        WHERE BASE_YM <= '$$STD_Y4MM'
                                      )
/*                  AND BOND_TYP = '은행채'
                    AND CRD_GRD = 'AAA(산금채)'    */
                )
,    T_BBB AS(
               SELECT A.BASE_YM
                    , A.IRC_NM
                    , TO_NUMBER(SUBSTR(A.MAT_CD, 2))                                                               AS CUR_IDX
                    , A.CRD_GRD_SPREAD                                                                             AS CUR_IRATE
                    , NVL(TO_NUMBER(SUBSTR(LEAD(A.MAT_CD) OVER (PARTITION BY A.IRC_NM ORDER BY A.MAT_CD), 2))
                        , TO_NUMBER(SUBSTR(A.MAT_CD, 2)))                                                          AS NEXT_IDX
                    , NVL(LEAD(A.CRD_GRD_SPREAD) OVER (PARTITION BY A.IRC_NM ORDER BY A.MAT_CD), A.CRD_GRD_SPREAD) AS NEXT_IRATE
                 FROM T_AAA A
             )
,    T_CCC AS(
               SELECT A.BASE_YM
                    , A.IRC_NM
                    , 'M'||LPAD(B.IDX, 4, 0)                                                                       AS TERM
                    , CASE WHEN B.IDX <= A.CUR_IDX  THEN A.CUR_IRATE 
                           WHEN B.IDX >  A.NEXT_IDX THEN A.NEXT_IRATE               
                           ELSE (A.NEXT_IRATE - A.CUR_IRATE) / (A.NEXT_IDX - A.CUR_IDX) * (B.IDX - A.CUR_IDX) + A.CUR_IRATE
                           END                                                                                     AS SPREAD
                 FROM ( 
                         SELECT TO_NUMBER(LEVEL) AS IDX 
                           FROM DUAL
                        CONNECT BY LEVEL <= 1200
                      ) B     
                    , T_BBB A
                WHERE 1=1
                  AND (     ( B.IDX > A.CUR_IDX AND B.IDX < A.NEXT_IDX )
                         OR ( B.IDX = A.CUR_IDX )
                         OR ( B.IDX < ( SELECT MIN(CUR_IDX) FROM T_BBB ) AND A.CUR_IDX = ( SELECT MIN(CUR_IDX) FROM T_BBB ) )
                         OR ( B.IDX > ( SELECT MAX(CUR_IDX) FROM T_BBB ) AND A.CUR_IDX = ( SELECT MAX(CUR_IDX) FROM T_BBB ) )
                      )
            )
SELECT A.BASE_YM                  AS BASE_YM
     , A.TERM                     AS MAT_CD
     , B.IRC_ID
     , A.IRC_NM
     , ROUND(A.SPREAD, 8)/100     AS SPREAD
     , 'KRBH303BM'                AS LAST_MODIFIED_BY
     , SYSDATE                    AS LAST_UPDATE_DATE 
  FROM T_CCC A
     , Q_CM_IRC B
 WHERE A.IRC_NM = B.IRC_NM