SELECT /* SQL-ID : KRBH308BM */ A.AS_OF_DATE                    AS AS_OF_DATE      
     , A.FCONT_ID                      AS FCONT_ID        
     , B.SEG_ID                        AS SEG_ID          
     , A.COLL_TYP                      AS COLL_TYP        
     , A.COLL_DET_TYP                  AS COLL_DET_TYP    
     , 'KRBH308BM'                     AS LAST_MODIFIED_BY
     , SYSDATE                         AS LAST_UPDATE_DATE
  FROM Q_CB_INST_BOND_LOAN A
     , Q_IF_SEGMENT B
 WHERE A.AS_OF_DATE = LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))
--WHERE A.AS_OF_DATE = '$$BASE_DATE'
   AND A.INST_TYP = '1'
   AND B.SEG_GRP_ID = 'LGD_KICS'
   AND 'LGD_'||LPAD(A.COLL_TYP,2,'0') = B.SEG_ID