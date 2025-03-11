SELECT /* SQL-ID : KRBH306BM */ A.AS_OF_DATE                                           AS AS_OF_DATE      
     , A.CNTR_PTY_ID                                          AS CNTR_PTY_ID     
     , B.SEG_ID                                               AS SEG_ID          
     , 1                                                      AS CNTR_PTY_TYP    
     , SUBSTR(A.CNTR_PTY_ID, INSTR(A.CNTR_PTY_ID,'_')+1,2)    AS CRD_GRD_CD      
     , 'KRBH306BM'                                            AS LAST_MODIFIED_BY
     , SYSDATE                                                AS LAST_UPDATE_DATE
  FROM 
      (SELECT AS_OF_DATE
            , CNTR_PTY_ID
            , COUNT(*)     AS CNT
        FROM Q_CB_INST_BOND_LOAN
       WHERE AS_OF_DATE = LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))
    --WHERE A.AS_OF_DATE = '$$BASE_DATE'
         AND INST_TYP = '1'
       GROUP BY CNTR_PTY_ID, AS_OF_DATE     
       )A
     , Q_IF_SEGMENT B
 WHERE B.SEG_GRP_ID = 'PD_KICS'
   AND A.CNTR_PTY_ID = B.SEG_ID(+)