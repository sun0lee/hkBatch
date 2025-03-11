SELECT /* SQL-ID : KRBH309BM */ COLL_SEG_ID                                          AS SEG_ID          
     , LAST_DAY(TO_DATE(BASE_YM||'01','YYYYMMDD'))          AS AS_OF_DATE      
     , 'N'                                                  AS STD_PLAN_YN     
     , LGD/100                                              AS LGD             
     , NULL                                                 AS LGD_STDEV       
     , LGD/100                                              AS LGD_APL         
     , LGD/100                                              AS LGD_IFRS        
     , 'KRBH309BM'                                          AS LAST_MODIFIED_BY
     , SYSDATE                                              AS LAST_UPDATE_DATE
 FROM IKRUSH_FSS_LGD
WHERE BASE_YM = '$$STD_Y4MM'