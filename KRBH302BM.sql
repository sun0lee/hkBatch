SELECT /* SQL-ID : KRBH302BM */
       FCONT_ID                                  AS FCONT_ID                  
     , NULL                                      AS DESCT                     
     , IFRS_COA_ID                               AS IFRS_COA_ID               
     , F_PROD_ID                                 AS F_PROD_ID                 
     , NULL                                      AS ISIN                      
     , NULL                                      AS ACCOUNT_ID                
     , NULL                                      AS EXEC_ID                   
     , NULL                                      AS APPRV_ID                  
     , INST_TYP                                  AS INST_TYP                  
     , NULL                                      AS IFRS_FPROD_CATG_CD        
     , NULL                                      AS IFRS17_CATG_CD            
     , NULL                                      AS YLD_MTD                   
     , NULL                                      AS GL_CATG_R                 
     , NULL                                      AS GL_CATG_C                 
     , NULL                                      AS EUC_YN                    
     , NULL                                      AS FV_LVL                    
     , NULL                                      AS FV_BOOK_YN                
     , NULL                                      AS FVO_YN                    
     , NULL                                      AS IS_USE_EX_CF              
     , NULL                                      AS IS_USE_IRREG_IR           
     , NULL                                      AS IS_BOND_OPTION            
     , NULL                                      AS IR_GRD_LAYER_YN           
     , NULL                                      AS ONER_DATE                 
     , NULL                                      AS CF_GEN_YN                 
     , NULL                                      AS FV_GEN_YN                 
     , CASE WHEN INST_TYP = '1' THEN 7  /*부도율 적용*/
            ELSE 1                         /*부도율 미적용*/
          END                                    AS FV_MODEL_CD               
     , 'Y'                                       AS RS_GEN_YN                 
     , NULL                                      AS EIR_GEN_YN                
     , NULL                                      AS PPAID_COST_GEN_YN         
     , NULL                                      AS PPAID_COST_AMORT_METH_CD  
     , NULL                                      AS LP_OBJT_YN                
     , NULL                                      AS MS                        
     , NULL                                      AS CR                        
     , NULL                                      AS DA                        
     , NULL                                      AS ILS                       
     , NULL                                      AS LIQ_RUNOFF_BOOK_BAL_USE_YN
     , NULL                                      AS INIT_RS                   
     , NULL                                      AS INIT_EIR                  
     , NULL                                      AS PPAID_COST                
     , ISSUE_DATE                                AS ISSUE_DATE                
     , MATURITY_DATE                             AS MATURITY_DATE             
     , NULL                                      AS EXP_MATURITY_DATE         
     , NULL                                      AS COMP_MARGIN               
     , NULL                                      AS INIT_RA                   
     , NULL                                      AS INIT_CSM                  
     , NULL                                      AS LAST_APL_DATE             
     , NULL                                      AS LAST_STATUS               
     , NULL                                      AS NOTE                      
     , 'KRBH302BM'                               AS LAST_MODIFIED_BY          
     , SYSDATE                                   AS LAST_UPDATE_DATE          
  FROM Q_CB_INST_BOND_LOAN
 WHERE AS_OF_DATE = LAST_DAY(TO_DATE('$$STD_Y4MM'||'01','YYYYMMDD'))