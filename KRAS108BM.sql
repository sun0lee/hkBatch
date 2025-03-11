SELECT /* SQL-ID : KRAS108BM */
          '$$STD_Y4MD'      STND_DT                   /* 기준일자                  */         
        , '2'               CCLT_CLCD                 /* 산출구분코드              */
        , CUST_ID           CUST_NO                   /* 고객번호                  */
        , '01'              STAT_CLCD                 /* 상태구분코드              */
        , CUST_NAME         CUST_NM                   /* 고객명                    */
        , CUST_NAME         CUST_ABBR_NM              /* 고객약어명                */
        , NULL              CUST_ENG_NM               /* 고객영문명                */
        , NULL              CUST_ENG_ABBR_NM          /* 고객영문약어명            */
        , COUNTRY           ISO_NATN_SYM_CD           /* 소속ISO국가기호           */
        , CLNO              BRNB                      /* 사업자등록번호            */
        , BUCD              CORP_REG_NO               /* 법인등록번호              */
        , NULL              TRSN_CUST_CLCD            /* 거래자구분코드            */
        , STDD_INDT_CLCD    IND_CLSF_CD               /* 산업분류코드              */
        , NULL              TXTN_CD                   /* 과세코드                  */
        , NULL              ISSU_INT_SCUR_EDPS_CD     /* 발행기관증권전산코드      */
        , NULL              COBO_UNIQ_CD              /* 회사채고유코드            */
        , NULL              LSTN_CLCD                 /* 상장구분                  */
        , NULL              STAC_MMDD                 /* 결산월일                  */
        , NULL              BANK_YN                   /* 은행여부                  */
        , NULL              BROK_YN                   /* 브로커여부                */
        , NULL              SMT_INT_YN                /* 결제기관여부              */
        , NULL              ISSU_INT_YN               /* 발행기관여부              */
        , NULL              ITRM_INT_YN               /* 중개기관여부              */
        , NULL              FNDS_YN                   /* 펀드여부                  */
        , NULL              ALCO_YN                   /* 계열사여부                */
        , NULL              RLTC_YN                   /* 관계사여부                */
        , NULL              GOV_CLCD                  /* 정부구분코드              */
        , NULL              INT_YN                    /* 기관여부                  */
        , NULL              HDGE_YN                   /* 헷지여부                  */
        , NULL              WRKG_CO_YN                /* 운용사여부                */
        , NULL              TRST_CO_YN                /* 수탁사여부                */
        , NULL              OFWR_TRST_CO_YN           /* 사무수탁사여부            */
        , NULL              TRST_CO_CUST_NO           /* 수탁사고객번호            */
        , NULL              SUCO_YN                   /* 자회사여부                */
        , NULL              MJSH_YN                   /* 대주주여부                */
        , NULL              SPC_ITPR_YN               /* 특수관계인여부            */
        , NULL              PUBL_INT_YN               /* 공공기관여부              */
        , GRCODE            GOCO_CD                   /* 기업집단코드              */
        , NULL              GOCO_NM                   /* 기업집단명                */
        , 'KR_AS_KRAS108BM' LAST_MODIFIED_BY          /* 최종 변경자               */
        , SYSDATE           LAST_MODIFIED_DATE        /* 최종 변경 일시            */
     FROM IBD_ZTCFMELJ0040