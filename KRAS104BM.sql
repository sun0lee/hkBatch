SELECT /* SQL-ID : KRAS104BM */
        DISTINCT
        '$$STD_Y4MD'                            AS STND_DT                      /* 입력생성일자                      */
      , '2'                                     AS CCLT_CLCD                    /* 산출구분코드                      */
      , A.ISIN                                  AS  SECR_GDS_CD                  /* 상품코드                          */
      , A.SECURITY_ID_T                         AS SECR_PDNM                    /* 상품명                            */
      , A.ISSUE_DATE                            AS ISSU_DT                      /* 발행일                            */
      , A.EDDT                                  AS EXPR_DT                      /* 만기일                            */
      , A.SEC_CLASS                             AS FNDS_PTTN_CLSF_CD            /* 펀드유형분류코드                  */
      , MAX(A.POSITION_CURR)                    AS  CRNC_CD                      /* 통화구분                          */
      , AVG(A.AVR_RATE_BOOK)                    AS BNFC_STND_PRC                /* 기준가격         -- 기초기준가격  */
      , MAX(B.UNIT_PRICE_GUBUN)                 AS STRD_CALC_BNKN               /* 기준가산정좌수   -- 1좌당가격     */
      , A.ISIN                                  AS KOFI_FNDS_CD                 /* 표준코드                          */
      , 'KR_AS_KRAS104BM'                       AS LAST_MODIFIED_BY             /* 최종 변경자                       */
      , SYSDATE                                 AS LAST_MODIFIED_DATE           /* 최종 변경 일시                    */
   FROM IBD_ZTCFMPEBAPST A  -- SAP 보유원장
      , IBD_ZTCFMOSCLASS B -- IFRS9 수익증권 종목정보
  WHERE 1=1
    AND A.SECURITY_ID = B.SECURITY_ID(+)
    AND B.VERS(+) = '000'	  
    AND A.GRDAT = '$$STD_Y4MD'  
    AND A.VALUATION_AREA = '400'
    AND A.ASSETCODE IN ('OS') -- 채권,주식, 금융상품;수익증권
  GROUP BY A.ISIN, A.SECURITY_ID_T, A.ISSUE_DATE, A.EDDT, A.SEC_CLASS