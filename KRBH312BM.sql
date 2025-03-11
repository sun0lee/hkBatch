WITH /* SQL-ID : KRBH312BM */ T_AAA AS
(
   SELECT '$$STD_Y4MD'                         AS BASE_DATE
        , '1111111'                            AS IR_CURVE_ID  -- DEFAULT ID = '1111111'
        , 'M'||LPAD(TRUNC(EXPR_VAL*12), 4, 0)  AS MAT_CD
        , PSNT_MKT_ITRT / 100.0                AS INT_RATE
        , 0.0                                  AS ADD_SPRD
        , 'KRBH312BM'                          AS LAST_MODIFIED_BY
        , SYSDATE                              AS LAST_UPDATE_DATE
     FROM IDHKRH_RATE
    WHERE 1=1
      AND STND_DT = (
                       SELECT MAX(STND_DT)
                         FROM IDHKRH_RATE
                        WHERE STND_DT <= '$$STD_Y4MD'
                    )
      AND CRED_GRDE_CD = '1010000'
      AND EXPR_VAL BETWEEN 0.25 AND 20   -- IN (0.25, 0.5, 0.75, 1, 1.5, 2, 2.5, 3, 4, 5, 7, 10, 20)
)
SELECT * FROM T_AAA