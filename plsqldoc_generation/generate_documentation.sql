plugin plsqldoc delete
---------------------------------------
--xuprommgr.table
---------------------------------------
plugin plsqldoc generate XUPROMMGR.ZFTEADA XUPROMMGR.ZSBMLBS XUPROMMGR.ZSBTELE XUPROMMGR.ZSTHOSX_SAFE XUPROMMGR.ZTTTYPE
plugin plsqldoc generate XUPROMMGR.ZTVBLUM XUPROMMGR.ZTVBLUP XUPROMMGR.ZTVBLUS XUPROMMGR.ZTVRMSC
---------------------------------------
--xuprommgr.package
---------------------------------------
plugin plsqldoc generate XUPROMMGR.XU_HZSKUTED XUPROMMGR.XU_REEHER_FEEDS XUPROMMGR.XU_SSB_PREP XUPROMMGR.XU_SSB_UTILS XUPROMMGR.XU_ZGBPARM_PERSONA
plugin plsqldoc generate XUPROMMGR.ZAKFUNC XUPROMMGR.ZRKFUNC XUPROMMGR.ZRKNOLV XUPROMMGR.ZSKENRL
---------------------------------------
--xuprommgr.procedure
---------------------------------------
---------------------------------------
--xuprommgr.function
---------------------------------------
plugin plsqldoc generate XUPROMMGR.XF_REGISTRATION_CHECK XUPROMMGR.XF_VALIDATE_MULTIPLE XUPROMMGR.XF_VALIDATE_MULTIPLE_NOT_NULL XUPROMMGR.XF_VALIDATE_MULTIPLE_NULL XUPROMMGR.XF_VALIDATE_SINGLE
plugin plsqldoc generate XUPROMMGR.XF_WHAT_PREP XUPROMMGR.XF_WHAT_TICKET XUPROMMGR.XF_XTENDER_TERMS XUPROMMGR.XUF_GET_ORGN_DIVISION
---------------------------------------
--xuprommgr.view
---------------------------------------
plugin plsqldoc generate XUPROMMGR.XUV_PEVPRF1_UNI_JOB XUPROMMGR.XUV_PEVPRF1_UNI_POS XUPROMMGR.XUV_SPRING_ENROLLMENT XUPROMMGR.XUV_SUMMER_ENROLLMENT XUPROMMGR.XU_COMM_GIFT_OFFICER_VIEW
plugin plsqldoc generate XUPROMMGR.ZFVOPAL XUPROMMGR.ZGVSYST XUPROMMGR.ZPVLASTPAY XUPROMMGR.ZTVSBAL
---------------------------------------
--xuprommgr.materialized view
---------------------------------------
plugin plsqldoc generate XUPROMMGR.EMPLOYEE_JOBS_EFFDATE_VIEW XUPROMMGR.EMPLOYEE_UNIVERSAL_VIEW XUPROMMGR.ZRVABD1 XUPROMMGR.ZRVABD2 XUPROMMGR.ZR_BIODEMO
---------------------------------------
--xupersona.table
---------------------------------------
plugin plsqldoc generate XUPERSONA.ZGVCDAC
plugin plsqldoc rebuild
EXIT APPLICATION
/

PL/SQL procedure successfully completed.

