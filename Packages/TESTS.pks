CREATE OR REPLACE PACKAGE nhdplus_navigation2.tests
AUTHID CURRENT_USER
AS

   C_TFS_CHANGESET CONSTANT NUMBER := 0.0;
   C_JENKINS_JOBNM CONSTANT VARCHAR2(255 Char) := 'NULL';
   C_JENKINS_BUILD CONSTANT NUMBER := 0.0;
   C_JENKINS_BLDID CONSTANT VARCHAR2(255 Char) := 'NULL';
   
   C_PREREQUISITES CONSTANT MDSYS.SDO_STRING2_ARRAY := MDSYS.SDO_STRING2_ARRAY(
       'DZ_CRS'
      ,'DZ_DICT'
      ,'CMN_USGS_MEASURES'
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION prerequisites
   RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION version
   RETURN VARCHAR2;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION tests
   RETURN NUMBER;

END tests;
/

GRANT EXECUTE ON nhdplus_navigation2.tests TO public;

