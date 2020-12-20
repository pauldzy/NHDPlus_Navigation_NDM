CREATE OR REPLACE PACKAGE nhdplus_navigation2.navigator2_util
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_preprocessed_tables;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_search_tbl;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_catchment_tbl;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_tables;

END navigator2_util;
/

