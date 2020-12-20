CREATE OR REPLACE PROCEDURE nhdplus_navigation2.flush_all
AUTHID DEFINER
AS
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE nhdplus_navigation2.tmp_navigation2_search';

END flush_all;
/

