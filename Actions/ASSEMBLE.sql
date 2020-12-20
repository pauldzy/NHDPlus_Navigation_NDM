/* 

   Assemble Preprocessed and Temporary Tables 

*/
BEGIN
   nhdplus_navigation2.navigator2_util.build_preprocessed_tables();
   nhdplus_navigation2.navigator2_util.build_temporary_tables();
END;
/

