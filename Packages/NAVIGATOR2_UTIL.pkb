CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.navigator2_util
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_tables
   AS
      str_sql VARCHAR2(4000 Char);
      TYPE row_metadata IS TABLE OF all_sdo_network_metadata%ROWTYPE;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over environment
      --------------------------------------------------------------------------
      IF USER <> 'NHDPLUS_NAVIGATION2'
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'user must be NHDPLUS_NAVIGATION2'
         );
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Build results permanant table
      --------------------------------------------------------------------------
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_NAVIGATION_RESULTS'
      );

      str_sql := 'CREATE TABLE '
              || 'nhdplus_navigation2.tmp_navigation_results( '
              || '    objectid                    INTEGER NOT NULL '
              || '   ,session_id                  VARCHAR2(40 Char) NOT NULL '
              || '   ,permanent_identifier        VARCHAR2(40 Char) NOT NULL '
              || '   ,nhdplus_comid               INTEGER NOT NULL '
              || '   ,reachcode                   VARCHAR2(14 Char) NOT NULL '
              || '   ,fmeasure                    NUMBER NOT NULL '
              || '   ,tmeasure                    NUMBER NOT NULL '
              || '   ,totaldist                   NUMBER '
              || '   ,totaltime                   NUMBER '
              || '   ,hydroseq                    INTEGER NOT NULL '
              || '   ,levelpathid                 INTEGER NOT NULL '
              || '   ,terminalpathid              INTEGER NOT NULL '
              || '   ,uphydroseq                  INTEGER '
              || '   ,dnhydroseq                  INTEGER '
              || '   ,lengthkm                    NUMBER NOT NULL '
              || '   ,travtime                    NUMBER '
              || '   ,nhdplus_region              VARCHAR2(3 Char) '
              || '   ,nhdplus_version             VARCHAR2(6 Char) '
              || '   ,reachsmdate                 DATE '
              || '   ,ftype                       NUMBER(3) '
              || '   ,fcode                       NUMBER(5) '
              || '   ,gnis_id                     VARCHAR2(10 Char) '
              || '   ,gnis_name                   VARCHAR2(65 Char) '
              || '   ,wbarea_permanent_identifier VARCHAR2(40 Char) '
              || '   ,wbarea_nhdplus_comid        INTEGER '
              || '   ,wbd_huc12                   VARCHAR2(12 Char) '
              || '   ,catchment_featureid         INTEGER '
              || '   ,shape                       MDSYS.SDO_GEOMETRY '
              || '   ,se_anno_cad_data            BLOB '
              || '   ,CONSTRAINT tmp_navigation_results_pk PRIMARY KEY '
              || '       (session_id,permanent_identifier) '
              || '       USING INDEX TABLESPACE ow_ephemeral_orcwater '
              || '   ,CONSTRAINT tmp_navigation_results_u02 UNIQUE '
              || '       (session_id,nhdplus_comid) '
              || '       USING INDEX TABLESPACE ow_ephemeral_orcwater '
              || ') '
              || 'TABLESPACE ow_ephemeral_orcwater '
              || 'NOLOGGING ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation_results TO public';
      EXECUTE IMMEDIATE str_sql;
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_results_i01 ' ||
      'ON nhdplus_navigation2.tmp_navigation_results(session_id,hydroseq) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
    
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_results_i02 ' ||
      'ON nhdplus_navigation2.tmp_navigation_results(session_id,levelpathid) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_results_i03 ' ||
      'ON nhdplus_navigation2.tmp_navigation_results(session_id,terminalpathid) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';

      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_results_i06 ' ||
      'ON nhdplus_navigation2.tmp_navigation_results(session_id,reachcode) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      EXECUTE IMMEDIATE
      'CREATE UNIQUE INDEX nhdplus_navigation2.tmp_navigation_results_u01 ' ||
      'ON nhdplus_navigation2.tmp_navigation_results(objectid) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      dz_dict_main.drop_sequence(
          p_owner         => 'NHDPLUS_NAVIGATION2'
         ,p_sequence_name => 'TMP_NAVIGATION_RESULTS_SEQ'
      );
    
      EXECUTE IMMEDIATE
      'CREATE SEQUENCE nhdplus_navigation2.tmp_navigation_results_seq ' ||
      'MAXVALUE 99999999999999999999 ' ||
      'START WITH 1 CYCLE ' ||
      'CACHE 100 ';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON ' || 
      'nhdplus_navigation2.tmp_navigation_results_seq TO public';
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Build status permanant table
      --------------------------------------------------------------------------
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_NAVIGATION_STATUS'
      );

      str_sql := 'CREATE TABLE '
              || 'nhdplus_navigation2.tmp_navigation_status( '
              || '    objectid                    INTEGER NOT NULL '
              || '   ,session_id                  VARCHAR2(40 Char) NOT NULL '
              || '   ,return_code                 NUMBER '
              || '   ,status_message              VARCHAR2(255 Char) '
              || '   ,session_datestamp           DATE '
              || '   ,CONSTRAINT tmp_navigation_status_pk '
              || '       PRIMARY KEY(session_id) '
              || '       USING INDEX TABLESPACE ow_ephemeral_orcwater '
              || ') '
              || 'TABLESPACE ow_ephemeral_orcwater '
              || 'NOLOGGING ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation_status TO public';
      EXECUTE IMMEDIATE str_sql;
    
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_status_i01 ' ||
      'ON nhdplus_navigation2.tmp_navigation_status(session_datestamp) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      dz_dict_main.drop_sequence(
          p_owner         => 'NHDPLUS_NAVIGATION2'
         ,p_sequence_name => 'TMP_NAVIGATION_STATUS_SEQ'
      );
      
      EXECUTE IMMEDIATE
      'CREATE SEQUENCE nhdplus_navigation2.tmp_navigation_status_seq ' ||
      'MAXVALUE 99999999999999999999 ' ||
      'START WITH 1 CYCLE ' ||
      'CACHE 10 ';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON ' ||
      'nhdplus_navigation2.tmp_navigation_status_seq TO public';
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Build subpath temp table
      --------------------------------------------------------------------------
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_NAVIGATION_SUBPATH'
      );
      
      str_sql := 'CREATE GLOBAL TEMPORARY TABLE '
              || 'nhdplus_navigation2.tmp_navigation_subpath( '
              || '    end_link_id       NUMBER NOT NULL '
              || '   ,start_percentage  NUMBER NOT NULL '
              || '   ,end_percentage    NUMBER NOT NULL '
              || '   ,end_node_id       NUMBER NOT NULL '
              || '   ,end_cost          NUMBER NOT NULL '
              || '   ,handling_flag     NUMBER '
              || ') '
              || 'ON COMMIT DELETE ROWS '
              || 'NOCACHE ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation_subpath TO public';
      EXECUTE IMMEDIATE str_sql;
      
      EXECUTE IMMEDIATE
      'CREATE UNIQUE INDEX nhdplus_navigation2.tmp_navigation_subpath_pk ' ||
      'ON nhdplus_navigation2.tmp_navigation_subpath(end_link_id) ';
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_subpath_03i ' ||
      'ON nhdplus_navigation2.tmp_navigation_subpath(end_node_id) ';
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_subpath_05i ' ||
      'ON nhdplus_navigation2.tmp_navigation_subpath(handling_flag) ';        
            
   END build_temporary_tables;

END navigator2_util;
/

