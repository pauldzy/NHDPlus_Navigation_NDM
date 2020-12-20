CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.navigator2_util
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_preprocessed_tables
   AS
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
      -- Build navigation reference table
      --------------------------------------------------------------------------
      BEGIN
         EXECUTE IMMEDIATE 
         'DROP TABLE nhdplus_navigation2.plusflowlinevaa_nav';
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL; 
      END;
      
      EXECUTE IMMEDIATE 
         'CREATE TABLE '
      || 'nhdplus_navigation2.plusflowlinevaa_nav( '
      || '    nhdplusid              NUMBER(19) '
      || '   ,hydrosequence          NUMBER(19) '
      || '   ,levelpathid            NUMBER(19) '
      || '   ,divergence             INTEGER '
      || '   ,fmeasure               NUMBER '
      || '   ,tmeasure               NUMBER '
      || '   ,lengthkm               NUMBER '
      || '   ,totma                  NUMBER '
      || '   ,pathlength             NUMBER '
      || '   ,pathtimema             NUMBER '
      || '   ,uphydrosequence        NUMBER(19) '
      || '   ,downhydrosequence      NUMBER(19) '
      || '   ,downminorhydrosequence NUMBER(19) '
      || '   ,terminalpathid         NUMBER(19) '
      || '   ,fromnode               NUMBER(19) '
      || '   ,tonode                 NUMBER(19) '
      || '   ,force_main_line        INTEGER '
      || '   ,headwater              VARCHAR2(1 Char) '
      || '   ,coastal_connection     VARCHAR2(1 Char) '
      || '   ,network_end            VARCHAR2(1 Char) '
      || '   ,CONSTRAINT plusflowlinevaa_nav_pk '
      || '    PRIMARY KEY (hydrosequence) '
      || ') '
      || 'TABLESPACE ow_ephemeral_orcwater ';
      
      EXECUTE IMMEDIATE
         'GRANT SELECT ON '
      || 'nhdplus_navigation2.plusflowlinevaa_nav TO public';
      
      EXECUTE IMMEDIATE
         'CREATE UNIQUE INDEX nhdplus_navigation2.plusflowlinevaa_nav_u01 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(nhdplusid) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE INDEX nhdplus_navigation2.plusflowlinevaa_nav_i01 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(levelpathid) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE BITMAP INDEX nhdplus_navigation2.plusflowlinevaa_nav_i02 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(divergence) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE INDEX nhdplus_navigation2.plusflowlinevaa_nav_i03 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(uphydrosequence) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE INDEX nhdplus_navigation2.plusflowlinevaa_nav_i04 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(downhydrosequence) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE INDEX nhdplus_navigation2.plusflowlinevaa_nav_i05 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(downminorhydrosequence) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'CREATE INDEX nhdplus_navigation2.plusflowlinevaa_nav_i06 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(terminalpathid) '
      || 'TABLESPACE ow_ephemeral_orcwater ';
      
      EXECUTE IMMEDIATE
         'CREATE BITMAP INDEX nhdplus_navigation2.plusflowlinevaa_nav_i07 '
      || 'ON nhdplus_navigation2.plusflowlinevaa_nav(force_main_line) '
      || 'TABLESPACE ow_ephemeral_orcwater ';

      EXECUTE IMMEDIATE
         'INSERT INTO nhdplus_navigation2.plusflowlinevaa_nav( '
      || '    nhdplusid '
      || '   ,hydrosequence '
      || '   ,levelpathid '
      || '   ,divergence '
      || '   ,fmeasure '
      || '   ,tmeasure '
      || '   ,lengthkm '
      || '   ,totma '
      || '   ,pathlength '
      || '   ,pathtimema '
      || '   ,uphydrosequence '
      || '   ,downhydrosequence '
      || '   ,downminorhydrosequence '
      || '   ,terminalpathid '
      || '   ,fromnode '
      || '   ,tonode '
      || '   ,force_main_line '
      || '   ,headwater '
      || '   ,coastal_connection '
      || '   ,network_end '
      || ') '
      || 'SELECT '
      || ' a.comid AS nhdplusid'
      || ',a.hydroseq '
      || ',a.levelpathid '
      || ',a.divergence '
      || ',a.fmeasure '
      || ',a.tmeasure '
      || ',a.lengthkm '
      || ',a.totma '
      || ',a.pathlength '
      || ',a.pathtimema '
      || ',CASE '
      || ' WHEN a.uphydroseq = 0 '
      || ' THEN '
      || '   NULL '
      || ' ELSE '
      || '   a.uphydroseq '
      || ' END AS uphydrosequence '
      || ',CASE '
      || ' WHEN a.dnhydroseq = 0 '
      || ' THEN '
      || '   NULL '
      || ' ELSE '
      || '   a.dnhydroseq '
      || ' END AS dnhydrosequence '
      || ',CASE '
      || ' WHEN a.dnminorhyd = 0 '
      || ' THEN '
      || '   NULL '
      || ' ELSE '
      || '   a.dnminorhyd '
      || ' END AS dnminorhyd '
      || ',a.terminalpathid '
      || ',a.fromnode '
      || ',a.tonode '
      || ',CASE '
      || ' WHEN a.hydroseq IN ( '
      || '   /* Big Tributaries */ '
      || '    350009839  /* Arkansas */ '
      || '   ,550002171  /* Big Blue */ '
      || '   ,590012528  /* Big Sioux */ '
      || '   ,590004188  /* Bighorn */ '
      || '   ,350003361  /* Black */ '
      || '   ,390006004  /* Black (2) */ '
      || '   ,390000311  /* Canadian */ '
      || '   ,510002921  /* Cedar */ '
      || '   ,590007834  /* Cheyenne */ '
      || '   ,590010733  /* Cheyenne (2) */ '
      || '   ,390001215  /* Cimarron */ '
      || '   ,50004179   /* Clearwater */ '
      || '   ,430000065  /* Cumberland */ '
      || '   ,510002338  /* Des Moines */ '
      || '   ,10002380   /* Feather */ '
      || '   ,720000771  /* Gila */ '
      || '   ,760000231  /* Green */ '
      || '   ,430001637  /* Green */ '
      || '   ,510000257  /* Illinois */ '
      || '   ,510002770  /* Iowa */ '
      || '   ,590008899  /* James */ '
      || '   ,430000838  /* Kanawha */ '
      || '   ,550001526  /* Kansas */ '
      || '   ,430002658  /* Kentucky */ '
      || '   ,720001913  /* Little Colorado */ '
      || '   ,590006912  /* Little Missouri */ '
      || '   ,550008373  /* Loup */ '
      || '   ,590012003  /* Milk */ '
      || '   ,510003597  /* Minnesota */ '
      || '   ,350003335  /* Mississippi from Atchafalaya */ '
      || '   ,550000017  /* Missouri */ '
      || '   ,720001632  /* Muddy */ '
      || '   ,430002416  /* Muskingum */ '
      || '   ,390004971  /* Neosho */ '
      || '   ,590001226  /* Niobrara */ '
      || '   ,550003947  /* North Platte */ '
      || '   ,350003411  /* Ouachita */ '
      || '   ,430000004  /* Ohio */ '
      || '   ,550009800  /* Osage */ '
      || '   ,50003837   /* Owyhee */ '
      || '   ,680001003  /* Pecos */ '
      || '   ,550000622  /* Platte */ '
      || '   ,590006969  /* Powder */ '
      || '   ,550005927  /* Republican */ '
      || '   ,510001488  /* Rock */ '
      || '   ,50002910   /* Salmon */ '
      || '   ,720001660  /* Salt */ '
      || '   ,760000974  /* San Juan */ '
      || '   ,430002448  /* Scioto */ '
      || '   ,840000351  /* Sheyenne */ '
      || '   ,50001581   /* Snake */ '
      || '   ,550010594  /* Solomon */ '
      || '   ,510003688  /* St. Croix */ '
      || '   ,350005173  /* St. Francis */ '
      || '   ,470000012  /* Tennessee */ '
      || '   ,430001211  /* Wabash */ '
      || '   ,350003903  /* White */ '
      || '   ,590011506  /* White (2) */ '
      || '   ,50004305   /* Willamette */ '
      || '   ,510002581  /* Wisconsin */ '
      || '   ,350005918  /* Yazoo */ '
      || '   ,590001280  /* Yellowstone */ '
      || '   /* Born on the Port Allen Bayou */ '
      || '   ,350002673 '
      || '   ,350002676 '
      || '   ,350002718 '
      || '   ,350002733 '
      || '   ,350002775 '
      || '   ,350002785 '
      || '   ,350002783 '
      || '   ,350002835 '
      || '   ,350002844 '
      || '   ,350002873 '
      || '   ,350002878 '
      || '   ,350002894 '
      || '   ,350002915 '
      || '   ,350002946 '
      || '   ,350002973 '
      || '   ,350003025 '
      || '   ,350003055 '
      || '   ,350003153 '
      || '   ,350003177 '
      || '   ,350003182 '
      || '   ,350003196 '
      || '   ,350003274 '
      || '   ,350037594 '
      || '   ,350045866 '
      || '   ,350083155 '
      || '   /* Kaskaskia Old Course */ '
      || '   ,510000109 '
      || '   ,510000101 '
      || '   ,510000102 '
      || '   ,510000111 '
      || '   /* Other minor networks receiving big water */ '
      || '   ,510000080 '
      || '   ,510000089 '
      || '   ,510000143 '
      || '   ,550002456 '
      || '   ,550003310 '
      || ' ) '
      || ' THEN '
      || '   1 /* TRUE */ '
      || ' ELSE '
      || '   0 /* FALSE */ '
      || ' END AS force_main_line '
      || ',CASE '
      || ' WHEN a.startflag = 1 '
      || ' THEN '
      || '   CAST(''Y'' AS VARCHAR2(1 Char)) '
      || ' ELSE '
      || '   CAST(''N'' AS VARCHAR2(1 Char)) '
      || ' END AS headwater '
      || ',CASE '
      || ' WHEN EXISTS (SELECT 1 FROM nhdplus.plusflow_np21 d WHERE d.fromhydroseq = a.hydroseq AND d.direction = 714) '
      || ' THEN '
      || '   CAST(''Y'' AS VARCHAR2(1 Char)) '
      || ' ELSE '
      || '   CAST(''N'' AS VARCHAR2(1 Char)) '
      || ' END AS coastal_connection '
      || ',CASE '
      || ' WHEN EXISTS (SELECT 1 FROM nhdplus.plusflow_np21 d WHERE d.fromhydroseq = a.hydroseq AND d.direction = 713) '
      || ' THEN '
      || '   CAST(''Y'' AS VARCHAR2(1 Char)) '
      || ' ELSE '
      || '   CAST(''N'' AS VARCHAR2(1 Char)) '
      || ' END AS network_end '
      || 'FROM '
      || 'nhdplus.plusflowlinevaa_np21 a '
      || 'WHERE '
      || '    a.pathlength <> -9999 '
      || 'AND a.fcode <> 56600 ';
      
      COMMIT;
      
      SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName   => 'NHDPLUS_NAVIGATION2'
         ,TabName   => 'PLUSFLOWLINEVAA_NAV'
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Build up cross reference materialized view table
      --------------------------------------------------------------------------
      BEGIN
         EXECUTE IMMEDIATE 
         'DROP TABLE nhdplus_navigation2.plusflowlinevaa_up'; 
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
  
      EXECUTE IMMEDIATE
         'CREATE TABLE nhdplus_navigation2.plusflowlinevaa_up( '
      || '    hydrosequence          NUMBER(19) '
      || '   ,upstream_hydrosequence NUMBER(19) '
      || '   ,CONSTRAINT plusflowlinevaa_up_pk '
      || '    PRIMARY KEY (hydrosequence,upstream_hydrosequence) '
      || ') '
      || 'ORGANIZATION INDEX '
      || 'TABLESPACE ow_ephemeral_orcwater ';
      
      EXECUTE IMMEDIATE
         'GRANT SELECT ON '
      || 'nhdplus_navigation2.plusflowlinevaa_up TO public';
      
      EXECUTE IMMEDIATE 
         'INSERT INTO nhdplus_navigation2.plusflowlinevaa_up( '
      || '    hydrosequence '
      || '   ,upstream_hydrosequence '
      || ') '
      || 'SELECT '
      || ' a.hydroseq     AS hydrosequence'
      || ',b.fromhydroseq AS upstream_hydrosequence '
      || 'FROM '
      || 'nhdplus.plusflowlinevaa_np21 a '
      || 'JOIN '
      || 'nhdplus.plusflow_np21 b '
      || 'ON '
      || 'a.hydroseq = b.tohydroseq '
      || 'WHERE '
      || '    a.pathlength <> -9999 '
      || 'AND a.fcode <> 56600 '
      || 'AND b.fromhydroseq <> 0 ';
      
      COMMIT;
      
      SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName   => 'NHDPLUS_NAVIGATION2'
         ,TabName   => 'PLUSFLOWLINEVAA_UP'
      );
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Build down cross reference materialized view table
      --------------------------------------------------------------------------
      BEGIN
         EXECUTE IMMEDIATE 
         'DROP TABLE nhdplus_navigation2.plusflowlinevaa_dn'; 
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;
      
      EXECUTE IMMEDIATE
         'CREATE TABLE nhdplus_navigation2.plusflowlinevaa_dn( '
      || '    hydrosequence            NUMBER(19) '
      || '   ,downstream_hydrosequence NUMBER(19) '
      || '   ,CONSTRAINT plusflowlinevaa_dn_pk '
      || '    PRIMARY KEY (hydrosequence,downstream_hydrosequence) '
      || ') '
      || 'ORGANIZATION INDEX '
      || 'TABLESPACE ow_ephemeral_orcwater ';
      
      EXECUTE IMMEDIATE
         'GRANT SELECT ON '
      || 'nhdplus_navigation2.plusflowlinevaa_dn TO public';
  
      EXECUTE IMMEDIATE 
         'INSERT INTO nhdplus_navigation2.plusflowlinevaa_dn( '
      || '    hydrosequence '
      || '   ,downstream_hydrosequence '
      || ') '
      || 'SELECT '
      || ' a.hydroseq   AS hydrosequence'
      || ',b.tohydroseq AS downstream_hydrosequence '
      || 'FROM '
      || 'nhdplus.plusflowlinevaa_np21 a '
      || 'JOIN '
      || 'nhdplus.plusflow_np21 b '
      || 'ON '
      || 'a.hydroseq  = b.fromhydroseq '
      || 'WHERE '
      || '    a.pathlength <> -9999 '
      || 'AND a.fcode <> 56600 '
      || 'AND b.tohydroseq <> 0 '
      || 'ORDER BY a.hydroseq ';
      
      COMMIT;
      
      SYS.DBMS_STATS.GATHER_TABLE_STATS (
          OwnName   => 'NHDPLUS_NAVIGATION2'
         ,TabName   => 'PLUSFLOWLINEVAA_DN'
      );

   END build_preprocessed_tables;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_search_tbl
   AS
      str_sql VARCHAR2(4000 Char);
      
   BEGIN
   
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_NAVIGATION2_SEARCH'
      );

      str_sql := 'CREATE GLOBAL TEMPORARY TABLE '
              || 'nhdplus_navigation2.tmp_navigation2_search( '
              || '    nhdplusid                   NUMBER(19) NOT NULL '
              || '   ,reachcode                   VARCHAR2(14 Char) NOT NULL '
              || '   ,fmeasure                    NUMBER NOT NULL '
              || '   ,tmeasure                    NUMBER NOT NULL '
              || '   ,lengthkm                    NUMBER '
              || '   ,flowtimeday                 NUMBER '
              || '   ,xwalk_huc12                 VARCHAR2(12 Char) '
              || '   ,catchment_nhdplusid         NUMBER(19) '
              || '   ,navtermination_flag         INTEGER '
              || '   ,network_distancekm          NUMBER '
              || '   ,network_flowtimeday         NUMBER '
              || '   ,CONSTRAINT tmp_navigation2_connectionsu01 UNIQUE '
              || '       (nhdplusid) '
              || ') '
              || 'ON COMMIT PRESERVE ROWS ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation2_search TO public';
      EXECUTE IMMEDIATE str_sql;
      
      dz_dict_main.fast_index(
          p_owner       => 'NHDPLUS_NAVIGATION2'
         ,p_table_name  => 'TMP_NAVIGATION2_SEARCH'
         ,p_column_name => 'REACHCODE'
      );
      
      dz_dict_main.fast_index(
          p_owner       => 'NHDPLUS_NAVIGATION2'
         ,p_table_name  => 'TMP_NAVIGATION2_SEARCH'
         ,p_column_name => 'XWALK_HUC12'
      );
      
      dz_dict_main.fast_index(
          p_owner       => 'NHDPLUS_NAVIGATION2'
         ,p_table_name  => 'TMP_NAVIGATION2_SEARCH'
         ,p_column_name => 'CATCHMENT_NHDPLUSID'
      );
      
   END build_temporary_search_tbl;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE build_temporary_catchment_tbl
   AS
      str_sql VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Build catchments permanent table
      --------------------------------------------------------------------------
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_CATCHMENTS'
      );

      str_sql := 'CREATE TABLE '
              || 'nhdplus_navigation2.tmp_catchments( '
              || '    objectid                    INTEGER NOT NULL '
              || '   ,session_id                  VARCHAR2(40 Char) NOT NULL '
              || '   ,cat_joinkey                 VARCHAR2(40 Char) NOT NULL '
              || '   ,catchmentstatecode          VARCHAR2(2 Char)  NOT NULL '
              || '   ,nhdplusid                   NUMBER(19)        NOT NULL '
              || '   ,xwalk_huc12                 VARCHAR2(12 Char) '
              || '   ,areasqkm                    NUMBER            NOT NULL '
              || '   ,headwater                   VARCHAR2(1 Char)  NOT NULL '
              || '   ,coastal                     VARCHAR2(1 Char)  NOT NULL '
              || '   ,network_distancekm          NUMBER '
              || '   ,network_flowtimeday         NUMBER '
              || '   ,hydrosequence               NUMBER(19) '
              || '   ,globalid                    VARCHAR2(40 Char) NOT NULL '
              || '   ,shape                       MDSYS.SDO_GEOMETRY '
              || '   ,se_anno_cad_data            BLOB '
              || '   ,CONSTRAINT tmp_catchments_pk PRIMARY KEY ( '
              || '        session_id '
              || '       ,cat_joinkey '
              || '     ) '
              || ') '
              || 'TABLESPACE ow_ephemeral_orcwater '
              || 'NOLOGGING ';
              
      EXECUTE IMMEDIATE str_sql;
      
      EXECUTE IMMEDIATE
      'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
      'nhdplus_navigation2.tmp_catchments TO public ';
      
      EXECUTE IMMEDIATE
      'CREATE UNIQUE INDEX nhdplus_navigation2.tmp_catchments_u01 ' ||
      'ON nhdplus_navigation2.tmp_catchments(objectid) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_catchments_01i ' ||
      'ON nhdplus_navigation2.tmp_catchments(session_id,nhdplusid) ' ||
      'TABLESPACE ow_ephemeral_orcwater ' ||
      'NOLOGGING ';
      
      dz_dict_main.drop_sequence(
          p_owner         => 'NHDPLUS_NAVIGATION2'
         ,p_sequence_name => 'TMP_CATCHMENTS_SEQ'
      );
      
      EXECUTE IMMEDIATE
      'CREATE SEQUENCE nhdplus_navigation2.tmp_catchments_seq ' ||
      'MAXVALUE 99999999999999999999 ' ||
      'START WITH 1 CYCLE ';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON ' ||
      'nhdplus_navigation2.tmp_catchments_seq TO public';
      
   END build_temporary_catchment_tbl;
   
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
              || '   ,nhdplusid                   NUMBER(19) NOT NULL '
              || '   ,reachcode                   VARCHAR2(14 Char) NOT NULL '
              || '   ,fmeasure                    NUMBER NOT NULL '
              || '   ,tmeasure                    NUMBER NOT NULL '
              || '   ,hydrosequence               NUMBER(19) NOT NULL '
              || '   ,levelpathid                 NUMBER(19) NOT NULL '
              || '   ,terminalpathid              NUMBER(19) NOT NULL '
              || '   ,uphydrosequence             NUMBER(19) '
              || '   ,downhydrosequence           NUMBER(19) '
              -----
              || '   ,lengthkm                    NUMBER '
              || '   ,network_distancekm          NUMBER '
              || '   ,flowtimeday                 NUMBER '
              || '   ,network_flowtimeday         NUMBER '
              -----
              || '   ,vpuid                       VARCHAR2(3 Char) '
              || '   ,vpuversion                  VARCHAR2(8 Char) '
              || '   ,reachsmdate                 DATE '
              || '   ,ftype                       NUMBER(3) '
              || '   ,fcode                       NUMBER(5) '
              || '   ,gnis_id                     VARCHAR2(10 Char) '
              || '   ,gnis_name                   VARCHAR2(65 Char) '
              || '   ,wbarea_permanent_identifier VARCHAR2(40 Char) '
              || '   ,wbarea_nhdplusid            NUMBER(19) '
              || '   ,xwalk_huc12                 VARCHAR2(12 Char) '
              || '   ,catchment_nhdplusid         NUMBER(19) '
              || '   ,quality_marker              INTEGER '
              || '   ,navigable                   VARCHAR2(1 Char) '
              || '   ,coastal                     VARCHAR2(1 Char) '
              || '   ,innetwork                   VARCHAR2(1 Char) '
              || '   ,navtermination_flag         INTEGER '
              || '   ,nav_order                   INTEGER '
              || '   ,shape                       MDSYS.SDO_GEOMETRY '
              || '   ,se_anno_cad_data            BLOB '
              || '   ,CONSTRAINT tmp_navigation_results_pk PRIMARY KEY '
              || '       (session_id,permanent_identifier) '
              || '       USING INDEX TABLESPACE ow_ephemeral_orcwater '
              || '   ,CONSTRAINT tmp_navigation_results_u02 UNIQUE '
              || '       (session_id,nhdplusid) '
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
      'ON nhdplus_navigation2.tmp_navigation_results(session_id,hydrosequence) ' ||
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
         ,p_table_name => 'TMP_NAVIGATION_LINKS'
      );
      
      str_sql := 'CREATE GLOBAL TEMPORARY TABLE '
              || 'nhdplus_navigation2.tmp_navigation_links( '
              || '    link_index                   INTEGER NOT NULL '
              || '   ,link_id                      NUMBER(19) NOT NULL '
              || '   ,link_start_clip_percentage   NUMBER '
              || '   ,link_end_clip_percentage     NUMBER '
              || '   ,link_lengthkm                NUMBER '
              || '   ,link_flowtimeday             NUMBER '
              || '   ,link_network_distancekm      NUMBER '
              || '   ,link_network_flowtimeday     NUMBER '
              || '   ,link_navtermination_flag     INTEGER '
              || '   ,end_node_id                  NUMBER(19) '
              || '   ,end_node_cost                NUMBER '
              || '   ,end_node_network_distancekm  NUMBER '
              || '   ,end_node_network_flowtimeday NUMBER '
              || '   ,original_lengthkm            NUMBER '
              || '   ,original_flowtimeday         NUMBER '
              || ') '
              || 'ON COMMIT DELETE ROWS '
              || 'NOCACHE ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation_links TO public';
      EXECUTE IMMEDIATE str_sql;
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_links_pk ' ||
      'ON nhdplus_navigation2.tmp_navigation_links(link_id) ';
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_links_01i ' ||
      'ON nhdplus_navigation2.tmp_navigation_links(end_node_id) ';
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Build subpath temp table
      --------------------------------------------------------------------------
      dz_dict_main.drop_table_quietly(
          p_owner      => 'NHDPLUS_NAVIGATION2'
         ,p_table_name => 'TMP_NAVIGATION_NODES'
      );
      
      str_sql := 'CREATE GLOBAL TEMPORARY TABLE '
              || 'nhdplus_navigation2.tmp_navigation_nodes( '
              || '    node_index              INTEGER NOT NULL '
              || '   ,node_id                 NUMBER(19) NOT NULL '
              || '   ,end_cost                NUMBER '
              || '   ,end_network_distancekm  NUMBER '
              || '   ,end_network_flowtimeday NUMBER '
              || ') '
              || 'ON COMMIT DELETE ROWS '
              || 'NOCACHE ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'GRANT SELECT,INSERT,UPDATE,DELETE ON ' ||
         'nhdplus_navigation2.tmp_navigation_nodes TO public';
      EXECUTE IMMEDIATE str_sql;
      
      EXECUTE IMMEDIATE
      'CREATE INDEX nhdplus_navigation2.tmp_navigation_nodes_pk ' ||
      'ON nhdplus_navigation2.tmp_navigation_nodes(node_id) ';

      --------------------------------------------------------------------------
      -- Step 60
      -- Build search temp table
      --------------------------------------------------------------------------
      build_temporary_search_tbl();
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Build catchments permanent table
      --------------------------------------------------------------------------
      build_temporary_catchment_tbl();
            
   END build_temporary_tables;

END navigator2_util;
/

