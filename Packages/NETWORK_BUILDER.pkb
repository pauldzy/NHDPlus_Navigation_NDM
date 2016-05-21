CREATE OR REPLACE PACKAGE BODY nhdplus_toponet.network_builder
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE bump_sdo_metadata(
       p_table_name     IN  VARCHAR2
      ,p_column_name    IN  VARCHAR2
      ,p_min_x          IN  NUMBER
      ,p_max_x          IN  NUMBER
      ,p_min_y          IN  NUMBER
      ,p_max_y          IN  NUMBER
      ,p_tolerance      IN  NUMBER
      ,p_srid           IN  NUMBER
   )
   AS
      num_counter     NUMBER;
      str_table_name  VARCHAR2(30 Char) := UPPER(p_table_name);
      str_column_name VARCHAR2(30 Char) := UPPER(p_column_name);
   
   BEGIN
      
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      user_sdo_geom_metadata a
      WHERE
          a.table_name = str_table_name
      AND a.column_name = str_column_name;
      
      IF num_counter = 1
      THEN
         UPDATE user_sdo_geom_metadata a
         SET
          a.diminfo = MDSYS.SDO_DIM_ARRAY(
              SDO_DIM_ELEMENT('X',p_min_x,p_max_x,p_tolerance)
             ,SDO_DIM_ELEMENT('Y',p_min_y,p_max_y,p_tolerance)
          )
         ,a.srid = p_srid
         WHERE
             a.table_name = str_table_name
         AND a.column_name = str_column_name;   
      
      ELSE
        INSERT INTO user_sdo_geom_metadata a(
            table_name
           ,column_name
           ,diminfo
           ,srid
        ) VALUES (
            str_table_name
           ,str_column_name
           ,MDSYS.SDO_DIM_ARRAY(
                SDO_DIM_ELEMENT('X',p_min_x,p_max_x,p_tolerance)
               ,SDO_DIM_ELEMENT('Y',p_min_y,p_max_y,p_tolerance)
            )
           ,p_srid
        );
      
      END IF;
      
      COMMIT;

   END bump_sdo_metadata;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE drop_spatial_indexes(
       p_network                IN  VARCHAR2
   )
   AS
      str_network VARCHAR2(15 Char) := UPPER(p_network);
      num_counter NUMBER;
      
   BEGIN
   
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      user_indexes a
      WHERE
      a.index_name = str_network || '_NODE_SPX';
      
      IF num_counter > 0
      THEN
         EXECUTE IMMEDIATE 'DROP INDEX ' || str_network || '_NODE_SPX';
         
      END IF;      
   
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      user_indexes a
      WHERE
      a.index_name = str_network || '_LINK_SPX';
      
      IF num_counter > 0
      THEN
         EXECUTE IMMEDIATE 'DROP INDEX ' || str_network || '_LINK_SPX';
         
      END IF;
         
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      user_indexes a
      WHERE
      a.index_name = str_network || '_PATH_SPX';
      
      IF num_counter > 0
      THEN
         EXECUTE IMMEDIATE 'DROP INDEX ' || str_network || '_PATH_SPX';
         
      END IF;
         
      SELECT
      COUNT(*)
      INTO num_counter
      FROM
      user_indexes a
      WHERE
      a.index_name = str_network || '_SUBP_SPX';
      
      IF num_counter > 0
      THEN
         EXECUTE IMMEDIATE 'DROP INDEX ' || str_network || '_SUBP_SPX';
         
      END IF;

   END drop_spatial_indexes;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE add_spatial_indexes(
       p_network                IN  VARCHAR2
   )
   AS
      str_network VARCHAR2(15 Char) := UPPER(p_network);
      str_sql     VARCHAR2(4000 Char);
      
   BEGIN
      
      drop_spatial_indexes(
          p_network => p_network
      );
   
      str_sql := 'CREATE INDEX ' || str_network || '_NODE_SPX '
              || 'ON ' || str_network || '_NODE$(GEOMETRY) '
              || 'INDEXTYPE IS MDSYS.SPATIAL_INDEX ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_LINK_SPX '
              || 'ON ' || str_network || '_LINK$(GEOMETRY) '
              || 'INDEXTYPE IS MDSYS.SPATIAL_INDEX ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_PATH_SPX '
              || 'ON ' || str_network || '_PATH$(GEOMETRY) '
              || 'INDEXTYPE IS MDSYS.SPATIAL_INDEX ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_SUBP_SPX '
              || 'ON ' || str_network || '_SUBP$(GEOMETRY) '
              || 'INDEXTYPE IS MDSYS.SPATIAL_INDEX ';
                 
      EXECUTE IMMEDIATE str_sql;
   
   END add_spatial_indexes;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE create_sdo_lod_network(
       p_network                IN  VARCHAR2
      ,p_no_of_hierarchy_levels IN  NUMBER
      ,p_is_directed            IN  BOOLEAN
      ,p_node_with_cost         IN  BOOLEAN
      ,p_storage_parameters     IN  VARCHAR2
      ,p_min_x                  IN  NUMBER DEFAULT -180
      ,p_max_x                  IN  NUMBER DEFAULT 180
      ,p_min_y                  IN  NUMBER DEFAULT -90
      ,p_max_y                  IN  NUMBER DEFAULT 90
      ,p_srid                   IN  NUMBER DEFAULT 8265
      ,p_tolerance              IN  NUMBER DEFAULT 0.05
      ,p_defer_spatial_indexes  IN  BOOLEAN DEFAULT TRUE
   )
   AS
      str_network  VARCHAR2(30 Char) := UPPER(p_network);
      str_sql      VARCHAR2(4000 Char);
      str_results  VARCHAR2(4000 Char);
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF MDSYS.SDO_NET.NETWORK_EXISTS(
         network => str_network
      ) = 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'Error network already exists'
         );
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Call the usual create network
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.CREATE_SDO_NETWORK(
          network                => str_network
         ,no_of_hierarchy_levels => p_no_of_hierarchy_levels
         ,is_directed            => p_is_directed
         ,node_with_cost         => p_node_with_cost
         ,is_complex             => FALSE
         ,storage_parameters     => p_storage_parameters
      );
      
      MDSYS.SDO_NET.CREATE_SUBPATH_TABLE(
          table_name             => str_network || '_SUBP$'
         ,geom_column            => 'GEOMETRY'
         ,storage_parameters     => p_storage_parameters
      );
      
      MDSYS.SDO_NET.CREATE_PARTITION_TABLE(
          table_name             => str_network || '_PART$'
      );
      
      str_sql := 'CREATE TABLE ' || str_network || '_PBLOB$( '
              || '    link_level          NUMBER '
              || '   ,partition_id        NUMBER '
              || '   ,blob                BLOB   '
              || '   ,num_inodes          NUMBER '
              || '   ,num_enodes          NUMBER '
              || '   ,num_ilinks          NUMBER '
              || '   ,num_elinks          NUMBER '
              || '   ,num_inlinks         NUMBER '
              || '   ,num_outlinks        NUMBER '
              || '   ,user_data_included  VARCHAR2(1 Char) '
              || '   ,CONSTRAINT ' || str_network || '_PBLOB$_PK '
              || '    PRIMARY KEY(link_level,partition_id) '
              || '    ENABLE VALIDATE '
              || ') ';
              
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE TABLE ' || str_network || '_COMP$( '
              || '    link_level          NUMBER '
              || '   ,node_id             NUMBER '
              || '   ,component_id        NUMBER '
              || '   ,CONSTRAINT ' || str_network || '_COMP$_PK '
              || '    PRIMARY KEY(link_level,node_id) '
              || '    ENABLE VALIDATE '
              || ') ';
              
      EXECUTE IMMEDIATE str_sql;

      --------------------------------------------------------------------------
      -- Step 30
      -- Update the network metadata
      --------------------------------------------------------------------------
      UPDATE user_sdo_network_metadata a
      SET
       subpath_table_name        = str_network || '_SUBP$'
      ,subpath_geom_column       = 'GEOMETRY'
      ,partition_table_name      = str_network || '_PART$'
      ,partition_blob_table_name = str_network || '_PBLOB$'
      ,component_table_name      = str_network || '_COMP$'
      ,node_level_table_name     = str_network || '_SUBP$'
      WHERE
      a.network = str_network;
      COMMIT;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Add indexes 
      --------------------------------------------------------------------------
      str_sql := 'CREATE BITMAP INDEX ' || str_network || '_NODE_04I '
              || 'ON ' || str_network || '_NODE$(active) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE BITMAP INDEX ' || str_network || '_NODE_05I '
              || 'ON ' || str_network || '_NODE$(partition_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_LINK_03I '
              || 'ON ' || str_network || '_LINK$(start_node_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_LINK_04I '
              || 'ON ' || str_network || '_LINK$(end_node_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE BITMAP INDEX ' || str_network || '_LINK_06I '
              || 'ON ' || str_network || '_LINK$(active) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE BITMAP INDEX ' || str_network || '_LINK_07I '
              || 'ON ' || str_network || '_LINK$(link_level) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE BITMAP INDEX ' || str_network || '_LINK_10I '
              || 'ON ' || str_network || '_LINK$(bidirected) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_PART_01I '
              || 'ON ' || str_network || '_PART$(node_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_PATH_04I '
              || 'ON ' || str_network || '_PATH$(start_node_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_PATH_05I '
              || 'ON ' || str_network || '_PATH$(end_node_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_SUBP_04I '
              || 'ON ' || str_network || '_SUBP$(reference_path_id) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_SUBP_05I '
              || 'ON ' || str_network || '_SUBP$(start_link_index) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX ' || str_network || '_SUBP_06I '
              || 'ON ' || str_network || '_SUBP$(end_link_index) ';
                 
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Update the sdo metadata
      --------------------------------------------------------------------------
      bump_sdo_metadata(
          p_table_name     => str_network || '_NODE$'
         ,p_column_name    => 'GEOMETRY'
         ,p_min_x          => p_min_x
         ,p_max_x          => p_max_x
         ,p_min_y          => p_min_y
         ,p_max_y          => p_max_y
         ,p_tolerance      => p_tolerance
         ,p_srid           => p_srid
      );
      
      bump_sdo_metadata(
          p_table_name     => str_network || '_LINK$'
         ,p_column_name    => 'GEOMETRY'
         ,p_min_x          => p_min_x
         ,p_max_x          => p_max_x
         ,p_min_y          => p_min_y
         ,p_max_y          => p_max_y
         ,p_tolerance      => p_tolerance
         ,p_srid           => p_srid
      );
      
      bump_sdo_metadata(
          p_table_name     => str_network || '_PATH$'
         ,p_column_name    => 'GEOMETRY'
         ,p_min_x          => p_min_x
         ,p_max_x          => p_max_x
         ,p_min_y          => p_min_y
         ,p_max_y          => p_max_y
         ,p_tolerance      => p_tolerance
         ,p_srid           => p_srid
      );
      
      bump_sdo_metadata(
          p_table_name     => str_network || '_SUBP$'
         ,p_column_name    => 'GEOMETRY'
         ,p_min_x          => p_min_x
         ,p_max_x          => p_max_x
         ,p_min_y          => p_min_y
         ,p_max_y          => p_max_y
         ,p_tolerance      => p_tolerance
         ,p_srid           => p_srid
      );
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Add spatial indexes if requested
      --------------------------------------------------------------------------
      IF NOT p_defer_spatial_indexes
      THEN
         add_spatial_indexes(
             p_network  => str_network
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Validate results
      --------------------------------------------------------------------------
      str_results := MDSYS.SDO_NET.VALIDATE_NETWORK(
          network    => str_network
         ,check_data => 'TRUE'
      );
      
      IF str_results <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'Network does not validate: ' || str_results
         );
         
      END IF;
      
   END create_sdo_lod_network;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE plusflowline_builder
   AS
      num_max_num_nodes NUMBER            := 5000;
      str_log_loc       VARCHAR2(30 Char) := 'LOADING_DOCK';
      str_log_file      VARCHAR2(30 Char) := 'plusflow_main.log';
      
      str_sql     VARCHAR2(4000 Char);
      str_results VARCHAR2(4000 Char);
      
   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters and defaults
      --------------------------------------------------------------------------
      IF MDSYS.SDO_NET.NETWORK_EXISTS('PLUSFLOWLINE') = 'TRUE'
      THEN
         MDSYS.SDO_NET.DROP_NETWORK(
             network => 'PLUSFLOWLINE'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Create a simple sdo network
      --------------------------------------------------------------------------
      create_sdo_lod_network(
          p_network                => 'PLUSFLOWLINE'
         ,p_no_of_hierarchy_levels => 1
         ,p_is_directed            => TRUE
         ,p_node_with_cost         => FALSE
         ,p_storage_parameters     => NULL 
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Alter the link table to add user fields for mainpath information
      --------------------------------------------------------------------------
      str_sql := 'ALTER TABLE nhdplus_toponet.plusflowline_link$ '
              || 'ADD (uphydroseq  INTEGER)';      
      EXECUTE IMMEDIATE str_sql;

      str_sql := 'ALTER TABLE nhdplus_toponet.plusflowline_link$ '
              || 'ADD (dnhydroseq  INTEGER) ';
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'ALTER TABLE nhdplus_toponet.plusflowline_link$ '
              || 'ADD (divergence  INTEGER) ';
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'ALTER TABLE nhdplus_toponet.plusflowline_link$ '
              || 'ADD (fcode  INTEGER) ';
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Add the Plusflow nodes
      --------------------------------------------------------------------------
      str_sql := 'INSERT INTO nhdplus_toponet.plusflowline_node$( '
              || '    node_id, active, geometry ) '
              || 'SELECT '
              || ' a.nodenumber '
              || ',''Y'' '
              || ',CASE '
              || ' WHEN a.tocomid = 0 '
              || ' THEN '
              || '    ( '
              || '     SELECT ' 
              || '     dz_sdo_util.downsize_2d( '
              || '        dz_sdo_util.get_end_point(bb.shape) '
              || '     ) '
              || '     FROM '
              || '     nhdplus.nhdflowline_np21 bb '
              || '     WHERE '
              || '     bb.nhdplus_comid = a.fromcomid '
              || '    ) '
              || ' ELSE '
              || '    ( '
              || '     SELECT ' 
              || '     dz_sdo_util.downsize_2d( '
              || '        dz_sdo_util.get_start_point(cc.shape) '
              || '     ) '
              || '     FROM '
              || '     nhdplus.nhdflowline_np21 cc '
              || '     WHERE '
              || '     cc.nhdplus_comid = a.tocomid '
              || '    ) '
              || ' END AS shape '
              || 'FROM ( '
              || '   SELECT '
              || '   aa.nodenumber '
              || '   ,MAX(aa.tocomid) AS tocomid '
              || '  ,MAX(aa.fromcomid) AS fromcomid '
              || '  FROM '
              || '  nhdplus.plusflow_np21 aa '
              || '  GROUP BY '
              || '  aa.nodenumber '
              || ') a '
              || 'WHERE '
              || 'a.nodenumber <> 0 ';
      
      EXECUTE IMMEDIATE str_sql;
      COMMIT;
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'PLUSFLOWLINE_NODE$'
      );
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Add the plusflowline links
      --------------------------------------------------------------------------
      str_sql := 'INSERT INTO nhdplus_toponet.plusflowline_link$( '
              || '    link_id '
              || '   ,link_name '
              || '   ,start_node_id '
              || '   ,end_node_id '
              || '   ,active '
              || '   ,link_level '
              || '   ,geometry '
              || '   ,cost '
              || '   ,bidirected '
              || '   ,uphydroseq '  
              || '   ,dnhydroseq '
              || '   ,divergence '
              || '   ,fcode '
              || ') '
              || 'SELECT '
              || ' a.hydroseq '
              || ',TO_CHAR(a.comid) '
              || ',a.fromnode '
              || ',a.tonode '
              || ',''Y'' '
              || ',1 '
              || ',dz_sdo_util.downsize_2d(b.shape) '
              || ',a.lengthkm '
              || ',''N'' '
              || ',a.uphydroseq '
              || ',a.dnhydroseq '
              || ',a.divergence '
              || ',a.fcode '
              || 'FROM '
              || 'nhdplus.plusflowlinevaa_np21 a '
              || 'JOIN '
              || 'nhdplus.nhdflowline_np21 b '
              || 'ON '
              || 'a.comid = b.nhdplus_comid ';
              
      EXECUTE IMMEDIATE str_sql;
      COMMIT;
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'PLUSFLOWLINE_LINK$'
      );
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Add indexes to user link fields
      --------------------------------------------------------------------------
      str_sql := 'CREATE INDEX nhdplus_toponet.plusflowline_link_11I '
              || 'ON nhdplus_toponet.plusflowline_link$( '
              || '   uphydroseq '
              || ')';      
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX nhdplus_toponet.plusflowline_link_12I '
              || 'ON nhdplus_toponet.plusflowline_link$( '
              || '   dnhydroseq '
              || ')';      
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE BITMAP INDEX nhdplus_toponet.plusflowline_link_13I '
              || 'ON nhdplus_toponet.plusflowline_link$( '
              || '   divergence '
              || ')';      
      EXECUTE IMMEDIATE str_sql;
      
      str_sql := 'CREATE INDEX nhdplus_toponet.plusflowline_link_14I '
              || 'ON nhdplus_toponet.plusflowline_link$( '
              || '   fcode '
              || ')';      
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Update metadata with user fields
      --------------------------------------------------------------------------
      DELETE FROM user_sdo_network_user_data
      WHERE network = 'PLUSFLOWLINE';
      
      INSERT INTO user_sdo_network_user_data(
          network
         ,table_type
         ,data_name
         ,data_type
         ,category_id
      ) VALUES (
          'PLUSFLOWLINE'
         ,'LINK'
         ,'UPHYDROSEQ'
         ,'INTEGER'
         ,0
      );
      
      INSERT INTO user_sdo_network_user_data(
          network
         ,table_type
         ,data_name
         ,data_type
         ,category_id
      ) VALUES (
          'PLUSFLOWLINE'
         ,'LINK'
         ,'DNHYDROSEQ'
         ,'INTEGER'
         ,0
      );
      
      INSERT INTO user_sdo_network_user_data(
          network
         ,table_type
         ,data_name
         ,data_type
         ,category_id
      ) VALUES (
          'PLUSFLOWLINE'
         ,'LINK'
         ,'DIVERGENCE'
         ,'INTEGER'
         ,0
      );
      
      INSERT INTO user_sdo_network_user_data(
          network
         ,table_type
         ,data_name
         ,data_type
         ,category_id
      ) VALUES (
          'PLUSFLOWLINE'
         ,'LINK'
         ,'FCODE'
         ,'INTEGER'
         ,0
      );
      
      UPDATE user_sdo_network_metadata a
      SET a.user_defined_data = 'Y'
      WHERE a.network = 'PLUSFLOWLINE';
      
      COMMIT;
       
      --------------------------------------------------------------------------
      -- Step 80
      -- Partition the network
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.SPATIAL_PARTITION(
          network                => 'PLUSFLOWLINE'
         ,partition_table_name   => 'PLUSFLOWLINE_PART$'
         ,max_num_nodes          => num_max_num_nodes
         ,log_loc                => str_log_loc
         ,log_file               => str_log_file
         ,open_mode              => 'W'
         ,link_level             => 1
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'PLUSFLOWLINE_PART$'
      );
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Generate the pblobs
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.GENERATE_PARTITION_BLOBS(
          network                   => 'PLUSFLOWLINE'
         ,link_level                => 1
         ,partition_blob_table_name => 'PLUSFLOWLINE_PBLOB$'
         ,include_user_data         => TRUE
         ,commit_for_each_blob      => TRUE
         ,log_loc                   => str_log_loc
         ,log_file                  => str_log_file
         ,open_mode                 => 'W'
         ,regenerate_node_levels    => FALSE
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'PLUSFLOWLINE_PBLOB$'
      );
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Find the connected components
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.FIND_CONNECTED_COMPONENTS(
          network                => 'PLUSFLOWLINE'
         ,link_level             => 1
         ,component_table_name   => 'PLUSFLOWLINE_COMP$'
         ,log_loc                => str_log_loc
         ,log_file               => str_log_file
         ,open_mode              => 'W'
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'PLUSFLOWLINE_COMP$'
      );
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Build the spatial indexes
      --------------------------------------------------------------------------
      add_spatial_indexes(
          p_network => 'PLUSFLOWLINE'
      );
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Grant parts to public
      --------------------------------------------------------------------------
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_comp$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_link$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_node$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_part$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_path$ TO public';

      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_pblob$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_plink$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.plusflowline_subp$ TO public';
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Validate the final results
      --------------------------------------------------------------------------
      str_results := MDSYS.SDO_NET.VALIDATE_NETWORK(
          network    => 'PLUSFLOWLINE'
         ,check_data => 'TRUE'
      );
      
      IF str_results <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'Network does not validate: ' || str_results
         );
         
      END IF;
      
   END plusflowline_builder;
   
   /*
     REVERSE FLOW NETWORK
        
      str_sql := 'INSERT INTO ' || str_network || '_NODE$( '
              || '    node_id, active, geometry ) '
              || 'SELECT '
              || ' a.nodenumber '
              || ',''Y'' '
              || ',CASE '
              || ' WHEN a.tocomid = 0 '
              || ' THEN '
              || '    ( '
              || '     SELECT '
              || '     dz_sdo_util.downsize_2d( '
              || '        dz_sdo_util.get_end_point(bb.shape) '
              || '     ) '
              || '     FROM '
              || '     nhdplus.nhdflowline_np21 bb '
              || '     WHERE '
              || '     bb.nhdplus_comid = a.fromcomid '
              || '    ) '
              || ' ELSE '
              || '    ( '
              || '     SELECT '
              || '     dz_sdo_util.downsize_2d( '
              || '        dz_sdo_util.get_start_point(cc.shape) '
              || '     ) '
              || '     FROM '
              || '     nhdplus.nhdflowline_np21 cc '
              || '     WHERE '
              || '     cc.nhdplus_comid = a.tocomid '
              || '    ) '
              || ' END AS shape '
              || 'FROM ( '
              || '   SELECT '
              || '   aa.nodenumber '
              || '   ,MAX(aa.tocomid) AS tocomid '
              || '  ,MAX(aa.fromcomid) AS fromcomid '
              || '  FROM '
              || '  nhdplus.plusflow_np21 aa '
              || '  GROUP BY '
              || '  aa.nodenumber '
              || ') a '
              || 'WHERE '
              || 'a.nodenumber <> 0 ';

      str_sql := 'INSERT INTO ' || str_network || '_LINK$( '
              || '    link_id '
              || '   ,link_name '
              || '   ,start_node_id '
              || '   ,end_node_id '
              || '   ,active '
              || '   ,link_level '
              || '   ,geometry '
              || '   ,cost '
              || '   ,bidirected '
              || '   ,uphydroseq '  
              || '   ,dnhydroseq '
              || '   ,divergence '
              || ') '
              || 'SELECT '
              || ' a.hydroseq '
              || ',TO_CHAR(a.comid) '
              || ',a.tonode '
              || ',a.fromnode '
              || ',''Y'' '
              || ',1 '
              || ',MDSYS.SDO_UTIL.REVERSE_LINESTRING(dz_sdo_util.downsize_2d(b.shape)) '
              || ',a.lengthkm '
              || ',''N'' '
              || ',a.uphydroseq '
              || ',a.dnhydroseq '
              || ',a.divergence '
              || 'FROM '
              || 'nhdplus.plusflowlinevaa_np21 a '
              || 'JOIN '
              || 'nhdplus.nhdflowline_np21 b '
              || 'ON '
              || 'a.comid = b.nhdplus_comid ';
      
      */
      
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE bareflowline_builder
   AS
      num_max_num_nodes NUMBER            := 5000;
      str_log_loc       VARCHAR2(30 Char) := 'LOADING_DOCK';
      str_log_file      VARCHAR2(30 Char) := 'bareflow_main.log';
      
      str_sql     VARCHAR2(4000 Char);
      str_results VARCHAR2(4000 Char);
      
      TYPE rec_nhdlink IS RECORD(
          permanent_identifier VARCHAR2(40 Char)
         ,nhdplus_comid        INTEGER
         ,fcode                INTEGER
         ,lengthkm             NUMBER
         ,shape                MDSYS.SDO_GEOMETRY 
      );
      TYPE tbl_nhdlink IS TABLE OF rec_nhdlink;
      ary_nhdlink         tbl_nhdlink;
      sdo_start           MDSYS.SDO_GEOMETRY;
      sdo_end             MDSYS.SDO_GEOMETRY;
      int_start_node_id   NUMBER;
      int_end_node_id     NUMBER;
      int_running_node_id NUMBER;
      
      CURSOR curs_nhdlink IS
      SELECT
       a.permanent_identifier
      ,a.nhdplus_comid 
      ,a.fcode 
      ,a.lengthkm
      ,dz_sdo_util.downsize_2d(a.shape) AS shape
      FROM
      nhdplus.nhdflowline_np21 a;
      
      FUNCTION get_node_id(
         p_input MDSYS.SDO_GEOMETRY
      ) RETURN NUMBER
      AS
         str_sql     VARCHAR2(4000 Char);
         int_node_id NUMBER;
         
      BEGIN
      
         str_sql := 'SELECT '
                 || 'a.node_id '
                 || 'FROM '
                 || 'nhdplus_toponet.bareflowline_node$ a '
                 || 'WHERE '
                 || 'MDSYS.SDO_EQUAL(a.geometry,:p01) = ''TRUE'' ';
          
         EXECUTE IMMEDIATE str_sql 
         INTO int_node_id 
         USING p_input;
         
         RETURN int_node_id;
         
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RETURN NULL;
            
         WHEN OTHERS
         THEN
            RAISE;
      
      END get_node_id;
      
      PROCEDURE insert_node(
          p_node_id NUMBER
         ,p_input   MDSYS.SDO_GEOMETRY
      )
      AS
         str_sql     VARCHAR2(4000 Char);
         
      BEGIN
      
         str_sql := 'INSERT INTO nhdplus_toponet.bareflowline_node$( '
                 || '    node_id '
                 || '   ,active '
                 || '   ,geometry '
                 || ') VALUES ( '
                 || '    :p01 '
                 || '   ,''Y'' '
                 || '   ,:p02 '
                 || ') ';
                 
         EXECUTE IMMEDIATE str_sql
         USING 
          p_node_id
         ,p_input;
      
      END insert_node;         
      
   BEGIN
/*
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters and defaults
      --------------------------------------------------------------------------
      IF MDSYS.SDO_NET.NETWORK_EXISTS('BAREFLOWLINE') = 'TRUE'
      THEN
         MDSYS.SDO_NET.DROP_NETWORK(
             network => 'BAREFLOWLINE'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Create a simple sdo network
      --------------------------------------------------------------------------
      create_sdo_lod_network(
          p_network                => 'BAREFLOWLINE'
         ,p_no_of_hierarchy_levels => 1
         ,p_is_directed            => TRUE
         ,p_node_with_cost         => FALSE
         ,p_storage_parameters     => NULL 
      );
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Alter the link table to add user fields for mainpath information
      --------------------------------------------------------------------------
      str_sql := 'ALTER TABLE nhdplus_toponet.bareflowline_link$ '
              || 'ADD (fcode  INTEGER) ';
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Add spatial index to nodes
      --------------------------------------------------------------------------
      str_sql := 'CREATE INDEX nhdplus_toponet.bareflowline_node_spx '
              || 'ON nhdplus_toponet.bareflowline_node$(geometry) '
              || 'INDEXTYPE IS MDSYS.SPATIAL_INDEX ';   
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Loop through each flowline
      --------------------------------------------------------------------------
      int_running_node_id := 1;
      
      str_sql := 'INSERT INTO nhdplus_toponet.bareflowline_link$( '
              || '    link_id '
              || '   ,link_name '
              || '   ,start_node_id '
              || '   ,end_node_id '
              || '   ,active '
              || '   ,link_level '
              || '   ,geometry '
              || '   ,cost '
              || '   ,bidirected '
              || '   ,fcode '
              || ') VALUES ( '
              || '    :p01 '
              || '   ,:p02 '
              || '   ,:p03 '
              || '   ,:p04 '
              || '   ,''Y'' '
              || '   ,1 '
              || '   ,:p05 '
              || '   ,:p06 '
              || '   ,''Y'' '
              || '   ,:p07 '
              || ') ';

      OPEN curs_nhdlink;
      
      LOOP
         FETCH curs_nhdlink
         BULK COLLECT INTO ary_nhdlink
         LIMIT 100;
         
         EXIT WHEN ary_nhdlink.COUNT = 0;

         FOR i IN 1 .. ary_nhdlink.COUNT
         LOOP
            sdo_start         := dz_sdo_util.get_start_point(ary_nhdlink(i).shape); 
            int_start_node_id := get_node_id(sdo_start);

            IF int_start_node_id IS NULL
            THEN
               int_start_node_id := int_running_node_id;
               int_running_node_id := int_running_node_id + 1;
               insert_node(
                   p_node_id => int_start_node_id
                  ,p_input   => sdo_start
               );
               
            END IF;
            
            sdo_end         := dz_sdo_util.get_end_point(ary_nhdlink(i).shape);
            int_end_node_id := get_node_id(sdo_end);
            
            IF int_end_node_id IS NULL
            THEN
               int_end_node_id := int_running_node_id;
               int_running_node_id := int_running_node_id + 1;
               insert_node(
                   p_node_id => int_end_node_id
                  ,p_input   => sdo_end
               );
               
            END IF;
            
            EXECUTE IMMEDIATE str_sql
            USING
             ary_nhdlink(i).nhdplus_comid
            ,ary_nhdlink(i).permanent_identifier
            ,int_start_node_id
            ,int_end_node_id
            ,ary_nhdlink(i).shape
            ,ary_nhdlink(i).lengthkm
            ,ary_nhdlink(i).fcode;

         END LOOP;
         
         COMMIT;
         
      END LOOP;
 */
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'BAREFLOWLINE_NODE$'
      );

      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'BAREFLOWLINE_LINK$'
      );
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Drop the node spatial index
      --------------------------------------------------------------------------
      str_sql := 'DROP INDEX nhdplus_toponet.bareflowline_node_spx '; 
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Add indexes to user link fields
      --------------------------------------------------------------------------
      str_sql := 'CREATE INDEX nhdplus_toponet.bareflowline_link_14I '
              || 'ON nhdplus_toponet.bareflowline_link$( '
              || '   fcode '
              || ')';      
      EXECUTE IMMEDIATE str_sql;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Update metadata with user fields
      --------------------------------------------------------------------------
      DELETE FROM user_sdo_network_user_data
      WHERE network = 'BAREFLOWLINE';
      
      INSERT INTO user_sdo_network_user_data(
          network
         ,table_type
         ,data_name
         ,data_type
         ,category_id
      ) VALUES (
          'BAREFLOWLINE'
         ,'LINK'
         ,'FCODE'
         ,'INTEGER'
         ,0
      );
      
      UPDATE user_sdo_network_metadata a
      SET a.user_defined_data = 'Y'
      WHERE a.network = 'BAREFLOWLINE';
      
      COMMIT;
       
      --------------------------------------------------------------------------
      -- Step 90
      -- Partition the network
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.SPATIAL_PARTITION(
          network                => 'BAREFLOWLINE'
         ,partition_table_name   => 'BAREFLOWLINE_PART$'
         ,max_num_nodes          => num_max_num_nodes
         ,log_loc                => str_log_loc
         ,log_file               => str_log_file
         ,open_mode              => 'W'
         ,link_level             => 1
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'BAREFLOWLINE_PART$'
      );
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Generate the pblobs
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.GENERATE_PARTITION_BLOBS(
          network                   => 'BAREFLOWLINE'
         ,link_level                => 1
         ,partition_blob_table_name => 'BAREFLOWLINE_PBLOB$'
         ,include_user_data         => TRUE
         ,commit_for_each_blob      => TRUE
         ,log_loc                   => str_log_loc
         ,log_file                  => str_log_file
         ,open_mode                 => 'W'
         ,regenerate_node_levels    => FALSE
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'BAREFLOWLINE_PBLOB$'
      );
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Find the connected components
      --------------------------------------------------------------------------
      MDSYS.SDO_NET.FIND_CONNECTED_COMPONENTS(
          network                => 'BAREFLOWLINE'
         ,link_level             => 1
         ,component_table_name   => 'BAREFLOWLINE_COMP$'
         ,log_loc                => str_log_loc
         ,log_file               => str_log_file
         ,open_mode              => 'W'
      );
      
      DBMS_STATS.gather_table_stats(
          'NHDPLUS_TOPONET'
         ,'BAREFLOWLINE_COMP$'
      );
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Build the spatial indexes
      --------------------------------------------------------------------------
      add_spatial_indexes(
          p_network => 'BAREFLOWLINE'
      );
      
      --------------------------------------------------------------------------
      -- Step 130
      -- Grant parts to public
      --------------------------------------------------------------------------
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_comp$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_link$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_node$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_part$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_path$ TO public';

      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_pblob$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_plink$ TO public';
      
      EXECUTE IMMEDIATE
      'GRANT SELECT ON nhdplus_toponet.bareflowline_subp$ TO public';
      
      --------------------------------------------------------------------------
      -- Step 140
      -- Validate the final results
      --------------------------------------------------------------------------
      str_results := MDSYS.SDO_NET.VALIDATE_NETWORK(
          network    => 'BAREFLOWLINE'
         ,check_data => 'TRUE'
      );
      
      IF str_results <> 'TRUE'
      THEN
         RAISE_APPLICATION_ERROR(
             -20001
            ,'Network does not validate: ' || str_results
         );
         
      END IF;
      
   END bareflowline_builder;

END network_builder;
/

