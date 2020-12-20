CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.navigator2_main
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION run_fix(
       pNetworkName              IN  VARCHAR2
      ,pDirection                IN  VARCHAR2
   ) RETURN NUMBER
   AS
      str_sql      VARCHAR2(32000 Char);
      int_inserted PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF pDirection = 'DOWN'
      THEN
         -- 0 down and 1 up
         -- 1 down and 0 up
         str_sql := 'INSERT INTO nhdplus_navigation2.tmp_navigation_links( '
                 || '    link_index '
                 || '   ,link_id '
                 || '   ,link_start_clip_percentage '
                 || '   ,link_end_clip_percentage '
                 || '   ,end_node_id '
                 || ') '
                 || 'SELECT /*+ dynamic_sampling(t 3) */ '
                 || ' -1 '
                 || ',a.link_id '
                 || ',0 '
                 || ',1 '
                 || ',a.end_node_id '
                 || 'FROM '
                 || 'nhdplus_toponet.' || pNetworkName || '_link$ a '
                 || 'WHERE '
                 || 'a.link_id NOT IN ( '
                 || '   SELECT b.link_id  '
                 || '   FROM nhdplus_navigation2.tmp_navigation_links b '
                 || ') AND a.start_node_id IN ( '
                 || '   SELECT c.end_node_id '
                 || '   FROM nhdplus_navigation2.tmp_navigation_links c '
                 || '   WHERE c.link_end_clip_percentage = 1 '
                 || ') AND a.end_node_id IN ( '
                 || '   SELECT d.end_node_id '
                 || '   FROM nhdplus_navigation2.tmp_navigation_links d '
                 || '   WHERE d.link_end_clip_percentage = 1 '
                 || ') ';
         
      ELSE
         -- 0 down and 1 up
         -- 1 down and 0 up
         str_sql := 'INSERT INTO nhdplus_navigation2.tmp_navigation_links( '
                 || '    link_index '
                 || '   ,link_id '
                 || '   ,link_start_clip_percentage '
                 || '   ,link_end_clip_percentage '
                 || '   ,end_node_id '
                 || ') '
                 || 'SELECT /*+ dynamic_sampling(t 3) */ '
                 || ' -1 '
                 || ',a.link_id '
                 || ',1 '
                 || ',0 '
                 || ',a.end_node_id '
                 || 'FROM '
                 || 'nhdplus_toponet.' || pNetworkName || '_link$ a '
                 || 'WHERE '
                 || 'a.link_id NOT IN ( '
                 || '   SELECT b.link_id '
                 || '   FROM nhdplus_navigation2.tmp_navigation_links b '
                 || ') AND a.start_node_id IN ( '
                 || '   SELECT c.end_node_id '
                 || 'FROM nhdplus_navigation2.tmp_navigation_links c '
                 || '   WHERE c.link_end_clip_percentage = 0 '
                 || ') AND a.end_node_id IN ( '
                 || '   SELECT d.end_node_id '
                 || '   FROM nhdplus_navigation2.tmp_navigation_links d '
                 || '   WHERE d.link_end_clip_percentage = 0  '
                 || ')';
         
      END IF; 
      
      EXECUTE IMMEDIATE str_sql;
      int_inserted := SQL%ROWCOUNT; 

      --------------------------------------------------------------------------
      -- Step 30
      -- Corrective action to fix missing cost on missing links
      --------------------------------------------------------------------------
      IF int_inserted > 0
      THEN
         UPDATE /*+ dynamic_sampling(t 3) */ 
         nhdplus_navigation2.tmp_navigation_links a 
         SET (
             link_lengthkm
            ,link_flowtimeday
            ,link_network_distancekm
            ,link_network_flowtimeday
         )= (
            SELECT
             c.lengthkm
            ,c.totma
            ,b.link_network_distancekm
            ,b.link_network_flowtimeday
            FROM
            nhdplus_navigation2.tmp_navigation_links b
            JOIN
            nhdplus_navigation2.plusflowlinevaa_nav c
            ON
            b.link_id = c.hydrosequence
            WHERE
                b.link_index <> -1
            AND b.end_node_id = a.end_node_id
            AND rownum <= 1
         )
         WHERE
         a.link_index = -1;

      END IF;
      
      RETURN int_inserted;
   
   END run_fix;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight(
       pNetworkName     IN  VARCHAR2
      ,pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      int_results  PLS_INTEGER := 0;
      int_inserted PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      int_results := traceOutLight_java(
          pNetworkName     => pNetworkName
         ,pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold
      );
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF int_results > 0
      THEN
         int_inserted := run_fix(
             pNetworkName => pNetworkName
            ,pDirection   => 'DOWN'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN int_results + int_inserted;
      
   END traceOutLight;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight(
       pNetworkName     IN  VARCHAR2
      ,pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      int_results  PLS_INTEGER := 0;
      int_inserted PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      int_results := traceInLight_java(
          pNetworkName     => pNetworkName
         ,pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold 
      );
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF int_results > 0
      THEN
         int_inserted := run_fix(
             pNetworkName => pNetworkName
            ,pDirection   => 'UP'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN int_results + int_inserted;
      
   END traceInLight;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight_mainstem(
       pNetworkName     IN  VARCHAR2
      ,pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      int_results  PLS_INTEGER := 0;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      int_results := traceOutLight_java_mainstem(
          pNetworkName     => pNetworkName
         ,pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold
      );

      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN int_results;
      
   END traceOutLight_mainstem;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight_mainstem(
       pNetworkName     IN  VARCHAR2
      ,pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      int_results  PLS_INTEGER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      int_results := traceInLight_java_mainstem(
          pNetworkName     => pNetworkName
         ,pStartLinkID     => pStartLinkID
         ,pStartPercentage => pStartPercentage
         ,pStartNodeID     => pStartNodeID
         ,pCostThreshold   => pCostThreshold 
      );
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN int_results;
      
   END traceInLight_mainstem;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE get_flowline(
       p_direction            IN  VARCHAR2
      ,p_permanent_identifier IN  VARCHAR2
      ,p_nhdplusid            IN  NUMBER
      ,p_reachcode            IN  VARCHAR2
      ,p_hydrosequence        IN  NUMBER
      ,p_measure              IN  NUMBER
      ,p_flowline             OUT flowline_rec
      ,p_return_code          OUT INTEGER
      ,p_status_message       OUT VARCHAR2
   )
   AS
   BEGIN
   
      p_return_code := 0;
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check for ComID submittal
      --------------------------------------------------------------------------
      IF p_nhdplusid IS NOT NULL
      THEN
         IF p_measure IS NULL
         THEN
            SELECT
             a.permanent_identifier 
            ,a.comid   
            ,a.reachcode 
            ,a.fmeasure 
            ,a.tmeasure
            ,a.hydroseq   
            ,NULL
            ,NULL   -- start_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
            a.comid = p_nhdplusid;
            
            IF p_direction = 'DOWN'
            THEN
               p_flowline.pt_measure  := p_flowline.tmeasure;
               
            ELSE
               p_flowline.pt_measure  := p_flowline.fmeasure;
            
            END IF;
            
         ELSE
            SELECT
             a.permanent_identifier 
            ,a.comid
            ,a.reachcode
            ,a.fmeasure
            ,a.tmeasure
            ,a.hydroseq
            ,p_measure AS pt_measure
            ,NULL   -- pt_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
                a.comid = p_nhdplusid
            AND p_measure >= a.fmeasure
            AND p_measure <= a.tmeasure;
         
         END IF;
         
      ELSIF p_hydrosequence IS NOT NULL
      THEN
         IF p_measure IS NULL
         THEN
            SELECT
             a.permanent_identifier 
            ,a.comid
            ,a.reachcode
            ,a.fmeasure
            ,a.tmeasure
            ,a.hydroseq
            ,NULL
            ,NULL   -- start_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
            a.hydroseq = p_hydrosequence;
            
            IF p_direction = 'DOWN'
            THEN
               p_flowline.pt_measure  := p_flowline.tmeasure;
               
            ELSE
               p_flowline.pt_measure  := p_flowline.fmeasure;
            
            END IF;
            
         ELSE
            SELECT
             a.permanent_identifier 
            ,a.comid   
            ,a.reachcode 
            ,a.fmeasure 
            ,a.tmeasure
            ,a.hydroseq
            ,p_measure AS pt_measure 
            ,NULL   -- pt_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
                a.hydroseq = p_hydrosequence
            AND p_measure >= a.fmeasure
            AND p_measure <= a.tmeasure;
         
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Check for Permanent Identifier submittal
      --------------------------------------------------------------------------
      ELSIF p_permanent_identifier IS NOT NULL
      THEN
         IF p_measure IS NULL
         THEN
            SELECT
             a.permanent_identifier 
            ,a.comid   
            ,a.reachcode 
            ,a.fmeasure 
            ,a.tmeasure 
            ,a.hydroseq
            ,NULL
            ,NULL   -- start_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
            a.permanent_identifier = p_permanent_identifier;
            
            IF p_direction = 'DOWN'
            THEN
               p_flowline.pt_measure  := p_flowline.tmeasure;
               
            ELSE
               p_flowline.pt_measure  := p_flowline.fmeasure;
            
            END IF;
            
         ELSE
            SELECT
             a.permanent_identifier 
            ,a.comid
            ,a.reachcode
            ,a.fmeasure
            ,a.tmeasure
            ,a.hydroseq   
            ,p_measure AS pt_measure
            ,NULL   -- pt_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
                a.permanent_identifier = p_permanent_identifier
            AND p_measure >= a.fmeasure
            AND p_measure <= a.tmeasure;
         
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for Reachcode submittal
      --------------------------------------------------------------------------
      ELSIF p_reachcode IS NOT NULL
      THEN
         IF p_measure IS NULL
         THEN
            IF p_direction = 'DOWN'
            THEN
               SELECT
                a.permanent_identifier
               ,a.comid   
               ,a.reachcode
               ,a.fmeasure
               ,a.tmeasure
               ,a.hydroseq
               ,NULL
               ,100   -- start_percentage
               ,a.fromnode
               ,a.tonode
               ,a.uphydroseq
               ,a.dnhydroseq
               ,a.fcode
               INTO p_flowline 
               FROM
               nhdplus.plusflowlinevaa_np21 a
               WHERE
                   a.reachcode = p_reachcode
               AND a.tmeasure = 100;
            
            ELSE
               SELECT
                a.permanent_identifier 
               ,a.comid   
               ,a.reachcode 
               ,a.fmeasure
               ,a.tmeasure
               ,a.hydroseq
               ,NULL
               ,0   -- start_percentage
               ,a.fromnode
               ,a.tonode
               ,a.uphydroseq
               ,a.dnhydroseq
               ,a.fcode
               INTO p_flowline
               FROM
               nhdplus.plusflowlinevaa_np21 a
               WHERE
                   a.reachcode = p_reachcode
               AND a.fmeasure = 0;
            
            END IF;
            
         ELSE
            SELECT
             a.permanent_identifier 
            ,a.comid   
            ,a.reachcode
            ,a.fmeasure
            ,a.tmeasure
            ,a.hydroseq
            ,p_measure AS pt_measure
            ,NULL   -- pt_percentage
            ,a.fromnode
            ,a.tonode
            ,a.uphydroseq
            ,a.dnhydroseq
            ,a.fcode
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
                a.reachcode = p_reachcode
            AND p_measure >= a.fmeasure
            AND p_measure <= a.tmeasure;
         
         END IF;
         
      --------------------------------------------------------------------------
      -- Step 40
      -- Error condition
      --------------------------------------------------------------------------
      ELSE
         p_return_code    := -9;
         p_status_message := 'Navigation requires a flowline identifier of comid, permanent_identifier or reachcode';
         RETURN;
         
      END IF;
      
      p_flowline.pt_percentage   := (p_flowline.tmeasure - p_flowline.pt_measure) / (p_flowline.tmeasure - p_flowline.fmeasure);   
      
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         p_return_code    := -1;
         p_status_message := 'Flowline comid ' || p_nhdplusid 
                          || ' at measure ' || p_measure 
                          || ' not found in NHDPlus ';
      
      WHEN OTHERS
      THEN
         RAISE;
         
   END get_flowline;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_up(
       pNetworkName              IN  VARCHAR2
      ,pCostAmount               IN  NUMBER
      ,pMainStem                 IN  BOOLEAN
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartNHDPlusID           IN  INTEGER
      ,pStartReachCode           IN  VARCHAR2
      ,pStartHydroSequence       IN  INTEGER
      ,pStartMeasure             IN  NUMBER
      ,pFlowlineCount            OUT INTEGER
      ,pReturnCode               OUT INTEGER
      ,pStatusMessage            OUT VARCHAR2
      ,pSessionID                IN OUT VARCHAR2
   )
   AS
      obj_flowline flowline_rec;
      num_paths    NUMBER;
      num_cost     NUMBER;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get the start percentage
      --------------------------------------------------------------------------
      get_flowline(
          p_direction            => 'UP'
         ,p_permanent_identifier => pStartPermanentIdentifier
         ,p_nhdplusid            => pStartNHDPlusID
         ,p_reachcode            => pStartReachCode
         ,p_hydrosequence        => pStartHydroSequence
         ,p_measure              => pStartMeasure
         ,p_flowline             => obj_flowline
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      IF obj_flowline.fcode = 56600
      THEN
         pReturnCode := -56600;
         pStatusMessage := 'Navigation from or to coastal flowlines is not valid.';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for upstream navigation on top of headwater
      --------------------------------------------------------------------------
      IF obj_flowline.pt_percentage = 0 AND obj_flowline.uphydrosequence = 0
      THEN
         pStatusMessage := 'Cannot trace upstream from top of headwater flowline';
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Execute trace out light to fill temp table
      --------------------------------------------------------------------------
      IF pMainStem
      THEN
         num_paths := traceInLight_mainstem(
             pNetworkName     => pNetworkName
            ,pStartLinkID     => obj_flowline.hydrosequence
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.fromnode
            ,pCostThreshold   => pCostAmount
         );
         
      ELSE
         num_paths := traceInLight(
             pNetworkName     => pNetworkName
            ,pStartLinkID     => obj_flowline.hydrosequence
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.fromnode
            ,pCostThreshold   => pCostAmount
         );
         
      END IF;
      
      IF num_paths < 0
      THEN
         pReturnCode    := num_paths;
         pStatusMessage := 'error from traceInLight';
         RETURN;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Load the usual navigation table
      --------------------------------------------------------------------------
      INSERT INTO nhdplus_navigation2.tmp_navigation_results(
          objectid
         ,session_id
         ,permanent_identifier
         ,nhdplusid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,hydrosequence
         ,levelpathid
         ,terminalpathid
         ,uphydrosequence
         ,downhydrosequence
         ---
         ,lengthkm
         ,network_distancekm
         ,flowtimeday
         ,network_flowtimeday
         ---
         ,vpuid
         ,vpuversion
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplusid
         ,xwalk_huc12
         ,catchment_nhdplusid
         ,navigable
         ,coastal
         ,innetwork
         ,navtermination_flag
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplusid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydrosequence
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydrosequence
      ,a.downhydrosequence
      ------
      ,a.lengthkm
      ,a.network_distancekm
      ,a.flowtimeday
      ,a.network_flowtimeday
      ------
      ,a.vpuid
      ,a.vpuversion
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplusid
      ,a.xwalk_huc12
      ,a.catchment_nhdplusid
      ,a.navigable
      ,a.coastal
      ,a.innetwork
      ,a.navtermination_flag
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
              geom_segment  => a.shape
             ,start_measure => a.tmeasure
             ,end_measure   => a.fmeasure
          )
       ELSE
          a.shape
       END AS shape
      FROM (
         SELECT /*+ dynamic_sampling(t 3) */
          aa.permanent_identifier
         ,aa.comid AS nhdplusid
         ,aa.reachcode
         -------------
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             obj_flowline.pt_measure
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             ROUND(aa.tmeasure - (bb.link_end_clip_percentage * (aa.tmeasure - aa.fmeasure)),5)
          WHEN bb.link_end_clip_percentage <> 0 
          THEN
             ROUND(aa.tmeasure - (bb.link_end_clip_percentage * (aa.tmeasure - aa.fmeasure)),5)
          ELSE
             aa.tmeasure
          END AS tmeasure
         -------------
         ,aa.hydroseq       AS hydrosequence
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq     AS uphydrosequence
         ,aa.dnhydroseq     AS downhydrosequence
         -------------
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             aa.lengthkm * (bb.link_start_clip_percentage - bb.link_end_clip_percentage)
          WHEN bb.link_end_clip_percentage <> 1 
          THEN
             aa.lengthkm * (1 - bb.link_end_clip_percentage)
          ELSE
             aa.lengthkm
          END AS lengthkm
         ,bb.link_network_distancekm  AS network_distancekm
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             aa.flowtimeday * (bb.link_start_clip_percentage - bb.link_end_clip_percentage)
          WHEN bb.link_end_clip_percentage <> 1
          THEN
             aa.flowtimeday * (1 - bb.link_end_clip_percentage)
          ELSE
             aa.flowtimeday
          END AS flowtimeday
         ,CASE
          WHEN bb.link_network_flowtimeday < 0
          THEN
            NULL
          ELSE
            bb.link_network_flowtimeday 
          END AS network_flowtimeday 
         ------------
         ,aa.vpuid
         ,aa.vpuversion
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_comid        AS wbarea_nhdplusid
         ,aa.wbd_huc12           AS xwalk_huc12
         ,aa.catchment_featureid AS catchment_nhdplusid
         ,aa.navigable
         ,aa.coastal
         ,aa.innetwork
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
            0
          WHEN bb.link_end_clip_percentage = 0
          AND  aa.uphydroseq <> 0 
          THEN
            1
          WHEN bb.link_end_clip_percentage = 0
          AND  aa.uphydroseq = 0 
          THEN
            4
          WHEN bb.link_end_clip_percentage > 0
          THEN
            2
          ELSE
            0
          END AS navtermination_flag
         ,aa.shape
         ,CASE
          WHEN bb.link_end_clip_percentage <> 0 
          OR   aa.comid = obj_flowline.nhdplusid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_links bb
         ON
         aa.hydroseq = bb.link_id
      ) a;
      
      pFlowlineCount := SQL%ROWCOUNT;

      COMMIT;
      
   END navigate_up;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_down(
       pNetworkName              IN  VARCHAR2
      ,pCostAmount               IN  NUMBER
      ,pMainStem                 IN  BOOLEAN
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartNHDPlusID           IN  INTEGER
      ,pStartReachCode           IN  VARCHAR2
      ,pStartHydroSequence       IN  INTEGER
      ,pStartMeasure             IN  NUMBER
      ,pFlowlineCount            OUT INTEGER
      ,pReturnCode               OUT INTEGER
      ,pStatusMessage            OUT VARCHAR2
      ,pSessionID                IN OUT VARCHAR2
   )
   AS
      obj_flowline flowline_rec;
      num_paths    NUMBER;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get the start percentage
      --------------------------------------------------------------------------
      get_flowline(
          p_direction            => 'DOWN'
         ,p_permanent_identifier => pStartPermanentIdentifier
         ,p_nhdplusid            => pStartNHDPlusID
         ,p_reachcode            => pStartReachCode
         ,p_hydrosequence        => pStartHydroSequence
         ,p_measure              => pStartMeasure
         ,p_flowline             => obj_flowline
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      IF obj_flowline.fcode = 56600
      THEN
         pReturnCode := -56600;
         pStatusMessage := 'Navigation from or to coastal flowlines is not valid.';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for upstream navigation on top of headwater
      --------------------------------------------------------------------------
      IF obj_flowline.pt_percentage = 0 AND obj_flowline.downhydrosequence = 0
      THEN
         pStatusMessage := 'Cannot trace downstream from final flowline';
         RETURN;
      
      END IF;

      --------------------------------------------------------------------------
      -- Step 40
      -- Execute trace out light to fill temp table
      --------------------------------------------------------------------------
      IF pMainStem
      THEN
         num_paths := traceOutLight_mainstem(
             pNetworkName     => pNetworkName
            ,pStartLinkID     => obj_flowline.hydrosequence
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.tonode
            ,pCostThreshold   => pCostAmount
         );
         
      ELSE
         num_paths := traceOutLight(
             pNetworkName     => pNetworkName
            ,pStartLinkID     => obj_flowline.hydrosequence
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.tonode
            ,pCostThreshold   => pCostAmount
         );
         
      END IF;
      
      IF num_paths < 0
      THEN
         pReturnCode    := num_paths;
         pStatusMessage := 'error from traceOutLight';
         RETURN;
         
      END IF; 
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Load the usual navigation table
      --------------------------------------------------------------------------
      INSERT INTO nhdplus_navigation2.tmp_navigation_results(
          objectid
         ,session_id
         ,permanent_identifier
         ,nhdplusid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,hydrosequence
         ,levelpathid
         ,terminalpathid
         ,uphydrosequence
         ,downhydrosequence
         -----
         ,lengthkm
         ,network_distancekm
         ,flowtimeday
         ,network_flowtimeday
         ------
         ,vpuid
         ,vpuversion
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplusid
         ,xwalk_huc12
         ,catchment_nhdplusid
         ,navigable
         ,coastal
         ,innetwork
         ,navtermination_flag
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplusid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydrosequence
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydrosequence
      ,a.downhydrosequence
      ------
      ,a.lengthkm
      ,a.network_distancekm
      ,a.flowtimeday
      ,a.network_flowtimeday
      ------
      ,a.vpuid
      ,a.vpuversion
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplusid
      ,a.xwalk_huc12
      ,a.catchment_nhdplusid
      ,a.navigable
      ,a.coastal
      ,a.innetwork
      ,a.navtermination_flag
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
              geom_segment  => a.shape
             ,start_measure => a.tmeasure
             ,end_measure   => a.fmeasure
          )
       ELSE
          a.shape
       END AS shape
      FROM (
         SELECT /*+ dynamic_sampling(t 3) */
          aa.permanent_identifier
         ,aa.comid AS nhdplusid
         ,aa.reachcode
         -------------------------
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             ROUND(aa.tmeasure - (bb.link_end_clip_percentage * (aa.tmeasure - aa.fmeasure)),5)
          WHEN bb.link_end_clip_percentage <> 1 
          THEN
             ROUND( aa.tmeasure - (bb.link_end_clip_percentage * (aa.tmeasure - aa.fmeasure)),5)
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             obj_flowline.pt_measure
          ELSE
             aa.tmeasure
          END AS tmeasure
         -------------------------
         ,aa.hydroseq AS hydrosequence
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq AS uphydrosequence
         ,aa.dnhydroseq AS downhydrosequence
         -------------------------
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             aa.lengthkm * (bb.link_end_clip_percentage - bb.link_start_clip_percentage)
          WHEN bb.link_end_clip_percentage <> 1 
          THEN
             aa.lengthkm * bb.link_end_clip_percentage
          ELSE
             aa.lengthkm
          END AS lengthkm
         ,bb.link_network_distancekm  AS network_distancekm
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
             aa.flowtimeday * (bb.link_end_clip_percentage - bb.link_start_clip_percentage)
          WHEN bb.link_end_clip_percentage <> 1 
          THEN
             aa.flowtimeday * bb.link_end_clip_percentage
          ELSE
             aa.flowtimeday
          END AS flowtimeday
         ,CASE
          WHEN bb.link_network_flowtimeday < 0
          THEN
            NULL
          ELSE
            bb.link_network_flowtimeday 
          END AS network_flowtimeday 
         -------------------------
         ,aa.vpuid
         ,aa.vpuversion
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_comid AS wbarea_nhdplusid
         ,aa.wbd_huc12 AS xwalk_huc12
         ,aa.catchment_featureid AS catchment_nhdplusid
         ,aa.navigable
         ,aa.coastal
         ,aa.innetwork
         ,CASE
          WHEN aa.comid = obj_flowline.nhdplusid
          THEN
            0
          WHEN bb.link_end_clip_percentage = 1
          AND  aa.coastal <> 'Y' 
          THEN
            1
          WHEN bb.link_end_clip_percentage = 1
          AND  aa.coastal = 'Y' 
          THEN
            3
          WHEN bb.link_end_clip_percentage < 1 
          THEN
            2
          ELSE
            0
          END AS navtermination_flag
         ,aa.shape
         ,CASE
          WHEN bb.link_end_clip_percentage <> 1 
          OR   aa.comid = obj_flowline.nhdplusid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_links bb
         ON
         aa.hydroseq = bb.link_id
      ) a;
      
      pFlowlineCount := SQL%ROWCOUNT;
     
      COMMIT;
      
   END navigate_down;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_pp(
       pNetworkName              IN  VARCHAR2
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartNHDPlusID           IN  INTEGER
      ,pStartReachCode           IN  VARCHAR2
      ,pStartHydroSequence       IN  INTEGER
      ,pStartMeasure             IN  NUMBER
      ,pStopPermanentIdentifier  IN  VARCHAR2
      ,pStopNHDPlusID            IN  INTEGER
      ,pStopReachCode            IN  VARCHAR2
      ,pStopHydroSequence        IN  INTEGER
      ,pStopMeasure              IN  NUMBER
      ,pFlowlineCount            OUT INTEGER
      ,pReturnCode               OUT NUMBER
      ,pStatusMessage            OUT VARCHAR2
      ,pSessionID                IN OUT VARCHAR2
   )
   AS
      obj_flowstart flowline_rec;
      obj_flowstop  flowline_rec;
      obj_flowtemp  flowline_rec;
      int_paths     PLS_INTEGER;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Get the start percentage
      --------------------------------------------------------------------------
      get_flowline(
          p_direction            => 'DOWN'
         ,p_permanent_identifier => pStartPermanentIdentifier
         ,p_nhdplusid            => pStartNHDPlusID
         ,p_reachcode            => pStartReachCode
         ,p_hydrosequence        => pStartHydroSequence
         ,p_measure              => pStartMeasure
         ,p_flowline             => obj_flowstart
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Get the stop percentage
      --------------------------------------------------------------------------
      get_flowline(
          p_direction            => 'UP'
         ,p_permanent_identifier => pStopPermanentIdentifier
         ,p_nhdplusid            => pStopNHDPlusID
         ,p_reachcode            => pStopReachCode
         ,p_hydrosequence        => pStopHydroSequence
         ,p_measure              => pStopMeasure
         ,p_flowline             => obj_flowstop
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;

      IF obj_flowstart.fcode = 56600 
      OR obj_flowstop.fcode  = 56600 
      THEN
         pReturnCode := -56600;
         pStatusMessage := 'Navigation from or to coastal flowlines is not valid.';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Alter start and stop depending on hydroseq
      --------------------------------------------------------------------------
      IF obj_flowstop.hydrosequence > obj_flowstart.hydrosequence
      THEN
         obj_flowtemp  := obj_flowstop;
         obj_flowstop  := obj_flowstart;
         obj_flowstart := obj_flowtemp;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- May need accounting for end of flowlines?
      --------------------------------------------------------------------------
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Execute shortestPath to fill temp tables
      --------------------------------------------------------------------------
      int_paths := shortestPath(
          pNetworkName     => pNetworkName
         ,pStartLinkID     => obj_flowstart.hydrosequence
         ,pStartPercentage => obj_flowstart.pt_percentage
         ,pStartNodeID     => obj_flowstart.fromnode
         ,pStopLinkID      => obj_flowstop.hydrosequence
         ,pStopPercentage  => obj_flowstop.pt_percentage
      );
      
      IF int_paths < 0
      THEN
         pReturnCode    := int_paths;
         pStatusMessage := 'error from shortestPath';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Transfer the costs to the link table
      --------------------------------------------------------------------------
      UPDATE nhdplus_navigation2.tmp_navigation_links a
      SET (
          end_node_id
         ,original_lengthkm
         ,original_flowtimeday
      ) = (
         SELECT
          b.tonode
         ,b.lengthkm
         ,CASE
          WHEN b.totma IN (-9998,-9999)
          THEN
            NULL
          ELSE
            b.totma
          END
         FROM
         nhdplus_navigation2.plusflowlinevaa_nav b
         WHERE
         b.hydrosequence = a.link_id
      );
      
      UPDATE nhdplus_navigation2.tmp_navigation_links a
      SET (
          end_node_cost
         ,end_node_network_distancekm
         ,end_node_network_flowtimeday
      ) = (
         SELECT
          b.end_cost
         ,b.end_network_distancekm
         ,b.end_network_flowtimeday
         FROM
         nhdplus_navigation2.tmp_navigation_nodes b
         WHERE
         b.node_id = a.end_node_id
      );
      
      UPDATE nhdplus_navigation2.tmp_navigation_links a
      SET
       link_lengthkm = CASE
         WHEN a.link_start_clip_percentage = 0
         AND  a.link_end_clip_percentage = 1
         THEN
            a.original_lengthkm
         ELSE
            a.original_lengthkm * (a.link_end_clip_percentage - a.link_start_clip_percentage)
         END
      ,link_flowtimeday = CASE
         WHEN a.link_start_clip_percentage = 0
         AND  a.link_end_clip_percentage = 1
         THEN
            a.original_flowtimeday
         ELSE
            a.original_flowtimeday * (a.link_end_clip_percentage - a.link_start_clip_percentage)
         END
      ,link_network_distancekm = CASE
         WHEN a.link_network_distancekm IS NOT NULL
         THEN
            a.link_network_distancekm
         WHEN a.link_end_clip_percentage < 1
         THEN
            a.end_node_network_distancekm - (a.original_lengthkm * (1 - a.link_end_clip_percentage))
         ELSE
            a.end_node_network_distancekm
         END
      ,link_network_flowtimeday = CASE
         WHEN a.link_network_flowtimeday IS NOT NULL
         THEN
            a.link_network_flowtimeday
         WHEN a.original_flowtimeday IS NULL
         OR a.end_node_network_flowtimeday < 0
         THEN
            NULL
         WHEN a.link_end_clip_percentage < 1
         THEN
            a.end_node_network_flowtimeday - (a.original_flowtimeday * (1 - a.link_end_clip_percentage))
         ELSE
            a.end_node_network_flowtimeday
         END
      ;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Load the usual navigation table
      --------------------------------------------------------------------------
      INSERT INTO nhdplus_navigation2.tmp_navigation_results(
          objectid
         ,session_id
         ,permanent_identifier
         ,nhdplusid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,hydrosequence
         ,levelpathid
         ,terminalpathid
         ,uphydrosequence
         ,downhydrosequence
         ----
         ,lengthkm
         ,network_distancekm
         ,flowtimeday
         ,network_flowtimeday
         ----
         ,vpuid
         ,vpuversion
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplusid
         ,xwalk_huc12
         ,catchment_nhdplusid
         ,quality_marker
         ,navigable
         ,coastal
         ,innetwork
         ,navtermination_flag
         ,nav_order
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplusid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.hydrosequence
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydrosequence
      ,a.downhydrosequence
      -----
      ,a.lengthkm
      ,a.network_distancekm
      ,a.flowtimeday
      ,a.network_flowtimeday
      -----
      ,a.vpuid
      ,a.vpuversion
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplusid
      ,a.xwalk_huc12
      ,a.catchment_nhdplusid
      ,NULL
      ,a.navigable
      ,a.coastal
      ,a.innetwork
      ,a.navtermination_flag
      ,a.nav_order
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
              geom_segment  => a.shape
             ,start_measure => a.tmeasure
             ,end_measure   => a.fmeasure
          )
       ELSE
          a.shape
       END AS shape
      FROM (
         SELECT /*+ dynamic_sampling(t 3) */
          aa.permanent_identifier
         ,aa.comid AS nhdplusid
         ,aa.reachcode
         ,CASE
          WHEN aa.comid = obj_flowstop.nhdplusid
          THEN
             obj_flowstop.pt_measure
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.comid = obj_flowstart.nhdplusid
          THEN
             obj_flowstart.pt_measure
          ELSE
             aa.tmeasure
          END AS tmeasure
         ,aa.hydroseq   AS hydrosequence
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq AS uphydrosequence
         ,aa.dnhydroseq AS downhydrosequence
         -----
         ,bb.link_lengthkm            AS lengthkm
         ,bb.link_network_distancekm  AS network_distancekm
         ,bb.link_flowtimeday         AS flowtimeday
         ,bb.link_network_flowtimeday AS network_flowtimeday
         -----
         ,aa.vpuid
         ,aa.vpuversion
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_comid        AS wbarea_nhdplusid
         ,aa.wbd_huc12           AS xwalk_huc12
         ,aa.catchment_featureid AS catchment_nhdplusid
         ,NULL
         ,aa.navigable
         ,aa.coastal
         ,aa.innetwork
         ,CASE
          WHEN aa.comid = obj_flowstart.nhdplusid
          THEN
            0
          WHEN aa.comid = obj_flowstop.nhdplusid
          AND  bb.link_end_clip_percentage = 1
          AND  aa.coastal <> 'Y' 
          THEN
            1
          WHEN aa.comid = obj_flowstop.nhdplusid
          AND  bb.link_end_clip_percentage = 1
          AND  aa.coastal = 'Y' 
          THEN
            3
          WHEN aa.comid = obj_flowstop.nhdplusid
          AND  bb.link_end_clip_percentage < 1 
          THEN
            2
          ELSE
            0
          END AS navtermination_flag
         ,bb.link_index AS nav_order
         ,aa.shape
         ,CASE
          WHEN aa.comid = obj_flowstart.nhdplusid
          OR   aa.comid = obj_flowstop.nhdplusid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_links bb
         ON
         aa.hydroseq = bb.link_id      
      ) a;
      
      pFlowlineCount := SQL%ROWCOUNT;
      
      IF pFlowlineCount = 0
      THEN
         pReturnCode    := 0;
         pStatusMessage := 'warning, no results returned from this navigation';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Commit results and clean out temp tables
      --------------------------------------------------------------------------
      COMMIT;
      
   END navigate_pp;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate(
       pSearchType                    IN  VARCHAR2
      ,pStartPermanentIdentifier      IN  VARCHAR2
      ,pStartNHDPlusID                IN  INTEGER
      ,pStartReachCode                IN  VARCHAR2
      ,pStartHydroSequence            IN  INTEGER
      ,pStartMeasure                  IN  NUMBER
      ,pStopPermanentIdentifier       IN  VARCHAR2
      ,pStopNHDPlusID                 IN  INTEGER
      ,pStopReachCode                 IN  VARCHAR2
      ,pStopHydroSequence             IN  INTEGER
      ,pStopMeasure                   IN  NUMBER
      ,pMaxDistanceKm                 IN  NUMBER
      ,pMaxFlowTimeDay                IN  NUMBER
      ,pReturnCatchments              IN  VARCHAR2 DEFAULT 'FALSE'
      ,pLoadSearchTable               IN  VARCHAR2 DEFAULT 'FALSE'
      ,pFlowlineCount                 OUT INTEGER
      ,pCatchmentCount                OUT INTEGER
      ,pReturnCode                    OUT INTEGER
      ,pStatusMessage                 OUT VARCHAR2
      ,pSessionID                     IN OUT VARCHAR2
   )
   AS
      str_search_type             VARCHAR2(4000 Char) := UPPER(pSearchType);
      num_max_distancekm          NUMBER              := pMaxDistanceKm;
      num_max_flowtimeday         NUMBER              := pMaxFlowTimeDay;
      str_return_catchments       VARCHAR2(4000 Char) := UPPER(pReturnCatchments);
      str_load_search_table       VARCHAR2(4000 Char) := UPPER(pLoadSearchTable);
      str_network_name            VARCHAR2(60 Char);
      num_cost_amount             NUMBER;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      pReturnCode := 0;
      nhdplus_navigation2.flush_all();
      
      IF str_search_type NOT IN ('UT','UM','DD','DM','PP')
      THEN
         pReturnCode := -1;
         pStatusMessage := 'Invalid navigation type';
         RETURN;
      
      END IF;
      
      IF num_max_distancekm = 0
      OR num_max_flowtimeday    = 0
      THEN
         pReturnCode := -3;
         pStatusMessage := 'Navigation for zero distance or flowtime is not valid.';
         RETURN;
      
      END IF;
      
      IF str_return_catchments IS NULL
      OR str_return_catchments NOT IN ('TRUE','FALSE')
      THEN
         str_return_catchments := 'FALSE';
         
      END IF;
      
      IF str_load_search_table IS NULL
      OR str_load_search_table NOT IN ('TRUE','FALSE')
      THEN
         str_load_search_table := 'FALSE';

      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Bump up the heap if we think this is going a long way
      -- There is a limit as to what we can give the user
      --------------------------------------------------------------------------
      IF  num_max_distancekm IS NOT NULL
      AND num_max_distancekm > 100
      THEN
         MDSYS.SDO_NET.SET_MAX_JAVA_HEAP_SIZE(524288000);
         
      ELSIF num_max_distancekm IS NULL
      OR    num_max_distancekm > 1000
      THEN
         MDSYS.SDO_NET.SET_MAX_JAVA_HEAP_SIZE(2147483648);
         
      ELSIF num_max_flowtimeday IS NOT NULL
      AND num_max_flowtimeday > 1
      THEN
         MDSYS.SDO_NET.SET_MAX_JAVA_HEAP_SIZE(524288000);
         
      ELSIF num_max_flowtimeday IS NULL
      OR    num_max_flowtimeday > 10
      THEN
         MDSYS.SDO_NET.SET_MAX_JAVA_HEAP_SIZE(2147483648);
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Verify or create the session id
      --------------------------------------------------------------------------
      IF pSessionID IS NULL
      THEN
         pSessionID := dz_dict_util.get_guid();
         
         INSERT INTO
         nhdplus_navigation2.tmp_navigation_status(
             session_id
            ,session_datestamp
            ,objectid
         ) VALUES (
             pSessionID
            ,SYSTIMESTAMP
            ,nhdplus_navigation2.tmp_navigation_status_seq.NEXTVAL
         );
         
         COMMIT;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Determine the network to use
      --------------------------------------------------------------------------
      IF  num_max_distancekm IS NULL
      AND num_max_flowtimeday IS NOT NULL
      THEN
         str_network_name := 'PLUSFLOW_FLOWTIME';
         num_cost_amount  := num_max_flowtimeday;
         
      ELSE
         str_network_name := 'PLUSFLOW_LENGTHKM';
         num_cost_amount  := num_max_distancekm;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Upstream with tribs
      --------------------------------------------------------------------------
      IF str_search_type = 'UT'
      THEN
         navigate_up(
             pNetworkName              => str_network_name
            ,pCostAmount               => num_cost_amount
            ,pMainstem                 => FALSE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartNHDPlusID           => pStartNHDPlusID
            ,pStartReachCode           => pStartReachCode
            ,pStartHydroSequence       => pStartHydroSequence
            ,pStartMeasure             => pStartMeasure
            ,pFlowlineCount            => pFlowlineCount
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 60
      -- Downstream with divergences
      --------------------------------------------------------------------------   
      ELSIF str_search_type = 'DD'
      THEN
         navigate_down(
             pNetworkName              => str_network_name
            ,pCostAmount               => num_cost_amount
            ,pMainstem                 => FALSE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartNHDPlusID           => pStartNHDPlusID
            ,pStartReachCode           => pStartReachCode
            ,pStartHydroSequence       => pStartHydroSequence
            ,pStartMeasure             => pStartMeasure
            ,pFlowlineCount            => pFlowlineCount
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- 
      --------------------------------------------------------------------------
      ELSIF str_search_type = 'UM'
      THEN
         navigate_up(
             pNetworkName              => str_network_name
            ,pCostAmount               => num_cost_amount
            ,pMainstem                 => TRUE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartNHDPlusID           => pStartNHDPlusID
            ,pStartReachCode           => pStartReachCode
            ,pStartHydroSequence       => pStartHydroSequence
            ,pStartMeasure             => pStartMeasure
            ,pFlowlineCount            => pFlowlineCount
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
         
      --------------------------------------------------------------------------
      -- Step 80
      -- 
      --------------------------------------------------------------------------
      ELSIF str_search_type = 'DM'
      THEN
         navigate_down(
             pNetworkName              => str_network_name
            ,pCostAmount               => num_cost_amount
            ,pMainstem                 => TRUE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartNHDPlusID           => pStartNHDPlusID
            ,pStartReachCode           => pStartReachCode
            ,pStartHydroSequence       => pStartHydroSequence
            ,pStartMeasure             => pStartMeasure
            ,pFlowlineCount            => pFlowlineCount
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
         
      --------------------------------------------------------------------------
      -- Step 90
      -- 
      --------------------------------------------------------------------------
      ELSIF str_search_type = 'PP'
      THEN
         navigate_pp(
             pNetworkName              => str_network_name
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartNHDPlusID           => pStartNHDPlusID
            ,pStartReachCode           => pStartReachCode
            ,pStartHydroSequence       => pStartHydroSequence
            ,pStartMeasure             => pStartMeasure
            ,pStopPermanentIdentifier  => pStopPermanentIdentifier
            ,pStopNHDPlusID            => pStopNHDPlusID
            ,pStopReachCode            => pStopReachCode
            ,pStopHydroSequence        => pStopHydroSequence
            ,pStopMeasure              => pStopMeasure
            ,pFlowlineCount            => pFlowlineCount
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 100
      -- Add search table if requested
      --------------------------------------------------------------------------
      IF str_load_search_table = 'TRUE'
      THEN
         INSERT INTO nhdplus_navigation2.tmp_navigation2_search(
             nhdplusid
            ,reachcode
            ,fmeasure
            ,tmeasure
            ,lengthkm
            ,flowtimeday
            ,xwalk_huc12
            ,catchment_nhdplusid
            ,navtermination_flag
            ,network_distancekm
            ,network_flowtimeday
         )
         SELECT
          a.nhdplusid
         ,a.reachcode
         ,a.fmeasure
         ,a.tmeasure
         ,a.lengthkm
         ,a.flowtimeday
         ,a.xwalk_huc12
         ,a.catchment_nhdplusid
         ,a.navtermination_flag
         ,a.network_distancekm
         ,a.network_flowtimeday
         FROM
         nhdplus_navigation2.tmp_navigation_results a
         WHERE
         a.session_id = pSessionID;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 110
      -- Add catchments to table if requested
      --------------------------------------------------------------------------
      IF str_return_catchments = 'TRUE'
      THEN
         INSERT INTO nhdplus_navigation2.tmp_catchments(
             objectid
            ,session_id
            ,cat_joinkey
            ,catchmentstatecode
            ,nhdplusid
            ,xwalk_huc12
            ,areasqkm
            ,headwater
            ,coastal
            ,network_distancekm
            ,network_flowtimeday
            ,hydrosequence
            ,globalid
            ,shape
         )
         SELECT
          nhdplus_navigation2.tmp_catchments_seq.NEXTVAL
         ,pSessionID
         ,a.catchmentstatecode || TO_CHAR(a.nhdplusid)
         ,a.catchmentstatecode
         ,a.nhdplusid
         ,a.xwalk_huc12
         ,a.areasqkm
         ,a.headwater
         ,a.coastal
         ,b.network_distancekm
         ,b.network_flowtimeday
         ,b.hydrosequence
         ,a.globalid
         ,a.shape
         FROM
         waters_xwalk.catchment_fabric a
         JOIN (
            SELECT
             bb.nhdplusid
            ,bb.hydrosequence
            ,MIN(bb.network_distancekm)  AS network_distancekm
            ,MIN(bb.network_flowtimeday) AS network_flowtimeday
            FROM
            nhdplus_navigation2.tmp_navigation_results bb
            WHERE
            bb.session_id = pSessionID
            GROUP BY
             bb.nhdplusid
            ,bb.hydrosequence
         ) b
         ON
         b.nhdplusid = a.nhdplusid;
         
         pCatchmentCount := SQL%ROWCOUNT;

      ELSE
         pCatchmentCount := 0;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 120
      -- Finalize the status table
      --------------------------------------------------------------------------
      UPDATE nhdplus_navigation2.tmp_navigation_status a
      SET
       a.return_code    = pReturnCode
      ,a.status_message = pStatusMessage
      WHERE
      a.session_id = pSessionID;

      COMMIT;

   END navigate;

END navigator2_main;
/

