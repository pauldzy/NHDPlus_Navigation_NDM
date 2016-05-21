CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.navigator2_main
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION run_fix(
       pDirection       IN  VARCHAR2
   ) RETURN NUMBER
   AS
      num_inserted NUMBER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF pDirection = 'DOWN'
      THEN
         INSERT INTO nhdplus_navigation2.tmp_navigation_subpath(
             end_link_id
            ,start_percentage
            ,end_percentage
            ,end_node_id
            ,end_cost
            ,handling_flag
         )
         SELECT /*+ dynamic_sampling(t 3) */
          a.link_id
         ,0  -- 0 down and 1 up
         ,1 -- 1 down and 0 up
         ,a.end_node_id
         ,0
         ,1
         FROM
         nhdplus_toponet.plusflowline_link$ a
         WHERE 
         a.link_id NOT IN ( 
            SELECT b.end_link_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath b 
         ) AND a.start_node_id IN ( 
            SELECT c.end_node_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath c 
            WHERE c.end_percentage = 1  -- 1 down and 0 up
         ) AND a.end_node_id IN ( 
            SELECT d.end_node_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath d 
            WHERE d.end_percentage = 1  -- 1 down and 0 up
         );
         
      ELSE
         INSERT INTO nhdplus_navigation2.tmp_navigation_subpath(
             end_link_id
            ,start_percentage
            ,end_percentage
            ,end_node_id
            ,end_cost
            ,handling_flag
         )
         SELECT /*+ dynamic_sampling(t 3) */
          a.link_id
         ,1  -- 0 down and 1 up
         ,0 -- 1 down and 0 up
         ,a.end_node_id
         ,0
         ,1
         FROM
         nhdplus_toponet.plusflowline_link$ a
         WHERE 
         a.link_id NOT IN ( 
            SELECT b.end_link_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath b 
         ) AND a.start_node_id IN ( 
            SELECT c.end_node_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath c 
            WHERE c.end_percentage = 0  -- 1 down and 0 up
         ) AND a.end_node_id IN ( 
            SELECT d.end_node_id 
            FROM nhdplus_navigation2.tmp_navigation_subpath d 
            WHERE d.end_percentage = 0  -- 1 down and 0 up
         );
         
      END IF; 
      
      num_inserted := SQL%ROWCOUNT; 
 
      --------------------------------------------------------------------------
      -- Step 30
      -- Corrective action to fix missing cost on missing links
      --------------------------------------------------------------------------
      IF num_inserted > 0
      THEN
         UPDATE /*+ dynamic_sampling(t 3) */ 
         nhdplus_navigation2.tmp_navigation_subpath a 
         SET 
         a.end_cost = ( 
            SELECT
            b.end_cost
            FROM 
            nhdplus_navigation2.tmp_navigation_subpath b 
            WHERE
                b.handling_flag = 0 
            AND b.end_node_id = a.end_node_id 
            AND rownum <= 1 
         )
         WHERE 
         a.handling_flag = 1;
         
      END IF;
      
      RETURN num_inserted;
   
   END run_fix;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      num_results  NUMBER := 0;
      num_inserted NUMBER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      num_results := traceOutLight_java(
          pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold
      );

      --------------------------------------------------------------------------
      -- Step 20
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF num_results > 0
      THEN
         num_inserted := run_fix(
             pDirection     => 'DOWN'
         );
         
      END IF;
 
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN num_results + num_inserted;
      
   END traceOutLight;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      num_results  NUMBER := 0;
      num_inserted NUMBER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      num_results := traceInLight_java(
          pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold 
      );
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Corrective action to fix missing link ids
      --------------------------------------------------------------------------
      IF num_results > 0
      THEN
         num_inserted := run_fix(
             pDirection     => 'UP'
         );
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN num_results + num_inserted;
      
   END traceInLight;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      num_results  NUMBER := 0;
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      num_results := traceOutLight_java_mainstem(
          pStartLinkID     => pStartLinkID
         ,pStartNodeID     => pStartNodeID
         ,pStartPercentage => pStartPercentage
         ,pCostThreshold   => pCostThreshold
      );

      --------------------------------------------------------------------------
      -- Step 30
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN num_results;
      
   END traceOutLight_mainstem;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
      num_results  NUMBER := 0;
      
   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- Execute the java wrapper
      --------------------------------------------------------------------------
      num_results := traceInLight_java_mainstem(
          pStartLinkID     => pStartLinkID
         ,pStartPercentage => pStartPercentage
         ,pStartNodeID     => pStartNodeID
         ,pCostThreshold   => pCostThreshold 
      );
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Return what we got
      --------------------------------------------------------------------------
      RETURN num_results;
      
   END traceInLight_mainstem;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE get_flowline(
       p_direction            IN  VARCHAR2
      ,p_permanent_identifier IN  VARCHAR2
      ,p_comid                IN  INTEGER
      ,p_reachcode            IN  VARCHAR2
      ,p_measure              IN  NUMBER
      ,p_flowline             OUT flowline_rec
      ,p_return_code          OUT NUMBER
      ,p_status_message       OUT VARCHAR2
   )
   AS
   BEGIN
   
      p_return_code := 0;
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check for ComID submittal
      --------------------------------------------------------------------------
      IF p_comid IS NOT NULL
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
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
            a.comid = p_comid;
            
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
            INTO p_flowline 
            FROM
            nhdplus.plusflowlinevaa_np21 a
            WHERE
                a.comid = p_comid
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
         p_status_message := 'Flowline comid ' || p_comid 
                          || ' at measure ' || p_measure 
                          || ' not found in NHDPlus ';
      
      WHEN OTHERS
      THEN
         RAISE;
         
   END get_flowline;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_up(
       pMainStem                 IN  BOOLEAN
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartComid               IN  INTEGER
      ,pStartReachcode           IN  VARCHAR2
      ,pStartMeasure             IN  NUMBER
      ,pMaxDistanceKm            IN  NUMBER
      ,pReturnCode               OUT NUMBER
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
          p_direction            => 'UP'
         ,p_permanent_identifier => pStartPermanentIdentifier
         ,p_comid                => pStartComid
         ,p_reachcode            => pStartReachcode
         ,p_measure              => pStartMeasure
         ,p_flowline             => obj_flowline
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for upstream navigation on top of headwater
      --------------------------------------------------------------------------
      IF obj_flowline.pt_percentage = 0 AND obj_flowline.uphydroseq = 0
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
             pStartLinkID     => obj_flowline.hydroseq
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.fromnode
            ,pCostThreshold   => pMaxDistanceKm
         );
         
      ELSE
         num_paths := traceInLight(
             pStartLinkID     => obj_flowline.hydroseq
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.fromnode
            ,pCostThreshold   => pMaxDistanceKm
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
         ,nhdplus_comid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,totaldist
         ,totaltime
         ,hydroseq
         ,levelpathid
         ,terminalpathid
         ,uphydroseq
         ,dnhydroseq
         ,lengthkm
         ,travtime
         ,nhdplus_region
         ,nhdplus_version
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplus_comid
         ,wbd_huc12
         ,catchment_featureid
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplus_comid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.totaldist
      ,a.totaltime
      ,a.hydroseq
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydroseq
      ,a.dnhydroseq
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          ROUND(nhdplus_navigation2.cmn_usgs_measures.usgs_length(
              MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
                  geom_segment  => a.shape
                 ,start_measure => a.tmeasure
                 ,end_measure   => a.fmeasure
              )
             ,0.05
             ,'UNIT=KM'
          ),6)
       ELSE
          a.lengthkm
       END AS lengthkm
      ,NULL AS travtime
      ,a.nhdplus_region
      ,a.nhdplus_version
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplus_comid
      ,a.wbd_huc12
      ,a.catchment_featureid
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
         ,aa.nhdplus_comid
         ,aa.reachcode
         -------------
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             obj_flowline.pt_measure
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             ROUND(aa.tmeasure - (bb.end_percentage * (aa.tmeasure - aa.fmeasure)),5)
          WHEN bb.end_percentage <> 0 
          THEN
             ROUND(aa.tmeasure - (bb.end_percentage * (aa.tmeasure - aa.fmeasure)),5)
          ELSE
             aa.tmeasure
          END AS tmeasure
         -------------
         ,bb.end_cost AS totaldist
         ,bb.end_percentage  AS totaltime -- stash for QA
         ,aa.hydroseq
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.nhdplus_region
         ,aa.nhdplus_version
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_nhdplus_comid
         ,aa.wbd_huc12
         ,aa.catchment_featureid
         ,aa.shape
         ,CASE
          WHEN bb.end_percentage <> 0 
          OR   aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_subpath bb
         ON
         aa.hydroseq = bb.end_link_id
      ) a;

      COMMIT;
      
   END navigate_up;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_down(
       pMainStem                 IN  BOOLEAN
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartComid               IN  INTEGER
      ,pStartReachcode           IN  VARCHAR2
      ,pStartMeasure             IN  NUMBER
      ,pMaxDistanceKm            IN  NUMBER
      ,pReturnCode               OUT NUMBER
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
         ,p_comid                => pStartComid
         ,p_reachcode            => pStartReachcode
         ,p_measure              => pStartMeasure
         ,p_flowline             => obj_flowline
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 30
      -- Check for upstream navigation on top of headwater
      --------------------------------------------------------------------------
      IF obj_flowline.pt_percentage = 0 AND obj_flowline.dnhydroseq = 0
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
             pStartLinkID     => obj_flowline.hydroseq
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.tonode
            ,pCostThreshold   => pMaxDistanceKm
         );
         
      ELSE
         num_paths := traceOutLight(
             pStartLinkID     => obj_flowline.hydroseq
            ,pStartPercentage => obj_flowline.pt_percentage
            ,pStartNodeID     => obj_flowline.tonode
            ,pCostThreshold   => pMaxDistanceKm
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
         ,nhdplus_comid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,totaldist
         ,totaltime
         ,hydroseq
         ,levelpathid
         ,terminalpathid
         ,uphydroseq
         ,dnhydroseq
         ,lengthkm
         ,travtime
         ,nhdplus_region
         ,nhdplus_version
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplus_comid
         ,wbd_huc12
         ,catchment_featureid
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplus_comid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.totaldist
      ,NULL AS totaltime
      ,a.hydroseq
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydroseq
      ,a.dnhydroseq
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          ROUND(nhdplus_navigation2.cmn_usgs_measures.usgs_length(
              MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
                  geom_segment  => a.shape
                 ,start_measure => a.tmeasure
                 ,end_measure   => a.fmeasure
              )
             ,0.05
             ,'UNIT=KM'
          ),6)
       ELSE
          a.lengthkm
       END AS lengthkm
      ,NULL AS travtime
      ,a.nhdplus_region
      ,a.nhdplus_version
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplus_comid
      ,a.wbd_huc12
      ,a.catchment_featureid
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
         ,aa.nhdplus_comid
         ,aa.reachcode
         -------------------------
         ------------------------- 
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             ROUND(aa.tmeasure - (bb.end_percentage * (aa.tmeasure - aa.fmeasure)),5)
          WHEN bb.end_percentage <> 1 
          THEN
             ROUND( aa.tmeasure - (bb.end_percentage * (aa.tmeasure - aa.fmeasure)),5)
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             obj_flowline.pt_measure
          ELSE
             aa.tmeasure
          END AS tmeasure
          -------------------------
          -------------------------
         ,bb.end_cost AS totaldist
         ,aa.hydroseq
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.nhdplus_region
         ,aa.nhdplus_version
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_nhdplus_comid
         ,aa.wbd_huc12
         ,aa.catchment_featureid
         ,aa.shape
         ,CASE
          WHEN bb.end_percentage <> 1 
          OR   aa.nhdplus_comid = obj_flowline.nhdplus_comid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_subpath bb
         ON
         aa.hydroseq = bb.end_link_id
      ) a;
     
      COMMIT;
      
   END navigate_down;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate_pp(
       pStartPermanentIdentifier IN  VARCHAR2
      ,pStartComid               IN  INTEGER
      ,pStartReachcode           IN  VARCHAR2
      ,pStartMeasure             IN  NUMBER
      ,pStopPermanentIdentifier  IN  VARCHAR2
      ,pStopComid                IN  INTEGER
      ,pStopReachcode            IN  VARCHAR2
      ,pStopMeasure              IN  NUMBER
      ,pReturnCode               OUT NUMBER
      ,pStatusMessage            OUT VARCHAR2
      ,pSessionID                IN OUT VARCHAR2
   )
   AS
      obj_flowstart flowline_rec;
      obj_flowstop  flowline_rec;
      obj_flowtemp  flowline_rec;
      num_paths     NUMBER;
      
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
         ,p_comid                => pStartComid
         ,p_reachcode            => pStartReachcode
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
         ,p_comid                => pStopComid
         ,p_reachcode            => pStopReachcode
         ,p_measure              => pStopMeasure
         ,p_flowline             => obj_flowstop
         ,p_return_code          => pReturnCode
         ,p_status_message       => pStatusMessage
      );
      
      IF pReturnCode <> 0
      THEN
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 40
      -- Alter start and stop depending on hydroseq
      --------------------------------------------------------------------------
      IF obj_flowstop.hydroseq > obj_flowstart.hydroseq
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
      -- Execute trace out light to fill temp table
      --------------------------------------------------------------------------
      num_paths := shortestPath(
          pStartLinkID     => obj_flowstart.hydroseq
         ,pStartPercentage => obj_flowstart.pt_percentage
         ,pStartNodeID     => obj_flowstart.fromnode
         ,pStopLinkID      => obj_flowstop.hydroseq
         ,pStopPercentage  => obj_flowstop.pt_percentage
      );
      
      IF num_paths < 0
      THEN
         pReturnCode    := num_paths;
         pStatusMessage := 'error from shortestPath';
         RETURN;
         
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 70
      -- Load the usual navigation table
      --------------------------------------------------------------------------
      INSERT INTO nhdplus_navigation2.tmp_navigation_results(
          objectid
         ,session_id
         ,permanent_identifier
         ,nhdplus_comid
         ,reachcode
         ,fmeasure
         ,tmeasure
         ,totaldist
         ,totaltime
         ,hydroseq
         ,levelpathid
         ,terminalpathid
         ,uphydroseq
         ,dnhydroseq
         ,lengthkm
         ,travtime
         ,nhdplus_region
         ,nhdplus_version
         ,reachsmdate
         ,ftype
         ,fcode
         ,gnis_id
         ,gnis_name
         ,wbarea_permanent_identifier
         ,wbarea_nhdplus_comid
         ,wbd_huc12
         ,catchment_featureid
         ,shape
      )
      SELECT
       nhdplus_navigation2.tmp_navigation_results_seq.NEXTVAL
      ,pSessionID
      ,a.permanent_identifier
      ,a.nhdplus_comid
      ,a.reachcode
      ,a.fmeasure
      ,a.tmeasure
      ,a.totaldist
      ,a.totaltime
      ,a.hydroseq
      ,a.levelpathid
      ,a.terminalpathid
      ,a.uphydroseq
      ,a.dnhydroseq
      ,CASE
       WHEN a.clip_flag = 1
       THEN
          ROUND(nhdplus_navigation2.cmn_usgs_measures.usgs_length(
              MDSYS.SDO_LRS.CLIP_GEOM_SEGMENT(
                  geom_segment  => a.shape
                 ,start_measure => a.tmeasure
                 ,end_measure   => a.fmeasure
              )
             ,0.05
             ,'UNIT=KM'
          ),6)
       ELSE
          a.lengthkm
       END AS lengthkm
      ,NULL AS travtime
      ,a.nhdplus_region
      ,a.nhdplus_version
      ,a.reachsmdate
      ,a.ftype
      ,a.fcode
      ,a.gnis_id
      ,a.gnis_name
      ,a.wbarea_permanent_identifier
      ,a.wbarea_nhdplus_comid
      ,a.wbd_huc12
      ,a.catchment_featureid
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
         ,aa.nhdplus_comid
         ,aa.reachcode
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowstop.nhdplus_comid
          THEN
             obj_flowstop.pt_measure
          ELSE
             aa.fmeasure
          END AS fmeasure
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowstart.nhdplus_comid
          THEN
             obj_flowstart.pt_measure
          ELSE
             aa.tmeasure
          END AS tmeasure
         ,bb.end_cost AS totaldist
         ,bb.end_percentage  AS totaltime -- stash for QA
         ,aa.hydroseq
         ,aa.levelpathid
         ,aa.terminalpathid
         ,aa.uphydroseq
         ,aa.dnhydroseq
         ,aa.lengthkm
         ,aa.nhdplus_region
         ,aa.nhdplus_version
         ,aa.reachsmdate
         ,aa.ftype
         ,aa.fcode
         ,aa.gnis_id
         ,aa.gnis_name
         ,aa.wbarea_permanent_identifier
         ,aa.wbarea_nhdplus_comid
         ,aa.wbd_huc12
         ,aa.catchment_featureid
         ,aa.shape
         ,CASE
          WHEN aa.nhdplus_comid = obj_flowstart.nhdplus_comid
          OR   aa.nhdplus_comid = obj_flowstop.nhdplus_comid
          THEN
             1
          ELSE
             0
          END AS clip_flag
         FROM
         nhdplus.nhdflowline_np21 aa
         JOIN
         nhdplus_navigation2.tmp_navigation_subpath bb
         ON
         aa.hydroseq = bb.end_link_id         
      ) a;
      
      --------------------------------------------------------------------------
      -- Step 80
      -- Update the total real length of the path
      --------------------------------------------------------------------------
      UPDATE nhdplus_navigation2.tmp_navigation_results a
      SET a.totaldist = (
         SELECT
         aa.balance 
         FROM (
            SELECT
             aaa.rowid AS rid
            ,SUM(aaa.lengthkm) OVER(PARTITION BY aaa.session_id ORDER BY aaa.hydroseq) AS balance
            FROM
            nhdplus_navigation2.tmp_navigation_results aaa
            WHERE
            aaa.session_id = pSessionID
         ) aa  
         WHERE
         a.rowid = aa.rid
      )
      WHERE
      a.session_id = pSessionID;
      
      --------------------------------------------------------------------------
      -- Step 90
      -- Grab the total cost of the path
      --------------------------------------------------------------------------
      COMMIT;
      
   END navigate_pp;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE navigate(
       pNavigationType           IN  VARCHAR2
      ,pStartPermanentIdentifier IN  VARCHAR2
      ,pStartComid               IN  INTEGER
      ,pStartReachcode           IN  VARCHAR2
      ,pStartMeasure             IN  NUMBER
      ,pStopPermanentIdentifier  IN  VARCHAR2
      ,pStopComid                IN  INTEGER
      ,pStopReachcode            IN  VARCHAR2
      ,pStopMeasure              IN  NUMBER
      ,pMaxDistanceKm            IN  NUMBER
      ,pReturnCode               OUT NUMBER
      ,pStatusMessage            OUT VARCHAR2
      ,pSessionID                IN OUT VARCHAR2
   )
   AS
      str_navigation_type VARCHAR2(4000 Char) := UPPER(pNavigationType);
      
   BEGIN
      
      --------------------------------------------------------------------------
      -- Step 10
      -- Check over incoming parameters
      --------------------------------------------------------------------------
      IF str_navigation_type NOT IN ('UT','UM','DD','DM','PP')
      THEN
         pReturnCode := -1;
         pStatusMessage := 'Invalid navigation type';
         RETURN;
      
      END IF;
      
      --------------------------------------------------------------------------
      -- Step 20
      -- Bump up the heap if we think this is going a long way
      -- There is a limit as to what we can give the user
      --------------------------------------------------------------------------
      IF pMaxDistanceKm > 100
      THEN
         MDSYS.SDO_NET.SET_MAX_JAVA_HEAP_SIZE(524288000);
         
      ELSIF pMaxDistanceKm > 1000
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
      -- Upstream with tribs
      --------------------------------------------------------------------------
      IF str_navigation_type = 'UT'
      THEN
         navigate_up(
             pMainstem                 => FALSE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartComid               => pStartComid
            ,pStartReachcode           => pStartReachcode
            ,pStartMeasure             => pStartMeasure
            ,pMaxDistanceKm            => pMaxDistanceKm
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
      
      --------------------------------------------------------------------------
      -- Step 50
      -- Downstream with divergences
      --------------------------------------------------------------------------   
      ELSIF str_navigation_type = 'DD'
      THEN
         navigate_down(
             pMainstem                 => FALSE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartComid               => pStartComid
            ,pStartReachcode           => pStartReachcode
            ,pStartMeasure             => pStartMeasure
            ,pMaxDistanceKm            => pMaxDistanceKm
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
      -- 
      --------------------------------------------------------------------------
      ELSIF str_navigation_type = 'UM'
      THEN
         navigate_up(
             pMainstem                 => TRUE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartComid               => pStartComid
            ,pStartReachcode           => pStartReachcode
            ,pStartMeasure             => pStartMeasure
            ,pMaxDistanceKm            => pMaxDistanceKm
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
      ELSIF str_navigation_type = 'DM'
      THEN
         navigate_down(
             pMainstem                 => TRUE
            ,pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartComid               => pStartComid
            ,pStartReachcode           => pStartReachcode
            ,pStartMeasure             => pStartMeasure
            ,pMaxDistanceKm            => pMaxDistanceKm
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
      ELSIF str_navigation_type = 'PP'
      THEN
         navigate_pp(
             pStartPermanentIdentifier => pStartPermanentIdentifier
            ,pStartComid               => pStartComid
            ,pStartReachcode           => pStartReachcode
            ,pStartMeasure             => pStartMeasure
            ,pStopPermanentIdentifier  => pStopPermanentIdentifier
            ,pStopComid                => pStopComid
            ,pStopReachcode            => pStopReachcode
            ,pStopMeasure              => pStopMeasure
            ,pReturnCode               => pReturnCode
            ,pStatusMessage            => pStatusMessage
            ,pSessionID                => pSessionID
         );
         
         IF pReturnCode <> 0
         THEN
            RETURN;
            
         END IF;
         
      END IF;

   END navigate;

END navigator2_main;
/

