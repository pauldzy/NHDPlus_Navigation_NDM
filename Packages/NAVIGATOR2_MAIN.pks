CREATE OR REPLACE PACKAGE nhdplus_navigation2.navigator2_main
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   header: NHDPlus v2.1 in WATERS SDO_NET Navigation Prototype
     
   - Build ID: DZBUILDIDDZ
   - TFS Change Set: DZTFSCHANGESETDZ
   
   This code was originally prototyped as a replacement for the existing NHDPlus 
   navigation logic in 2015.  However due to problems in the manner by which
   SDO_NET marshalls final results, the code is highly susceptible to Java
   heap errors which limit the results to navigations which can fit within 
   currently allocated heap limits.

   However, the code can and does function as an equivalent to the old navigation
   logic.  Smaller tests can be successfully run against both versions of 
   navigation for QA and comparative purposes.
   
   For more information see 
   https://docs.oracle.com/database/121/TOPOL/sdo_net_concepts.htm#TOPOL700
   
   */
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------
   
   TYPE flowline_rec IS RECORD(
       permanent_identifier VARCHAR2(40 Char)
      ,nhdplus_comid        INTEGER
      ,reachcode            VARCHAR2(14 Char)
      ,fmeasure             NUMBER
      ,tmeasure             NUMBER
      ,hydroseq             INTEGER
      ,pt_measure           NUMBER
      ,pt_percentage        NUMBER
      ,fromnode             INTEGER
      ,tonode               INTEGER
      ,uphydroseq           INTEGER
      ,dnhydroseq           INTEGER
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight_java(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
   LANGUAGE JAVA NAME 
   'navigator2_main.traceOutLight(oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER) return oracle.sql.NUMBER';
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight_java(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
   LANGUAGE JAVA NAME 
   'navigator2_main.traceInLight(oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER) return oracle.sql.NUMBER';
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight_java_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
   LANGUAGE JAVA NAME 
   'navigator2_main.traceOutLight_mainstem(oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER) return oracle.sql.NUMBER';
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceOutLight_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight_java_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER
   AS
   LANGUAGE JAVA NAME 
   'navigator2_main.traceInLight_mainstem(oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER) return oracle.sql.NUMBER';
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION traceInLight_mainstem(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pCostThreshold   IN  NUMBER
   ) RETURN NUMBER;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION shortestPath(
       pStartLinkID     IN  NUMBER
      ,pStartPercentage IN  NUMBER
      ,pStartNodeID     IN  NUMBER
      ,pStopLinkID      IN  NUMBER
      ,pStopPercentage  IN  NUMBER
   ) RETURN NUMBER
   AS
   LANGUAGE JAVA NAME 
   'navigator2_main.shortestPath(oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER,oracle.sql.NUMBER) return oracle.sql.NUMBER';

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   Procedure: nhdplus_navigation2.navigate

   Primary procedure for the execution of the navigation 2.0 logic.  The results of
   all navigation activities are stored in the NHDPLUS_NAVIGATION2.TMP_NAVIGATION_STATUS
   and NHDPLUS_NAVIGATION2.TMP_NAVIGATION_RESULTS tables using the session id 
   guuid.  Depending on the usage case, this table or views derived from the table
   may be registered to ArcSDE to channel outputs to Esri tools and clients.

   Parameters:

      pNavigationType - keyword to indicate the type of navigation desired.  Valid
      options include 'UM' for upstream mainstem navigation, 'UT' for upstream with 
      tributaries navigation, 'DM' for downstream mainstream navigation, 'DD' for 
      downstream with divergences navigation and 'PP' for point-to-point navigation.
      pStartPermanentIdentifier - start location on the network identified by the guuid
      NHDPlus permanent identifier of the flowline.
      pStartComid - start location on the network identified by the integer NHDPlus 
      comid of the flowline.
      pStartReachcode - start location on the network identified by string value of
      the NHDPlus reach code.
      pStartMeasure - the measure at which to begin navigation. Must be between 0 and 
      100 inclusive, or NULL.  A value of NULL means that the measure will be calculated 
      to be either the bottom or the top of the flowline. (e.g. depends on whether the 
      navigation type is upstream or downstream and whether it is a start or stop measure). 
      pStopPermanentIdentifier - for point-to-point navigation only, the stop location 
      on the network identified by the guuid NHDPlus permanent identifier of the flowline.
      pStopComid - for point-to-point navigation only, the stop location on the network 
      identified by an integer NHDPlus comid of the flowline.
      pStopReachcode - for point-to-point navigation only, the stop location on the 
      network identified by string value of the NHDPlus reach code.
      pStopMeasure - for point-to-point navigation only, the measure at which to cease 
      navigation. Must be between 0 and 100 inclusive, or NULL.  A value of NULL means that 
      the measure will be calculated to be either the bottom or the top of the flowline. 
      (e.g. depends on whether the navigation type is upstream or downstream and whether 
      it is a start or stop measure).
      pMaxDistanceKm - distance in KM to navigate. If pMaxDistance is not provided, then 
      the maximum distance to travel defaults to 50 km.
      pReturnCode - results code of the process, zero indicates success.
      pStatusMessage - status message giving additional information on any problems encountered
      during the process.
      pSessionID - optional session id guuid to use with the navigation process.  When chaining
      together logic calls within the database it is often useful to generate a single 
      guuid value for multiple database processes.  It is the responsibility of the user
      to make sure the guuid is unique.  If left NULL then this field will be populated
      with a new generated guuid value used in the current results.
      
   Notes:
   
   -  Unlike Navigation 1.0, point-to-point indexing does not require the start point to be upstream
      of the end point.  If they discovered to be reversed, the code swaps them around for you.
      
   -  Point-to-point indexing still abides by network flow and does not allow arbitrary traversal of
      the network (say down one river and up another) between points.  However the NHDPLUS_TOPONET
      schema also contains a so-called "bare" network which includes all flowlines (whether part of 
      the NHDPlus network or not) and without flow direction information.  With some minor changes
      this logic could be altered to use the bare network to allow such "strange" navigation.

   */
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
   );
   
END navigator2_main;
/

GRANT EXECUTE ON nhdplus_navigation2.navigator2_main TO public;

