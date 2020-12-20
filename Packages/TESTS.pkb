CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.tests
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION prerequisites
   RETURN INTEGER
   AS
      int_check INTEGER;
      
   BEGIN
      
      FOR i IN 1 .. C_PREREQUISITES.COUNT
      LOOP
         SELECT 
         COUNT(*)
         INTO int_check
         FROM 
         user_objects a
         WHERE 
             a.object_name = C_PREREQUISITES(i) || '_TEST'
         AND a.object_type = 'PACKAGE';
         
         IF int_check <> 1
         THEN
            RETURN 1;
         
         END IF;
      
      END LOOP;
      
      RETURN 0;
   
   END prerequisites;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION version
   RETURN VARCHAR2
   AS
   BEGIN
      RETURN '{"TFS":' || C_TFS_CHANGESET || ','
      || '"JOBN":"' || C_JENKINS_JOBNM || '",'   
      || '"BUILD":' || C_JENKINS_BUILD || ','
      || '"BUILDID":"' || C_JENKINS_BLDID || '"}';
      
   END version;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   FUNCTION tests
   RETURN INTEGER
   AS
      int_flowline_count   PLS_INTEGER;
      int_catchment_count  PLS_INTEGER;
      int_return_code      PLS_INTEGER;
      str_status_message   VARCHAR2(255 Char);
      str_session_id       VARCHAR2(40 Char);

   BEGIN
   
      --------------------------------------------------------------------------
      -- Step 10
      -- UM test
      --------------------------------------------------------------------------
      navigator2_main.navigate(
          pSearchType               => 'UM'
         ,pStartPermanentIdentifier => NULL
         ,pStartNHDPlusID           => 22893569
         ,pStartReachCode           => NULL
         ,pStartHydroSequence       => NULL
         ,pStartMeasure             => 15.7403
         ,pStopPermanentIdentifier  => NULL
         ,pStopNHDPlusID            => NULL
         ,pStopReachCode            => NULL
         ,pStopHydroSequence        => NULL
         ,pStopMeasure              => NULL
         ,pMaxDistanceKm            => 15
         ,pMaxFlowTimeDay           => NULL
         ,pFlowlineCount            => int_flowline_count
         ,pCatchmentCount           => int_catchment_count
         ,pReturnCode               => int_return_code
         ,pStatusMessage            => str_status_message
         ,pSessionID                => str_session_id
      );
      
      DBMS_OUTPUT.PUT_LINE(int_flowline_count);
      
      RETURN 0;
      
   END tests;
   
END tests;
/

