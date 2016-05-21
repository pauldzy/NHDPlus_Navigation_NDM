CREATE OR REPLACE PACKAGE BODY nhdplus_navigation2.flusher
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE flush_temp_tables(
      p_flush_time IN  TIMESTAMP DEFAULT NULL
   )
   AS
      time_flush_time TIMESTAMP := p_flush_time;
      ary_guids       MDSYS.SDO_STRING2_ARRAY;

   BEGIN

      --------------------------------------------------------------------------
      -- Step 10
      -- Set flush time to 12 hours previous
      --------------------------------------------------------------------------
      IF time_flush_time IS NULL
      THEN
         time_flush_time := SYSTIMESTAMP - (12/24);

      END IF;

      --------------------------------------------------------------------------
      -- Step 20
      -- Check for any globalids to delete
      --------------------------------------------------------------------------
      SELECT
      a.session_id
      BULK COLLECT INTO ary_guids
      FROM
      nhdplus_navigation2.tmp_navigation_status a
      WHERE
      a.session_datestamp < time_flush_time;

      IF ary_guids IS NULL
      OR ary_guids.COUNT = 0
      THEN
         RETURN;
         
      END IF;

      --------------------------------------------------------------------------
      -- Step 30
      -- Delete the status records
      --------------------------------------------------------------------------
      DELETE FROM
      nhdplus_navigation2.tmp_navigation_status a
      WHERE
      a.session_id IN (SELECT * FROM TABLE(ary_guids));

      --------------------------------------------------------------------------
      -- Step 40
      -- Delete the catchment records
      --------------------------------------------------------------------------
      DELETE FROM
      nhdplus_navigation2.tmp_navigation_results a
      WHERE
      a.session_id IN (SELECT * FROM TABLE(ary_guids));

      --------------------------------------------------------------------------
      -- Step 50
      -- Commit the removals
      --------------------------------------------------------------------------
      COMMIT;

   END flush_temp_tables;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE start_flush
   AS
      num_results NUMBER;
      
   BEGIN
   
      SELECT 
      COUNT(*)
      INTO num_results
      FROM 
      user_scheduler_jobs a
      WHERE
      a.job_name = 'FLUSH_TEMP_TABLES';
      
      IF num_results > 0
      THEN
         stop_flush();
      
      END IF;
      
      DBMS_SCHEDULER.CREATE_JOB(
          job_name            => 'nhdplus_navigation2.FLUSH_TEMP_TABLES'
         ,job_type            => 'STORED_PROCEDURE'
         ,job_action          => 'nhdplus_navigation2.flusher.flush_temp_tables'
         ,number_of_arguments => 0
         ,start_date          => sysdate +1/24/59 -- sysdate + 1 minute
         ,repeat_interval     => 'freq=DAILY;interval=1'
         ,enabled             => TRUE
         ,auto_drop           => TRUE
         ,comments            => 'Flush Temp Tables'
      );

   END start_flush;

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE stop_flush
   AS
   BEGIN

      DBMS_SCHEDULER.DROP_JOB(
         job_name            => 'nhdplus_navigation2.FLUSH_TEMP_TABLES'
      );

   END stop_flush;

END flusher;
/

