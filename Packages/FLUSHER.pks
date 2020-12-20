CREATE OR REPLACE PACKAGE nhdplus_navigation2.flusher
AUTHID DEFINER
AS

   c_retention CONSTANT INTERVAL DAY TO SECOND := INTERVAL '5' HOUR;
   
   c_job_frequency CONSTANT VARCHAR2(255) := 'freq=DAILY;byhour=1,5,12,17,22';
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE flush_temp_tables(
      p_flush_time IN  TIMESTAMP DEFAULT NULL
   );
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE start_flush;
   
   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   PROCEDURE stop_flush;
   
END flusher;
/

