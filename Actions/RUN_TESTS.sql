SHOW ERROR;

DECLARE
   l_num_errors PLS_INTEGER;

BEGIN

   SELECT
   COUNT(*)
   INTO l_num_errors
   FROM
   user_errors a
   WHERE
   a.name LIKE 'NAVIGATOR2%';

   IF l_num_errors <> 0
   THEN
      RAISE_APPLICATION_ERROR(-20001,'COMPILE ERROR');

   END IF;

   l_num_errors := nhdplus_navigation2.tests.tests();

   IF l_num_errors <> 0
   THEN
      RAISE_APPLICATION_ERROR(-20001,'NHDPLUS NAVIGATION20 TEST ERROR');

   END IF;

END;
/

