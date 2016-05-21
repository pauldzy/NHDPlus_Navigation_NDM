CREATE OR REPLACE PACKAGE nhdplus_toponet.network_builder
AUTHID CURRENT_USER
AS

   -----------------------------------------------------------------------------
   -----------------------------------------------------------------------------
   /*
   header: NHDPlus v2.1 in Waters Topologies and Network Data Models
     
   - Build ID: DZBUILDIDDZ
   - TFS Change Set: DZTFSCHANGESETDZ
   
   Logic to build and maintain NHDPlus derived topologies and network data models.
   
   For more information see the Oracle Spatial documentation at
   
   https://docs.oracle.com/database/121/TOPOL/toc.htm
      
   */
   ----------------------------------------------------------------------------
   ----------------------------------------------------------------------------

   --------------------------------------------------------------------------------
   --------------------------------------------------------------------------------
   PROCEDURE plusflowline_builder;
   
   --------------------------------------------------------------------------------
   --------------------------------------------------------------------------------
   PROCEDURE bareflowline_builder;
   
END network_builder;
/

