--- ##### LCPR SPRINT 5  OPERATIONAL DRIVERS - TICKET REITERATIONS #####

--- Initial steps

WITH
  
 parameters as (
 SELECT date_trunc('month', date('2023-02-01')) --- Input month you wish the code run for
 )

, fmc_table as ( --- For now, the fixed table is taken, but the code is designed to run using the fmc table
SELECT
  month, 
   B_Final_TechFlag, 
   B_FMCSegment, 
   B_FMCType, 
   E_Final_Tech_Flag, 
   E_FMCSegment, 
   E_FMCType, 
   )
