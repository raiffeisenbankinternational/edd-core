ALTER TABLE command_response_log
 ADD COLUMN fx_total integer DEFAULT 0, 
 ADD COLUMN fx_remaining integer DEFAULT 0, 
 ADD COLUMN fx_error integer DEFAULT 0,
 ADD COLUMN fx_exception integer DEFAULT 0;


