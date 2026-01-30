# TODO

## Summary Generation

* Response summary should always be generated for proper logging and tracing.

* postgres-event and view store should setup DB by using register. We should not fill in manually DB parameters. Same is for opensearch. We need to try opening connection on startup in background. In tests and other places we should nowhere be parsing explicitly DB connections. I register something that needs postgres and this impl should take care . 


NOW: 
- SQL support multiple partitions for history table
- register method must accept second parameter optional with config. Add malli schema with propper documentation for each parameter
- 
- Dont do upsert in postgres view postgres-event
- Version 0 handling  sould also never happen
- rename parameter to query (defmethod get-snapshot [ctx query]) must be a map
- check we are not fetching env variables but using ctx 
