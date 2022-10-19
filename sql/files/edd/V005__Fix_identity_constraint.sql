ALTER TABLE identity_store
DROP CONSTRAINT identity_store_pkey CASCADE,
ADD PRIMARY KEY(service_name, id); 

ALTER TABLE identity_store DROP CONSTRAINT unique_per_service 
