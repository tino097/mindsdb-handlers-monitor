-- Create a new user for sample data
CREATE USER sampleuser IDENTIFIED BY SamplePass123;

-- Grant necessary privileges
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE MATERIALIZED VIEW TO sampleuser;
GRANT UNLIMITED TABLESPACE TO sampleuser;

-- Grant additional privileges for development
GRANT CREATE SESSION TO sampleuser;
GRANT CREATE TABLE TO sampleuser;
GRANT CREATE SEQUENCE TO sampleuser;
GRANT CREATE TRIGGER TO sampleuser;

EXIT;
