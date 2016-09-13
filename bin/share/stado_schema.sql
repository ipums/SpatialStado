CREATE SCHEMA _stado;

CREATE VIEW _stado.pg_database AS
SELECT (string_to_array(datname, '__'))[2] datname, datdba, encoding, 
       datcollate, datctype, datistemplate, datallowconn, datconnlimit,
       datlastsysoid, datfrozenxid, dattablespace, datacl
  FROM pg_catalog.pg_database
 WHERE datname like '\__%';


