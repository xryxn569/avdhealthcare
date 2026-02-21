CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-26767.bronze_dataset.departments` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://a2412-bucket-healthcare/landing/healthcare/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-26767.bronze_dataset.encounters` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://a2412-bucket-healthcare/landing/healthcare/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-26767.bronze_dataset.patients` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://a2412-bucket-healthcare/landing/healthcare/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-26767.bronze_dataset.providers` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://a2412-bucket-healthcare/landing/healthcare/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `healthcare-26767.bronze_dataset.transactions` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://a2412-bucket-healthcare/landing/healthcare/transactions/*.json']
);