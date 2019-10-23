### Bigquery encryption account
`
bq show --encryption_service_account
`

Grant Cloud KMS CryptoKey Encrypter/Decrypter role to BigQuery service account: bq-<>@bigquery-encryption.iam.gserviceaccount.com

### Create table with KMS
`
bq load --autodetect --source_format=NEWLINE_DELIMITED_JSON -destination_kms_key projects/<>/locations/<>/keyRings/d<>/cryptoKeys/<> ds_kms.test_table_kms_sample gs://cloud-samples-data/bigquery/us-states/us-states.json
`
