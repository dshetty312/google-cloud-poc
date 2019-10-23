### KMS keyring creation
`
gcloud kms keyrings create ds-kms-keyring --location us-east1
`

### KMS key creation
`
gcloud kms keys create ds-kms-key --location us-east1 --keyring ds-kms-keyring --purpose encryption
`

### KMS Keys list
`
gcloud kms keys list --location us-east1 --keyring ds-kms-keyring
`

### KMS encryption
`
gcloud kms encrypt   --location=us-east1  --keyring=ds-kms-keyring   --key=ds-kms-key   --plaintext-file=<> --ciphertext-file=<>
`

### KMS decryption
`
gcloud kms decrypt  --location=us-east1  --keyring=ds-kms-keyring   --key=ds-kms-key --plaintext-file=<> --ciphertext-file=<>
`
