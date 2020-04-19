## abrir tunel SSH
ssh -i chavePrivada.pem -N -L DNS:PORTA Usuario@Host

## configuracoes possiveis de credenciais em SparkContext
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config.get('AWS_CREDENTIALS','AWS_ACCESS_KEY_ID'))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config.get('AWS_CREDENTIALS','AWS_SECRET_ACCESS_KEY'))
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
