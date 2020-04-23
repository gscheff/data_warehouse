# start redshift cluster with aws cli

REDSHIFT_CLUSTER_IDENTIFIER=redshift-cluster-1
REDSHIFT_DB=dev
REDSHIFT_DB_USER=awsuser
# REDSHIFT_DB_PASSWORD=passw0rd  # export env variable
REDSHIFT_PORT=5439
REDSHIFT_CLUSTER_TYPE=multi-node
REDSHIFT_NODE_TYPE=dc2.large
REDSHIFT_NUM_NODES=4
REDSHIFT_VPC_SECURITY_GROUP_ID=sg-09a19fe8b50b3626f

REDSHIFT_IAM_ROLE_ARN_1='arn:aws:iam::569126059190:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift'
REDSHIFT_IAM_ROLE_ARN_2='arn:aws:iam::569126059190:role/dwh_role'

aws redshift create-cluster \
    --db-name $REDSHIFT_DB \
    --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER \
    --cluster-type $REDSHIFT_CLUSTER_TYPE \
    --node-type $REDSHIFT_NODE_TYPE \
    --master-username $REDSHIFT_DB_USER \
    --master-user-password $REDSHIFT_DB_PASSWORD \
    --number-of-nodes $REDSHIFT_NUM_NODES \
    --publicly-accessible \
    --vpc-security-group-ids "$REDSHIFT_VPC_SECURITY_GROUP_ID" \
    --iam-roles "$REDSHIFT_IAM_ROLE_ARN_1" "$REDSHIFT_IAM_ROLE_ARN_2"
