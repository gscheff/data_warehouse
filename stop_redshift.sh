# stop redshift

REDSHIFT_CLUSTER_IDENTIFIER=redshift-cluster-1

aws redshift delete-cluster \
    --cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER \
    --skip-final-cluster-snapshot
