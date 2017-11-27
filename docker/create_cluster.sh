CLUSTER_NAME=pipeline-cluster
az group create --name rs-$CLUSTER_NAME --location westeurope
az aks create --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME --node-count 2 --generate-ssh-keys --node-vm-size Standard_D64s_v3
az aks get-credentials --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME
kubectl get nodes
