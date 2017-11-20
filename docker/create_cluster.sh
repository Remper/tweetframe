CLUSTER_NAME=frame-cluster
az group create --name rs-$CLUSTER_NAME --location eastus
az aks create --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME --node-count 2 --generate-ssh-keys --node-vm-size Standard_E8s_v3
az aks get-credentials --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME
kubectl get nodes
