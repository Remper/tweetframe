CLUSTER_NAME=pipeline-cluster
az group create --name rs-$CLUSTER_NAME --location eastus
az aks create --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME --agent-count 5 --generate-ssh-keys --agent-vm-size Standard_D16s_v3
az aks get-credentials --resource-group rs-$CLUSTER_NAME --name $CLUSTER_NAME
kubectl get nodes
