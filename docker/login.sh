POD_NAME="$(kubectl get pods | grep flink-jobmanager | cut -d " " -f1)"
echo "Name of the head pod: $POD_NAME"
kubectl exec -it $POD_NAME -- /bin/bash
