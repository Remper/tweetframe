POD_NAME="$(kubectl get pods | grep flink-jobmanager | cut -d " " -f1)"
echo "Name of the head pod: $POD_NAME"
kubectl cp /Users/remper/Projects/tweetframe/target/tweetframe-1.0-SNAPSHOT-jar-with-dependencies.jar $POD_NAME:/opt/
kubectl cp /Users/remper/Projects/tweetframe/data/azure-wiki-pipeline.json $POD_NAME:/opt/
kubectl cp /Users/remper/Projects/tweetframe/data/azure-wiki.json $POD_NAME:/opt/
