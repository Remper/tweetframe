POD_NAME="$(kubectl get pods | grep flink-jobmanager | cut -d " " -f1)"
echo "Name of the head pod: $POD_NAME"
#for name in $(kubectl get pods | grep flink | cut -d " " -f1)
#do
#  echo "Uploading for pod: $name"
#  kubectl cp /Users/remper/Projects/tweetframe/target/tweetframe-1.0-SNAPSHOT-jar-with-dependencies.jar $name:/opt/flink/lib/
#done
echo "Jar"
kubectl cp /Users/remper/Projects/tweetframe/target/tweetframe-1.0-SNAPSHOT-jar-with-dependencies.jar $POD_NAME:/opt/
echo "azure-wiki-pipeline.json"
kubectl cp /Users/remper/Projects/tweetframe/data/azure-wiki-pipeline.json $POD_NAME:/opt/
echo "azure-wiki.json"
kubectl cp /Users/remper/Projects/tweetframe/data/azure-wiki.json $POD_NAME:/opt/
echo "azure-tweets-pipeline.json"
kubectl cp /Users/remper/Projects/tweetframe/data/azure-tweets-pipeline.json $POD_NAME:/opt/
echo "azure-tweets.json"
kubectl cp /Users/remper/Projects/tweetframe/data/azure-tweets.json $POD_NAME:/opt/
