protoc --proto_path=$1 --java_out=../src/main/java/ $1/tensorflow/core/example/{example,feature}.proto
