docker run --name stargate \
  -p 8080:8080 -p 8081:8081 \
  -p 8082:8082 -p 127.0.0.1:9042:9042 \
  -e CLUSTER_NAME=stargate \
  -e CLUSTER_VERSION=6.8 \
  -e DEVELOPER_MODE=true \
  -e DSE=1 \
  stargateio/stargate-dse-68:v1.0.52
