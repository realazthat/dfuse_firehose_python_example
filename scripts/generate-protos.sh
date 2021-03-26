# See https://github.com/dfuse-io/graphql-over-grpc
#
#

set -exv

function protoc() {
  docker run --rm -v $PWD:$PWD -w $PWD znly/protoc:0.4.0 "$@"
}

PROJECT_PATH=$PWD

mkdir -p "$PROJECT_PATH/libs"
cd libs

rm -rf graphql-over-grpc
git clone https://github.com/dfuse-io/graphql-over-grpc.git
cd graphql-over-grpc
git checkout de32323f10fb83321c253bb5645f2448da7652fd
protoc graphql/graphql.proto --plugin=protoc-gen-grpc=/usr/bin/grpc_python_plugin --python_out=. --grpc_out=. -I.

mkdir -p "$PROJECT_PATH/src/python/graphql"
cp graphql/*.py "$PROJECT_PATH/src/python/graphql/."
################################################################################
cd $PROJECT_PATH/libs

rm -rf dfuse-io-proto
git clone https://github.com/dfuse-io/proto.git dfuse-io-proto
cd dfuse-io-proto
git checkout 4bca1300d6e75a04724925ef2c9341dfe50c5263
protoc dfuse/bstream/v1/bstream.proto --plugin=protoc-gen-grpc=/usr/bin/grpc_python_plugin --python_out=. --grpc_out=. -I.

mkdir -p "$PROJECT_PATH/src/python/dfuse"
cp -R dfuse/* "$PROJECT_PATH/src/python/dfuse/."

################################################################################
cd $PROJECT_PATH/libs
rm -rf proto-eosio
git clone https://github.com/dfuse-io/proto-eosio.git
cd proto-eosio
git checkout 6e8632d67a83918eb4cfc5e99ac51fc15c301b3a
protoc dfuse/eosio/codec/v1/codec.proto --plugin=protoc-gen-grpc=/usr/bin/grpc_python_plugin --python_out=. --grpc_out=. -I.

mkdir -p "$PROJECT_PATH/src/python/dfuse"
cp -R dfuse/* "$PROJECT_PATH/src/python/dfuse/."
