#!/usr/bin/env bash
protos=$(find . -name "*.proto")

for proto in $protos
do
	echo $proto
	pipenv run python -m grpc_tools.protoc -I"$(dirname $proto)" --python_out=autotest/grpc/proto_buffers --grpc_python_out=autotest/grpc/proto_buffers --proto_path=autotest/protos/entity $proto

done
echo "Adding ProtoBuffers output folder to PYTHONPATH, to enable imports in python"
pipenv run export $PYTHONPATH="$(pwd)/autotest/grpc/proto_buffers:$PYTHONPATH"
