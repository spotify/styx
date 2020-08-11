# Protos

Styx uses these grpc and protobuf definitions to build the client and communicate with Flyte.

The definitions are taken from the [flyteidl repo](https://github.com/lyft/flyteidl/tree/master/protos/flyteidl) and are built locally.

## Update protos
```
cd into this directory
$ chmod +x update_protos.sh
$ ./update_protos.sh
