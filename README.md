#Scylla

##Building Scylla

In addition to required packages by Seastar, the following packages are required by Scylla.

### Submodules
Scylla uses submodules, so make sure you pull the submodules first by doing:
```
git submodule init
git submodule update --recursive
```

### Building scylla on Fedora
Installing required packages:

```
sudo yum install yaml-cpp-devel lz4-devel zlib-devel snappy-devel jsoncpp-devel thrift-devel antlr3-tool antlr3-C++-devel libasan libubsan
```

## Building Fedora RPM

As a pre-requisite, you need to install [Mock](https://fedoraproject.org/wiki/Mock) on your machine:

```
# Install mock:
sudo yum install mock

# Add user to the "mock" group:
usermod -a -G mock $USER && newgrp mock
```

Then, to build an RPM, run:

```
./dist/redhat/build_rpm.sh
```

The built RPM is stored in ``/var/lib/mock/<configuration>/result`` directory.
For example, on Fedora 21 mock reports the following:

```
INFO: Done(scylla-server-0.00-1.fc21.src.rpm) Config(default) 20 minutes 7 seconds
INFO: Results and/or logs in: /var/lib/mock/fedora-21-x86_64/result
```

## Building Fedora-based Docker image

Build a Docker image with:

```
cd dist/docker
docker build -t <image-name> .
```

Run the image with:

```
docker run -i -p 9042:9042 -p 7001:7001 -p 10000:10000 -p 9160:9160 -p 7000:7000  -t <image name>
```
