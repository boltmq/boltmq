#!/bin/bash

WORKSPACE=$(cd $(dirname $0)/; pwd)
cd $WORKSPACE

module=target/smartgo
smartgo=smartgo
stgregistry=stgregistry
stgbroker=stgbroker
stgclient=stgclient


function version() {
    cd $WORKSPACE/$stgregistry/start
    version=`./control.sh build`
    echo $version
}

function pack() {
    initialize
    registry
    broker
    topic
    tps
    producer
    consumer
    build-all
}

function initialize() {
    rm -rf $module
    mkdir -p $module
    cd $module
    mkdir -p $stgregistry $stgbroker $stgclient
    cd $WORKSPACE
}


function registry() {
    cd $WORKSPACE/stgregistry/start
    app=registry
    version=`./control.sh build`
    registryPkg=$app-$version.tar.gz
    ./control.sh pack
    cp $registryPkg $WORKSPACE/$module/$stgregistry/
    echo -n "build $registryPkg successful ... "
    echo -e "\n"
}

function broker() {
    cd $WORKSPACE/stgbroker/start
    app=broker
    version=`./control.sh build`
    brokerPkg=$app-$version.tar.gz
    ./control.sh pack
    cp $brokerPkg $WORKSPACE/$module/$stgbroker/
    echo -n "build $brokerPkg successful ... "
    echo -e "\n"
}

function topic() {
    cd $WORKSPACE/example/stgclient/producer/sync/topic/
    app=topic
    pkg=create-topic
    rm -rf $app
    go get ./...
    go build
    cp $app $pkg
    mv $pkg $WORKSPACE/$module/$stgclient/
    echo -n "build $pkg successful ... "
    echo -e "\n"
}

function tps() {
    cd $WORKSPACE/example/stgclient/producer/sync/tps/
    app=tps
    pkg=producer-sync-tps
    rm -rf $app
    go get ./...
    go build
    cp $app $pkg
    mv $pkg $WORKSPACE/$module/$stgclient/
    echo -n "build $pkg successful ... "
    echo -e "\n"
}

function producer() {
    cd $WORKSPACE/example/stgclient/producer/sync/simple/
    app=simple
    pkg=producer-sync
    rm -rf $app
    go get ./...
    go build
    cp $app $pkg
    mv $pkg $WORKSPACE/$module/$stgclient/
    echo -n "build $pkg successful ... "
    echo -e "\n"
}

function consumer() {
    cd $WORKSPACE/example/stgclient/consumer/push/
    app=push
    pkg=consumer-push
    rm -rf $app
    go get ./...
    go build
    cp $app $pkg
    mv $pkg $WORKSPACE/$module/$stgclient/
    echo -n "build $pkg successful ... "
    echo -e "\n"
}

function build-all() {
    echo -e "\n\n"
    cd $WORKSPACE/bin/
    app=os.sh
    cp $app $WORKSPACE/$module/

    cd $WORKSPACE/$stgregistry/start
    version=`./control.sh build`

    cd $WORKSPACE/target
    packName=$smartgo-$version.tar.gz
    tar -zcvf $packName $smartgo
    echo -n "build $packName successful ... "
    echo -e "\n"
}


function help() {
    echo "$0 pack|version"
}


if [ "$1" == "" ]; then
    help
elif [ "$1" == "version" ];then
    version
elif [ "$1" == "pack" ];then
    pack
else
    help
fi
