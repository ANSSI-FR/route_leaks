#!/usr/bin/env bash

usage(){
    echo "git pull route_leaks project into docker container to run test in a clean environment"
    echo "usage: ./docker_test.sh [-k] [-b branch]"
    echo "option -k should be use in case docker image should be kept at the end of the script (removed otherwise)"
    echo "option -b should be use to clone another branch than master"
}

keep=0
branch=master
while getopts "hkb:" arg
do
    case ${arg} in
    h)
        usage
        exit
        ;;
    k)
        keep=1
        ;;
    b)
        branch=${OPTARG}
        ;;
    *)
        echo "Unrecognized option: ${arg}"
        exit
    esac
done

if [ -z "$(docker images |grep python_dev_base)" ]
then
    docker build -t python_dev_base docker_base
fi


docker ps -a | grep leaks_test_image | awk '{print $1 }' | xargs docker rm 2> /dev/null
docker rmi leaks_test_image

git clone --recursive -b ${branch} -l .. tmp_leaks

docker build -t leaks_test_image .

if [ ${keep} = 0 ]
then
    docker rmi leaks_test_image
fi

rm -rf tmp_leaks
