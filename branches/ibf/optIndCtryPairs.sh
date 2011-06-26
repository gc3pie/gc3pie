#!/bin/bash
# Script to run optimizations for the below set of Ctry pairs. 

echo "Current folder: " $(pwd)
sleep 1

pathToModel=$1

echo pathToModel = $pathToModel

cat $pathToModel/base/input/parameters.in
echo Please check parameters.in file
sleep 1.5

cat $pathToModel/base/input/markovA.in
echo Please check markovA.in file
sleep 1.5

cat $pathToModel/base/input/markovB.in
echo Please check markovB.in file
sleep 1.5

Ctrys=( AU CH CA DE FR JP UK US )

for Ctry1 in ${Ctrys[@]}; do
  for Ctry2 in ${Ctrys[@]}; do

    echo "Ctry1: " $Ctry1
    echo "Ctry2: " $Ctry2

    if [ $Ctry1 == $Ctry2 ]; then
      echo "skipping diagnol"
      continue
    fi

    saveDir=${Ctry1}-${Ctry2}
    if [ -d $saveDir ]; then
      echo skipping: ${saveDir}
      continue
    fi

    read -p "Do you want to start job: ${saveDir} (y/n)? " answer
    if ! [[ ${answer} =~ "y" ]]; then
      exit
    fi

    sed -i "/Ctry                 /c\Ctry                 ${Ctry1}" $pathToModel/base/input/markovA.in
    sed -i "/Ctry                 /c\Ctry                 ${Ctry2}" $pathToModel/base/input/markovB.in

    mkdir -p ${saveDir}
    cp -R ${pathToModel}/base/ ${saveDir}/base/
    sleep 1
    cd ${saveDir}
    ~/workspace/fpProj/model/code/gPremiumScripts/gParaSearch.py -b base -x ~/workspace/fpProj/model/bin/forwardPremiumOut -C 16 -N -NP 60 -xVars EA -xVarsDom '0.5 0.9' -sv warning &
    cd ..
  done
done
