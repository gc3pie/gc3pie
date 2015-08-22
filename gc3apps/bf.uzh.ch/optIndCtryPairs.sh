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
nCtrys=${#Ctrys[@]}

nCtry1=0
for Ctry1 in ${Ctrys[@]}; do
  nCtry1=$((nCtry1+1))
  nCtry2=0
  for Ctry2 in ${Ctrys[@]}; do

    nCtry2=$((nCtry2+1))

    echo "Ctry1: " $Ctry1
    echo "Ctry2: " $Ctry2
    echo "nCtry1: " $nCtry1
    echo "nCtry2: " $nCtry2

    # if [ $Ctry1 == $Ctry2 ]; then
    #   echo "skipping diagnol"
    #   continue
    # fi

    if [ $nCtry1 -ge $nCtry2 ]; then
	echo "only lower triangular. skipping.."
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
    read -p "Execute:    /home/jonen/workspace/fpProj/model/code/gPremiumScripts/gParaSearch.py -b base -x /home/jonen/workspace/fpProj/model/bin/forwardPremiumOut -C 16 -N -NP 100 -xVars 'EA sigmaA' -xVarsDom '0.5 1.0 0.000 0.010' -sv info -t one4eachPair -e /home/jonen/workspace/fpProj/empirical/ --itermax 20 -yC 1.e-2"  --countryList "${Ctry1} ${Ctry2}" answer
    if ! [[ ${answer} =~ "y" ]]; then
      exit
    fi
    /home/jonen/workspace/fpProj/model/code/gPremiumScripts/gParaSearch.py -b base -x /home/jonen/workspace/fpProj/model/bin/forwardPremiumOut -C 16 -N -NP 100 -xVars 'EA sigmaA' -xVarsDom '0.5 1.0 0.000 0.010' -sv info -t one4eachPair -e /home/jonen/workspace/fpProj/empirical/ --itermax 20 -yC 1.e-2 --countryList "${Ctry1} ${Ctry2}"
     cd ..
  done
done
