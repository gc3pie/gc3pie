#!/bin/sh

sim_box=$1
shift 

# check tar
RES=`command tar --version 2>&1`
if [ $? -ne 0 ]; then
     echo "[tar command is not present]"
     echo $RES
     exit 1
else
     echo "[OK, tar command is present, continue...]"
fi

# Untar the input files in the
tar -xzvf input.tgz -C .
if [ $? -ne 0 ]; then
     echo "[untar failed]"
     exit 1
else
    echo "[OK, untar of the input archive command finished successfully, continue...]"
fi
# Remove input.tgz
rm input.tgz

# create the sym link to the topo directory
echo -n "Checking Input folder..."
if [ -d ./sim/_master/topo ]; then
    echo "[${PWD}/sim/_master/topo]"
elif [ -d ${HOME}/sim/_master/topo ]; then
    ln -s ${HOME}/sim/_master/topo ./sim/_master/topo
    if [ $? -ne 0 ]; then
        echo "[creating sim link to the available topo directory has failed]"
        exit 1
    else
        echo "[${HOME}/sim_master/topo]"
        echo "[OK, sym link to the available topo data has been created, continue...]"
    fi
else
    echo "[FAILED: no folder found in './sim/_master/topo' nor in '${HOME}/sim/_master/topo']"
    exit 1
fi

# change the root dir be used
sed -i -e "s|root=|root='${PWD}/'|g" ./src/TopoAPP/topoApp_complete.r
if [ $? -ne 0 ]; then
     echo "[sed the ROOT directory in topoApp_complete.r failed]"
     exit 1
else
    echo "[OK, changing the root directory in the topoApp_complete.r, continue...]"
fi

# copy parfile if it is present in the root directory
if [ -f ./parfile.r ]; then
        echo "[It seems that a different parfile has been passed to the application, replacing the current one...]"
        cp ./parfile.r ./src/TopoAPP/
        if [ $? -ne 0 ]; then
            echo "[Some problems copying the specified parfile.r occured, please check...]"
            exit 1
        fi
else
    echo "[No parameter file has been specified, using the one passed through the input.tgz file]"
fi

# change the box sequence to be used
sed -i -e "s/nboxSeq=/nboxSeq=$sim_box/g" ./src/TopoAPP/parfile.r
if [ $? -ne 0 ]; then
     echo "[sed the nboxSeq sequence failed]"
     exit 1
else
    echo "[OK, changing the nboxSeq has been done correctly, continue...]"
fi

# check R
RES=`command R --version 2>&1`
if [ $? -ne 0 ]; then
     echo "[failed]"
     echo $RES
     exit 1
else
     echo "[OK, R command is present, starting R script]"
fi
R CMD BATCH --no-save --no-restore ./src/TopoAPP/topoApp_complete.r
