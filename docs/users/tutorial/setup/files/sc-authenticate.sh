#!/bin/bash

# OS_AUTH_URL is only for the Identity API served through Keystone.
export OS_AUTH_URL=https://cloud.s3it.uzh.ch:5000/v2.0

# With the addition of Keystone we have standardized on the term **tenant**
# as the entity that owns the resources.
#export OS_TENANT_ID=07149ca7c2584bd884fe085219b3a667
#export OS_TENANT_NAME="mpe.econ.uzh"
#export OS_PROJECT_NAME="mpe.econ.uzh"

echo -n "Please enter your ScienceCloud project name: "
read -r
export OS_TENANT_NAME="$REPLY"
export OS_PROJECT_NAME="$REPLY"

echo -n "Please enter your ScienceCloud username: "
read -r
export OS_USERNAME="$REPLY"

echo -n "Please enter your OpenStack Password: "
read -sr
export OS_PASSWORD="$REPLY"
unset REPLY

echo
