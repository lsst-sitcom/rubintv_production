#!/bin/bash

umask 002


# Perform updates to all repos before script launch
# We start in rubintv_production/scripts and need to return there
cd ../

#
for directory in ${RA_PULL_DIRECTORIES}
do
  echo -e "\nSetting up ${directory}..."
  cd /repos/${directory}
  # git config --global --add safe.directory /repos/${directory}
  branch=$(git rev-parse --abbrev-ref HEAD)
  git fetch --all
  git checkout ${DEPLOY_BRANCH} && (git reset --hard origin/${DEPLOY_BRANCH}) || (git checkout ${branch} && git reset --hard origin/${branch})
  commit_info=$(git log -1 --pretty=format:"%h %s")
  echo -e "${directory} is at: ${commit_info}\n"
done

# Move back to the scripts directory
cd "${SCRIPTS_LOCATION:-/repos/rubintv_production/scripts}"

source ${WORKDIR}/loadLSST.bash

setup lsst_distrib

ALL_REPOS="obs_lsst drp_pipe Spectractor atmospec summit_utils summit_extras rubintv_production eo_pipe rubintv_analysis_service ts_wep donut_viz TARTS"

for REPO in ${ALL_REPOS}
do
    cd /repos/${REPO}
    setup -k -r .
    echo $REPO:
    # Why is this broken?
    # git show --oneline | head -n1
done

cd /repos/rubintv_production/scripts

python $RUN_ARG
