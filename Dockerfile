
ARG STACK_TAG="w_2025_41"
# For USDF, UID=17951
# For summit, UID=GID=73006?


FROM ghcr.io/lsst/scipipe:al9-${STACK_TAG}

ENV UID=73006
ENV GID=73006

ENV obs_lsst_branch="w.2025.41"
ENV drp_pipe_branch="w.2025.41"
ENV spectractor_branch="w.2025.41"
ENV atmospec_branch="w.2025.41"
ENV summit_utils_branch="w.2025.41"
ENV summit_extras_branch="w.2025.41"
ENV eo_pipe_branch="w_2025_12"
ENV ts_wep_branch="v14.7.0"
ENV donut_viz_branch="5de0465"
# no tags for TARTS yet, so default to main if not using deployment branch
ENV tarts_branch="main"

ENV USER=${USER:-saluser}
ENV WORKDIR=/opt/lsst/software/stack

USER root

# Workaround for centos
RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo && \
    sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo && \
    sed -i s/^mirrorlist=http/#mirrorlist=http/g /etc/yum.repos.d/*.repo

# Create user and group
RUN if [ ${UID} -eq 1000 ] && [ ${GID} -eq 1000 ]; then  \
        echo "Renaming lsst to saluser" && \
        usermod -l saluser lsst && \
        usermod -d /home/saluser -m saluser ; \
    else \
        groupadd --gid ${GID} saluser && \
        adduser -u ${UID} -m -g ${GID} -s /bin/bash saluser ; \
    fi

COPY checkout_repo.sh /home/saluser/.checkout_repo.sh

RUN chown saluser:saluser /home/saluser/.checkout_repo.sh && \
    chmod a+x /home/saluser/.checkout_repo.sh

RUN mkdir -p /repos && \
    chmod a+rw /repos && \
    chown saluser:saluser /repos

RUN yum install -y \
      nano \
      mesa-libGL-devel \
      rsync \
  && yum clean all \
  && rm -rf /var/cache/yum

RUN df -h

USER lsst

RUN source ${WORKDIR}/loadLSST.bash && \
    conda config --set solver libmamba && \
    conda install -y \
    -c conda-forge \
    # lsstts channel required for ts-ofc
    -c lsstts \
    rubin-env-rsp \
    astrometry \
    redis-py \
    batoid \
    danish \
    rubin-libradtran \
    ts-ofc \
    timm \
    peft \
    && conda clean -afy

USER saluser

RUN source ${WORKDIR}/loadLSST.bash && \
    pip install google-cloud-storage \
    lsst-efd-client \
    pytorch_lightning \
    easyocr \
    && rm -rf ~/.cache/pip

WORKDIR /repos

# Clone all repos
RUN git clone https://github.com/lsst/Spectractor.git && \
    git clone https://github.com/lsst/atmospec.git && \
    git clone https://github.com/lsst-sitcom/summit_utils.git && \
    git clone https://github.com/lsst-sitcom/summit_extras.git && \
    git clone https://github.com/lsst-sitcom/rubintv_production.git && \
    git clone https://github.com/lsst-ts/rubintv_analysis_service.git && \
    git clone https://github.com/lsst-ts/ts_wep.git && \
    git clone https://github.com/lsst-ts/donut_viz.git && \
    git clone https://github.com/PetchMa/TARTS.git && \
    git clone https://github.com/lsst-camera-dh/eo_pipe.git

# TODO: (DM-43475) Resync RA images with the rest of the summit.
RUN git clone https://github.com/lsst/obs_lsst.git && \
    git clone https://github.com/lsst/drp_pipe.git

WORKDIR /repos/obs_lsst

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${obs_lsst_branch} && \
    eups declare -r . -t saluser && \
    setup obs_lsst -t saluser && \
    SCONSFLAGS="--no-tests" scons

RUN df -h

WORKDIR /repos/drp_pipe

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${drp_pipe_branch} && \
    eups declare -r . -t saluser && \
    setup drp_pipe -t saluser && \
    scons version

WORKDIR /repos/Spectractor


RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${spectractor_branch} && \
    eups declare -r . -t saluser

WORKDIR /repos/atmospec


RUN df -h

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${atmospec_branch} && \
    eups declare -r . -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst -j && \
    setup sconsUtils -j && \
    setup spectractor -j -t saluser && \
    setup atmospec -j -t saluser && \
    eups list && \
    scons version

WORKDIR /repos/summit_utils


RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${summit_utils_branch} && \
    eups declare -r . -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst && \
    setup atmospec -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup sconsUtils && \
    scons version

WORKDIR /repos/summit_extras


RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${summit_extras_branch} && \
    eups declare -r . -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst && \
    setup atmospec -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup summit_extras -j -t saluser && \
    setup sconsUtils && \
    scons version


WORKDIR /repos/eo_pipe

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${eo_pipe_branch} && \
    eups declare -r . -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst && \
    setup atmospec -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup summit_extras -j -t saluser && \
    setup eo_pipe -j -t saluser && \
    setup sconsUtils && \
    scons version

WORKDIR /repos/rubintv_analysis_service

RUN df -h

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh main && \
    eups declare -r . -t saluser && \
    setup atmospec -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup summit_extras -j -t saluser && \
#    setup rubintv_production -j -t saluser && \
    setup rubintv_analysis_service -j -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst && \
    setup sconsUtils && \
    scons version

WORKDIR /repos/ts_wep

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${ts_wep_branch} && \
    eups declare -r . ts_wep ${ts_wep} -t saluser && \
    setup ts_wep -t saluser && \
    scons version

WORKDIR /repos/donut_viz

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${donut_viz_branch} && \
    eups declare -r . donut_viz ${ts_wep} -t saluser && \
    setup donut_viz -t saluser && \
    scons version

WORKDIR /repos/TARTS

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${tarts_branch} && \
    eups declare -r . tarts ${tarts} -t saluser && \
    setup tarts -t saluser

WORKDIR /repos/rubintv_production

COPY . /repos/rubintv_production


USER root
RUN chown -R ${UID}:${GID} /repos/rubintv_production

USER saluser

RUN git remote set-url origin https://github.com/lsst-sitcom/rubintv_production.git && \
    git config --local --unset http."https://github.com/".extraheader

RUN source ${WORKDIR}/loadLSST.bash && \
    eups declare -r . -t saluser && \
    setup atmospec -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup summit_extras -j -t saluser && \
    setup rubintv_production -j -t saluser && \
    setup lsst_distrib && \
    setup obs_lsst && \
    setup sconsUtils && \
    scons version

RUN df -h

ENV RUN_ARG="-v"
ENV OPENBLAS_NUM_THREADS=1
ENV GOTO_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV MKL_DOMAIN_NUM_THREADS=1
ENV MPI_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1
ENV NUMEXPR_MAX_THREADS=1
ENV MPLBACKEND=Agg

COPY startup.sh /repos/.startup.sh

USER root

RUN chown saluser:saluser /repos/.startup.sh && \
    chmod a+x /repos/.startup.sh && \
    chmod -R a+w /repos && \
    chmod a+rx /home/saluser && \
    rm -rf /home/saluser/.eups/_caches_ && \
    chmod a+w /home/saluser/.eups && \
    chmod a+rwx /tmp

RUN git config --system --add safe.directory /repos/obs_lsst && \
    git config --system --add safe.directory /repos/drp_pipe && \
    git config --system --add safe.directory /repos/Spectractor && \
    git config --system --add safe.directory /repos/atmospec && \
    git config --system --add safe.directory /repos/summit_utils && \
    git config --system --add safe.directory /repos/summit_extras && \
    git config --system --add safe.directory /repos/rubintv_production && \
    git config --system --add safe.directory /repos/eo_pipe && \
    git config --system --add safe.directory /repos/rubintv_analysis_service && \
    git config --system --add safe.directory /repos/ts_wep && \
    git config --system --add safe.directory /repos/donut_viz && \
    git config --system --add safe.directory /repos/TARTS

USER saluser
ENV USER=saluser
ENV SHELL=/bin/bash
ENV EUPS_USERDATA=/home/saluser/.eups
ENV MPLCONFIGDIR=/tmp


WORKDIR /repos/rubintv_production/scripts

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["/repos/.startup.sh"]
