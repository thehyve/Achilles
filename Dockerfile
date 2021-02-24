# A Docker container to run the OHDSI/Achilles analysis tool
FROM thehyve/ohdsi-r-base:3.6.1

MAINTAINER Joris Borgdorff <joris@thehyve.nl>

RUN install.r \
      dplyr \
      readr \
    && installGithub.r \
      OHDSI/ParallelLogger \
    && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# Configure workspace
WORKDIR /opt/app
ENV PATH /opt/app:$PATH
VOLUME /opt/app/output

# Add project files to container
COPY . /opt/app/

# Install Achilles from source
RUN install.r . \
    && rm -rf /tmp/downloaded_packages/ /tmp/*.rds \
    && find /opt/app -mindepth 1 -not \( -wholename /opt/app/docker-run -or -wholename /opt/app/output \) -delete

# Define run script as default command
CMD ["/opt/app/docker-run"]
