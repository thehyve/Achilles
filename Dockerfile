# A Docker container to run the OHDSI/Achilles analysis tool
FROM r-base:3.6.1

MAINTAINER Joris Borgdorff <joris@thehyve.nl>

# Install java, R and required packages and clean up.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      r-cran-devtools \
      r-cran-xml \
      r-cran-rjava \
      r-cran-rjson \
      r-cran-bit \
      r-cran-dbi \
      r-cran-pkgconfig \
      r-cran-bh \
      r-cran-plogr \
      r-cran-tibble \
      r-cran-snow \
      r-cran-fastmatch \
      r-cran-triebeard \
      r-cran-fansi \
      r-cran-utf8 \
      r-cran-zeallot \
      r-cran-urltools \
      r-cran-ellipsis \
      r-cran-purrr \
      r-cran-tidyselect \
      r-cran-vctrs \
      r-cran-vctrs \
      r-cran-dplyr \
      r-cran-devtools \
      r-cran-httr \
      r-cran-rjson \
      r-cran-stringr \
      r-cran-rjava \
      r-cran-dbi \
      r-cran-urltools \
      default-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && R CMD javareconf

RUN install.r  \
     rlang \
     BH \
     pillar \
     tibble \
  && installGithub.r \
      OHDSI/SqlRender \
      OHDSI/DatabaseConnectorJars \
      OHDSI/DatabaseConnector \
      OHDSI/ParallelLogger \
    && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

# Set default locale
RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
	&& locale-gen en_US.utf8 \
	&& /usr/sbin/update-locale LANG=en_US.UTF-8

ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8

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
CMD ["docker-run"]
