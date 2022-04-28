RubinTV Production
------------------

A set of libraries and scripts for running the backend services for `RubinTV <https://roundtable.lsst.codes/rubintv/>`_.

Each script in ``/scripts`` is an entry point for a Kubernetes daemon, designed to be run at the summit and serve a single "channel" for the RubinTV frontend.


Requirements
------------
The following requirements are in addition to everything contained in the `lsst_sitcom metapackage <https://github.com/lsst/lsst_sitcom/>`_.

The environment variable ``DAF_BUTLER_REPOSITORY_INDEX`` must be set in the running environment and point to a yaml file defining the butler repos.

The environment variable ``GOOGLE_APPLICATION_CREDENTIALS`` must be set and point to a secrets file with credentials for uploading to the Google Cloud Storage bucket.

``google.cloud.storage`` must be installed and available.

``lsst_efd_client`` must be installed and available.
