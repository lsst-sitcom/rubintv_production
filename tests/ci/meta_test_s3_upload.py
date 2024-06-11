from lsst.rubintv.production import MultiUploader


def main():
    s3Uploader = MultiUploader()
    assert isinstance(s3Uploader, MultiUploader)
    assert s3Uploader.localUploader is not None
    assert s3Uploader.remoteUploader is None

    # TODO: add tests for the actual upload here


if __name__ == "__main__":
    main()
