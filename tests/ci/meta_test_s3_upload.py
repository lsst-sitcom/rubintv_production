from lsst.rubintv.production import MultiUploader


def main():
    s3Uploader = MultiUploader(allowNoRemote=True)
    assert isinstance(s3Uploader, MultiUploader)
    assert s3Uploader.localUploader is not None
    assert s3Uploader.remoteUploader is not None  # the mock now provides this


if __name__ == "__main__":
    main()
