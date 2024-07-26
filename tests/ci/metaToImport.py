class TestClass:
    def someFunction(self):
        import lsstDebug

        remoteConnection = lsstDebug.Info("ciutils").getConnection()
        connection = {
            "port": 4444,
            "addr": "127.0.0.1",
        }
        if connection != remoteConnection:
            raise RuntimeError("Failed to collect debug connection in imported class")
        return True
