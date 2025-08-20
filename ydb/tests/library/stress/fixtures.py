# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

from ydb.tests.oss.ydb_sdk_import import ydb


class StressFixture:
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = [kikimr_driver_path()]

    def setup_cluster(self, **kwargs):
        extra_feature_flags = kwargs.get("extra_feature_flags", [])
        extra_feature_flags = list(extra_feature_flags)
        extra_feature_flags.append("enable_table_datetime64")
        extra_feature_flags.append("enable_parameterized_decimal")
        kwargs["extra_feature_flags"] = extra_feature_flags
        column_shard_config = kwargs.get("column_shard_config", {})
        column_shard_config["disabled_on_scheme_shard"] = False
        kwargs["column_shard_config"] = column_shard_config

        self.config = KikimrConfigGenerator(
            binary_paths=self.all_binary_paths,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.database = "/Root"
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.mon_endpoint = f"http://localhost:{self.cluster.nodes[1].mon_port}"

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database,
                endpoint=self.endpoint
            )
        )
        self.driver.wait(timeout=60)
        yield
        self.cluster.stop()
