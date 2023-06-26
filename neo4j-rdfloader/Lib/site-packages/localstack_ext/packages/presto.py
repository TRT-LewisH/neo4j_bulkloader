import os
from typing import List
from localstack.constants import MAVEN_REPO_URL
from localstack.packages import Package
from localstack.packages.core import ArchiveDownloadAndExtractInstaller
from localstack.utils.files import cp_r,save_file
from localstack_ext import config as ext_config
URL_PATTERN_PRESTO=f"{MAVEN_REPO_URL}/io/trino/trino-server/<version>/trino-server-<version>.tar.gz"
PRESTO_DEFAULT_VERSION='389'
PRESTO_VERSIONS=[PRESTO_DEFAULT_VERSION]
PRESTO_JVM_CONFIG='\n-server\n-Xmx1G\n-XX:-UseBiasedLocking\n-XX:+UseG1GC\n-XX:+UseGCOverheadLimit\n-XX:+ExplicitGCInvokesConcurrent\n-XX:+HeapDumpOnOutOfMemoryError\n-XX:+ExitOnOutOfMemoryError\n-XX:ReservedCodeCacheSize=150M\n-Duser.timezone=UTC\n-Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n-Dpresto-temporarily-allow-java8=true\n'
PRESTO_CONFIG_PROPS='\nnode.id=presto-master\nnode.environment=test\ncoordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=8080\nquery.max-memory=512MB\nquery.max-memory-per-node=512MB\n# query.max-total-memory-per-node=512MB\ndiscovery-server.enabled=true\ndiscovery.uri=http://localhost:8080\nprotocol.v1.alternate-header-name=Presto\n'
class PrestoInstaller(ArchiveDownloadAndExtractInstaller):
	def __init__(A,version):super().__init__(name='presto',version=version,extract_single_directory=True)
	def _get_install_marker_path(A,install_dir):return os.path.join(install_dir,'bin','launcher')
	def _get_download_url(A):return URL_PATTERN_PRESTO.replace('<version>',A.version)
	def is_installed(A):
		if not ext_config.BIGDATA_MONO_CONTAINER:return True
		return super().is_installed()
	def copy_into_container(J):
		F='iceberg.jar';E='config.properties';D='jvm.config';from localstack_ext.packages.hive import ICEBERG_JAR_URL as G;from localstack_ext.packages.spark import download_and_cache_jar_file as H
		if not ext_config.BIGDATA_MONO_CONTAINER:return
		B=presto_package.get_installer().get_installed_dir();A=os.path.join(B,'etc')
		if not os.path.exists(os.path.join(A,D)):save_file(os.path.join(A,D),PRESTO_JVM_CONFIG)
		if not os.path.exists(os.path.join(A,E)):save_file(os.path.join(A,E),PRESTO_CONFIG_PROPS)
		C=os.path.join(B,'lib');I=H(G)
		if not os.path.exists(os.path.join(C,F)):cp_r(I,os.path.join(C,F))
class PrestoPackage(Package):
	def __init__(A,default_version=PRESTO_DEFAULT_VERSION):super().__init__(name='Presto',default_version=default_version)
	def get_versions(A):return PRESTO_VERSIONS
	def _get_installer(A,version):return PrestoInstaller(version)
presto_package=PrestoPackage()