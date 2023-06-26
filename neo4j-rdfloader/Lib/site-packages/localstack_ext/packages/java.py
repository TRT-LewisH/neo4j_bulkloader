_A='java'
import glob,logging,os
from typing import List
from localstack import config
from localstack.packages import InstallTarget,Package
from localstack.utils.http import download
from localstack.utils.run import is_command_available,run
from localstack_ext.packages import OSPackageInstaller
LOG=logging.getLogger(__name__)
DEFAULT_JAVA_VERSION='11'
JAVA_VERSIONS=['8','11']
JAVA_11_HOME=os.environ.get('JAVA_11_HOME')or'/usr/lib/jvm/java-11'
JAVA_8_HOME=os.environ.get('JAVA_8_HOME')or'/usr/lib/jvm/java-8'
class JavaPackageInstaller(OSPackageInstaller):
	def __init__(A,version):super().__init__(_A,version)
	def is_installed(B):A=B.get_java_home();return A and os.path.exists(os.path.join(A,'bin',_A))
	def _debian_get_install_dir(A,target):return A._get_jvm_install_dir()
	def _debian_get_install_marker_path(A,install_dir):return os.path.join(install_dir,'bin',_A)
	def _debian_packages(A):
		if A.version==DEFAULT_JAVA_VERSION:return[]
		return[f"adoptopenjdk-{A.version}-hotspot"]
	def _debian_prepare_install(D,target):
		C='add-apt-repository';A=os.path.join(config.TMP_FOLDER,'adoptopenjdk.key');B='https://adoptopenjdk.jfrog.io/adoptopenjdk'
		if D.version!=DEFAULT_JAVA_VERSION and not os.path.exists(A):
			download(f"{B}/api/gpg/key/public",A);run(['apt-key','add',A])
			if not is_command_available(C):run(['apt','install','-y','software-properties-common'])
			run([C,'-y',f"{B}/deb/"])
		super()._debian_prepare_install(target)
	def _post_process(A,target):
		B=A._get_jvm_install_dir();C=glob.glob(f"/usr/lib/jvm/*jdk-{A.version}-*")[0]
		if not os.path.exists(B):run(['ln','-s',C,B])
	def _get_jvm_install_dir(A):return A.get_java_home()
	def get_java_home(A):
		if A.version=='8':return JAVA_8_HOME
		if A.version=='11':return JAVA_11_HOME
		return f"/usr/lib/jvm/java-{A.version}"
class JavaPackage(Package):
	def __init__(A,default_version=DEFAULT_JAVA_VERSION):super().__init__(name='Java',default_version=default_version)
	def get_versions(A):return JAVA_VERSIONS
	def _get_installer(A,version):return JavaPackageInstaller(version)
java_package=JavaPackage()