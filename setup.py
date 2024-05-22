# Need fastentrypoints to monkey patch setuptools for faster console_scripts
# noinspection PyUnresolvedReferences
import fastentrypoints
from setuptools import setup, find_packages
import versioneer

setup(name='firex_flame',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='FireX event processor and web server.',
      url='https://github.com/FireXStuff/firex-flame',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      zip_safe=True,
      install_requires=[
            "Flask",
            "Flask-AutoIndex",
            "Werkzeug",

            # Be very careful changing socketio/engineio versions, as even middle numbers have included breakages in the past.
            # Always test version changes of socketio/engineio against the target UI version of socketio-client.
            "python-socketio==5.3.0",

            'firexapp',
            # Middle UI version number indicates expected server API version.
            # e.g 0.20.x means this server adheres to contract version 0.20 between UI and server.
            "firex_flame_ui<0.30",
            "requests",
            "beautifulsoup4",
            "paramiko",
            "gevent-websocket",
            "gevent",
            "importlib-resources",
            "jsonpath-ng",

            # pin to debug seg fail in Open Source CI.
            # "setuptools==69.5.1",

            "zope.interface==6.3",
            "zipp==3.18.1",
            "bcrypt==4.1.2",
            "cryptography==42.0.5",
            "rapidfuzz==3.8.1",
      ],
      package_data={
        'firex_flame': ['templates/*.html'],
      },
      entry_points={
          'console_scripts': ['firex_flame = firex_flame.__main__:main'],
          'firex_tracking_service': ['flame_launcher = firex_flame.launcher:FlameLauncher', ],
      },)
