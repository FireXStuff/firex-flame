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
            "Flask==1.0.2",
            "Flask-AutoIndex",
            "python-socketio==4.1.0",
            'firexapp',
            "eventlet==0.21.0",
            # Middle UI version number indicates expected server API version.
            # e.g 0.20.x means this server adheres to contract version 0.20 between UI and server.
            "firex_flame_ui<0.21",
            "requests",
            "beautifulsoup4",
      ],
      package_data={
        'firex_flame': ['templates/*.html'],
      },
      entry_points={
          'console_scripts': ['firex_flame = firex_flame.__main__:main',
                              'flame_dump = firex_flame.event_file_processor:dumper_main'],
          'firex_tracking_service': ['flame_launcher = firex_flame.launcher:FlameLauncher', ],
      },)
