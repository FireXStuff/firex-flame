from setuptools import setup, find_packages

setup(name='firex_flame',
      version="0.2",
      description='Core firex libraries',
      url='https://github.com/FireXStuff/firex-flame',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      zip_safe=True,
      install_requires=[
            "Flask==0.12.1",
            "Flask-AutoIndex",
            "python-socketio==2.0.0",
            "eventlet==0.21.0",
      ],
      package_data={
        'firex_flame': ['ui/*.html', 'ui/js/*.js', 'ui/img/*', 'ui/css/*.css'],
      },
      entry_points={
          'console_scripts': ['firex_flame = firex_flame.__main__:main', ],
          'firex_tracking_service': ['flame_launcher = firex_flame.launcher:FlameLauncher', ]
      },)
