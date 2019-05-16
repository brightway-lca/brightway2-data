from setuptools import setup
import os

packages = []
root_dir = os.path.dirname(__file__)
if root_dir:
    os.chdir(root_dir)

for dirpath, dirnames, filenames in os.walk('bw2data'):
    # Ignore dirnames that start with '.'
    if '__init__.py' in filenames:
        pkg = dirpath.replace(os.path.sep, '.')
        if os.path.altsep:
            pkg = pkg.replace(os.path.altsep, '.')
        packages.append(pkg)

setup(
    name='bw2data',
    version="3.5",
    packages=packages,
    author="Chris Mutel",
    author_email="cmutel@gmail.com",
    license="3-clause BSD",
    install_requires=[
        "appdirs",
        "bw2parameters",
        "docopt",
        "eight",
        "fasteners",
        "future",
        "lxml",
        "numpy",
        "peewee>=3",
        "psutil",
        "pyprind",
        "requests>=1.1.0",
        "scipy",
        "stats_arrays",
        "unicodecsv",
        "voluptuous",
        "whoosh",
        "wrapt",
    ],
    url="https://bitbucket.org/cmutel/brightway2-data",
    long_description=open('README.rst').read(),
    description=('Tools for the management of inventory databases '
                 'and impact assessment methods. Part of the Brightway2 LCA Framework'),
    entry_points = {
        'console_scripts': [
            'bw2-uptodate = bw2data.bin.bw2_uptodate:main',
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Scientific/Engineering :: Visualization',
    ],)
