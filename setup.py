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
    version="0.11RC2",
    packages=packages,
    author="Chris Mutel",
    author_email="cmutel@gmail.com",
    license=open('LICENSE.txt').read(),
    # Use a fork of progressbar that support ipython notebooks
    dependency_links = ['https://github.com/fnoble/python-progressbar/tarball/master#egg=progressbar-2.4'],
    install_requires=[
        "brightway2",
        "colorama",
        "docopt",
        "lxml",
        "numpy",
        "progressbar>=2.4",
        "requests>=1.1.0",
        "scipy",
        "stats_arrays",
        "voluptuous",
    ],
    url="https://bitbucket.org/cmutel/brightway2-data",
    long_description=open('README.rst').read(),
    scripts=["bw2data/bin/bw2-uptodate.py"],
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
        'Programming Language :: Python :: 2 :: Only',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: Scientific/Engineering :: Visualization',
    ],)
