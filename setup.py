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
  version="0.8.2",
  packages=packages,
  author="Chris Mutel",
  author_email="cmutel@gmail.com",
  license=open('LICENSE.txt').read(),
  install_requires=["voluptuous", "progressbar", "numpy", "lxml", "scipy"],
  url="https://bitbucket.org/cmutel/brightway2-data",
  long_description=open('README').read(),
)
