# bw2data

[![PyPI](https://img.shields.io/pypi/v/bw2data.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/bw2data.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/bw2data)][pypi status]
[![License](https://img.shields.io/pypi/l/bw2data)][license]

[![Tests](https://github.com/brightway-lca/brightway2-data/actions/workflows/python-test.yml/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/brightway-lca/bw2data/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi status]: https://pypi.org/project/bw2data/
[read the docs]: https://bw2data.readthedocs.io/
[tests]: https://github.com/brightway-lca/brightway2-data/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/brightway-lca/bw2data
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

This package provides tools for the management of inventory databases and impact assessment methods.
It is part of the [Brightway LCA framework](https://brightway.dev).
[Online documentation](https://docs.brightway.dev/) is available, and the source code is hosted on [GitHub brightway-lca organization](https://github.com/brightway-lca/brightway2-data).

Note that version 4.0 and higher are only compatible with Brightway 2.5, as described in the [changelog](https://github.com/brightway-lca/brightway2-data/blob/main/CHANGES.md).

## Installation

You can install _bw2data_ via [pip] from [PyPI]:

```console
$ pip install bw2data
```

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide][Contributor Guide].

## License

Distributed under the terms of the [BSD 3 Clause license][License],
_bw2data_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue][Issue Tracker] along with a detailed description.


<!-- github-only -->

[command-line reference]: https://bw2data.readthedocs.io/en/latest/usage.html
[License]: https://github.com/brightway-lca/bw2data/blob/main/LICENSE
[Contributor Guide]: https://github.com/brightway-lca/bw2data/blob/main/CONTRIBUTING.md
[Issue Tracker]: https://github.com/brightway-lca/bw2data/issues


## Building the Documentation

You can build the documentation locally by installing the documentation Conda environment:

```bash
conda env create -f docs/environment.yml
```

activating the environment

```bash
conda activate sphinx_bw2data
```

and [running the build command](https://www.sphinx-doc.org/en/master/man/sphinx-build.html#sphinx-build):

```bash
sphinx-build docs _build/html --builder=html --jobs=auto --write-all; open _build/html/index.html
```
