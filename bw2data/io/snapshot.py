from .. import Database, Weighting, Normalization, Weighting, databases, \
    weightings, normalizations, methods, Method, config
from .bw2package import BW2Package
import datetime
import os
import progressbar
import shutil
import tarfile
import tempfile


class BW2Snapshot(object):
    @classmethod
    def _move_to_tmpdir(cls, filepaths, tempdir):
        for fp in filepaths:
            os.rename(fp,
                os.path.join(tempdir, os.path.basename(fp))
            )

    @classmethod
    def _create_tarfile(cls, tempdir, filename):
        tar_fp = os.path.join(
            config.request_dir("snapshots"),
            filename
        )
        with tarfile.open(tar_fp, "w") as tf:
            tf.add(tempdir, "bw2snapshot")
        return tar_fp

    @classmethod
    def take(cls):
        """Take a snapshot of the current Brightway2 data directory. Includes the following objects:

        * Database
        * Weighting
        * Normalization
        * Method

        We can't load all the databases into memory and serialize to a giant JSON file, so instead we will just tar together a bunch of BW2Packages.

        .. warning: This will empty the current Database in-memory cache. Any changes you have made that haven't been `.write`-n will be lost.

        Returns a filepath of the snapshot file.

        """
        tempdir = tempfile.mkdtemp()
        snapshots_tmp_dir = config.request_dir("snapshots-tmp")

        try:
            filename = u"Brightway2.%s.snapshot" % datetime.datetime.now(
                ).strftime("%d-%B-%Y-%I-%M%p")
            use_cache = config.p.get(u"use_cache", False)
            config.cache = {}
            config.p[u"use_cache"] = False

            widgets = [
                progressbar.SimpleProgress(sep="/"), " (",
                progressbar.Percentage(), ') ',
                progressbar.Bar(marker=progressbar.RotatingMarker()), ' ',
                progressbar.ETA()
            ]
            pbar = progressbar.ProgressBar(
                widgets=widgets,
                maxval=len(databases) + 3
            ).start()

            norm_fp = BW2Package.export_objs(
                (Normalization(obj) for obj in normalizations),
                "normalizations.snapshot",
                folder="snapshots-tmp"
            )
            pbar.update(0)
            meth_fp = BW2Package.export_objs(
                (Method(obj) for obj in methods.list[:10]),
                "methods.snapshot",
                folder="snapshots-tmp"
            )
            pbar.update(1)
            weig_fp = BW2Package.export_objs(
                (Weighting(obj) for obj in weightings),
                "weightings.snapshot",
                folder="snapshots-tmp"
            )
            pbar.update(2)
            db_filepaths = []
            for index, name in enumerate(databases.list[:1]):
                db_filepaths.append(
                    BW2Package.export_obj(Database(name), folder="snapshots-tmp")
                )
                pbar.update(2 + index)
            pbar.finish()

            cls._move_to_tmpdir(
                [norm_fp, meth_fp, weig_fp] + db_filepaths,
                tempdir
            )
            tarfile = cls._create_tarfile(tempdir, filename)

            config.p[u"use_cache"] = use_cache
            return tarfile
        except:
            raise
        finally:
            shutil.rmtree(tempdir)
            shutil.rmtree(snapshots_tmp_dir)

