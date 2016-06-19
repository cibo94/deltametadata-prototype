#!/usr/bin/python3
import os
import re
import sys
import urllib.request
from argparse import ArgumentParser, ArgumentTypeError
from datetime import datetime, timedelta
from glob import glob
from http.client import IncompleteRead
from io import StringIO

from dnf_zsync import PluginImpl

ERROR_404 = True


class CaptureStdOutContext(list):
    " captures std output of any block of code inside with statement "

    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


def date_range(_f, _t, _s=1):
    " creates generator through range of dates with certain step "
    assert _f <= _t, "cannot go backwards: {} -> {}".format(_f, _t)
    while _f <= _t:
        yield _f
        _f += timedelta(days=_s)


def meta_data(_from, _to, step=1,
              cloud_url="http://dmd-deltametadata.rhcloud.com/backup/",
              log_file=sys.stdout):
    """
    Return MetaData generator for date range and certain cloud
    generator is containing lambda with cache_dir parameter, because
    you can change the output path/test it again/...
    """

    def _download(repomd, url):
        class __xml_gz:
            """
            this contains file name, url from which this file was downloaded
            raw data (not written to disk), size and downloaded size
            which are the same
            """

            def __init__(self, file_name, url, data):
                self.file_name = file_name
                self.url = url
                self.data = data
                self.size = len(self.data)
                self.downloaded_size = self.size

            def write(self, cache_dir):
                " This writes data into file in cache_dir "
                with open(cache_dir + "/" + self.file_name, 'wb') as output:
                    output.write(self.data)

        for loc in re.finditer(
                r"<location href=\"repodata/(.*\.gz)\"",
                repomd.data):
            try:
                tries = 10
                while tries:
                    with urllib.request.urlopen(url + loc.group(1)) as url_file:
                        try:
                            yield __xml_gz(loc.group(1), url, url_file.read())
                            break
                        except IncompleteRead as ex:
                            print("Try #{}: {}".format(tries, str(ex)))
                            tries -= 1
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    if ERROR_404:
                        print(str(e))
                else:
                    raise e
                continue

    def _sync(cache_dir, plugin):
        class __xml_gz:
            """
            this contains file name, path to repodata dir,
            total size downloaded and total size used from local file
            """

            def __init__(self, cache_dir, match, _f):
                self.file_name = _f
                self.path = cache_dir + "/repodata/"
                self.downloaded_size = int(match.group(2))
                self.local_size = os.path.getsize(self.file_name)

        # replacing 'stdout' so we can get output of zsync
        with CaptureStdOutContext() as zsync_output:
            try:
                plugin.sync_metadata(cache_dir)
            except Exception as ex:
                print(str(ex), file=sys.stderr)
        with open("zsync.log", "a") as log:
            print("\n".join(zsync_output) + "\nEND OF ZSYNC LOG\n\n", file=log)
        # every file that was synchronized should be in output of zsync
        # so we can get how much data was downloaded or used from local file
        for _f in glob(cache_dir + r"/repodata/*"):
            match = re.search(
                os.path.basename(
                    _f) + r"(?=[\w\W]*?used\s(\d+)\slocal,\sfetched\s(\d+))",
                "\n".join(zsync_output))
            if match:
                yield __xml_gz(cache_dir, match, _f)

    class MetaData:
        """
        main class of the metadata containing
        generators of .xml.gz files
        """

        def __init__(self, cache_dir, url, date, repomd,
                     down_xml_gzs, sync_xml_gzs):
            self.url = url
            self.cache_dir = cache_dir
            self.date = date
            # just as shortcut
            self.date_str = date.strftime("%Y-%m-%d")
            self.down_xml_gzs = down_xml_gzs
            self.sync_xml_gzs = sync_xml_gzs
            self.repomd = repomd

    class __repo_md:

        def __init__(self, data):
            self.data = data

        def write(self, cache_dir):
            " if you need to specify different directory for repomd ... "
            with open(cache_dir + "/repomd.xml", "w") as _f:
                _f.write(self.data)

    for date in date_range(_from, _to, step):
        i = 0
        while True:
            try:
                date_str = date.strftime("%Y-%m-%d")
                base_url = cloud_url + date_str + "/" + str(i) + "/"
                url = cloud_url + date_str + "/" + str(i) + "/repomd.xml"
                repo_md_url = urllib.request.urlopen(url)
                if re.match(r".*\/xml", repo_md_url.headers['Content-Type']):
                    repomd_obj = __repo_md(repo_md_url.read().decode('utf-8'))
                    # wrapp constructor so only you have to specify cache_dir
                    yield lambda cache_dir: MetaData(
                        cache_dir, url, date, repomd_obj,
                        _download(repomd_obj, base_url),
                        _sync(cache_dir, PluginImpl(base_url, True))
                    )
                    i += 1
            except Exception as e:
                if ERROR_404:
                    print(str(e), url)
                break


def cleanup_cachedir(s):
    try:
        if s[-1] == "/":
            os.makedirs(s + "repodata")
            return s + ""
        else:
            os.makedirs(s + "/repodata")
            return s + "/"
    except FileExistsError:
        from shutil import rmtree
        rmtree(s, ignore_errors=False, onerror=None)
        return cleanup_cachedir(s)


def parse(argparser):
    def valid_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            raise ArgumentTypeError("Not a valid date: '{0}'.".format(s))

    def unsinged(s):
        cnvrtd = int(s)
        if cnvrtd < 0:
            raise ArgumentTypeError(
                "Step has to be unsigned: '{0}'.".format(s)
            )
        return cnvrtd

    def path(s):
        if not os.path.exists(s):
            raise ArgumentTypeError(
                "Path '{}' does not exists!".format(s)
            )
        return cleanup_cachedir(s + "/cache/")

    argparser.add_argument(
        'from_date', type=valid_date,
        help='date from which simulation of metadata download will start')
    argparser.add_argument(
        'to_date', type=valid_date,
        help='date to which this will iterativelly downloads metadata')
    argparser.add_argument(
        '--step', type=unsinged, default='1',
        help='step of iteration from from_date to to_date')
    argparser.add_argument(
        '--cache_dir', type=path, default='./',
        help='directory where repomd.xml and repodata/ will be')
    argparser.add_argument(
        '--no_404', action='store_false',
        help='disables reporting 404 error when download fails')

    return argparser.parse_args()


def bytes_to_kb(num):
    return int(num / 1024)


if __name__ == '__main__':
    args = parse(ArgumentParser(
        description='Comparition of downloading metadata with and without zsync'
    ))
    ERROR_404 = args.no_404
    cache_dir = args.cache_dir
    metadata_iter = meta_data(
        args.from_date, args.to_date, args.step
    )

    def cleanup(repomd, files):
        cleanup_cachedir(cache_dir)
        # download and save into cache dir
        repomd.write(cache_dir)
        for dwnl_file in files:
            dwnl_file.write(cache_dir + "/repodata/")
    if True:
        # TODO: print header switch
        print(" ".join(["{: <20}".format("'" + header + "'") for header in ["From date", "To date", "Original size", "Downloaded size", "Synchronized size"]]))

    # create instance of first metadata and cleanup cachedir
    first_repo = next(metadata_iter)(cache_dir)
    dwnld_files = list(first_repo.down_xml_gzs)

    for exact_metadata in metadata_iter:
        cleanup(first_repo.repomd, dwnld_files)
        repo = exact_metadata(cache_dir)
        metadata_tuples = sorted([
            (_d, _s)
            for _s in repo.sync_xml_gzs
            for _d in dwnld_files
            if _s.file_name.split("-")[-1] == _d.file_name.split("-")[-1]
        ], key=lambda x: x[0].file_name.split("-")[-1])

        if len(metadata_tuples) == 1:
            print("WARN: Only 1 file was synced! {}".format(
                    str([(_d.file_name, _s.file_name) for _d, _s in metadata_tuples])
                ), file=sys.stderr)
            if False:
                # TODO: Implement skip single metadata switch
                continue
        if len(metadata_tuples) == 0:
            print("WARN: There was no synchronization!")
            if True:
                # TODO: Implement skip single metadata if 0 files switch
                continue

        # these are generators so we can use full power of it with next()
        print("{: <20} {: <20} {: <20} ".format(
            first_repo.date.strftime("%Y-%m-%d"),
            repo.date.strftime("%Y-%m-%d"),
            "{: <20} {: <20} {: <20}".format(
                *[sum(col) for col in zip(*[
                    (bytes_to_kb(dwn.size), bytes_to_kb(xgz.local_size), bytes_to_kb(xgz.downloaded_size))
                    for dwn, xgz in metadata_tuples
                ])
            ])
        ))
        if True:
            # TODO: switch that enables date folowing with a certain gap
            first_repo = repo
            dwnld_files = list(first_repo.down_xml_gzs)
