#!/usr/bin/env python
from __future__ import division
from __future__ import print_function

import argparse
import os, os.path
import re
import sys
import samweb_client as swc
import json
import subprocess
import shlex
import pycurl
from io import BytesIO


# Check if X509_USER_PROXY is already set in the environment first and use it if so.
# Fall back to the /tmp/x509up... only if X509_USER_PROXY is not set.
X509_USER_PROXY = os.getenv("X509_USER_PROXY", "/tmp/x509up_u%d" % os.getuid())
PNFS_DIR_PATTERN = re.compile(r"/pnfs/(?P<area>[^/]+)")
# enstore locations look like
# "enstore:/path/to/directory(weird_tape_id)", except that sometimes
# the "weird_tape_id" part is missing (eg in the output of
# samweb.listFilesAndLocations()), so we make that part optional,
# which gets us this unreadable re
ENSTORE_PATTERN = re.compile(r"^enstore:([^(]+)(\([^)]+\))?")
# The base URL for the Fermilab instance of the dcache REST API.
#
# We use this for finding the online status of files and for requesting prestaging. The full dcache REST API is described in the dcache User Guide:
#
# https://www.dcache.org/manuals/UserGuide-6.0/frontend.shtml
DCACHE_REST_BASE_URL = "https://fndca.fnal.gov:3880/api/v1/namespace"

################################################################################
class ProgressBar(object):
    def __init__(self, total, announce_threshold=50):
        self.total = total
        self._total_div10 = total // 10

        self.announce = total >= announce_threshold
        self._last_announce_decile = -1

        self.Update(0)

    def Update(self, n):
        current_decile = None
        if self.total > 10:
            current_decile = n // self._total_div10
        if self.announce:
            if current_decile is None:
                print( " %d" % n, end=" " )
            if (current_decile > self._last_announce_decile or n == self.total):  # always want to announce 100%
                curr_perc = int(n / self.total * 100)
                print( " %d%%" % curr_perc, end=" " )

                self._last_announce_decile = n // self._total_div10

            sys.stdout.flush()

################################################################################
def make_curl():
    """Returns a pycurl object with the necessary fields set for Fermilab
    authentication.

    The object can be reused for multiple requests to the
    dcache REST API and curl will reuse the connection, which should speed
    things up"""
    
    c = pycurl.Curl()
    c.setopt(c.CAINFO, X509_USER_PROXY);
    c.setopt(c.SSLCERT, X509_USER_PROXY);
    c.setopt(c.SSLKEY, X509_USER_PROXY);
    c.setopt(c.SSH_PRIVATE_KEYFILE, X509_USER_PROXY);
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(c.CAPATH, "/etc/grid-security/certificates");

    return c

################################################################################
def filename_to_namespace(filename):
    filename_out=filename
    if filename.startswith("root://fndca1.fnal.gov:1094"):
        filename_out=filename.replace("root://fndca1.fnal.gov:1094", "")
    elif filename.startswith("/pnfs/uboone"):
        filename_out=filename.replace("/pnfs/uboone", "/pnfs/fnal.gov/usr/uboone")
    elif filename.startswith("enstore:/pnfs/uboone"):
        filename_out=filename.replace("enstore:/pnfs/uboone", "/pnfs/fnal.gov/usr/uboone")

    return filename_out

################################################################################
def get_file_qos(c, filename):
    """Using curl object `c`, find the "QoS" of `filename`.

    QoS is "disk", "tape" or "disk+tape", with the obvious meanings

    Returns: (currentQos, targetQos) where targetQos is non-empty if
             there is an outstanding prestage request. currentQos will
             be empty if there is an error (eg, file does not exist)
    
    Uses the dcache REST API frontend, documented in the dcache User Guide, eg:

    https://www.dcache.org/manuals/UserGuide-6.0/frontend.shtml

    """

    # qos=true in the URL causes dcache to tell us whether the file's
    # on disk or tape, and also the "targetQos", which exists if
    # there's an outstanding prestage request.
    #
    # Update 2020-10-02: it looks like qos is sometimes incorrect, or
    # at least, not what I thought it was, since online files can have
    # fileLocality=ONLINE_AND_NEARLINE but qos=tape. So we use
    # fileLocality for the online-ness of the file, but still request
    # qos because it gives us the target qos if there's an outstanding
    # prestage request
    url="{host}/{path}?qos=true&locality=true".format(host=DCACHE_REST_BASE_URL, path=filename_to_namespace(filename))
    c.setopt(c.URL, url)
    mybuffer = BytesIO()
    c.setopt(c.WRITEFUNCTION, mybuffer.write)
    c.perform()

    # Body is a byte string.
    # We have to know the encoding in order to print it to a text file
    # such as standard output.
    body = mybuffer.getvalue().decode('iso-8859-1')
    
    j=json.loads(body)
    qos=""
    locality=""
    targetQos=""
    # "qos" turns out to not quite be right - see comment above
    # if "currentQos" in j:
    #    qos=j["currentQos"]
    if "fileLocality" in j:
        locality=j["fileLocality"]
    if "targetQos" in j:
        targetQos=j["targetQos"]
        
    return (locality, targetQos)

################################################################################
def is_file_online(c, filename):
    """Using curl object `c`, returns whether `filename` is online"""
    return "ONLINE" in get_file_qos(c, filename)[0]

################################################################################
def request_prestage(c, filename):
    """Using curl object `c`, request a prestage for `filename`

    Returns whether the request succeeded (according to dcache)
    
    Uses a HTTP post request in a very specific format to request a prestage of a file. Adapted from:

    https://github.com/DmitryLitvintsev/scripts/blob/master/bash/bring-online.sh

    Uses the dcache REST API frontend, documented in the dcache User Guide, eg:

    https://www.dcache.org/manuals/UserGuide-6.0/frontend.shtml
    """
    c.setopt(c.POSTFIELDS, """{"action" : "qos", "target" : "disk+tape"}""")
    c.setopt(c.HTTPHEADER, ["Accept: application/json", "Content-Type: application/json"])
    c.setopt(c.POST, 1)
    c.setopt(c.URL, "{host}/{path}".format(host=DCACHE_REST_BASE_URL, path=filename_to_namespace(filename)))
    mybuffer = BytesIO()
    c.setopt(c.WRITEFUNCTION, mybuffer.write)
    c.perform()

    # Body is a byte string.
    # We have to know the encoding in order to print it to a text file
    # such as standard output.
    body = mybuffer.getvalue().decode('iso-8859-1')
    j=json.loads(body)
    return "status" in j and j["status"]=="success"

################################################################################
def is_file_online_pnfs(f):
    path, filename = os.path.split(f)
    stat_file="%s/.(get)(%s)(locality)"%(path,filename)
    theStatFile=open(stat_file)
    state=theStatFile.readline()
    theStatFile.close()
    return 'ONLINE' in state

################################################################################
def FilelistCacheCount(files, verbose_flag, METHOD="rest"):
    assert(METHOD in ("rest", "pnfs"))

    if len(files) > 1:
        print( "Checking %d files:" % len(files) )
    cached = 0
    cache_list = []
    pending = 0
    n = 0

    # If we're in verbose mode, the per-file output fights with
    # the progress bar, so disable the progress bar
    progbar = None if verbose_flag else ProgressBar(len(files)) 

    c=make_curl() if METHOD=="rest" else None
    
    for f in files:
        if METHOD=="rest":
            qos,targetQos=get_file_qos(c, f)
            if "ONLINE" in qos: 
                cached += 1
                cache_list.append(f)
            if "disk" in targetQos: pending += 1
            if verbose_flag:
                print( f, qos, "pending" if targetQos else "")
        elif METHOD=="pnfs":
            this_cached=is_file_online_pnfs(f)
            if this_cached: 
                cached += 1
                cache_list.append(f)
            if verbose_flag:
                print( f, "ONLINE" if this_cached else "NEARLINE")

        n += 1
        # If we're in verbose mode, the per-file output fights with
        # the progress bar, so disable the progress bar
        if not verbose_flag: progbar.Update(n)

    if not verbose_flag: progbar.Update(progbar.total)

    # We don't count pending files with the pnfs method, so set it to
    # something meaningless
    if METHOD=="pnfs":
        pending = -1
    return (cached, pending, n, cache_list)

################################################################################
def FilelistPrestageRequest(files, verbose_flag):
    announce=len(files) > 1
    if announce:
        print( "Prestaging %d files:" % len(files) )

    c=make_curl()
    n = len(files)
    n_request_succeeded = 0
    for f in files:
        success=request_prestage(c, f)
        if success: n_request_succeeded += 1
        if verbose_flag:
            print( f, "request succeeded" if success else "request failed" )

    return (n_request_succeeded, n)

################################################################################
def enstore_locations_to_paths(samlist, sparsification=1):
    """Convert a list of enstore locations as returned by
       samweb.listFilesAndLocations() into plain pnfs paths. Sparsify by
       `sparsification`"""
    pnfspaths=[]
    for f in samlist[::sparsification]:
        m=ENSTORE_PATTERN.match(f[0])
        if m:
            directory=m.group(1)
            filename=f[1]
            pnfspaths.append(os.path.join(directory, filename))
        else:
            print( "enstore_locations_to_paths got a non-enstore location", f[0] )
    return pnfspaths

examples="""
Examples:

 Find the cache state of one file:

    %(prog)s np04_raw_run004513_0008_dl5.root

 Find the cache state of multiple files. With -v, each file's status
 is shown; otherwise just a count is shown. Can mix-and-match full
 paths and SAM filenames:

    %(prog)s -v /pnfs/dune/tape_backed/myfile.root np04_raw_run004513_0008_dl5.root

 Summarize the cache state of a SAM dataset:

    %(prog)s -d protodune-sp_runset_4513_raw_v0

 Show the cache state of each file matching a SAM query:

    %(prog)s -v --dim 'run_type protodune-sp and run_number 4513 and data_tier raw'

 Prestage an individual file, by its SAM filename:

    %(prog)s -p np04_raw_run004513_0008_dl5.root

 (In subsequent queries, the file will show up as "pending" until it
 arrives on disk)

 Prestage an entire dataset (like samweb prestage-dataset):

    %(prog)s -p -d protodune-sp_runset_4513_raw_v0
"""

################################################################################
if __name__=="__main__":
    parser= argparse.ArgumentParser(epilog=examples, formatter_class=argparse.RawDescriptionHelpFormatter)

    gp = parser.add_mutually_exclusive_group()
    gp.add_argument("files",
                    nargs="*",
                    default=[],
                    metavar="FILE",
                    help="Files to consider. Can be specified as a full /pnfs path, or just the SAM filename",
    )
    gp.add_argument("-d", "--dataset",
                    metavar="DATASET",
                    dest="dataset_name",
                    help="Name of the SAM dataset to check cache status of",
    )
    gp.add_argument("-q", "--dim",
                    metavar="\"DIMENSION\"",
                    dest="dimensions",
                    help="sam dimensions to check cache status of",
                    )

    parser.add_argument("-s","--sparse", type=int, dest='sparse',help="Sparsification factor.  This is used to check only a portion of a list of files",default=1)
    parser.add_argument("-ss", "--snapshot", dest="snapshot", help="[Also requires -d]  Use this snapshot ID for the dataset.  Specify 'latest' for the most recent one.")
    parser.add_argument("-v","--verbose", action="store_true", dest="verbose", default=False, help="Print information about individual files")
    parser.add_argument("-p","--prestage", action="store_true", dest="prestage", default=False, help="Prestage the files specified")
    parser.add_argument("-m", "--method", choices=["rest", "pnfs"], default="rest", help="Use this method to look up file status.")

    args=parser.parse_args()

    # gotta make sure you have a valid certificate.
    # otherwise the results may lie...
    #if args.method in ("rest"):
    #    try:
    #        subprocess.check_call(shlex.split("./setup_fnal_security --check"), stdout=open(os.devnull), stderr=subprocess.STDOUT)
    #    except subprocess.CalledProcessError:
    #        print( "Your proxy is expired or missing.  Please run `setup_fnal_security` and then try again." )
    #        sys.exit(2)

    filelist = None if args.dataset_name else args.files

    sam = swc.SAMWebClient("uboone")

    cache_count = 0

    # Figure out where we want to get our list of files from

    # See if a SAM dataset was specified
    if args.dataset_name:
        print( "Retrieving file list for SAM dataset definition name: '%s'..." % args.dataset_name, end="" )
        sys.stdout.flush()
        try:
            dimensions = None
            if args.snapshot == "latest":
                dimensions = "dataset_def_name_newest_snapshot %s" % args.dataset_name
            elif args.snapshot:
                dimensions = "snapshot_id %s" % args.snapshot
            if dimensions:
                samlist = sam.listFilesAndLocations(dimensions=dimensions, filter_path="enstore")
            else:
                #samlist  = sam.listFilesAndLocations(defname=args.dataset_name, filter_path="enstore")
                thislist = sam.listFiles(defname=args.dataset_name)
                print(len(thislist))
                samlist = []
                a = 0
                for f in thislist:
                  if not (a%100): print("Locating files: %i/%i"%(a, len(thislist)), end='\r')
                  locs = sam.locateFile(f)
                  for l in locs:
                    if l['full_path'].split(':')[0] == 'enstore':
                      samlist.append((l['full_path'], f))
                      break 
                  a += 1
                print()
                print(len(samlist))

            filelist = enstore_locations_to_paths(list(samlist), args.sparse) 
            print( " done." )
        except Exception as e:
            print( e )
            print()
            print( 'Unable to retrieve SAM information for dataset: %s' %(args.dataset_name) )
            exit(-1)
            # Take the rest of the commandline as the filenames
            filelist = args
    elif args.dimensions:
        print( "Retrieving file list for SAM dimensions: '%s'..." % args.dimensions, end="" )
        sys.stdout.flush()
        try:
            samlist = sam.listFilesAndLocations(dimensions=args.dimensions, filter_path="enstore")
            filelist = enstore_locations_to_paths(list(samlist), args.sparse) 
            print( " done." )
        except Exception as e:
            print( e )
            print()
            print( 'Unable to retrieve SAM information for dimensions: %s' %(args.dimensions) )
            exit(-1)
    else:
        filelist=[]
        # We were passed a list of files. Loop over them and try to locate each one
        for f in args.files:
            if os.path.isfile(f):
                # We got a path to an actual file
                # If the file's not on pnfs, just assume it's on a
                # regular filesystem that is always "cached". Otherwise, add it to the list
                if f.startswith("/pnfs"):
                    filelist.append(f)
                else:
                    cache_count += 1
                    continue
            else:
                # The argument isn't a file on the file system. Assume
                # it's a filename in samweb and ask samweb for the
                # location
                try:
                    locs = sam.locateFile(f)
                    # locateFile potentially produces multiple
                    # locations. We look through them for the enstore
                    # one, and add it to the list, but without the
                    # "enstore:/" at the front
                    for loc in locs:
                        l=loc["location"]
                        m=ENSTORE_PATTERN.match(l)
                        if m:
                            directory=m.group(1)
                            fullpath=os.path.join(directory, f)
                            filelist.append(fullpath)
                except (swc.exceptions.FileNotFound, swc.exceptions.HTTPNotFound):
                    print("File is not known to SAM and is not a full path:", f, file=sys.stderr)
                    sys.exit(2)

    miss_count = 0

    n_files = len(filelist)
    announce = n_files > 1  # some status notes if there are lots of files

    if args.prestage:
        ngood,n=FilelistPrestageRequest(filelist, args.verbose)
        sys.exit(0 if ngood==n else 1)
    else:
        cache_count, pending_count, total, cache_list = FilelistCacheCount(filelist, args.verbose, args.method)
        miss_count = total - cache_count

        # Save cache_list to a text file
        with open("cache_list.txt", "w") as f:
            for item in cache_list:
                f.write("%s\n" % item)

        total = float(cache_count + miss_count)
        cache_frac_str = (" (%d%%)" % round(cache_count/total*100)) if total > 0 else ""
        miss_frac_str = (" (%d%%)" % round(miss_count/total*100)) if total > 0 else ""

        if total > 1:
            print()
            pending_string=""
            if pending_count>=0:
                pending_string="\tPending: %d (%d%%)" % (pending_count, round(pending_count/total*100))
            print( "Cached: %d%s\tTape only: %d%s%s" % (cache_count, cache_frac_str, miss_count, miss_frac_str, pending_string))
        elif total == 1:
            print( "CACHED" if cache_count > 0 else "NOT CACHED", end="")
            print( " PENDING" if pending_count > 0 else "" )

        if miss_count == 0:
            sys.exit(0)
        else:
            sys.exit(1)

# Local Variables:
# python-indent-offset: 4
# End:
