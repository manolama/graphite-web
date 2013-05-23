import os, time, fnmatch, socket, errno
from os.path import isdir, isfile, join, exists, splitext, basename, realpath
import whisper
from graphite.remote_storage import RemoteStore
from django.conf import settings
from graphite.logger import log
import httplib, json, re

try:
  import rrdtool
except ImportError:
  rrdtool = False

try:
  import gzip
except ImportError:
  gzip = False

try:
  import cPickle as pickle
except ImportError:
  import pickle


DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'



class Store:
  def __init__(self, directories=[], remote_hosts=[]):
    print "Initialized the Store class with directories: "
    print directories
    self.directories = directories
    self.remote_hosts = remote_hosts
    self.remote_stores = [ RemoteStore(host) for host in remote_hosts if not is_local_interface(host) ]

    if not (directories or remote_hosts):
      raise valueError("directories and remote_hosts cannot both be empty")


  def get(self, metric_path): #Deprecated
    print "Called GET()"
    for directory in self.directories:
      relative_fs_path = metric_path.replace('.', '/') + '.wsp'
      absolute_fs_path = join(directory, relative_fs_path)

      if settings.TSDB_HOST:
        return OpenTSDBData(absolute_fs_path, metric_path)

      if exists(absolute_fs_path):
        return WhisperFile(absolute_fs_path, metric_path)
        


  def find(self, query):
#    if query.isdigit():
    print "storage.find: query " + query + ""
    
    splits = query.split(":")
    
    if splits[0] == "branch" or splits[0] == "*":
      print "storage.find: looks like a hash"
      for match in self.find_all(splits[1]):
        yield match
    else:
      if (len(splits) > 1):
        print "Storage.find: looks like a TSUID"
        match = self.find_first(splits[1])
        if match is not None:
          yield match
      else:
        yield tsdb_find_specific(query)
      
#    elif is_pattern(query):
#      print "find: Is a pattern"
#      for match in self.find_all(query):
#        yield match
#
#    else:
#      print "find: Is not a pattern"
#      match = self.find_first(query)
#
#      if match is not None:
#        yield match
    print "Finished the storage.find call"

  def find_first(self, query):
    
    if settings.TSDB_HOST:
      print "find_first: query [" + query + "]"
      return tsdb_find_specific(query)
#      q = OpenTSDBData(query, query)
#      q.fs_path = query
#      q.name = "my metric"
#      q.metric_path = "metric path"
#      q.real_metric = "real metric"
#      return q
      
    # Search locally first
    for directory in self.directories:
      for match in find(directory, query):
        return match

    # If nothing found earch remotely
    remote_requests = [ r.find(query) for r in self.remote_stores if r.available ]

    for request in remote_requests:
      for match in request.get_results():
        return match


  def find_all(self, query):
    print "find_all: query: " + query + ""
    # Start remote searches
    found = set()
#    remote_requests = [ r.find(query) for r in self.remote_stores if r.available ]

    if settings.TSDB_HOST:
      print "Calling tsdb tree..."
      for match in self.tsdb_tree("1", query):
        yield match
        found.add(match.metric_path)
#      for match in tsdb_group(query):
#        yield match
#        found.add(match.metric_path)
    else:
      # Search locally
      for directory in self.directories:
        for match in find(directory, query):
          if match.metric_path not in found:
            print "FIND_ALL: Metric path: " + match.metric_path
            yield match
            found.add(match.metric_path)
  
      # Gather remote search results
#      for request in remote_requests:
#        for match in request.get_results():
#  
#          if match.metric_path not in found:
#            yield match
#            found.add(match.metric_path)
    print "Finished the find_all call"

  def tsdb_tree(self, tree_id, branch_id):
    print "Calling TSDB: \/api\/tree\/branch\?branch=" + branch_id
    conn = httplib.HTTPConnection(settings.TSDB_HOST, settings.TSDB_PORT)    
    conn.request("GET", "/api/tree/branch?branch=" + branch_id)
    print conn.request
    resp = conn.getresponse()
    
    print "Sent call for tree"
    if resp.status == 200:
      print "Received data!"
      d = json.loads(resp.read())
  
      nodes = list()
      try:
        if (d['branches']):
          for i in xrange(len(d['branches'])):
            nodes.append(Branch("", "", d['branches'][i]['displayName'], d['branches'][i]['branchId']))
      except:
        print "No branches for this branch"
      try:
        if (d['leaves']):
          for i in xrange(len(d['leaves'])):
            nodes.append(Leaf("", "", d['leaves'][i]['displayName'], d['leaves'][i]['tsuid']))
      except:
        print "No leaves for this branch"
      for n in nodes:
        yield n
    else:
      print "Failed to get data from TSDB"

  def drilldown_tsdb_tree(self, tree_id, branch_id, path=""):
  # lets us drill down into a tree for dashboards
    if path:
      print "[drilldown_tsdb_tree] Path: " + path
      current_branch = "0"
      parts_idx = 0
      parts = path.split("|")
      matches = list()
      path = ""
      while current_branch:
        print "[drilldown_tsdb_tree] parent path: [" + path + "]"
        if parts_idx >= len(parts):
          local_matches = self.tsdb_tree(tree_id, current_branch)
          for node in local_matches:
            matches.append(node)
          print "[drilldown_tsdb_tree] returning entire branch: [" + current_branch + "]"
          current_branch = ""
          continue
        
        print "[drilldown_tsdb_tree] part: [" + parts[parts_idx] + "]  branch [" + current_branch + "]"
        local_matches = list(self.tsdb_tree(tree_id, current_branch))     
              
        for node in local_matches:
          if parts[parts_idx] == node.name:
            # drill down further
            current_branch = str(node.branch_id)
            path += node.name + "|"
            print "[drilldown_tsdb_tree] matched part [" + parts[parts_idx] + "] on branch: " + str(current_branch)
            break
          else:
            current_branch = ""
            if len(parts[parts_idx]) < 1:
              matches.append(node)
            elif re.match("^" + parts[parts_idx], node.name):
              print "[drilldown_tsdb_tree] matched regex on node: " + node.name
              matches.append(node)
        parts_idx += 1
      
      # fix up the path
      for node in matches:
        node.metric_path = path + node.metric_path
        yield node
    else:
      matches = self.tsdb_tree(tree_id, branch_id)
      for node in matches:
        yield node

def is_local_interface(host):
  print "Determined the interface is local"
  if ':' in host:
    host = host.split(':',1)[0]

  for port in xrange(1025, 65535):
    try:
      sock = socket.socket()
      sock.bind( (host,port) )
      sock.close()

    except socket.error, e:
      if e.args[0] == errno.EADDRNOTAVAIL:
        return False
      else:
        continue

    else:
      print "Bound to port [" + str(port) + "] for some reason..."
      return True

  raise Exception("Failed all attempts at binding to interface %s, last exception was %s" % (host, e))


def is_pattern(s):
  return '*' in s or '?' in s or '[' in s or '{' in s

def is_escaped_pattern(s):
  for symbol in '*?[{':
    i = s.find(symbol)
    if i > 0:
      if s[i-1] == '\\':
        return True
  return False

def find_escaped_pattern_fields(pattern_string):
  pattern_parts = pattern_string.split('.')
  for index,part in enumerate(pattern_parts):
    if is_escaped_pattern(part):
      yield index


def find(root_dir, pattern):
  print "FIND: Processing Dir: " + root_dir
  print "FIND: Processing Pattern: " + pattern
  "Generates nodes beneath root_dir matching the given pattern"
  clean_pattern = pattern.replace('\\', '')
  pattern_parts = clean_pattern.split('.')
  
  for absolute_path in _find(root_dir, pattern_parts):

    if DATASOURCE_DELIMETER in basename(absolute_path):
      (absolute_path,datasource_pattern) = absolute_path.rsplit(DATASOURCE_DELIMETER,1)
    else:
      datasource_pattern = None

    relative_path = absolute_path[ len(root_dir): ].lstrip(os.pathsep)
    metric_path = relative_path.replace(os.pathsep,'.')
    print "FIND: Relative Path: " + relative_path
    print "FIND: metric_path: " + metric_path
    
    # Preserve pattern in resulting path for escaped query pattern elements
    metric_path_parts = metric_path.split('.')
    for field_index in find_escaped_pattern_fields(pattern):
      metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
    metric_path = '.'.join(metric_path_parts)

    if isdir(absolute_path):
      print "FIND: Absolute path found: " + absolute_path
      yield Branch(absolute_path, metric_path)

    elif isfile(absolute_path):
      (metric_path,extension) = splitext(metric_path)
      print "FIND: Found file met [" + metric_path + "] ext [" + extension + "]"
      if extension == '.wsp':
        #print ("ab: " + absolute_path + "  mp: " + metric_path)
        yield WhisperFile(absolute_path, metric_path)
        #yield OpenTSDBData(absolute_path, metric_path)

      elif extension == '.gz' and metric_path.endswith('.wsp'):
        metric_path = splitext(metric_path)[0]
        yield GzippedWhisperFile(absolute_path, metric_path)

      elif rrdtool and extension == '.rrd':
        rrd = RRDFile(absolute_path, metric_path)

        if datasource_pattern is None:
          yield rrd

        else:
          for source in rrd.getDataSources():
            if fnmatch.fnmatch(source.name, datasource_pattern):
              yield source


def _find(current_dir, patterns):
  """Recursively generates absolute paths whose components underneath current_dir
  match the corresponding pattern in patterns"""
  print "_find: Processing Dir: " + current_dir
  #print "_find: Processing Pattern: " + patterns
  pattern = patterns[0]
  patterns = patterns[1:]
  entries = os.listdir(current_dir)

  subdirs = [e for e in entries if isdir( join(current_dir,e) )]
  matching_subdirs = match_entries(subdirs, pattern)

  if len(patterns) == 1 and rrdtool: #the last pattern may apply to RRD data sources
    files = [e for e in entries if isfile( join(current_dir,e) )]
    rrd_files = match_entries(files, pattern + ".rrd")

    if rrd_files: #let's assume it does
      datasource_pattern = patterns[0]

      for rrd_file in rrd_files:
        absolute_path = join(current_dir, rrd_file)
        yield absolute_path + DATASOURCE_DELIMETER + datasource_pattern

  if patterns: #we've still got more directories to traverse
    for subdir in matching_subdirs:

      absolute_path = join(current_dir, subdir)
      for match in _find(absolute_path, patterns):
        yield match

  else: #we've got the last pattern
    files = [e for e in entries if isfile( join(current_dir,e) )]
    matching_files = match_entries(files, pattern + '.*')

    for basename in matching_subdirs + matching_files:
      yield join(current_dir, basename)


def _deduplicate(entries):
  yielded = set()
  for entry in entries:
    if entry not in yielded:
      yielded.add(entry)
      yield entry


def match_entries(entries, pattern):
  # First we check for pattern variants (ie. {foo,bar}baz = foobaz or barbaz)
  v1, v2 = pattern.find('{'), pattern.find('}')

  if v1 > -1 and v2 > v1:
    variations = pattern[v1+1:v2].split(',')
    variants = [ pattern[:v1] + v + pattern[v2+1:] for v in variations ]
    matching = []

    for variant in variants:
      matching.extend( fnmatch.filter(entries, variant) )

    return list( _deduplicate(matching) ) #remove dupes without changing order

  else:
    matching = fnmatch.filter(entries, pattern)
    matching.sort()
    return matching

def tsdb_group(query):
  if query == "*":
    conn = httplib.HTTPConnection(settings.TSDB_HOST, settings.TSDB_PORT)
    conn.request("GET", "/group?terms=true")
    resp = conn.getresponse()
    
    if resp.status == 200:
      print "Received data!"
      d = json.loads(resp.read())
      print d['total_pages']
  
      for i in xrange(15):
        #for item in d['results'][key]:
          #print key + "." + item['metric']
          #metrics.append(d['results'][i])
          host = d['results'][i].replace(".", "_")
          yield Branch(host, host)
  else:
    conn = httplib.HTTPConnection(settings.TSDB_HOST, settings.TSDB_PORT)
    host = query.replace(".*", "").replace("_", ".")
    url = "/search?query=host:" + host + "*"
    print "Querying: " + url
    conn.request("GET", url)
    resp = conn.getresponse()
    
    if resp.status == 200:
      print "Received data!"
      d = json.loads(resp.read())
      for ts in d['results']:
        yield OpenTSDBData(ts['tsuid'], ts['metric'])

def tsdb_find_specific(query):
  print "in storage.tsdb_find_Specific(): " + query
  if (query == "*"):
    return None
  
  conn = httplib.HTTPConnection(settings.TSDB_HOST, settings.TSDB_PORT)
  url = "/api/uid/tsmeta?tsuid=" + query
  print "Calling URL: " + url
  conn.request("GET", url)
  resp = conn.getresponse()
  
  if resp.status == 200:
    print "Received data!"
    d = json.loads(resp.read())
    
    leaf = OpenTSDBData(query, query)
    
    tags = ""
    for x in range(len(d['tags'])):
      if d['tags'][x]['type'] == "TAGV":
        if (len(tags) > 1):
          tags += " | "
        tags += d['tags'][x]['name']
    
    leaf.metric_path = d['metric']['name'] + ": " + tags
    return leaf
  else:
    print "WARNING: Couldn't make the Metadata call successfully"
    print resp.status
    return None        

# Node classes
class Node:
  context = {}

  def __init__(self, fs_path, metric_path, branch_name="", branch_id=0, tsuid=0):
    self.fs_path = str(fs_path)
    self.metric_path = str(branch_name)
    self.real_metric = str(branch_name)
    if branch_name:
      self.name = branch_name
    else:
      self.name = self.metric_path.split('.')[-1]
    self.branch_id = branch_id
    self.tsuid = tsuid
    #print "NODE: fs [" + self.fs_path + "] met [" + self.metric_path +"] name [" + self.name + "]"

  def getIntervals(self):
    return []

  def updateContext(self, newContext):
    raise NotImplementedError()


class Branch(Node):
  "Node with children"
  def fetch(self, startTime, endTime):
    "No-op to make all Node's fetch-able"
    print "Fetching on a branch, does NOTHING!"
    return []

  def isLeaf(self):
    return False


class Leaf(Node):
  "(Abstract) Node that stores data"
  def isLeaf(self):
    return True

# CL Trying to hack this stuff
class OpenTSDBData(Leaf):
  cached_context_data = None
  
  def __init__(self, *args, **kwargs):
    print "Initialized an OpenTSDBData leaf"
    Leaf.__init__(self, *args, **kwargs)
    
  def getIntervals(self):
    start = 0
    end = int(time.time())
    return [ (start, end) ]

  def fetch(self, startTime, endTime):
    print "OpenTSDBData.fetch: Start time: [" + str(startTime) + "] end: [" + str(endTime) + "]"

    tsd_url = "/api/query?start=" + str(int(startTime)) + "&end=" + str(int(endTime)) + "&tsuid=sum:1m-avg:" + self.fs_path
    print ("OpenTSDBData.fetch: Calling url: " + tsd_url)
    conn = httplib.HTTPConnection(settings.TSDB_HOST, settings.TSDB_PORT)
    conn.request("GET", tsd_url)
    print ("OpenTSDBData.fetch: Finished the request...")
       
    print ("OpenTSDBData.fetch: Getting response...")
    resp = conn.getresponse()
    if resp.status == 200:
      print ("OpenTSDBData.fetch: Response was ok!!")
      jd = resp.read()
      d = json.loads(jd)
      expected_dps = int((endTime - startTime) / 60)
      valueList = list(None for i in range(expected_dps))
      
      if d and len(d) > 0 and 'dps' in d[0]:
        counter = 0
        keys = sorted(d[0]['dps'].iterkeys())
        first_ts = int(keys[0])
        print "First ts: " + str(first_ts)
        
        if (first_ts >= int(startTime)):
          cur_ts = int(startTime)
          while (cur_ts < first_ts):
            counter += 1
            cur_ts += 60
            if (counter > expected_dps):
              break;
        print "Starting counter at: " + str(counter)
        
        for key in keys:
          if (int(key) < int(startTime)):
            print "Key was less than start time: " + str(key)
            continue
          elif (int(key) > int(endTime)):
            print "Key " + str(key) + " was greater than end time: " + str(int(endTime))
            break
          
          valueList[counter] = d[0]['dps'][key]
          counter += 1
          
#          if counter >= len(valueList):
#            break;
        print "Retrieved [" + str(counter) + "] datapoints"
        
#        step = 0
#        if len(ts_delta) > 1:
#          total = 0
#          for i in ts_delta:
#            total += i
#          step = total / len(ts_delta)
        timeInfo = (startTime, endTime, 60)
        print ("OpenTSDBData.fetch: set timeInfo")    
        
      else:
        print ("OpenTSDBData.fetch: Couldn't find the data points")
        log.info("Couldn't find the data points")
        return None
    else:
      print ("OpenTSDBData.fetch: Failed to get any data")
      log.info("Failed to get any data")
      return None
    
    conn.close()
    return (timeInfo, valueList)
    
# Database File classes
class WhisperFile(Leaf):
  cached_context_data = None
  extension = '.wsp'

  def __init__(self, *args, **kwargs):
    Leaf.__init__(self, *args, **kwargs)
    real_fs_path = realpath(self.fs_path)

    if real_fs_path != self.fs_path:
      relative_fs_path = self.metric_path.replace('.', '/') + self.extension
      base_fs_path = realpath(self.fs_path[ :-len(relative_fs_path) ])
      relative_real_fs_path = real_fs_path[ len(base_fs_path)+1: ]
      self.real_metric = relative_real_fs_path[ :-len(self.extension) ].replace('/', '.')

  def getIntervals(self):
    start = time.time() - whisper.info(self.fs_path)['maxRetention']
    end = max( os.stat(self.fs_path).st_mtime, start )
    return [ (start, end) ]

  def fetch(self, startTime, endTime):
    print "Fetching a whisper value"
    (timeInfo,values) = whisper.fetch(self.fs_path, startTime, endTime)
    return (timeInfo,values)

  @property
  def context(self):
    if self.cached_context_data is not None:
      return self.cached_context_data

    context_path = self.fs_path[ :-len(self.extension) ] + '.context.pickle'

    if exists(context_path):
      fh = open(context_path, 'rb')
      context_data = pickle.load(fh)
      fh.close()
    else:
      context_data = {}

    self.cached_context_data = context_data
    return context_data

  def updateContext(self, newContext):
    self.context.update(newContext)
    context_path = self.fs_path[ :-len(self.extension) ] + '.context.pickle'

    fh = open(context_path, 'wb')
    pickle.dump(self.context, fh)
    fh.close()


class GzippedWhisperFile(WhisperFile):
  extension = '.wsp.gz'

  def fetch(self, startTime, endTime):
    print "Fetching a gzipped whisper file"
    if not gzip:
      raise Exception("gzip module not available, GzippedWhisperFile not supported")

    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      return whisper.file_fetch(fh, startTime, endTime)
    finally:
      fh.close()

  def getIntervals(self):
    if not gzip:
      return []

    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      start = time.time() - whisper.__readHeader(fh)['maxRetention']
      end = max( os.stat(self.fs_path).st_mtime, start )
    finally:
      fh.close()
    return [ (start, end) ]


class RRDFile(Branch):
  def getDataSources(self):
    info = rrdtool.info(self.fs_path)
    if 'ds' in info:
      return [RRDDataSource(self, datasource_name) for datasource_name in info['ds']]
    else:
      ds_keys = [ key for key in info if key.startswith('ds[') ]
      datasources = set( key[3:].split(']')[0] for key in ds_keys )
      return [ RRDDataSource(self, ds) for ds in datasources ]

  def getRetention(self):
    info = rrdtool.info(self.fs_path)
    if 'rra' in info:
      rras = info['rra']
    else:
      # Ugh, I like the old python-rrdtool api better..
      rra_count = max([ int(key[4]) for key in info if key.startswith('rra[') ]) + 1
      rras = [{}] * rra_count
      for i in range(rra_count):
        rras[i]['pdp_per_row'] = info['rra[%d].pdp_per_row' % i]
        rras[i]['rows'] = info['rra[%d].rows' % i]

    retention_points = 0
    for rra in rras:
      points = rra['pdp_per_row'] * rra['rows']
      if points > retention_points:
        retention_points = points

    return  retention_points * info['step']


class RRDDataSource(Leaf):
  def __init__(self, rrd_file, name):
    Leaf.__init__(self, rrd_file.fs_path, rrd_file.metric_path + '.' + name)
    self.rrd_file = rrd_file

  def getIntervals(self):
    start = time.time() - self.rrd_file.getRetention()
    end = max( os.stat(self.rrd_file.fs_path).st_mtime, start )
    return [ (start, end) ]

  def fetch(self, startTime, endTime):
    print "Fetching an RRD Data source"
    startString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(startTime))
    endString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(endTime))

    if settings.FLUSHRRDCACHED:
      rrdtool.flushcached(self.fs_path, '--daemon', settings.FLUSHRRDCACHED)
    (timeInfo,columns,rows) = rrdtool.fetch(self.fs_path,'AVERAGE','-s' + startString,'-e' + endString)
    colIndex = list(columns).index(self.name)
    rows.pop() #chop off the latest value because RRD returns crazy last values sometimes
    values = (row[colIndex] for row in rows)

    return (timeInfo,values)



# Exposed Storage API
LOCAL_STORE = Store(settings.DATA_DIRS)
STORE = Store(settings.DATA_DIRS, remote_hosts=settings.CLUSTER_SERVERS)
