import os
import time
import fnmatch
from os.path import islink, isdir, isfile, realpath, join, dirname, basename
from glob import glob
import httplib, json, re
from graphite.node import BranchNode, LeafNode
from graphite.readers import CeresReader, WhisperReader, GzippedWhisperReader, RRDReader, OpenTSDBReader
from graphite.util import find_escaped_pattern_fields, is_pattern
from graphite.intervals import Interval

from graphite.logger import log
#setDefaultSliceCachingBehavior('all')

try:
  from ceres import CeresTree, CeresNode, setDefaultSliceCachingBehavior
  class CeresFinder:
    def __init__(self, directory):
      self.directory = directory
      self.tree = CeresTree(directory)
  
    def find_nodes(self, query):
      for fs_path in glob( self.tree.getFilesystemPath(query.pattern) ):
        metric_path = self.tree.getNodePath(fs_path)
  
        if CeresNode.isNodeDir(fs_path):
          ceres_node = self.tree.getNode(metric_path)
  
          if ceres_node.hasDataForInterval(query.startTime, query.endTime):
            real_metric_path = get_real_metric_path(fs_path, metric_path)
            reader = CeresReader(ceres_node, real_metric_path)
            yield LeafNode(metric_path, reader)
  
        elif isdir(fs_path):
          yield BranchNode(metric_path)

except ImportError:
  log.debug("Ceres library not installed");

class OpenTSDBFinder:
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.conn = httplib.HTTPConnection(self.host, self.port)
    
  def find_nodes(self, query):
    if (query.drilldown == True):
      log.debug("Drilling down...")
      for node in self.drilldown(query.pattern):
        yield node
      return
    log.debug("Searching OpenTSDB for branches with query [" + query.pattern + "]")
    if (query.pattern == "*" or query.pattern == "" or len(query.pattern) < 1):
      log.debug("Resetting query to 0001")
      query.pattern = "0001"
    elif (query.pattern.startswith("branch:")):
      query.pattern = query.pattern.replace("branch:", "");
    else:
      log.debug("Fetching leaf: " + query.pattern)
      # we likely have a leaf, so return the leaf node
      yield self.get_meta(query.pattern)
      return
      
    # query = branch ID
    for node in self.fetch_branch(query):
      yield node
  
  def get_meta(self, tsuid):
    """ Returns a leaf object after loading the TSUIDMeta object from storage """
    
    conn = httplib.HTTPConnection(self.host, self.port)
    conn.request("GET", "/api/uid/tsmeta?tsuid=" + tsuid)
    resp = conn.getresponse()
    if resp.status != 200:
      log.warn("Could not find a meta entry for [" + tsuid + "]. Status: " + str(resp.status))
    else:
      log.debug("Positive response from OpenTSDB for the tsuid at [" + tsuid + "]")
      json_data = json.loads(resp.read())
      
      # compile a list of tags without the tagk, just dump the tagvs
      tags = ""
      for x in range(len(json_data['tags'])):
        if json_data['tags'][x]['type'] == "TAGV":
          if (len(tags) > 1):
            tags += " | "
          tags += json_data['tags'][x]['name']
      
      reader = OpenTSDBReader(self.host, self.port, tsuid)
      leaf = LeafNode(tsuid, reader)
      leaf.name = json_data['metric']['name'] + ": " + tags
      return leaf
  
  def drilldown(self, path):
    log.debug("HERE Drilling down...")
    # if we don't need to drill down, just return the root
    if path == None or path == "" or len(path) < 1:
      log.debug("No pattern specified, returning root")
      query = FindQuery("", None, None)
      for node in self.find_nodes(query):
        yield node
      return
    
    current_branch = "0001"
    current_path = ""
    parts_idx = 0
    parts = path.split("|")
    log.debug("Starting at root parts: " + str(parts) + "  Len: " + str(len(parts)))
    while current_branch:
      query = FindQuery(current_branch, None, None)
      if parts_idx >= len(parts):
        log.debug("Returning entire branch [" + current_branch + "]")
        for node in self.fetch_branch(query):
          yield node
        return
      
      for node in self.fetch_branch(query):
        if parts[parts_idx] == node.name:
          # drill down further
          current_branch = str(node.uid)
          current_path += node.name + "|"
          log.debug("Matched part [" + parts[parts_idx] + "] on branch [" + str(current_branch) + "]")
          break
        else:
          current_branch = None
          if (len(parts[parts_idx]) < 1):
            yield node
          elif re.match("^" + parts[parts_idx], node.name):
            log.debug("Matched regex on node [" + node.name + "]")
            yield node
      parts_idx += 1
    log.debug("Done with drill down")    

  def fetch_branch(self, query):
    self.conn.request("GET", "/api/tree/branch?branch=" + query.pattern)
    resp = self.conn.getresponse()
    log.debug("API request has returned")
    if resp.status != 200:
      log.warn("Could not find a branch for [" + query.pattern + "]. Status: " + str(resp.status))
    else:
      log.debug("Positive response from OpenTSDB for the branch at [" + query.pattern + "]")
      json_data = json.loads(resp.read())

      if (json_data['branches'] and (json_data['branches']) > 0):
        for i in xrange(len(json_data['branches'])):
          #nodes.append(BranchNode(json_data['branches'][i]['displayName']))
          branch = BranchNode(json_data['branches'][i]['path'])
          #branch.id = json_data['branches'][i]['branchId']
          branch.uid = json_data['branches'][i]['branchId']
          # override the name
          #branch.name = json_data['branches'][i]['displayName']
          yield branch
      else:
        log.debug("No child branches found for [" + query.pattern + "]")

      if (json_data['leaves']):
        for i in xrange(len(json_data['leaves'])):
          #reader = GzippedWhisperReader(absolute_path, real_metric_path)
          reader = OpenTSDBReader(self.host, self.port, json_data['leaves'][i]['tsuid'])
          #leaf = LeafNode(json_data['leaves'][i]['tsuid'], reader)
          path = json_data['path'].copy()
          keys = sorted(path.iterkeys())
          key = str(int(keys[-1]) + 1)
          path[key] = json_data['leaves'][i]['displayName']
          leaf = LeafNode(path, reader)
          #leaf.uid = "leaf:" + json_data['leaves'][i]['tsuid']
          leaf.uid = json_data['leaves'][i]['tsuid']
          leaf.name = json_data['leaves'][i]['displayName']
          yield leaf 
      else:
        log.debug("No child leaves for branch [" + query.pattern + "]")
        
class StandardFinder:
  DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'

  def __init__(self, directories):
    self.directories = directories

  def find_nodes(self, query):
    clean_pattern = query.pattern.replace('\\', '')
    pattern_parts = clean_pattern.split('.')

    for root_dir in self.directories:
      for absolute_path in self._find_paths(root_dir, pattern_parts):
        if basename(absolute_path).startswith('.'):
          continue

        if self.DATASOURCE_DELIMETER in basename(absolute_path):
          (absolute_path, datasource_pattern) = absolute_path.rsplit(self.DATASOURCE_DELIMETER, 1)
        else:
          datasource_pattern = None

        relative_path = absolute_path[ len(root_dir): ].lstrip('/')
        metric_path = fs_to_metric(relative_path)
        real_metric_path = get_real_metric_path(absolute_path, metric_path)

        metric_path_parts = metric_path.split('.')
        for field_index in find_escaped_pattern_fields(query.pattern):
          metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
        metric_path = '.'.join(metric_path_parts)

        # Now we construct and yield an appropriate Node object
        if isdir(absolute_path):
          yield BranchNode(metric_path)

        elif isfile(absolute_path):
          if absolute_path.endswith('.wsp') and WhisperReader.supported:
            reader = WhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.wsp.gz') and GzippedWhisperReader.supported:
            reader = GzippedWhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.rrd') and RRDReader.supported:
            if datasource_pattern is None:
              yield BranchNode(metric_path)

            else:
              for datasource_name in RRDReader.get_datasources(absolute_path):
                if match_entries([datasource_name], datasource_pattern):
                  reader = RRDReader(absolute_path, datasource_name)
                  yield LeafNode(metric_path + "." + datasource_name, reader)

  def _find_paths(self, current_dir, patterns):
    """Recursively generates absolute paths whose components underneath current_dir
    match the corresponding pattern in patterns"""
    pattern = patterns[0]
    patterns = patterns[1:]
    entries = os.listdir(current_dir)

    subdirs = [e for e in entries if isdir( join(current_dir,e) )]
    matching_subdirs = match_entries(subdirs, pattern)

    if len(patterns) == 1 and RRDReader.supported: #the last pattern may apply to RRD data sources
      files = [e for e in entries if isfile( join(current_dir,e) )]
      rrd_files = match_entries(files, pattern + ".rrd")

      if rrd_files: #let's assume it does
        datasource_pattern = patterns[0]

        for rrd_file in rrd_files:
          absolute_path = join(current_dir, rrd_file)
          yield absolute_path + self.DATASOURCE_DELIMETER + datasource_pattern

    if patterns: #we've still got more directories to traverse
      for subdir in matching_subdirs:

        absolute_path = join(current_dir, subdir)
        for match in self._find_paths(absolute_path, patterns):
          yield match

    else: #we've got the last pattern
      files = [e for e in entries if isfile( join(current_dir,e) )]
      matching_files = match_entries(files, pattern + '.*')

      for basename in matching_files + matching_subdirs:
        yield join(current_dir, basename)


def fs_to_metric(path):
  dirpath = dirname(path)
  filename = basename(path)
  return join(dirpath, filename.split('.')[0]).replace('/','.')


def get_real_metric_path(absolute_path, metric_path):
  # Support symbolic links (real_metric_path ensures proper cache queries)
  if islink(absolute_path):
    real_fs_path = realpath(absolute_path)
    relative_fs_path = metric_path.replace('.', '/')
    base_fs_path = absolute_path[ :-len(relative_fs_path) ]
    relative_real_fs_path = real_fs_path[ len(base_fs_path): ]
    return fs_to_metric( relative_real_fs_path )

  return metric_path

def _deduplicate(entries):
  yielded = set()
  for entry in entries:
    if entry not in yielded:
      yielded.add(entry)
      yield entry

def match_entries(entries, pattern):
  """A drop-in replacement for fnmatch.filter that supports pattern
  variants (ie. {foo,bar}baz = foobaz or barbaz)."""
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

class FindQuery:
  def __init__(self, pattern, startTime, endTime):
    self.pattern = pattern
    self.startTime = startTime
    self.endTime = endTime
    self.isExact = is_pattern(pattern)
    self.drilldown = False
    self.interval = Interval(float('-inf') if startTime is None else startTime,
                             float('inf') if endTime is None else endTime)

  def setDrilldown(self, drilldown):
    self.drilldown = drilldown

  def __repr__(self):
    if self.startTime is None:
      startString = '*'
    else:
      startString = time.ctime(self.startTime)

    if self.endTime is None:
      endString = '*'
    else:
      endString = time.ctime(self.endTime)

    return '<FindQuery: %s from %s until %s>' % (self.pattern, startString, endString)
