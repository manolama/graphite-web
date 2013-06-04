

class Node(object):
  __slots__ = ('name', 'path', 'local', 'is_leaf', 'uid')

  def __init__(self, path):
    if (type(path) == dict):
      keys = sorted(path.iterkeys())
      for key in keys:
        if (int(key) == 1):
          self.path = path[key]
        elif (int(key) > 0):
          self.path = self.path + "." + path[key]
      self.name = path[keys[-1]]
    else:
      self.path = path
      self.name = path.split('.')[-1]
      
    self.local = True
    self.is_leaf = False
    self.uid = None

  def __repr__(self):
    return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


class BranchNode(Node):
  pass


class LeafNode(Node):
  __slots__ = ('reader', 'intervals')

  def __init__(self, path, reader):
    Node.__init__(self, path)
    self.reader = reader
    self.intervals = reader.get_intervals()
    self.is_leaf = True
    self.uid = None

  def fetch(self, startTime, endTime):
    return self.reader.fetch(startTime, endTime)

  def __repr__(self):
    return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)
