import logging
import os
import string
import time
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from httpmr import base
from wsgiref import handlers


class Error(Exception): pass
class UnknownTaskError(Error): pass
class MissingRequiredParameterError(Error): pass


# Some constants for URL handling.  These values must correspond to the base
# name of the template that should be rendered at task completion.  For
# instance, when a mapper task is completed, the name of the template that will
# be rendered is MAPPER_TASK_NAME + ".html"
MAP_MASTER_TASK_NAME = "map_master"
MAPPER_TASK_NAME = "mapper"
REDUCE_MASTER_TASK_NAME = "reduce_master"
REDUCER_TASK_NAME = "reducer"
VALID_TASK_NAMES = [MAP_MASTER_TASK_NAME,
                    MAPPER_TASK_NAME,
                    REDUCE_MASTER_TASK_NAME,
                    REDUCER_TASK_NAME]

SOURCE_START_POINT = "source_start_point"
SOURCE_END_POINT = "source_end_point"
SOURCE_MAX_ENTRIES = "source_max_entries"
GREATEST_UNICODE_CHARACTER = "\xEF\xBF\xBD"


def tobase(base, number):
  """Ugly.
  
  I really wish I didn't have to copy this over, why doesn't Python have a
  built-in function for representing an int as a string in an arbitrary base?
  
  Copied from:
    http://www.megasolutions.net/python/How-to-convert-a-number-to-binary_-78436.aspx
  """
  number = int(number) 
  base = int(base)         
  if base < 2 or base > 36: 
    raise ValueError, "Base must be between 2 and 36"     
  if not number: 
    return 0
  symbols = string.digits + string.lowercase[:26] 
  answer = [] 
  while number: 
    number, remainder = divmod(number, base) 
    answer.append(symbols[remainder])       
  return ''.join(reversed(answer)) 

def tob36(number):
  return tobase(36, number)


class TaskSetTimer(object):
  
  def __init__(self, timeout_sec=10.0):
    self.timeout_sec = timeout_sec
    self.task_completion_times = []
  
  def Start(self):
    self.start_time = time.time()
  
  def TaskCompleted(self):
    self.task_completion_times.append(time.time())
  
  def ShouldStop(self):
    if len(self.task_completion_times) == 0:
      return False
    max_execution_time = 0
    for i in xrange(len(self.task_completion_times)):
      if i == 0: continue
      start_time = self.task_completion_times[i-1]
      end_time = self.task_completion_times[i]
      max_execution_time = max(max_execution_time,
                               end_time - start_time)
    worst_case_completion_time = time.time() + max_execution_time
    worst_case_completion_time_since_start_time = \
        worst_case_completion_time - self.start_time
    return (worst_case_completion_time_since_start_time >
            self.timeout_sec * 0.8)


class Master(webapp.RequestHandler):
  """The MapReduce master coordinates mappers, reducers, and data."""
  
  def QuickInit(self,
                jobname,
                mapper=None,
                reducer=None,
                source=None,
                mapper_sink=None,
                reducer_source=None,
                sink=None):
    logging.debug("Beginning QuickInit.")
    assert jobname is not None
    self._jobname = jobname
    self.SetMapper(mapper)
    self.SetReducer(reducer)
    self.SetSource(source)
    self.SetMapperSink(mapper_sink)
    self.SetReducerSource(reducer_source)
    self.SetSink(sink)
    logging.debug("Done QuickInit.")
    return self
  
  def SetMapper(self, mapper):
    """Set the Mapper that should be used for mapping operations."""
    assert isinstance(mapper, base.Mapper)
    self._mapper = mapper
    return self
  
  def SetReducer(self, reducer):
    """Set the Reducer that should be used for reduce operations."""
    assert isinstance(reducer, base.Reducer)
    self._reducer = reducer
    return self
    
  def SetSource(self, source):
    """Set the data source from which mapper input should be read."""
    self._source = source
    return self
  
  def SetMapperSink(self, sink):
    """Set the data sink to which mapper output should be written."""
    self._mapper_sink = sink
    return self
  
  def SetReducerSource(self, source):
    """Set the data source from which reducer input should be read."""
    self._reducer_source = source
    return self
  
  def SetSink(self, sink):
    """Set the data sink to which reducer output should be written."""
    self._sink = sink
    return self
  
  def get(self):
    """Handle task dispatch."""
    logging.debug("MapReduce Master Dispatching Request.")

    task = None
    try:
      task = self.request.params["task"]
    except KeyError, e:
      pass
    if task is None:
      task = MAP_MASTER_TASK_NAME
    
    template_data = {}
    if task == MAP_MASTER_TASK_NAME:
      template_data = self.GetMapMaster()
    elif task == MAPPER_TASK_NAME:
      template_data = self.GetMapper()
    elif task == REDUCE_MASTER_TASK_NAME:
      template_data = self.GetReduceMaster()
    elif task == REDUCER_TASK_NAME:
      template_data = self.GetReducer()
    else:
      raise UnknownTaskError("Task name '%s' is not recognized.  Valid task "
                             "values are %s" % (task, VALID_TASK_NAMES))
    self.RenderResponse("%s.html" % task, template_data)
  
  def _NextUrl(self, path_data):
    logging.debug("Rendering next url with path data %s" % path_data)
    path = self.request.path_url
    path_data["path"] = path
    return ("%(path)s?task=%(task)s"
            "&source_start_point=%(source_start_point)s"
            "&source_end_point=%(source_end_point)s"
            "&source_max_entries=%(source_max_entries)d") % path_data
  
  def _GetShardBoundaries(self):
    boundaries = [""]
    for i in xrange(35):
      j = (i + 1)
      boundaries.append(tob36(j))
    boundaries.append(GREATEST_UNICODE_CHARACTER)
    return boundaries
  
  def _GetShardBoundaryTuples(self):
    boundaries = self._GetShardBoundaries()
    boundary_tuples = []
    for i in xrange(len(boundaries)):
      if i == 0:
        continue
      boundary_tuples.append((boundaries[i-1], boundaries[i]))
    return boundary_tuples
  
  def _GetUrlsForShards(self, task):
    urls = []
    for boundary_tuple in self._GetShardBoundaryTuples():
      start_point = boundary_tuple[0]
      end_point = boundary_tuple[1]
      urls.append(self._NextUrl({"task": task,
                                 SOURCE_START_POINT: start_point,
                                 SOURCE_END_POINT: end_point,
                                 SOURCE_MAX_ENTRIES: 1000}))
    return urls
  
  def GetMapMaster(self):
    """Handle Map controlling page."""
    return {'urls': self._GetUrlsForShards(MAPPER_TASK_NAME)}

  def GetMapper(self):
    """Handle mapper tasks."""
    # Grab the parameters for this map task from the URL
    start_point = self.request.params[SOURCE_START_POINT]
    end_point = self.request.params[SOURCE_END_POINT]
    max_entries = int(self.request.params[SOURCE_MAX_ENTRIES])
    mapper_data = self._source.Get(start_point, end_point, max_entries)
    
    # Initialize the timer, and begin timing our operations
    timer = TaskSetTimer()
    timer.Start()
    last_key_mapped = None
    values_mapped = 0
    for key_value_pair in mapper_data:
      if timer.ShouldStop():
        break
      key = key_value_pair[0]
      value = key_value_pair[1]
      for output_key_value_pair in self._mapper.Map(key, value):
        output_key = output_key_value_pair[0]
        output_value = output_key_value_pair[1]
        self._mapper_sink.Put(output_key, output_value)
      last_key_mapped = key
      values_mapped += 1
      timer.TaskCompleted()
    
    next_url = None
    if values_mapped > 0:
      logging.debug("Completed %d map operations" % values_mapped)
      next_url = self._NextUrl({"task": MAPPER_TASK_NAME,
                                SOURCE_START_POINT: last_key_mapped,
                                SOURCE_END_POINT: end_point,
                                SOURCE_MAX_ENTRIES: max_entries})
    else:
      next_url = None
    return { "next_url": next_url }
      
  def GetReduceMaster(self):
    """Handle Reduce controlling page."""
    return {'urls': self._GetUrlsForShards(REDUCER_TASK_NAME)}

  
  def GetReducer(self):
    """Handle reducer tasks."""
        # Grab the parameters for this map task from the URL
    start_point = self.request.params[SOURCE_START_POINT]
    end_point = self.request.params[SOURCE_END_POINT]
    max_entries = int(self.request.params[SOURCE_MAX_ENTRIES])
    reducer_data = self._reducer_source.Get(start_point, end_point, max_entries)
    
    # Retrieve the mapped data from the datastore and sort it by key.
    # WARNING: There are no guarantees that this will retrieve all the values
    # for a given key.  TODO: Resolve this
    reducer_keys_values = {}
    for key_value_pair in reducer_data:
      key = key_value_pair[0]
      value = key_value_pair[1].intermediate_value
      logging.info("Reducing %s: %s" % (key, value))
      if key in reducer_keys_values:
        reducer_keys_values[key].append(value)
      else:
        reducer_keys_values[key] = [value]
    
    last_key_reduced = None
    keys_reduced = 0
    # Initialize the timer, and begin timing our operations
    timer = TaskSetTimer()
    timer.Start()
    for key in reducer_keys_values:
      if timer.ShouldStop():
        break
      values = reducer_keys_values[key]
      for output_key_value_pair in self._reducer.Reduce(key, values):
        output_key = output_key_value_pair[0]
        output_value = output_key_value_pair[1]
        self._sink.Put(output_key, output_value)
      last_key_reduced = key
      keys_reduced += 1
      timer.TaskCompleted()
    
    next_url = None
    if keys_reduced > 0:
      logging.debug("Completed %d reduce operations" % keys_reduced)
      next_url = self._NextUrl({"task": REDUCER_TASK_NAME,
                                SOURCE_START_POINT: last_key_reduced,
                                SOURCE_END_POINT: end_point,
                                SOURCE_MAX_ENTRIES: max_entries})
    else:
      next_url = None
    return { "next_url": next_url }
  
  def RenderResponse(self, template_name, template_data):
    path = os.path.join(os.path.dirname(__file__),
                        'templates',
                        template_name)
    logging.debug("Rendering template at path %s" % path)
    self.response.out.write(template.render(path, template_data))