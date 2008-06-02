import logging
import os
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


class TaskSetTimer(object):
  
  def __init__(self, timeout_sec=10.0):
    self.timeout_sec = timeout_sec
    self.task_completion_times = []
  
  def Start(self):
    self.start_time = time.time()
  
  def TaskCompleted(self):
    self.task_completion_times.append(time.time())
  
  def ShouldStop(self):
    return (time.time() - self.start_time) > (self.timeout_sec * 0.8) 


class Master(webapp.RequestHandler):
  """The MapReduce master coordinates mappers, reducers, and data."""
  
  def QuickInit(self,
                jobname,
                mapper=None,
                reducer=None,
                source=None,
                mapper_sink=None,
                reducer_source=None,
                sink=None,
                num_mappers=-1,
                num_reducers=-1):
    logging.debug("Beginning QuickInit.")
    assert jobname is not None
    self._jobname = jobname
    self.SetMapper(mapper)
    self.SetReducer(reducer)
    self.SetSource(source)
    self.SetMapperSink(mapper_sink)
    self.SetReducerSource(reducer_source)
    self.SetSink(sink)
    self.SetNumMappers(num_mappers)
    self.SetNumReducers(num_reducers)
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
  
  def SetNumMappers(self, num_mappers):
    """Set the ntarget number of concurrent mappers that should be used."""
    assert isinstance(num_mappers, int)
    self._num_mappers = num_mappers
    return self
  
  def SetNumReducers(self, num_reducers):
    """Set the target number of concurrent reducers that should be used."""
    assert isinstance(num_reducers, int)
    self._num_reducers = num_reducers
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
    
  def GetMapMaster(self):
    """Handle Map controlling page."""
    splits = ['', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p']
    urls = []
    for i in xrange(len(splits)):
      start_index = splits[i]
      if i == len(splits) - 1:
        # final index is the greatest unicode character
        end_index = GREATEST_UNICODE_CHARACTER
      else:
        end_index = splits[i+1]
      urls.append(self._NextUrl({"task": MAPPER_TASK_NAME,
                                 SOURCE_START_POINT: start_index,
                                 SOURCE_END_POINT: end_index,
                                 SOURCE_MAX_ENTRIES: 1000}))
    return {'urls': urls}

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
    splits = ['', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p']
    urls = []
    for i in xrange(len(splits)):
      start_index = splits[i]
      if i == len(splits) - 1:
        # final index is the greatest unicode character
        end_index = GREATEST_UNICODE_CHARACTER
      else:
        end_index = splits[i+1]
      urls.append(self._NextUrl({"task": REDUCER_TASK_NAME,
                                 SOURCE_START_POINT: start_index,
                                 SOURCE_END_POINT: end_index,
                                 SOURCE_MAX_ENTRIES: 1000}))
    return {'urls': urls}
  
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