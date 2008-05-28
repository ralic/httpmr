import time
from google.appengine.ext import webapp
from httpmr import base
from httpmr import sinks
from httpmr import sources
from wsgiref import handlers


class Error(Exception): pass
class UnknownTaskError(Error): pass
class MissingRequiredParameterError(Error): pass


# Some constants for URL handling
MAPPER_TASK_NAME = "mapper"
REDUCER_TASK_NAME = "reducer"
VALID_TASK_NAMES = [MAPPER_TASK_NAME, REDUCER_TASK_NAME]

SOURCE_START_POINT = "source_start_point"
SOURCE_END_POINT = "source_end_point"
SOURCE_MAX_ENTRIES = "source_max_entries"


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
                mapper=None,
                reducer=None,
                source=None,
                sink=None,
                num_mappers=-1,
                num_reducers=-1):
    self.SetMapper(mapper)
    self.SetReducer(reducer)
    self.SetSource(source)
    self.SetSink(sink)
    self.SetNumMappers(num_mappers)
    self.SetNumReducers(num_reducers)
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
    """Handle mapper and reducer tasks."""
    task = self.request.params["task"]
    if task is None:
      GetController()
    elif task is MAPPER_TASK_NAME:
      GetMapper()
    elif task is REDUCER_TASK_NAME:
      GetReducer()
    else:
      raise UnknownTaskError("Task name '%s' is not recognized.  Valid task "
                             "values are %s" % (task, VALID_TASK_NAMES))
    
  def GetController(self):
    """Handle controlling page."""

  def GetMapper(self):
    """Handle mapper tasks."""
    start_point = self.request.params(SOURCE_START_POINT)
    end_point = self.request.params(SOURCE_END_POINT)
    max_entries = self.request.params(SOURCE_MAX_ENTRIES) or 1000
    mapper_data = self.source.Get(start_point, end_point, max_entries)
    timer = TaskSetTimer()
    timer.Start()
    for key_value_pair in mapper_data:
      if timer.ShouldStop():
        # Return the last key that was put
        return "The last key that was put, or None"
      
      # Map, and put its returned values into the sink, retain the last key
      # put
      True
  
  def GetReducer(self):
    """Handle reducer tasks."""


def main():
  application = webapp.WSGIApplication([('/', Master)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)

if __name__ == "__main__":
  main()