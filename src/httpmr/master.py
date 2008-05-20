from google.appengine.ext import webapp
from httpmr import base
from httpmr import sinks
from httpmr import sources
from wsgiref import handlers


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
  
  def SetMapper(self, mapper):
    """Set the Mapper that should be used for mapping operations."""
    assert isinstance(mapper, base.Mapper)
    self._mapper = mapper
  
  def SetReducer(self, reducer):
    """Set the Reducer that should be used for reduce operations."""
    assert isinstance(reducer, base.Reducer)
    self._reducer = reducer
    
  def SetSource(self, source):
    """Set the data source from which mapper input should be read."""
    self._source = source
  
  def SetSink(self, sink):
    """Set the data sink to which reducer output should be written."""
    self._sink = sink
  
  def SetNumMappers(self, num_mappers):
    """Set the ntarget number of concurrent mappers that should be used."""
    assert isinstance(num_mappers, int)
    self._num_mappers = num_mappers
  
  def SetNumReducers(self, num_reducers):
    """Set the target number of concurrent reducers that should be used."""
    assert isinstance(num_reducers, int)
    self._num_reducers = num_reducers
  
  def get(self):
    """Handle mapper and reducer tasks."""

  def GetMapper(self):
    """Handle mapper tasks."""
  
  def GetReducer(self):
    """Handle reducer tasks."""


def main():
  application = webapp.WSGIApplication([('/', Master)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)

if __name__ == "__main__":
  main()