import logging
import random
import sys
from google.appengine.ext import db
from httpmr import base
from httpmr import master


class IntermediateValueHolder(db.Model):
  job_name = db.StringProperty(required=True)
  nonsense = db.IntegerProperty(required=True)
  intermediate_key = db.StringProperty(required=True)
  intermediate_value = db.TextProperty(required=True)


class AppEngineSink(base.Sink):
  
  def Put(self, key, value):
    """Puts the provided value into the AppEngine datastore.  Key discarded.
    
    Args:
      key: Ignored
      value: An instance of a db.Model descendent
    
    Returns: None
    
    Raises: httpmr.base.SinkError on any datastore errors
    """
    assert isinstance(value, db.Model)
    try:
      value.put()
    except db.Error, e:
      raise base.SinkError(e)


class AppEngineIntermediateSink(AppEngineSink):
  
  def __init__(self, job_name):
    self._job_name = job_name
  
  def Put(self, key, value):
    max = sys.maxint
    # For the intermediate value sink to function properly, we have to guarantee
    # that no values are written with the _actual_ minimum integer value.
    min = 2 - max
    nonsense = random.randint(min, max)
    IntermediateValueHolder(job_name=self._job_name,
                            nonsense = nonsense,
                            intermediate_key=key,
                            intermediate_value=value).put()
                            

class AppEngineSource(base.Source):

  def __init__(self, base_query, key_parameter):
    """Initialize the AppEngineSource with a base GQL object and key parameter.
    
    The supplied base_query
    
    for example:
      class Story(db.Model):
        title = db.StringProperty()
        active = db.BooleanProperty()
      
      query = Story.all().filter('active = True')
      source = AppEngineSource(query, 'title')
    
    The source constructed in the example will be used as:
    
      query.filter("%s >= " % key_parameter, start_point)
      query.filter("%s <= " % key_parameter, end_point)
      for entry in query.fetch():
        mapper.Map(getattr(entry, key_parameter), entry)

    Args:
      base_query: A db.Query instance that defines the base filters for
        all mapper operations
      key_parameter: The parameter of the model that will be retrieved by the
        base_query that should be used as the mapper key, and which will be
        used to shard map operations.
    """
    assert isinstance(base_query, db.Query)
    self.base_query = base_query
    self.key_parameter = key_parameter

  def Get(self,
        start_point,
        end_point,
        max_entries):
    assert isinstance(max_entries, int)
    self.base_query.filter("%s > " % self.key_parameter, start_point)
    self.base_query.filter("%s <= " % self.key_parameter, end_point)
    self.base_query.order(self.key_parameter)
    for model in self.base_query.fetch(limit=max_entries):
      key = getattr(model, self.key_parameter)
      yield key, model


class IntermediateAppEngineSource(base.Source):
  """A Source for the intermediate values output by the MapReduce Mappers
  
  """

  def __init__(self, job_name):
    self.job_name = job_name

  def Get(self,
          start_point,
          end_point,
          max_entries):
    assert isinstance(max_entries, int)
    values_returned = 0
    while True:
      if values_returned > max_entries:
        return
      
      current_key = self._GetNextKey(start_point, end_point)
      if current_key is None:
        return
      else:
        # The next time we loop, the next key we fetch should be > the key we're
        # currently serving
        start_point = current_key
      
      for intermediate_value in self._GetIntermediateValuesForKey(current_key):
        yield current_key, intermediate_value
        values_returned = values_returned + 1
      
  def _GetIntermediateValuesForKey(self, intermediate_key):
    """For the given intermediate value key, get all intermediate values.
    
    Get all intermediate values from the Datastore, querying multiple times if
    necessary to get absolutely every intermediate value (past the 1000-result
    limits).
    """
    # We're guaranteed by the intermediate value sink that no intermediate
    # values are written with the _actual_ minimum value.  Always 1 greater.
    current_nonsense = 1 - sys.maxint
    while True:
      # Loop through all possible intermediate values
      query = IntermediateValueHolder.all()
      query.filter("job_name = ", self.job_name)
      query.filter("intermediate_key = ", intermediate_key)
      query.filter("nonsense > ", current_nonsense)
      query.order("nonsense")
      
      intermediate_values_fetched = 0
      limit = 1000
      for intermediate_value in query.fetch(limit=limit):
        yield intermediate_value
        intermediate_values_fetched = intermediate_values_fetched + 1
      if intermediate_values_fetched < limit:
        return
  
  def _GetNextKey(self, greater_than_key, less_than_eq_key):
    """Determine the value of the next key that should be reduced.
    
    Args:
      greater_than_key: The value that the next key should be greater than.
      less_than_eq_key: The value that the next key should be less than or
        equal to.
    """
    get_next_key_query = IntermediateValueHolder.all()
    get_next_key_query.filter("job_name = ", self.job_name)
    get_next_key_query.filter("intermediate_key > ", greater_than_key)
    get_next_key_query.filter("intermediate_key <= ", less_than_eq_key)
    value = get_next_key_query.get()
    if value is None:
      return None
    else:
      return value.intermediate_key


class AppEngineMaster(master.Master):
  
  def QuickInit(self,
                jobname,
                mapper=None,
                reducer=None,
                source=None,
                sink=None):
    logging.debug("Beginning QuickInit.")
    assert jobname is not None
    self._jobname = jobname
    self.SetMapper(mapper)
    self.SetReducer(reducer)
    self.SetSource(source)

    self.SetMapperSink(AppEngineIntermediateSink(jobname))
    self.SetReducerSource(IntermediateAppEngineSource(jobname))
    
    self.SetSink(sink)
    logging.debug("Done QuickInit.")
    return self