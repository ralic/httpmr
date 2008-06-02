import logging
from google.appengine.ext import db
from httpmr import base


class IntermediateValueHolder(db.Model):
  job_name = db.StringProperty(required=True)
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
    IntermediateValueHolder(job_name=self._job_name,
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