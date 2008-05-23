from google.appengine.ext import db
from httmr import base

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
        max_entries,
        start_point_inclusive=True,
        end_point_inclusive=True):
    start_filter_operator = " > "
    if start_point_inclusive:
      start_filter_operator = " >= "
    start_filter = "".join((self.key_parameter, start_filter_operator))
    self.base_query.filter(start_filter, start_point)
    
    end_filter_operator = " < "
    if end_point_inclusive:
      end_filter_operator = " <= "
    end_filter = "".join((self.key_parameter, end_filter_operator))
    self.base_query.filter(end_filter, end_point)
    
    for model in self.base_query.fetch(limit=max_entries):
      yield getattr(model, key_parameter), model