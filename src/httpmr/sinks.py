from google.appengine.ext import db
from httpmr import base

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
