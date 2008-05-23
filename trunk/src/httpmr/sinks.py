from google.appengine.ext import db
from httpmr import base

class AppEngineSink(httpmr.base.Sink):
  
  def Put(self, key, value):
    """Puts the provided value into the AppEngine datastore.  Key discarded.
    
    Args:
      key: Ignored
      value: An instance of a db.Model descendent
    
    Returns: None
    
    Raises: httpmr.base.SinkError on any datastore errors
    """
    assert isinstance(db.Model, value)
    try:
      db.put(value)
    except db.Error, e:
      raise base.SinkError(e)