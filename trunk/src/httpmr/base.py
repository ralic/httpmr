class Mapper(object):

  def Map(self, key, value):
    """Map the provided key and value to a set of intermediate keys and values.
    
    Mapper#Map must be implemented as a generator, outputting its key-value
    pairs via the 'yield' keyword.

    For example, a simple Map method that would be used to count the number of
    times a given value is seen by outputting 1 for every value:
      def Map(self, key, value):
        yield str(value), "1"
    
    All yielded output must be a (str, str) tuple.
    
    For more information on generators, see
    the official Python documentation at
    http://www.python.org/doc/2.5/tut/node11.html#SECTION00111000000000000000000
    or consider the stock Mappers for more examples.
    """


class Reducer(object):
  
  def Reduce(self, key, values):
    """Operate on all values output for the given key simultaneously.
    
    Reduce must be implemented as a generator, outputting its reduced key-value
    pairs via the 'yield' operator.
    
    For example, a simple Reduce method that counts the number of times a given
    key was seen:
      def Reduce(self, key, values):
        yield key, len(values)
    
    All yielded output must be a (str, str) tuple.
    
    For more information on generators, see
    the official Python documentation at
    http://www.python.org/doc/2.5/tut/node11.html#SECTION00111000000000000000000
    or consider the stock Reducers for more examples.
    
    Args:
      key: The key (str) to which all of the values correspond
      values: A list of values (all str) corresponding to the key.
    
    Returns:
      A generator
    """


class Source(object):
  
  def Get(self,
          start_point,
          end_point,
          max_entries,
          start_point_inclusive=True,
          end_point_inclusive=True):
    """Get a set of data for Mapping
    
    Args:
      start_point: The starting point for data segmentation
      end_point: The ending point for data segmentation
      max_entries: The maximum number of data points that should be retrieved
      start_point_inclusive: Whether or not the starting point should be
        considered inclusive.
      end_point_inclusive: Whether or not the ending point should be considered
        inclusive.
    """
  

class Sink(object):
  
  def Put(self, key, value):
    """Output the provided key and value to persistent storage.
    
    """
