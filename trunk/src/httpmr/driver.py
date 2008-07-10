#!/usr/bin/python

import HTMLParser
import logging
import optparse
import time
import sys
import threading
import urllib
import urllib2
import urlparse

MAP_MASTER_TASK_NAME = "map_master"
REDUCE_MASTER_TASK_NAME = "reduce_master"
INFINITE_PARAMETER_VALUE = -1


class OperationResult(object):
  
  def __init__(self):
    self.url = None
    self.next_url = None
    self.statistics = {}


class OperationResultHTMLParser(HTMLParser.HTMLParser):
  
  def handle_starttag(self, tag, attrs):
    if tag == "a":
      self.handle_start_a_tag(attrs)
    elif tag == "pre":
      self.handle_start_pre_tag(attrs)
    
  def handle_start_a_tag(self, attrs):
    self.url = None
    for tuple in attrs:
      if tuple[0] == "href":
        self.url = tuple[1]
  
  def handle_start_pre_tag(self, attrs):
    logging.info(attrs)
    self.statistics = {}


class MasterPageResultHTMLParser(HTMLParser.HTMLParser):

  def Init(self):
    self.urls = []
  
  def handle_starttag(self, tag, attrs):
    if tag == "a":
      self.handle_start_a_tag(attrs)
    
  def handle_start_a_tag(self, attrs):
    logging.debug("Reading 'a' tag: %s" % attrs)
    for tuple in attrs:
      if tuple[0] == "href":
        self.urls.append(tuple[1])


class TooManyTriesError(Exception): pass


class OperationThread(threading.Thread):
  
  def SetOperationCallback(self, callback, **kwargs):
    """Set the callback that will be invoked when this operation is finished.
    
    args:
      callback: A callable that takes one parameter, the OperationResult
          constructed by this thread when the operation has completed, and the
          supplied keyword arguments.
      kwargs: Keyword arguments that should be passed to the callback.
    """
    self.operation_callback = callback
    self.operation_callback_kwargs = kwargs
  
  def SetUnrecoverableErrorCallback(self, callback, **kwargs):
    """Set the callback that will be invoked on unrecoverable errors.
    
    args:
      callback: A callable that takes the failed URL as its first argument, the
          unrecoverable exception as its second, and the supplied keyword
          arguments.
      kwargs: The keyword arguments that should be supplied to the callback.
    """
    self.error_callback = callback
    self.error_callback_kwargs = kwargs
  
  def SetMaxTries(self, max_tries):
    self.max_tries = max_tries
  
  def SetUrl(self, url):
    self.url = url
  
  def run(self):
    assert self.url is not None
    assert self.operation_callback is not None
    assert self.error_callback is not None
    self.html = None
    try:
      self.html = self._FetchWithRetries(self.url, self.max_tries)
      logging.debug("Retrieved HTML %s" % self.html)
    except Exception, e:
      self.error_callback(self.url, e, **self.error_callback_kwargs)
    self.operation_callback(self._GetResults(),
                            **self.operation_callback_kwargs)
  
  def _FetchWithRetries(self, url, max_tries):
    tries = 0
    while tries < max_tries or max_tries == INFINITE_PARAMETER_VALUE:
      try:
        tries += 1
        return self._Fetch(url)
      except urllib2.HTTPError, e:
        logging.warning("HTTPError on fetch of %s: %s" % (url, str(e)))
        self._WaitForRetry(tries)
    raise TooManyTriesError("Too many tries on URL %s" % url)
  
  def _WaitForRetry(self, tries):
    wait_time_sec = min(tries * 2, 60)
    logging.debug("Sleeping for %s seconds." % wait_time_sec)
    time.sleep(wait_time_sec)
    
  def _Fetch(self, url):
    safe_url = self._GetSafeUrl(url)
    logging.debug("Fetching %s" % safe_url)
    return urllib2.urlopen(safe_url).read()
  
  def _GetSafeUrl(self, url):
    parts = urlparse.urlsplit(url)
    safe_query = \
        urllib.quote(parts.query).replace("%26", "&").replace("%3D", "=")
    parts = (parts.scheme,
             parts.netloc,
             parts.path,
             safe_query,
             parts.fragment)
    return urlparse.urlunsplit(parts)
  
  def _GetResults(self):
    parser = OperationResultHTMLParser()
    parser.feed(self.html)
    parser.close()
    
    results = OperationResult()
    results.url = self.url
    results.next_url = None
    if hasattr(parser, "url"):
      results.next_url = parser.url
    results.statistics = None
    if hasattr(parser, "statistics"):
      result.statistics = parser.statistics
    return results
  

class HTTPMRDriver(object):
  
  def __init__(self,
               httpmr_base,
               max_operation_tries=-1,
               max_operations_inflight=-1):
    self.httpmr_base = httpmr_base
    self.max_operation_tries = max_operation_tries
    self.max_operations_inflight = max_operations_inflight
    self.results = []
    self.lock = threading.Lock()
    
  def Run(self):
    logging.info("Beginning HTTPMR Driver Run with base URL %s" %
                 self.httpmr_base)
    self._Map()

  def _HandleUnrecoverableOperationError(self, url, error):
    # TODO(peterdolan): Something more intelligent than just exiting, not sure
    # what though...
    logging.error("Unrecoverable error on url %s: %s; %s" %
                  (url, type(error), error))
    sys.exit(1)
    
  def _Map(self):
    self._LaunchPhase(MAP_MASTER_TASK_NAME, self._AllMapOperationsComplete)
  
  def _AllMapOperationsComplete(self):
    self._Reduce()
  
  def _Reduce(self):
    self._LaunchPhase(REDUCE_MASTER_TASK_NAME, self._AllReduceOperationsComplete)
  
  def _AllReduceOperationsComplete(self):
    logging.info("Done!")
  
  def _LaunchPhase(self, phase_task_name, all_operations_complete_callback):
    base_urls = self._GetInitialUrls(phase_task_name)
    self.threads_inflight = 0
    self.threads_pending = []
    for url in base_urls:
      thread = self._CreateOperationThread(url,
                                           all_operations_complete_callback)
      if (self.threads_inflight < self.max_operations_inflight or
          self.max_operations_inflight == INFINITE_PARAMETER_VALUE):
        self.threads_inflight += 1
        thread.start()
      else:
        self.threads_pending.append(thread)

  def _GetInitialUrls(self, task):
    url = "%s?task=%s" % (self.httpmr_base, task) 
    html = urllib2.urlopen(url).read()
    parser = MasterPageResultHTMLParser()
    parser.Init()
    parser.feed(html)
    parser.close()
    return parser.urls

  def _CreateOperationThread(self, url, all_operations_complete_callback):
    thread = OperationThread()
    thread.SetUrl(url)
    # TODO(peterdolan): Make the maximum operation tries configurable via a
    # command-line parameter
    thread.SetMaxTries(self.max_operation_tries)
    thread.SetOperationCallback(
        self._HandleThreadCompletion,
        all_operations_complete_callback=all_operations_complete_callback)
    thread.SetUnrecoverableErrorCallback(
        self._HandleUnrecoverableOperationError)
    return thread
  
  def _HandleThreadCompletion(self, results, all_operations_complete_callback):
    self.lock.acquire()
    self.threads_inflight -= 1
    
    self.results.append(results)
    if results.next_url is not None:
      logging.debug("Initializing new thread to handle %s" % results.next_url)
      thread = self._CreateOperationThread(results.next_url,
                                           all_operations_complete_callback)
      self.threads_pending.insert(0, thread)

    if self.threads_pending:
      logging.debug("Starting the next pending thread.")
      thread = self.threads_pending.pop()
      self.threads_inflight += 1
      thread.start()
    
    if not self.threads_inflight:
      logging.debug("All threads completed for this phase.")
      all_operations_complete_callback()
    self.lock.release()
  

def main():
  logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s %(levelname)-8s %(message)s',
                      datefmt='%a, %d %b %Y %H:%M:%S',
                      stream=sys.stdout)
  options_parser = optparse.OptionParser()
  options_parser.add_option("-b",
                            "--httpmr_base",
                            action="store",
                            type="string",
                            dest="httpmr_base",
                            help="The base URL of the HTTPMR operation.")
  options_parser.add_option("-i",
                            "--max_operations_inflight",
                            action="store",
                            type="int",
                            dest="max_operations_inflight",
                            default=-1,
                            help="The maximum number of operations to keep "
                                + "simultaneously inflight.  -1 for inf.")
  options_parser.add_option("-f",
                            "--max_per_operation_failures",
                            action="store",
                            type="int",
                            dest="max_per_operation_failures",
                            default=-1,
                            help="The maximum number of times any given "
                                + "operation can fail before a fatal error is"
                                + " thrown.  -1 for inf.")
  (options, args) = options_parser.parse_args()
  
  driver = HTTPMRDriver(options.httpmr_base,
                        options.max_per_operation_failures,
                        options.max_operations_inflight)
  driver.Run()


if __name__ == "__main__":
  main()