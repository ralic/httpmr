import logging
import wsgiref
from google.appengine.ext import webapp
from httpmr import appengine
from httpmr import base
from wsgiref import handlers
from google.appengine.ext import db


class Document(db.Model):
  title = db.StringProperty(required=True)
  contents = db.TextProperty(required=True)


class DocumentIndex(db.Model):
  token = db.StringProperty(required=True)
  document_titles = db.StringListProperty()


class TokenMapper(base.Mapper):
  
  def Map(self, document_title, document):
    for token in document.contents.split(" "):
      if token:
        logging.info("Yielding %s: %s" % (token, document_title))
        yield token, document_title


class TokenReducer(base.Reducer):
  
  def Reduce(self, token, document_titles):
    document_titles = set(document_titles)
    logging.info("Reducing %s: %s" % (token, document_titles))
    document_index = DocumentIndex(token=token,
                                   document_titles=list(document_titles))
    yield None, document_index


class ConstructTokenIndexMapReduce(appengine.AppEngineMaster):
  
  def __init__(self):
    logging.info("Initializing")
    job_name = "construct_token_index"
    source = appengine.AppEngineSource(Document.all(),
                                       "title")
    self.QuickInit(job_name,
                   mapper=TokenMapper(),
                   reducer=TokenReducer(),
                   source=source,
                   sink=appengine.AppEngineSink(),
                   num_mappers=20,
                   num_reducers=10)


def main():
  for contents in ["dpqfoiew pqoifpqo wief qpwoeif jqpwoiefj pqowiejf qwoiejf",
                   "fkdjqpoweif poqiwefjp oqiwef npqowiefj qoiwejf ",
                   "fqpewofiqjp owefq wkenfoiquwef[o  pqweofqwekn nqjkwnef",
                   "feqpio qwiopjfqo wef pqwnepfo ijqewof qnwefn poqnewfp on"]:
    title = contents.split(" ")[0]
    logging.info("Filling store with document title: %s, contents: %s" %
                 (title, contents))
    Document(title=title, contents=contents).put()
  application = webapp.WSGIApplication([('/examples/construct_token_index',
                                         ConstructTokenIndexMapReduce)],
                                       debug=True)
  wsgiref.handlers.CGIHandler().run(application)

if __name__ == "__main__":
  main()