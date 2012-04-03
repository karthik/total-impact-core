from werkzeug import generate_password_hash, check_password_hash
import totalimpact.dao as dao
from totalimpact.config import Configuration
from totalimpact.providers.provider import ProviderFactory
import time, uuid, json, hashlib, inspect

class Error(dao.Dao):
    __type__ = 'error'


# FIXME: do we want a created and last modified property on the user?
class User():
    
    """
    {
        "id": "1234f",
        "name": "jason priem",
        "email": "abcd@foo.com",
        "password": "1234fd56", # hash
        "collection_ids": ["abcd3", "abcd4"], # tiid
        "created": 12134234242.234,
        "last_modified": 1239898237.234
    }
    """
    def __init__(self, id=None, password=None, password_hash=None, name=None, 
                        email=None, collections=None, seed=None):
        # for convenience with CouchDB we store all the properties in an internally
        # managed dict object which can just be json serialised out to the DAO
        # This object has a __getattr__ override below which makes the object 
        # appear as if all the dictionary keys are member attributes of this object
        
        # inherit the init
        super(User,self).__init__()
        
        # load from the seed first
        self.data = seed if seed is not None else {}
        
        # if there was no seed, load the properties, otherwise ignore them
        if seed is None:
            self.data['id'] = id if id is not None else str(uuid.uuid4())
            
            if password is not None:
                self.set_password(password)
            elif password_hash is not None:
                self.data['password'] = password_hash
            
            self.data['collections'] = collections if collections is not None else []
            self.data['name'] = name if name is not None else None
            self.data['email'] = email if email is not None else None
    
    def set_password(self, password):
        self.data['password'] = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.data['password'], password)
        
    def collection_ids(self):
        return self.data['collections']
        
    def add_collection(self, collection_id):
        if collection_id not in self.data['collections']:
            self.data['collections'].append(collection_id)
    
    def remove_collection(self, collection_id):
        if collection_id in self.data['collections']:
            self.data['collections'].remove(collection_id)
    

# FIXME: collection doesn't have an ID
# FIXME: may need to ditch the meta section
class Collection(dao.Dao):
    __type__ = 'collection'
    
    """
    {
        "id": "uuid-goes-here"
        "collection_name": "My Collection",
        "owner": "abcdef",
        "created": 1328569452.406,
        "last_modified": 1328569492.406,
        "ids": ["abcd3", "abcd4"]  #tiid
    }
    """
    def __init__(self, dao, id):

        def __init__(self, dao, id=None):
            if id is None:
                self.id = uuid.uuid4().hex
            else:
                self.id = id

        def load(self):
            doc = self.dao.get(id)
            for key in doc:
                self[key] = doc[key]

            self.last_requested = time.time()

        def save(self):
            doc = {}
            # put this objects relevant properties together in the doc.
            self.dao.save(doc)
        
    def item_ids(self):
        return self.data['ids']
        
    def add_item(self, item_id):
        if item_id not in self.data['ids']:
            self.data['ids'].append(item_id)
    
    def add_items(self, item_ids):
        for item in item_ids:
            self.add_item(item)
    
    def remove_item(self, item_id):
        if item_id in self.data['ids']:
            self.data['ids'].remove(item_id)
        

# FIXME: the code terminology and the docs terminology differ slightly:
# "alias" vs "aliases", "metric" vs "metrics"
# FIXME: do we want a created and last modified property on the item?
# FIXME: no id on the item? this should appear in the alias object?
class Item():
    """{
        "id": "uuid4-goes-here",
        "aliases": "aliases_object",
        "metrics": "metric_object",
        "biblio": "biblio_object",
        "created": 23112412414.234,
        "last_modified": 12414214.234,
        "last_requested": 124141245.234
    }
    """
    dao = None

    def __init__(self, dao, id=None, seed=None):
        self.dao = dao

        if id is None:
            self.id = uuid.uuid4().hex
        else: 
            self.id = id

        if seed:
            for key in seed:
                setattr(self, key, seed[key])


    def load(self):
        doc = self.dao.get(self.id)
        if doc is None:
            raise(LookupError)
        
        for key in doc:
            setattr(self, key, doc[key])

        setattr(self, "aliases", Aliases(seed=doc["aliases"]))

        setattr(self, "last_requested", time.time())
        return doc


    def save(self):
        doc = self.as_dict()
        # couch wants the underscore...should be fixed in dao, not here.
        doc["_id"] = doc.pop("id")

        try:
            self.dao.update_item(doc, self.id)
        except LookupError:
            self.dao.create_item(doc, self.id)
        return doc

    def keys_from_docstring(self):
        json_doc = inspect.getdoc(self)
        return json.loads(json_doc).keys()

    def as_dict(self):
        doc = {}
        for key in self.keys_from_docstring():
            try:
                temp_val = getattr(self, key)
                try:
                    val = temp_val.as_dict()
                except AttributeError:
                    val = temp_val

                doc[key] = val
                
            except AttributeError:
                doc[key] = None
        return(doc)

    def __str__(self):
        return str(self.as_dict())



class Biblio(object):
    """
    {
        "title": "An extension of de Finetti's theorem", 
        "journal": "Advances in Applied Probability", 
        "author": [
            "Pitman, J"
        ], 
        "collection": "pitnoid", 
        "volume": "10", 
        "id": "p78", 
        "year": "1978", 
        "pages": "268 to 270"
    }
    """
    def __init__(self, seed=None):
        self.data = seed if seed is not None else ""
            
    def __str__(self):
        return str(self.data)

    def __repr__(self):
        return str(self.data)


class Metrics(object):
    """
    {
        "meta": {
            "PROVIDER_ID": {
                "last_modified": 128798498.234,
                "last_requested": 2139841098.234,
                "ignore": false
            }
        },
        "bucket":{
            "hash" : "PROVIDER METRIC OBJECT", ...
        }
    }
    """
    def __init__(self, seed=None):
        self.data = seed if seed is not None else {}
        
        if 'meta' not in self.data.keys():
            self.data['meta'] = {}

        if 'bucket' not in self.data.keys():
            self.data['bucket'] = {}
        
        """ NOTE: code was inconsistent with documentation and parallel implementation in
        query.py; left in for reference for the time being
        # list all providers from config
        config = Configuration()
        for provider in config.providers:
            if provider['class'] not in self.data['meta'].keys():
                self.data['meta'][provider['class']] = {'last_modified':0, 'last_requested':time.time(), 'ignore':False}
        for item in self.data['meta']:
            self.data['meta'][item]['last_requested'] = time.time()
        """
        # FIXME: model objects shouldn't know about configuration
        # FIXME: this initialises all the providers
        config = Configuration()
        providers = ProviderFactory.get_providers(config)
        for p in providers:
            if p.id not in self.data['meta'].keys():
                self._update_last_modified(p.id)
                
        # FIXME: is constructing an object synonymous with requesting the data?  Aren't there
        # admin functions which might construct this object without "requesting" the data from
        # the provider
        now = time.time()
        for prov_id in self.data['meta'].keys():
            self.data['meta'][prov_id]['last_requested'] = now

    def meta(self, provider_id=None):
        return self.data['meta'] if provider_id is None else self.data['meta'].get(provider_id)
    
    # FIXME: assuming that ProviderMetric objects are deconstructed on ingest and
    # made part of the internal "data" object.  The object representations are then
    # re-constructed when they are requested.  This gives consistent behaviour at the
    # cost of being computationally a little expensive.  Alternative is to have a more
    # complex object which synchronises between in-memory ProviderMetric objects and their
    # "data" representations which are what actually get saved

    def add_provider_metric(self, provider_metric):
        hash = self._hash(provider_metric)
        self.data['bucket'][hash] = provider_metric.data
        self._update_last_modified(provider_metric.meta()["provider"])
        
    def list_provider_metrics(self, provider_id=None):
        if provider_id is None:
            return [ProviderMetric(seed=x) for x in self.data['bucket'].values()]
        return [ProviderMetric(seed=x) for x in self.data['bucket'].values() if x['id'] == provider_id]

    # FIXME: is this in use somewhere?
    def str_list_provider_metrics(self):
        return([str(val) for val in self.data['bucket'].values()])

    def _update_last_modified(self, provider_id):
        if self.data['meta'].has_key(provider_id):
            self.data['meta'][provider_id]['last_modified'] = time.time()
        else:
            self.data['meta'][provider_id] = {'last_modified':0, 'last_requested':time.time(), 'ignore':False}

    def _hash(self, provider_metric):
        # get a hash of the provider_metric's json representation
        j = self._canonical_repr(provider_metric.data)
        m = hashlib.md5()
        m.update(j)
        return m.hexdigest()
    
    def _canonical_repr(self, dict):
        canon = ""
        for key in sorted(dict.keys()):
            canon += unicode(key)
            v = dict[key]
            if hasattr(v, "keys"): # testing for 'dict' type doesn't work; go figure
                canon += "{" + self._canonical_repr(v) + "}"
            elif type(v) == list or type(v) == tuple:
                canon += "[" + "".join([unicode(x) for x in sorted(v)]) + "]"
            else:
                canon += unicode(v)
        return canon

    def __repr__(self):
        return str(self.data)

# FIXME: should this have a created property?
# FIXME: should things like "can_use_commercially" be true/false rather than the - yes
# string "0" or "1", or are there other values that can go in there
# FIXME: add a validation routine
# FIXME: we need a nicer interface to get at the contents of the inner data object
class ProviderMetric(object):
    """
    {
        "id": "Mendeley:readers",
        "value": 16,
        "created": 1233442897.234,
        "last_modified": 1328569492.406,
        "provenance_url": ["http:\/\/api.mendeley.com\/research\/public-chemical-compound-databases\/"],
        "meta": {
            "display_name": "readers"
            "provider": "Mendeley",
            "provider_url": "http:\/\/www.mendeley.com\/",
            "description": "Mendeley readers: the number of readers of the article",
            "icon": "http:\/\/www.mendeley.com\/favicon.ico",
            "category": "bookmark",
            "can_use_commercially": "0",
            "can_embed": "1",
            "can_aggregate": "1",
            "other_terms_of_use": "Must show logo and say 'Powered by Santa'",
        }
    }
    """
    def __init__(self, id=None, value=None, created=None, last_modified=None, provenance_url=None, meta=None, seed=None):
                        
        # load from the seed first
        self.data = seed if seed is not None else {}
        
        # if there was no seed, load the properties, otherwise ignore them
        if seed is None:            
            self.data['id'] = self._init(id, str(uuid.uuid4()))
            self.data['value'] = self._init(value, 0)
            self.data['created'] = self._init(created, time.time())
            self.data['last_modified'] = self._init(last_modified, time.time())
            self.data['meta'] = self._init(meta, {})
            
            # provenance url needs a bit of special treatment
            if not hasattr(provenance_url, "append"):
                self.data['provenance_url'] = [provenance_url]
            else:
                self.data['provenance_url'] = []

        if "meta" not in self.data.keys():
            self.data['meta'] = {}
        
    def value(self, val=None):
        if val is None:
            return self.data['value']
        else:
            self.data['value'] = val
            self.data['last_modified'] = time.time()
            
    def meta(self, meta=None):
        if meta is None:
            return self.data['meta']
        else:
            self.data['meta'] = meta
            self.data['last_modified'] = time.time()
    
    def __repr__(self):
        return str(self.data)


    # FIXME: this is not particularly intuitive, consider changing it
    def provenance(self, provenance=None):
        """
        get or set the provenance.
        
        This will retrieve the provenance array if urls is None
        If urls is not a list, the url will be appended to the existing urls
        If urls IS a list, it will overwrite the existing provenance list
        """
        if provenance is None:
            return self.data['provenance_url']
        else:
            if hasattr(provenance, "append"):
                self.data['provenance_url'] = provenance
            else:
                self.data['provenance_url'].append(provenance)
            self.data['last_modified'] = time.time()
    
    def _init(self, val, default=None):
        return val if val is not None else default
    
    def __getattribute__(self, att):
        try:
            return super(ProviderMetric, self).__getattribute__(att)
        except:
            return self.data[att]
    
    def __str__(self):
        return str(self.data)
        
    def __eq__(self, other):
        return self.data == other.data
    

class Aliases(object):
    """
    {
        "tiid":"123456",
        "title":["Why Most Published Research Findings Are False"],
        "url":["http:\/\/www.plosmedicine.org\/article\/info:doi\/10.1371\/journal.pmed.0020124"],
        "doi": ["10.1371\/journal.pmed.0020124"],
        "created": 12387239847.234,
        "last_modified": 1328569492.406
        ...
    }
    """
    
    not_aliases = ["created", "last_modified"]
    
    synonyms = {
        "DOI" : ["DIGITAL OBJECT IDENTIFIER"],
        "URI" : ["IRI"]
    }
    
    class Namespaces(object):
        TIID = "TIID"
        TITLE = "TITLE"
        URL = "URL"
        DOI = "DOI"
        URI = "URI"
        IRI = "URI" # deliberate
        GITHUB = "GITHUB"
        PMID = "PMID"
    
    NS = Namespaces
    
    def __init__(self, tiid=None, seed=None, **kwargs):
        # load from the seed first
        self._validate_seed(seed)
        self.data = self.canonical_dict(seed) if seed is not None else {}
        
        # if there was no seed, load the properties, otherwise ignore them
        if seed is None:
            self.data[self.NS.TIID] = self._init(tiid, str(uuid.uuid4()))
            for arg, val in kwargs.iteritems():
                arg = self.canonicalise(arg)
                if hasattr(val, "append"):
                    self.data[arg] = val
                else:
                    self.data[arg] = [val]
        else:
            if not self.data.has_key(self.NS.TIID):
                self.data[self.NS.TIID] = self._init(tiid, str(uuid.uuid4()))
    
    def add_alias(self, namespace, id):
        namespace = self.canonicalise(namespace)
        if namespace in self.data.keys():
            self.data[namespace].append(id)
        else:
            self.data[namespace] = [id]

    def add_unique(self, alias_list):
        for ns, id in alias_list:
            ns = self.canonicalise(ns)
            if id not in self.data.get(ns, []):
                self.add_alias(ns, id)
    
    def get_ids_by_namespace(self, namespace):
        ''' gets list of this object's ids in each given namespace
        
        returns [] if no ids
        >>> a = Aliases()
        >>> a.add_alias("foo", "id1")
        >>> a.get_ids_by_namespace("foo")
        ['id1']
        '''
        namespace = self.canonicalise(namespace)
        return self.data.get(namespace, [])
    
    def get_aliases_list(self, namespace_list=None): 
        ''' 
        gets list of this object's aliases in each given namespace
        
        returns a list of (namespace, id) tuples
        '''
        # if this is a get on everything, just summon up the
        # items
        if namespace_list is None:
            return [x for x in self.data.items() if x[0] not in self.not_aliases]
        
        # if the caller doesn't pass us a list, but just a single value, wrap it
        # up for them
        if not hasattr(namespace_list, "append"):
            namespace_list = [namespace_list]
        
        namespace_list = map(self.canonicalise, namespace_list)
        
        # otherwise, get for the specific namespaces
        ret = []
        for namespace in namespace_list:
            ids = self.get_ids_by_namespace(namespace)
            
            # crazy hack TODO fix lists/strings flying about
            if not hasattr(ids, "append"):
                ids = [ids]
            print ids
            ret += [(namespace, id) for id in ids]
        
        return ret
    
    def get_aliases_dict(self):
        return self.data

    def as_dict(self):
        # renamed for consistancy with Items(); TODO cleanup old one
        return self.data
        
    def _init(self, val, default=None):
        return val if val is not None else default
    
    def _validate_seed(self, seed):
        # FIXME: what does this actually do?
        pass
    
    def canonical_dict(self, seed):
        n = {}
        for k, v in seed.iteritems():
            if k not in self.not_aliases:
                n[self.canonicalise(k)] = v
            else:
                n[k] = v
        return n
    
    def canonicalise(self, namespace):
        canon = namespace.strip().upper()
        canon = self._synonym(canon)
        return canon
    
    def _synonym(self, namespace):
        for ns, syn in self.synonyms.iteritems():
            if namespace == ns:
                return namespace
            elif namespace in syn:
                return ns
        return namespace
    
    def __getattribute__(self, att):
        try:
            return super(Aliases, self).__getattribute__(att)
        except:
            att = self.canonicalise(att) if att not in self.not_aliases else att
            return self.data[att]
    
    def __repr__(self):
        return "TIID: " + self.tiid + " " + str(self.data)
        
    def __str__(self):
        return "TIID: " + self.tiid + " " + str(self.data)

