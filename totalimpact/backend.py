#!/usr/bin/env python

import time, sys, logging, os, traceback, datetime, threading, Queue
from totalimpact import default_settings, dao
from totalimpact.providers.provider import ProviderError, ProviderFactory

logger = logging.getLogger('ti.backend')
logger.setLevel(logging.DEBUG)

class backendException(Exception):
    pass

class Backend(object):
    
    def __init__(self, alias_workers, biblio_workers, metrics_workers, dao):
        self.alias_workers = alias_workers
        self.biblio_workers = biblio_workers
        self.metrics_workers = metrics_workers
        self.dao = dao


    def get_fresh_item(self, dao):
        res = dao.db.view("queues/needs_aliases")
        try:
            item = res.rows[0]["value"]
        except IndexError:
            item = None
        return item


    def run(self, run_once=False):
        while True:
            item = self.get_fresh_item(self.dao)
            if item is None:
                time.sleep(0.5)
            else:
                start = time.time()
                logger.info("item '{tiid}' dequeued; beginning update.".format(tiid=item["_id"]))
                item = self.alias_workers.update(item)
                item = self.biblio_workers.update(item)
                logger.info("item '{tiid}' finished alias, biblio updates in {s} seconds".format(
                    tiid=item["_id"],
                    s=round(time.time() - start, 2)
                ))
                self.metrics_workers.update(item)

            if run_once:
                break



class Worker():
    """
    Shared code for the Metrics, Aliases, and Biblio workers

    Mostly just for the ask_provider mega-method, which does
    logging and error-handling. Much of that should be in ItemFactory or
    Provider I think.
    """

    def __init__(self, provider, thread_id=1):
        self.provider = provider
        self.method = self.get_relevant_provider_method(provider)

        self.provider_name = provider.__class__.__name__
        self.method_name = self.method.__name__
        self.worker_id = "{provider}.{method} [{thread_id}]".format(
            provider=self.provider_name,
            method=self.method_name,
            thread_id=thread_id
        )
        self.threads_allowed = provider.threads_allowed


    def get_relevant_provider_method(self, provider):
        """ Gets the correct provider update method, using the name of this class.

        Relies on the fact that this class will always be subclassed by either
        BiblioWorker, AliasesWorker, or MetricsWorker classes, and that these use
        provider.biblio, .aliases, and .metrics respectively.
        """
        class_name = self.__class__.__name__
        method_name = class_name.replace("Worker", "").lower()
        method = getattr(provider, method_name)
        return method


    def log_error(self, aliases, tiid, error_msg, tb):
        # This method is called to record any errors which we obtain when
        # trying to call the provider.
        logger.error("%20s: exception for item(%s): %s" %
                     (self.worker_id, tiid, error_msg))
        
        e = backendException(error_msg)
        e.id = tiid
        e.provider = self.provider_name
        e.stack_trace = "".join(traceback.format_tb(tb))
        
        logger.debug(str(e.stack_trace))


    def ask_provider(self, aliases, tiid):
        """ Get a response from the provider, using the supplied aliases

            This will deal with retries and sleep / backoff as per the
            configuration for the given provider.  Should
            probably be split into smaller methods; some of these should be
            in the ItemFactory.
        """

        error_counts = 0
        success = False
        error_limit_reached = False
        error_msg = False
        max_retries = self.provider.get_max_retries()
        response = None

        while not error_limit_reached and not success:
            response = None

            try:
                cache_enabled = (error_counts == 0)

                if not cache_enabled:
                    logger.debug("%20s: cache NOT enabled %s %s for %s"
                    % (self.worker_id, self.provider_name, self.method_name, tiid))

                alias_tuples = self.alias_tuples_from_dict(aliases)
                if alias_tuples:
                    logger.debug("%20s: calling %s %s for %s" %
                                 (self.worker_id, self.provider_name, self.method_name, tiid))
                    try:
                        response = self.method(alias_tuples)
                    except NotImplementedError:
                        response = None
                else:
                    logger.debug("%20s: skipping %s %s %s for %s, no aliases"
                    % (self.worker_id, self.provider_name, self.method_name, str(aliases), tiid))
                    response = None

                success = True # didn't get any errors.

            except ProviderError, e:
                error_msg = repr(e)
            except Exception, e:
                # All other fatal errors. These are probably some form of
                # logic error. We consider these to be fatal.
                error_msg = repr(e)
                error_limit_reached = True

            if error_msg:
                # If we had any errors, update the error counts and sleep if
                # we need to do so, before retrying.
                tb = sys.exc_info()[2]
                self.log_error(
                    aliases,
                    tiid,
                    '%s on %s %s' % (error_msg, self.provider_name, self.method_name),
                    tb)

                error_counts += 1

                if error_counts > max_retries:
                    logger.error("%20s: error limit reached (%i/%i) for %s, aborting %s %s" % (
                        self.worker_id, error_counts, max_retries, tiid, self.provider_name, self.method_name))
                    error_limit_reached = True
                else:
                    duration = self.provider.get_sleep_time(error_counts)
                    logger.warning("%20s: error on %s, pausing thread for %i seconds." %
                        (self.worker_id, tiid, duration))
                    time.sleep(duration)

        if success:
            # response may be None for some methods and inputs
            if response:
                logger.debug("%20s: success %s %s for %s, got %i results"
                % (self.worker_id, self.provider_name, self.method_name, tiid, len(response)))
            else:
                logger.debug("%20s: success %s %s for %s, got 0 results"
                % (self.worker_id, self.provider_name, self.method_name, tiid))

        return (success, response)

    def alias_tuples_from_dict(self, aliases_dict):
        """
        Convert from aliases dict we use in items, to a list of alias tuples.

        The providers need the tuples list, which look like this:
        [(doi, 10.123), (doi, 10.345), (pmid, 1234567)]
        """
        alias_tuples = []
        for ns, ids in aliases_dict.iteritems():
            if isinstance(ids, basestring): # it's a date, not a list of ids
                alias_tuples.append((ns, ids))
            else:
                for id in ids:
                    alias_tuples.append((ns, id))

        return alias_tuples


class BiblioWorker(Worker):

    def update(self, item):
        (success, new_biblio) = self.ask_provider(item["aliases"], item["_id"])
        if success:
            if new_biblio:
                for (k, v) in new_biblio.iteritems():
                    if not item["biblio"].has_key(k):
                        item["biblio"][k] = v

                logger.info("%20s: in process_item biblio %s provider %s"
                % (self.worker_id, item["_id"], self.provider_name))

        else:
            logger.info("%20s: NOT SUCCESS in process_item %s, partial biblio only for provider %s"
            % (self.worker_id, item["_id"], provider.provider_name))

        logger.info("%20s: interm biblio for item %s after %s: %s"
        % (self.worker_id, item["_id"],  self.provider_name, str(item["biblio"])))
        return item


class AliasesWorker(Worker):

    def update(self, item):
        logger.info("%20s: initial alias list for %s is %s" 
                    % (self.worker_id, item["_id"], item["aliases"]))

        (success, new_aliases) = self.ask_provider(item["aliases"], item["_id"])
        if success:
            if new_aliases:
                logger.debug("here are the new aliases from {provider}: {aliases}.".format(
                    provider=self.provider_name,
                    aliases=str(new_aliases)
                ))
                item = self.add_aliases(new_aliases, item)
        else:
            logger.info("%20s: NOT SUCCESS in process_item %s, partial aliases only for provider %s"
                % (self.worker_id, item["_id"], self.provider_name))

        logger.info("%20s: interm aliases for item %s after %s: %s"
            % (self.worker_id, item["_id"],  self.provider_name, str(item["aliases"])))

        return item

    def add_aliases(self, new_alias_tuples, item):
        for ns, nid in new_alias_tuples:
            try:
                item["aliases"][ns].append(nid)
                item["aliases"][ns] = list(set(item["aliases"][ns]))
            except KeyError: # no ids for that namespace yet. make it.
                item["aliases"][ns] = [nid]
            except AttributeError:
                # nid is a string; overwrite.
                item["aliases"][ns] = nid
                logger.debug("aliases[{ns}] is a string ('{nid}'); overwriting".format(
                    ns=ns,
                    nid=nid
                ))
        return item


class SerialWorkers():
    """ Trivial class for running through a list of alias or biblio workers.

    Exists to be in paralell with the MetricsWorkers class
    """
    def __init__(self, workers):
        self.workers = workers

    def update(self, item):
        for worker in self.workers:
            item = worker.update(item)

        return item



class MetricsWorker(Worker):
    """ Gets all of one provider's metrics for a single item

        Don't forget to set the dao before use.
    """

    def update_from_queue(self, q):
        while True:
            item = q.get()
            item = self.update(item)
            q.task_done()

    def update(self, item):
        try:
            (success, metrics) = self.ask_provider(item["aliases"], item["_id"])
            if success:
                item = self.add_metrics_to_item(metrics, item)
        except ProviderError:
            pass
        finally:
            # update provider counter so api knows when all have finished
            item["num_providers_still_updating"] -= 1

        if item["num_providers_still_updating"] < 1:
            # unlikely to have concurrency problems, since we do this just once per item.
            # but possible.
            self.dao.save(item)

    def add_metrics_to_item(self, metrics, item):
        now = datetime.datetime.now().isoformat()
        if metrics:
            for name, metric_tuple in metrics.iteritems():
                item["metrics"].setdefault(name, {})
                item["metrics"][name]["drilldown_url"] = metric_tuple[1]
                item["metrics"][name]["values"] = {}
                item["metrics"][name]["values"][now] = metric_tuple[0]

        return item

class MetricsWorkers():
    queues = []
    threads = []

    def __init__(self, workers, dao):
        self.dao = dao
        self.workers = workers

    def run(self):
        for worker in self.workers:
            q = Queue.Queue()
            self.queues.append(q)

            for i in range(0, worker.threads_allowed):
                new_thread = threading.Thread(
                    target=worker.update_from_queue,
                    args=(q,)
                )
                new_thread.start()
                self.threads.append(new_thread)


    def update(self, item):
        for queue in self.queues:
            queue.put(item)





def make_workers(method):
    providers_that_have_this_method = \
        ProviderFactory.get_providers(default_settings.PROVIDERS, method)
    worker_class_name = method.title() + "Worker"
    workers = []
    for provider in providers_that_have_this_method:
        worker = globals()[worker_class_name](provider)
        workers.append(worker)
        
    return workers

def main():
    mydao = dao.Dao(os.environ["CLOUDANT_URL"], os.environ["CLOUDANT_DB"])
    mydao.update_design_doc()

    metrics_workers = MetricsWorkers(make_workers("metrics"), mydao)
    metrics_workers.run()

    backend = Backend(
        SerialWorkers(make_workers("aliases")),
        SerialWorkers(make_workers("biblio")),
        metrics_workers,
        mydao
    )
    backend.run()
 
if __name__ == "__main__":
    main()
    

