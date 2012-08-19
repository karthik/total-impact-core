#!/usr/bin/env python

import time, sys, logging, os, traceback, datetime
from totalimpact import default_settings, dao, tiredis
from totalimpact.tiqueue import Queue, QueueMonitor
from totalimpact.models import ItemFactory
from totalimpact.pidsupport import StoppableThread
from totalimpact.providers.provider import ProviderError, ProviderFactory

logger = logging.getLogger('ti.backend')
logger.setLevel(logging.DEBUG)

class backendException(Exception):
    pass

class TotalImpactBackend(object):
    
    def __init__(self, dao, redis, providers):
        self.threads = [] 
        self.dao = dao
        self.redis = redis
        self.dao.update_design_doc()
        self.providers = providers


    def _get_num_workers_from_config(self, provider_name, provider_config):
        relevant_provider_config = {"workers":1}
        for (key, provider_config_dict) in provider_config:
            if (key==provider_name):
                relevant_provider_config = provider_config_dict
        return relevant_provider_config["workers"]

    def run(self):

        # create provider threads
        for provider in self.providers:
            if not provider.provides_metrics:                            
                continue

            thread_count = self._get_num_workers_from_config(
                provider.provider_name, 
                default_settings.PROVIDERS)

            logger.info("%20s: spawning, n=%i" % (provider.provider_name, thread_count)) 
            # create and start the metrics threads
            for thread_id in range(thread_count):
                t = ProviderMetricsThread(self.dao, self.redis, provider)
                t.thread_id = t.thread_id + '[%i]' % thread_id
                t.start()
                self.threads.append(t)
        
        logger.info("%20s: spawning" % ("aliases"))
        t = ProvidersAliasThread(self.dao, self.redis, self.providers)
        t.start()
        self.threads.append(t)

        logger.info("%20s: spawning" % ("monitor_thread"))
        # Start the queue monitor
        # This will watch for newly created items appearing in the couchdb
        # which were requested through the API. It then queues them for the
        # worker processes to handle
        t = QueueMonitor(self.dao)
        t.start()
        t.thread_id = 'monitor_thread'
        self.threads.append(t)

    def other_run(self):
        # get the first item on the queue - this waits until
        # there is something to return
        #logger.debug("%20s: waiting for queue item" % self.thread_id)
        item = self.queue.dequeue()

        # Check we have an item, if we have been signalled to stop, then
        # item may be None
        if item:
            logger.debug("%20s: got an item!  dequeued %s" % (self.thread_id, item["_id"]))
            # if we get to here, an item has been popped off the queue and we
            # now want to calculate its metrics.
            # Repeatedly process this item until we hit the error limit
            # or we successfully process it
            logger.debug("%20s: processing item %s" % (self.thread_id, item["_id"]))

            # process item saves the item back to the db as necessary
            # also puts alias items on metrics queue when done
            self.process_item(item)


            # Flag for testing. We should finish the run loop as soon
            # as we've processed a single item.
        if run_only_once:
            return

        if not item:
            time.sleep(0.5)



class ProviderWrapper():
    """
    Utility methods for logging and error-handling of all provider calls.

    These three methods maybe might belong in the base Provider class,
    since they don't talk to the db or anything.
    """

    def __init__(self, provider, method_name):
        self.provider = provider
        self.provider_name = provider.__class__.__name__
        self.method = method_name
        self.worker_id = self.provider_name + "_worker"

    def log_error(self, item, error_msg, tb):
        # This method is called to record any errors which we obtain when
        # trying process an item.
        logger.error("exception for item(%s): %s" % (item["_id"], error_msg))
        
        e = backendException(error_msg)
        e.id = item["_id"]
        e.provider = self.worker_id
        e.stack_trace = "".join(traceback.format_tb(tb))
        
        logger.debug(str(e.stack_trace))



    def process_item_for_provider(self, item):
        """ Run the given method for the given provider on the given item

            This will deal with retries and sleep / backoff as per the
            configuration for the given provider. We will return true if
            the given method passes, or if it is not implemented. I
        """
        if method_name not in ('aliases', 'biblio', 'metrics'):
            raise NotImplementedError("Unknown method %s for provider class" % self.method_name)

        tiid = item["_id"]
        error_counts = 0
        success = False
        error_limit_reached = False
        error_msg = False
        max_retries = provider.get_max_retries()
        response = None

        while not error_limit_reached and not success:
            response = None

            try:
                cache_enabled = (error_counts == 0)

                if not cache_enabled:
                    logger.debug("%20s: cache NOT enabled %s %s for %s"
                    % (self.worker_id, self.provider_name, self.method_name, tiid))

                # convert the dict into a list of (namespace, id) tuples, like:
                # [(doi, 10.123), (doi, 10.345), (pmid, 1234567)]
                alias_tuples = []
                for ns, ids in item["aliases"].iteritems():
                    if isinstance(ids, basestring): # it's a date, not a list of ids
                        alias_tuples.append((ns, ids))
                    else:
                        for id in ids:
                            alias_tuples.append((ns, id))

                response = self.call_provider_method(
                    alias_tuples,
                    item["_id"],
                    cache_enabled=cache_enabled)
                success = True

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
                    item,
                    '%s on %s %s' % (error_msg, self.provider_name, self.method_name),
                    tb,
                    self.worker_id)

                error_counts += 1

                if ((error_counts > max_retries) and (max_retries != -1)):
                    logger.error("process_item_for_provider: error limit reached (%i/%i) for %s, aborting %s %s" % (
                        error_counts, max_retries, item["_id"], self.provider_name, self.method_name))
                    error_limit_reached = True
                else:
                    duration = provider.get_sleep_time(error_counts)
                    logger.warning("process_item_for_provider: error, pausing thread for %i %s %s, %s" % (duration, item["_id"], self.provider_name, self.method_name))
                    time.sleep(duration)

        if success:
            # response may be None for some methods and inputs
            if response:
                logger.debug("%20s: success %s %s for %s, got %i results"
                % (self.worker_id, self.provider, self.method_name, tiid, len(response)))
            else:
                logger.debug("%20s: success %s %s for %s, got 0 results"
                % (self.worker_id, self.provider, self.method_name, tiid))

        return (success, response)

    def call_provider_method(self,
            aliases,
            tiid,
            cache_enabled=True):

        if not aliases:
            logger.debug("%20s: skipping %s %s %s for %s, no aliases"
                % (self.worker_id, self.provider_name, self.method_name, str(aliases), tiid))
            return None

        provides_method_name = "provides_" + method_name
        provides_method_to_call = getattr(self.provider, provides_method_name)
        if not provides_method_to_call:
            logger.debug("%20s: skipping %s %s %s for %s, does not provide"
                % (self.worker_id, self.provider_name, self.method_name, str(aliases), tiid))
            return None

        method_to_call = getattr(self.provider, method_name)
        if not method_to_call:
            logger.debug("%20s: skipping %s %s %s for %s, no method"
                % (self.worker_id, self.provider_name, self.method_name, str(aliases), tiid))
            return None

        logger.debug("%20s: calling %s %s for %s" % (self.worker_id, self.provider_name, self.method_name, tiid))
        try:
            response = method_to_call(aliases)
        except NotImplementedError:
            response = None
        return response



class ProvidersAliasThread():
    
    def __init__(self, dao, redis, providers):
        self.providers = providers
        queue = Queue("aliases")
        ProviderThread.__init__(self, dao, redis, queue)
        self.providers = providers

        self.thread_id = "alias_thread"
        self.dao = dao


        
    def process_item(self, item):
        logger.info("%20s: initial alias list for %s is %s" 
                    % (self.thread_id, item["_id"], item["aliases"]))

        if not self.stopped():
            for provider in self.providers: 

                (success, new_aliases) = self.process_item_for_provider(item, provider, 'aliases')
                if success:
                    if new_aliases:
                        logger.debug("here are the new aliases from {provider}: {aliases}.".format(
                            provider=provider,
                            aliases=str(new_aliases)
                        ))
                        # add new aliases
                        for ns, nid in new_aliases:
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

                else:
                    logger.info("%20s: NOT SUCCESS in process_item %s, partial aliases only for provider %s" 
                        % (self.thread_id, item["_id"], provider.provider_name))

                (success, new_biblio) = self.process_item_for_provider(item, provider, 'biblio')
                if success:
                    if new_biblio:
                        for (k, v) in new_biblio.iteritems():
                            if not item["biblio"].has_key(k):
                                item["biblio"][k] = v

                        logger.info("%20s: in process_item biblio %s provider %s" 
                            % (self.thread_id, item["_id"], provider.provider_name))

                else:
                    logger.info("%20s: NOT SUCCESS in process_item %s, partial biblio only for provider %s" 
                        % (self.thread_id, item["_id"], provider.provider_name))

                logger.info("%20s: interm aliases for item %s after %s: %s" 
                    % (self.thread_id, item["_id"], provider.provider_name, str(item["aliases"])))
                logger.info("%20s: interm biblio for item %s after %s: %s" 
                    % (self.thread_id, item["_id"], provider.provider_name, str(item["biblio"])))

            logger.info("%20s: final alias list for %s is %s" 
                    % (self.thread_id, item["_id"], item["aliases"]))

            # Time to add this to the metrics queue
            logger.debug("%20s: FULL ITEM on metrics queue %s %s"
                % (self.thread_id, item["_id"],item))
            logger.debug("%20s: added to metrics queues complete for item %s " % (self.thread_id, item["_id"]))
            self.dao.save(item)

            self.queue.add_to_metrics_queues(item)


    

class MetricsWorker():
    """ Get's all of one provider's metrics for a single item

        It will deal with retries and timeouts as required. Doesn't know anything
        about threads.
    """
    def __init__(self, provider_wrapper, dao):
        self.provider_wrapper = provider_wrapper
        self.dao = dao

    def update_from_queue(self, q):
        while True:
            item = q.get()
            item = self.update(item)
            q.task_done()

    def update(self, item):
        try:
            (success, metrics) = self.provider_wrapper.process_item_for_provider(item)
            if success:
                print metrics
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





def main():
    mydao = dao.Dao(os.environ["CLOUDANT_URL"], os.environ["CLOUDANT_DB"])
    myredis = tiredis.from_url(os.getenv("REDISTOGO_URL"))


    # Start all of the backend processes
    providers = ProviderFactory.get_providers(default_settings.PROVIDERS)
    backend = TotalImpactBackend(mydao, myredis, providers)
    backend.run()

    logger.debug("Items on Queues: %s" 
        % (str([queue_name + " : " + str(Queue.queued_items_ids(queue_name)) for queue_name in Queue.queued_items.keys()]),))

 
if __name__ == "__main__":
    main()
    

