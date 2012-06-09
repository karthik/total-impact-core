#!/usr/bin/env python

import threading, time, sys, copy, datetime, logging, os, traceback
from totalimpact import default_settings, dao
from totalimpact.queue import Queue, QueueMonitor
from totalimpact.models import Error, Item, Collection, ItemFactory, CollectionFactory
from totalimpact.pidsupport import StoppableThread
from totalimpact.providers.provider import ProviderError,  Provider, ProviderFactory

logger = logging.getLogger('ti.backend')
logger.setLevel(logging.DEBUG)

class TotalImpactBackend(object):
    
    def __init__(self, dao, providers):
        self.threads = [] 
        self.dao = dao
        self.providers = providers
    
    def run(self):
        self._spawn_threads()
        try:
            self._monitor()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Interrupted ... exiting ...")
            self._cleanup()
    
    def _spawn_threads(self):
        
        for provider in self.providers:
            if not provider.provides_metrics:
                continue
            thread_count = default_settings.PROVIDERS[provider.provider_name]["workers"]
            logger.info("%20s: spawning, n=%i" % (provider.provider_name, thread_count)) 
            # create and start the metrics threads
            for idx in range(thread_count):
                t = ProviderMetricsThread(provider, self.dao)
                t.thread_id = t.thread_id + '[%i]' % idx
                t.start()
                self.threads.append(t)
        
        logger.info("%20s: spawning" % ("aliases"))
        t = ProvidersAliasThread(self.providers, self.dao)
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
        
    def _monitor(self):        
        # Install a signal handler so we'll break out of the main loop
        # on receipt of relevant signals
        class ExitSignal(Exception):
            pass
     
        def kill_handler(signum, frame):
            raise ExitSignal()

        import signal
        signal.signal(signal.SIGTERM, kill_handler)

        try:
            while True:
                # just spin our wheels waiting for interrupts
                time.sleep(1)
        except (KeyboardInterrupt, ExitSignal), e:
            pass
    
    def _cleanup(self):
        
        for t in self.threads:
            logger.info("%20s: stopping" % (t.thread_id))
            t.stop()
        for t in self.threads:
            logger.info("%20s: waiting to stop" % (t.thread_id))
            t.join()
            logger.info("%20s: stopped" % (t.thread_id))

        self.threads = []
    



class ProviderThread(StoppableThread):
    """ This is the basis for the threads processing items for a provider

        Subclasses should implement process_item to define how they want
        to use providers to obtain information about a given item. The
        method process_item_for_provider defined by this class should then
        be used to handle those updates. This method will deal with retries
        and backoff as per the provider configuration.  

        This base class is mostly to avoid code duplication between the 
        Metric and Alias providers.
    """


    def __init__(self, dao, queue):
        self.dao = dao
        StoppableThread.__init__(self)
        self.thread_id = "BaseProviderThread"
        self.run_once = False
        self.queue = queue

    def log_error(self, item, error_msg, tb):
        # This method is called to record any errors which we obtain when
        # trying process an item.
        logger.error("exception for item(%s): %s" % (item.id, error_msg))
        
        e = Error()
        e.message = error_msg
        e.id = item.id
        e.provider = self.thread_id
        e.stack_trace = "".join(traceback.format_tb(tb))
        
        logger.debug(str(e.stack_trace))
        

    def run(self, run_only_once=False):

        while not self.stopped():
            # get the first item on the queue - this waits until
            # there is something to return
            #logger.debug("%20s: waiting for queue item" % self.thread_id)
            item = self.queue.dequeue()
            
            # Check we have an item, if we have been signalled to stop, then
            # item may be None
            if item:
                logger.debug("%20s: got an item!  dequeued %s" % (self.thread_id, item.id))
                # if we get to here, an item has been popped off the queue and we
                # now want to calculate its metrics. 
                # Repeatedly process this item until we hit the error limit
                # or we successfully process it         
                logger.debug("%20s: processing item %s" % (self.thread_id, item.id))

                # process item saves the item back to the db as necessary
                # also puts alias items on metrics queue when done
                self.process_item(item) 


            # Flag for testing. We should finish the run loop as soon
            # as we've processed a single item.
            if run_only_once:
                return

            if not item:
                time.sleep(0.5)



    def call_provider_method(self, 
            provider, 
            method_name, 
            aliases, 
            tiid,
            cache_enabled=True):

        if not aliases:
            logger.debug("%20s: skipping %s %s %s for %s, no aliases" 
                % (self.thread_id, provider, method_name, str(aliases), tiid))
            return None

        provides_method_name = "provides_" + method_name
        provides_method_to_call = getattr(provider, provides_method_name)
        if not provides_method_to_call:
            logger.debug("%20s: skipping %s %s %s for %s, does not provide" 
                % (self.thread_id, provider, method_name, str(aliases), tiid))
            return None

        method_to_call = getattr(provider, method_name)
        if not method_to_call:
            logger.debug("%20s: skipping %s %s %s for %s, no method" 
                % (self.thread_id, provider, method_name, str(aliases), tiid))
            return None

        try:
            override_template_url = default_settings.PROVIDERS[provider.provider_name][method_name + "_url"]
        except KeyError:
            # No problem, the provider will use the template_url it knows about
            override_template_url = None

        logger.debug("%20s: calling %s %s for %s" % (self.thread_id, provider, method_name, tiid))
        try:
            response = method_to_call(aliases, override_template_url)
            #logger.debug("%20s: response from %s %s %s for %s, %s" 
            #    % (self.thread_id, provider, method_name, str(aliases), tiid, str(response)))
        except NotImplementedError:
            response = None
        return response


    def process_item_for_provider(self, item, provider, method_name):
        """ Run the given method for the given provider on the given item
            This will deal with retries and sleep / backoff as per the 
            configuration for the given provider. We will return true if
            the given method passes, or if it is not implemented.
        """
        if method_name not in ('aliases', 'biblio', 'metrics'):
            raise NotImplementedError("Unknown method %s for provider class" % method_name)

        tiid = item.id
        #logger.debug("%20s: processing %s %s for %s" 
        #    % (self.thread_id, provider, method_name, tiid))
        error_counts = 0
        success = False
        error_limit_reached = False
        error_msg = False
        max_retries = provider.get_max_retries()
        response = None

        while not error_limit_reached and not success and not self.stopped():
            response = None

            try:
                cache_enabled = (error_counts == 0)

                if not cache_enabled:
                    logger.debug("%20s: cache NOT enabled %s %s for %s"
                        % (self.thread_id, provider, method_name, tiid))

                response = self.call_provider_method(
                    provider, 
                    method_name, 
                    item.aliases.get_aliases_list(), 
                    item.id,
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
                self.log_error(item, '%s on %s %s' % (error_msg, provider, method_name), tb)

                error_counts += 1

                if ((error_counts > max_retries) and (max_retries != -1)):
                    logger.error("process_item_for_provider: error limit reached (%i/%i) for %s, aborting %s %s" % (
                        error_counts, max_retries, item.id, provider, method_name))
                    error_limit_reached = True
                else:
                    duration = provider.get_sleep_time(error_counts)
                    logger.warning("process_item_for_provider: error, pausing thread for %i %s %s, %s" % (duration, item.id, provider, method_name))
                    self._interruptable_sleep(duration)                

        if success:
            # response may be None for some methods and inputs
            if response:
                logger.debug("%20s: success %s %s for %s, got %i results" 
                    % (self.thread_id, provider, method_name, tiid, len(response)))
            else:
                logger.debug("%20s: success %s %s for %s, got 0 results" 
                    % (self.thread_id, provider, method_name, tiid))

        return (success, response)


class ProvidersAliasThread(ProviderThread):
    
    def __init__(self, providers, dao):
        self.providers = providers
        queue = Queue("aliases")
        ProviderThread.__init__(self, dao, queue)
        self.providers = providers
        self.thread_id = "alias_thread"
        self.dao = dao

        
    def process_item(self, item):
        logger.info("%20s: initial alias list for %s is %s" 
                    % (self.thread_id, item.id, item.aliases.get_aliases_list()))

        if not self.stopped():
            for provider in self.providers: 

                (success, new_aliases) = self.process_item_for_provider(item, provider, 'aliases')
                if success:
                    if new_aliases:
                        item.aliases.add_unique(new_aliases)
                else:
                    item.aliases.clear_aliases()
                    logger.info("%20s: NOT SUCCESS in process_item %s clear aliases provider %s" 
                        % (self.thread_id, item.id, provider.provider_name))

                    break

                (success, biblio) = self.process_item_for_provider(item, provider, 'biblio')
                if success:
                    if biblio:
                        # merge old biblio with new, favoring old in cases of conflicts
                        item.biblio = dict(biblio.items() + item.biblio.items())
                        logger.info("%20s: in process_item biblio %s provider %s" 
                            % (self.thread_id, item.id, provider.provider_name))

                else:
                    # This provider has failed and exceeded the 
                    # total number of retries. Don't process any 
                    # more providers, we abort this item entirely
                    break
                logger.info("%20s: interm aliases for item %s after %s: %s" 
                    % (self.thread_id, item.id, provider.provider_name, str(item.aliases.get_aliases_list())))
                logger.info("%20s: interm biblio for item %s after %s: %s" 
                    % (self.thread_id, item.id, provider.provider_name, str(item.biblio)))

            logger.info("%20s: final alias list for %s is %s" 
                    % (self.thread_id, item.id, item.aliases.get_aliases_list()))

            # Time to add this to the metrics queue
            self.queue.add_to_metrics_queues(item)
            logger.debug("%20s: FULL ITEM on metrics queue %s %s"
                % (self.thread_id, item.id,item.as_dict()))
            logger.debug("%20s: added to metrics queues complete for item %s " % (self.thread_id, item.id))
            self.dao.save(item.as_dict())


    

class ProviderMetricsThread(ProviderThread):
    """ The provider metrics thread will handle obtaining metrics for all
        requests for a single provider. It will deal with retries and 
        timeouts as required.
    """
    def __init__(self, provider, dao):
        self.provider = provider
        queue = Queue(provider.provider_name)
        ProviderThread.__init__(self, dao, queue)
        self.thread_id = self.provider.provider_name + "_thread"
        self.dao = dao


    def process_item(self, item):
        # used by logging

        (success, metrics) = self.process_item_for_provider(item, 
            self.provider, 'metrics')
        
        if success:
            if metrics:
                for metric_name in metrics.keys():
                    if metrics[metric_name]:
                        snap = ItemFactory.build_snap(item.id, metrics[metric_name], metric_name)
                        self.dao.save(snap)


def main():
    mydao = dao.Dao(
        default_settings.DB_NAME,
        default_settings.DB_URL,
        default_settings.DB_USERNAME,
        default_settings.DB_PASSWORD
    ) 


    # Start all of the backend processes
    providers = ProviderFactory.get_providers(default_settings.PROVIDERS)
    backend = TotalImpactBackend(mydao, providers)
    backend._spawn_threads()
    backend._monitor()
    backend._cleanup()
        
    logger.debug("Items on Queues: %s" 
        % (str([queue_name + " : " + str(Queue.queued_items_ids(queue_name)) for queue_name in Queue.queued_items.keys()]),))

 
if __name__ == "__main__":
    main()
    

