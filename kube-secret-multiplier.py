#!/usr/bin/env python3

import re
import sys
import copy
import queue
import signal
import logging
import argparse
import threading

import kubernetes.client
import kubernetes.config

log = logging.getLogger(__name__)

# TODO: program doesn't terminate itself on exception in a thread


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--in-cluster', action='store_true', help='configure with in cluster kubeconfig')
    arg_parser.add_argument('--log-level', default='INFO')
    arg_parser.add_argument('--origin-namespace', required=True)
    arg_parser.add_argument('--secret-name', required=True)
    arg_parser.add_argument('--destination-namespaces-regexp', required=True)
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)

    if args.in_cluster:
        kubernetes.config.load_incluster_config()
    else:
        configuration = kubernetes.client.Configuration()
        configuration.host = 'http://127.0.0.1:8001'
        kubernetes.client.Configuration.set_default(configuration)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    q = queue.Queue()

    secret_watch_thread = SecretWatchThread(q, namespace=args.origin_namespace, name=args.secret_name)
    secret_watch_thread.start()

    namespace_watch_thread = NamespaceWatchThread(q, match_regexp=args.destination_namespaces_regexp)
    namespace_watch_thread.start()

    handler = Handler(q)
    handler.handle()


class SecretWatchThread(threading.Thread):
    def __init__(self, queue, *, namespace, name):
        super().__init__(daemon=True)
        self.queue = queue
        self.namespace = namespace
        self.name = name

    def run(self):
        w = kubernetes.watch.Watch()
        v1 = kubernetes.client.CoreV1Api()

        while True:
            for change in w.stream(v1.list_namespaced_secret, namespace=self.namespace):
                self.handle_change(change)

    def handle_change(self, change):
        secret = change['object']

        if secret.metadata.name != self.name:
            return

        if change['type'] != 'DELETED':
            self.queue.put(('secret', secret))


class NamespaceWatchThread(threading.Thread):
    def __init__(self, queue, *, match_regexp):
        super().__init__(daemon=True)
        self.queue = queue
        self.match_re = re.compile(match_regexp)

    def run(self):
        w = kubernetes.watch.Watch()
        v1 = kubernetes.client.CoreV1Api()

        while True:
            for change in w.stream(v1.list_namespace):
                self.handle_change(change)

    def handle_change(self, change):
        name = change['object'].metadata.name

        if not self.match_re.match(name):
            return

        if change['type'] == 'ADDED':
            self.queue.put(('namespace', name))
        elif change['type'] == 'DELETED':
            self.queue.put(('namespace_deleted', name))


class Handler:
    def __init__(self, queue):
        self.queue = queue
        self.known_namespaces = set()
        self.secret = None
        self.api = kubernetes.client.CoreV1Api()

    def handle(self):
        while True:
            etype, data = self.queue.get()
            getattr(self, f'handle_{etype}')(data)

    def handle_secret(self, secret):
        self.secret = secret
        for name in self.known_namespaces:
            self.update_secret_in_namespace(name)

    def handle_namespace(self, name):
        self.known_namespaces.add(name)
        if self.secret:
            self.update_secret_in_namespace(name)

    def handle_namespace_deleted(self, name):
        self.known_namespaces.remove(name)

    def update_secret_in_namespace(self, namespace):
        name = self.secret.metadata.name
        log.info('Updating %s/%s', namespace, name)

        secret = copy.deepcopy(self.secret)
        secret.metadata = kubernetes.client.V1ObjectMeta(name=name, namespace=namespace)

        try:
            self.api.replace_namespaced_secret(name=name, namespace=namespace, body=secret)
        except kubernetes.client.rest.ApiException as e:
            if e.status == 404:
                self.create_secret_in_namespace(namespace, secret)
            elif e.status == 422:
                delete_options = kubernetes.client.V1DeleteOptions()
                self.api.delete_namespaced_secret(name=name, namespace=namespace, body=delete_options)
                self.create_secret_in_namespace(namespace, secret)

    def create_secret_in_namespace(self, namespace, secret):
        try:
            self.api.create_namespaced_secret(namespace=namespace, body=secret)
        except kubernetes.client.rest.ApiException as e:
            if e.status != 409:
                raise
            log.info('Conflict! Already created by somebody, requeue')
            self.queue.put(('namespace', namespace))


def shutdown(signum, frame):
    """
    Shutdown is called if the process receives a TERM signal. This way
    we try to prevent an ugly stacktrace being rendered to the user on
    a normal shutdown.
    """
    log.info("Shutting down")
    sys.exit(0)


if __name__ == '__main__':
    main()
