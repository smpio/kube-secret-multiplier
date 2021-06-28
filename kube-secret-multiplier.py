#!/usr/bin/env python3

import re
import copy
import queue
import logging
import argparse

import kubernetes.client

from utils.kubernetes.config import configure
from utils.signal import install_shutdown_signal_handlers
from utils.kubernetes.watch import KubeWatcher, WatchEventType
from utils.threading import SupervisedThread, SupervisedThreadGroup

log = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--master', help='kubernetes api server url')
    arg_parser.add_argument('--in-cluster', action='store_true', help='configure with in cluster kubeconfig')
    arg_parser.add_argument('--log-level', default='INFO')
    arg_parser.add_argument('--origin-namespace', required=True)
    arg_parser.add_argument('--secret-name', required=True)
    arg_parser.add_argument('--destination-namespaces-regexp', required=True)
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)
    configure(args.master, args.in_cluster)
    install_shutdown_signal_handlers()

    q = queue.Queue()
    threads = SupervisedThreadGroup()
    threads.add_thread(SecretWatchThread(q, namespace=args.origin_namespace, secret_name=args.secret_name))
    threads.add_thread(NamespaceWatchThread(q, match_regex=args.destination_namespaces_regexp))
    threads.add_thread(HandlerThread(q))
    threads.start_all()
    threads.wait_any()


class SecretWatchThread(SupervisedThread):
    def __init__(self, queue, *, namespace, secret_name):
        super().__init__(daemon=True)
        self.queue = queue
        self.namespace = namespace
        self.secret_name = secret_name

    def run_supervised(self):
        w = kubernetes.watch.Watch()
        v1 = kubernetes.client.CoreV1Api()
        watcher = iter(KubeWatcher(v1.list_namespaced_secret, namespace=self.namespace))

        for event_type, secret in watcher:
            if event_type == WatchEventType.DONE_INITIAL:
                continue
            if secret.metadata.name != self.secret_name:
                continue
            if event_type == WatchEventType.DELETED:
                continue
            self.queue.put(('secret', secret))


class NamespaceWatchThread(SupervisedThread):
    def __init__(self, queue, *, match_regex):
        super().__init__(daemon=True)
        self.queue = queue
        self.match_re = re.compile(match_regex)

    def run_supervised(self):
        w = kubernetes.watch.Watch()
        v1 = kubernetes.client.CoreV1Api()
        watcher = iter(KubeWatcher(v1.list_namespace))

        for event_type, ns in watcher:
            if event_type == WatchEventType.DONE_INITIAL:
                continue

            name = ns.metadata.name

            if not self.match_re.match(name):
                continue
            if event_type == WatchEventType.ADDED:
                self.queue.put(('namespace_added', name))
            if event_type == WatchEventType.DELETED:
                self.queue.put(('namespace_deleted', name))


class HandlerThread(SupervisedThread):
    def __init__(self, queue):
        super().__init__(daemon=True)
        self.queue = queue
        self.known_namespaces = set()
        self.secret = None
        self.api = kubernetes.client.CoreV1Api()

    def run_supervised(self):
        while True:
            event_name, data = self.queue.get()
            getattr(self, f'handle_{event_name}')(data)

    def handle_secret(self, secret):
        self.secret = secret
        for name in self.known_namespaces:
            self.update_secret_in_namespace(name)

    def handle_namespace_added(self, name):
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


if __name__ == '__main__':
    main()
