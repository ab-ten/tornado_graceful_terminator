# -*- coding:utf-8-unix; -*-
# Copyright (c) 2016,2020 Ab. <ab-ten@tkn.sakura.ne.jp>

import os
import time
import threading
import signal
import asyncio
from types import MethodType

import tornado.ioloop
import tornado.httpserver
import tornado.log


class GracefulTerminator(object):
  """A singleton class which implements graceful shutdown functionality.
  """
  _instance_lock = threading.Lock()
  SHUTDOWN_LIMIT = 15.0
  SHUTDOWN_INTERVAL = 1.0
  gen_log = tornado.log.gen_log
  verbose = False
  shutting_down = False

  def __init__(self):
    self.servers = []
    self.session_count = 0

  @staticmethod
  def _install_signal_handler():
    if os.name == 'nt':
      import win32api
      import win32con

      # handler for windows
      def consoleCtrlHandler(ctrlType):
        if ctrlType in (win32con.CTRL_C_EVENT,
                win32con.CTRL_BREAK_EVENT):
          GracefulTerminator._request_shutdown()
          return int(True)  # TRUE for ignore this event
        return int(False)
      win32api.SetConsoleCtrlHandler(consoleCtrlHandler, True)

    def sig_handler(sig, frame):
      GracefulTerminator._request_shutdown()
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

  @staticmethod
  def add_server(server):
    """Register server object, for stopping services on termination.
    """
    GracefulTerminator.instance().servers.append(server)

  def incr(self, count=1):
    """Increment active session count."""
    with GracefulTerminator._instance_lock:
      self.session_count += count
      result = self.session_count
    return result

  def decr(self, count=1):
    """Decrement active session count."""
    with GracefulTerminator._instance_lock:
      self.session_count -= count
      result = self.session_count
    return result

  @staticmethod
  def instance():
    """Returning a singleton instance."""
    if not hasattr(GracefulTerminator, "_instance"):
      with GracefulTerminator._instance_lock:
        if not hasattr(GracefulTerminator, "_instance"):
          GracefulTerminator._instance = GracefulTerminator()
    return GracefulTerminator._instance

  @staticmethod
  def install():
    gt = GracefulTerminator.instance()
    gt._install_signal_handler()
    gt._install_request_handler_hook()
    tornado.ioloop.IOLoop.current().spawn_callback(gt.ShutdownMonitorLoop)

  @staticmethod
  def request_shutdown():
    GracefulTerminator._request_shutdown()
    
  @staticmethod
  def _request_shutdown():
    """Called from signal handler, and starting shutdown process."""
    if not GracefulTerminator.shutting_down:
      GracefulTerminator.shutting_down = True
      GracefulTerminator.gen_log.info("starting shutdown...")

  async def ShutdownMonitorLoop(self):
    while not GracefulTerminator.shutting_down:
      await tornado.gen.sleep(self.SHUTDOWN_INTERVAL)

    for sv in self.servers:
      sv.stop()
    self.servers = []
    deadline = time.time() + GracefulTerminator.SHUTDOWN_LIMIT

    self.gen_log.info('shutdown monitor activated.')
    while True:
      now = time.time()
      if now >= deadline:
        self.gen_log.info('timeout, force shutdown.')
        asyncio.get_running_loop().stop()
        break
      with GracefulTerminator._instance_lock:
        sessions = GracefulTerminator.instance().session_count
      if sessions <= 0:
        self.gen_log.info('ioloop completed.')
        asyncio.get_running_loop().stop()
        break
      self.gen_log.debug('wait for completion [%d]' % sessions)
      await tornado.gen.sleep(self.SHUTDOWN_INTERVAL)

  @staticmethod
  def _install_request_handler_hook():
    """
    Install a hook routine for detecting finish() call of HTTP response.
    It is not appropriate to add a new class derived from RequestHandler.
    The reason is that some utility classes such as
    tornado.web.ErrorHandler are derived from tornado.web.RequestHandler,
    not from the new derived class.
    """
    from tornado.web import RequestHandler
    if not hasattr(RequestHandler, "finish"):
      raise RuntimeError(
        "incompatible version of tornado.web.RequestHandler")
    if hasattr(RequestHandler, "_finish_orig"):
      raise RuntimeError("install() called twice")
    RequestHandler._finish_orig = RequestHandler.finish

    def _finish_hook(self, *args, **kwargs):
      r = self._finish_orig(*args, **kwargs)
      count = GracefulTerminator.instance().decr()
      if GracefulTerminator.verbose:
        GracefulTerminator.gen_log.debug(
          "RequestHandler.finish [%d]" % count)
      return r

    setattr(RequestHandler, "finish", _finish_hook)


class GracefulHTTPServer(tornado.httpserver.HTTPServer):
  """Customized HTTPServer for detect active session starting.
  """
  def start_request(self, server_conn, request_conn):
    # In keep-alive state, start_request() is called again
    # immediately after the HTTP response is completed (to wait for
    # the next HTTP request),
    # Therefore, headers_received() callback is used to detect
    # the start of an HTTP session.
    result = super(GracefulHTTPServer, self).start_request(
      server_conn, request_conn)
    if (hasattr(result, 'headers_received')
      and not hasattr(result, '_headers_received_orig')):
      result._headers_received_orig = result.headers_received

      def _headers_received_hook(self, *args, **kwargs):
        count = GracefulTerminator.instance().incr()
        if GracefulTerminator.verbose:
          GracefulTerminator.gen_log.debug(
            "headers received [%d]" % count)
        return self._headers_received_orig(*args, **kwargs)

      result.headers_received = MethodType(
        _headers_received_hook, result)
    return result

setattr(GracefulTerminator, "GracefulHTTPServer", GracefulHTTPServer)


if __name__ == "__main__":
  # sample HTTP server
  import tornado.web
  import tornado.gen
  import tornado.options

  tornado.options.define(
    'host', default='127.0.0.1', help='host')
  tornado.options.define(
    'port', type=int, default='8008', help='port')
  tornado.options.define(
    'sleep', type=float, default=5, help='sleep sec.')
  tornado.options.parse_command_line()

  class MainHandler(tornado.web.RequestHandler):
    async def get(self):
      sleep_2 = tornado.options.options.sleep / 2.0
      await tornado.gen.sleep(sleep_2)
      self.write("<html><body>Hello, world!<br>\n")
      self.write("Hit Ctrl-C on python server console to start graceful terminate.</bory></html>")
      await tornado.gen.sleep(sleep_2)

  class ExitHandler(tornado.web.RequestHandler):
    async def get(self):
      GracefulTerminator.request_shutdown()
      self.write("exit")
      await tornado.gen.sleep(tornado.options.options.sleep)

  GracefulTerminator.install()
  server = GracefulTerminator.GracefulHTTPServer(tornado.web.Application([
    (r"/", MainHandler),
    (r"/x", ExitHandler),
    ]))
  GracefulTerminator.add_server(server)
  server.listen(
    tornado.options.options.port,
    address=tornado.options.options.host)
  tornado.ioloop.IOLoop.current().start()
