# Copyright 2014 Anton Schur

import logging
import struct
import json

from twisted.internet import protocol, defer, reactor
from twisted.python import log

from rethinkdb import repl  # For the repl connection
from rethinkdb import ql2_pb2 as p
from rethinkdb.ast import DB
from rethinkdb.errors import RqlDriverError, RqlRuntimeError, RqlClientError, RqlCompileError
from rethinkdb.net import Query, pQuery, pResponse, recursively_convert_pseudotypes


class FeedQueueOverflow(Exception):
    pass


class FeedQueue(object):

    def __init__(self, response, size=None, backlog=None):
        assert response.type == pResponse.SUCCESS_FEED
        super(FeedQueue, self).__init__()
        self.waiting = []
        self.pending = []
        self.size = size
        self.backlog = backlog
        self._response = response
        self._put_response_data()

    def _cancel_get(self, d):
        self.waiting.remove(d)

    def put(self, obj):
        if self.waiting:
            self.waiting.pop(0).callback(obj)
        elif self.size is None or len(self.pending) < self.size:
            self.pending.append(obj)
        else:
            raise FeedQueueOverflow('queue overflow')

    def get(self):
        if self.pending:
            return defer.succeed(self.pending.pop(0))
        elif self.backlog is None or len(self.waiting) < self.backlog:
            self._continue()
            d = defer.Deferred(canceller=self._cancel_get)
            self.waiting.append(d)
            return d
        else:
            raise FeedQueueOverflow('queue overflow')

    def _put_response_data(self):
        while len(self._response.data):
            self.put(self._response.data.pop(0))
        self._continue()

    def _extend(self, response):
        # TODO: close feed
        # response.type != pResponse.SUCCESS_PARTIAL and response.type != pResponse.SUCCESS_FEED
        self._response.data.extend(response.data)
        self._put_response_data()

    def _continue_callback(self, response):
        self._extend(response)

    def _continue_errback(self, *args, **kwargs):
        # TODO: close feed
        # self.end_flag = True
        pass

    def _continue(self):
        return self._response.continue_query(self._continue_callback, self._continue_errback)


class Response(object):
    def __init__(self, conn, query, opts, json_str):
        json_str = json_str.decode('utf-8')
        self._query = query
        self.token = query.token
        full_response = json.loads(json_str)
        self.type = full_response["t"]
        self.data = full_response["r"]
        self.backtrace = full_response.get("b", None)
        self.profile = full_response.get("p", None)
        self._opts = opts
        self._conn = conn
        self.end_flag = self.type != pResponse.SUCCESS_PARTIAL and self.type != pResponse.SUCCESS_FEED
        self._in_continue = False
        self.is_empty = len(self.data) == 0

    def extend(self, response):
        self.end_flag = response.type != pResponse.SUCCESS_PARTIAL and response.type != pResponse.SUCCESS_FEED
        self.data.extend(response.data)

    def _continue_callback(self, response):
        self.extend(response)
        self._in_continue = False

    def _continue_errback(self, *args, **kwargs):
        self.end_flag = True
        self._in_continue = False

    def continue_query(self, callback=None, errback=None):
        self._in_continue = True
        d = self._conn.continue_query(self._query, self._opts)
        if callable(callback):
            d.addCallback(callback)
        else:
            d.addCallback(self._continue_callback)
        if callable(errback):
            d.addErrback(errback)
        else:
            d.addErrback(self._continue_errback)
        return d

    def __iter__(self):
        while len(self.data) or not self.end_flag:
            if len(self.data):
                item = self.data.pop(0)
                item = recursively_convert_pseudotypes(item, self._opts)
                yield item
            elif not self._in_continue:
                self.continue_query()
            else:
                reactor.iterate()

    def close(self):
        if not self.end_flag:
            self.end_flag = True
            self._conn.end_query(self._query, self._opts)


class RethinkDBProtocol(protocol.Protocol):

    def __init__(self):
        self.__ready = False
        self.auth_key = ''
        self._response_buf = b""
        self.__result_queue = {}
        self.next_token = 1
        self._response_header = b''
        self._response_token = b''
        self._response_len = 0
        self.__raw_queue = []
        self.db = None

    def dataReceived(self, data):
        if not self.__ready:
            for c in data:
                if c == '\0':
                    self.got_raw_response(self._response_buf)
                    self._response_buf = b""
                else:
                    self._response_buf += c
        else:
            self._response_buf += data
            if not self._response_header and len(self._response_buf) >= 12:  # we got header
                self._response_header = self._response_buf[0:12]
                self._response_buf = self._response_buf[12:]
                (self._response_token, self._response_len,) = struct.unpack("<qL", self._response_header)
            if 0 < self._response_len <= len(self._response_buf):  # we got data
                response_data = self._response_buf[:self._response_len]
                # log.msg("response data: %s" % repr(response_data), level=logging.DEBUG)
                self._response_buf = self._response_buf[self._response_len:]
                self._response_header = b''
                self._response_len = 0
                self.got_response(self._response_token, response_data)

    def got_raw_response(self, response):
        deferred = self.__raw_queue.pop(0)
        deferred.callback(response)

    def got_response(self, response_token, response_data):

        if response_token not in self.__result_queue:
            deferred = defer.Deferred()
            deferred.errback(Exception('Token not found'))
            return None

        query, opts, deferred = self.__result_queue[response_token]
        del self.__result_queue[response_token]

        response = Response(self, query, opts, response_data)

        error = self._check_error_response(response, query.term)
        if error:
            deferred.errback(error)
            return None

        if response.type == pResponse.SUCCESS_ATOM:
            response = recursively_convert_pseudotypes(response.data[0], opts)
        elif response.type == pResponse.WAIT_COMPLETE:  # ? shouldn't happens ?
            response = None

        deferred.callback(response)

    @defer.inlineCallbacks
    def connectionMade(self):
        result = yield self._send_raw(
            struct.pack("<LL", p.VersionDummy.Version.V0_3,
                        len(self.auth_key)) + str.encode(self.auth_key, 'ascii') +
            struct.pack("<L", p.VersionDummy.Protocol.JSON))

        if result == 'SUCCESS':
            self.__ready = True
        else:
            # TODO: close connection if it is not initialized
            pass

        # TODO: Clear timeout so we don't timeout on long running queries
        # self.socket.settimeout(None)

        repl.default_connection = self

        # test
        # query = rethinkdb.query.db_list()
        # global_optargs = {}
        # data = Query(pQuery.START, self.next_token, query, global_optargs)
        # result = yield self._send_query(data)

        # result = yield rethinkdb.query.db_list().run()
        # print 'query result:', result

    def connectionLost(self, reason):
        self.__ready = False

    @classmethod
    def _check_error_response(cls, response, term):
        if response.type == pResponse.RUNTIME_ERROR:
            message = response.data[0]
            frames = response.backtrace
            return RqlRuntimeError(message, term, frames)
        elif response.type == pResponse.COMPILE_ERROR:
            message = response.data[0]
            frames = response.backtrace
            return RqlCompileError(message, term, frames)
        elif response.type == pResponse.CLIENT_ERROR:
            message = response.data[0]
            frames = response.backtrace
            return RqlClientError(message, term, frames)
        elif response.type not in (pResponse.SUCCESS_ATOM,
                                   pResponse.SUCCESS_SEQUENCE,
                                   pResponse.SUCCESS_PARTIAL,
                                   pResponse.SUCCESS_FEED,
                                   pResponse.WAIT_COMPLETE):
            return RqlDriverError("Unknown Response type %d encountered in response." % response.type)

    def _send_raw(self, data):
        deferred = defer.Deferred()
        self.__raw_queue.append(deferred)
        self.transport.write(data)
        return deferred

    def _start(self, term, **global_optargs):
        # Set global opt args
        # The 'db' option will default to this connection's default
        # if not otherwise specified.
        if 'db' in global_optargs:
            global_optargs['db'] = DB(global_optargs['db'])
        else:
            if self.db:
                global_optargs['db'] = DB(self.db)

        # Construct query
        query = Query(pQuery.START, self.next_token, term, global_optargs)
        self.next_token += 1
        return self._send_query(query, global_optargs)

    def _send_query(self, query, opts=None):
        # # Error if this connection has closed
        # if self.socket is None:
        #     raise RqlDriverError("Connection is closed.")

        query.accepts_r_json = True

        # Send json
        query_str = query.serialize().encode('utf-8')
        query_header = struct.pack("<QL", query.token, len(query_str))

        deferred = defer.Deferred()
        if opts is None:
            opts = {}
        self.__result_queue[query.token] = (query, opts, deferred)
        self.transport.write(query_header + query_str)
        return deferred

    def continue_query(self, query, opts):
        q = Query(pQuery.CONTINUE, query.token, None, None)
        return self._send_query(q, opts)

    def end_query(self, query, opts):
        query = Query(pQuery.STOP, query.token, None, None)
        return self._send_query(query, opts)


class RethinkDBProtocolFactory(protocol.ReconnectingClientFactory):
    protocol = RethinkDBProtocol
    maxDelay = 10

    def __init__(self):
        pass