#!/usr/bin/python
import nagiosplugin
import argparse
import logging
import re
import sys
from nagiosplugin.state import Ok, Warn, Critical

_log = logging.getLogger('nagiosplugin')

class InfluxDB(nagiosplugin.Resource):
    def __init__(self, host, port, version, user, password, database, query=None, node=None, time_range=None, metric=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._query = query
        self._node = node
        self._metric = metric
        self._trange = time_range

        if version == '0.8':
            from influxdb.influxdb08 import InfluxDBClient
            self._cnx = InfluxDBClient(self._host, self._port, self._user, self._password, self._database)
            self.query = self.query8
        else:
            from influxdb import InfluxDBClient
            self._cnx = InfluxDBClient(self._host, self._port, self._user, self._password, self._database)
            self.query = self.query9

    def query9(self):
        raise NotImplemented("InlfuxDB v0.9 not yet suported")

    def query8(self, query):
        #print query
        data = self._cnx.query(query)
        #print data
        if not data:
           raise nagiosplugin.CheckError('empty response (query: %s' % query)
        if len(data) > 0 and len(data[0][u'points']) > 0:
            return data[0][u'points'][0][1]

    def probe(self):
        raise NotImplemented('must be subclassed')


class RawQuery(InfluxDB):
    name = "influx"
    def probe(self):
        self.name = self._query
        val = self.query(self._query)
        return [nagiosplugin.Metric(self._metric, val, context=self._metric)]


class Memory(InfluxDB):
    q = 'select last(100.0*free.value/(used.value + free.value)) from "%s.memory.free" as free inner join "%s.memory.used" as used where time > now() - %s group by time(%s) order asc'

    def probe(self):
        q = self.q % (self._node, self._node,
                      self._trange, self._trange)
        v = self.query(q)
        return nagiosplugin.Metric('mem', v, context='memory')

class Cpu(InfluxDB):
    name = "cpu all"
    query_cpu_tpl = 'select mean(value) from merge(/%s.cpu.\d+.%s$/) where time > now() - %s group by time(%s)'

    def __init__(self, cpu_type, *args, **kwargs):
        super(Cpu, self).__init__(*args, **kwargs)
        self.cpu_type = cpu_type

    def _q_cpu_x(self, x):
        q = self.query_cpu_tpl % (self._node, x, self._trange, self._trange)
        return q

    def _probe_cpu_idle(self):
        v = self.query(self._q_cpu_x('idle'))
        return nagiosplugin.Metric('cpu_idle', v , context='cpu')

    def _probe_cpu_wait(self):
        v = self.query(self._q_cpu_x('wait'))
        return nagiosplugin.Metric('cpu_wait', v , context='cpu')

    def _probe_cpu_system(self):
        v = self.query(self._q_cpu_x('system'))
        return nagiosplugin.Metric('cpu_system', v , context='cpu')

    def _probe_cpu_user(self):
        v = self.query(self._q_cpu_x('user'))
        return nagiosplugin.Metric('cpu_user', v , context='cpu')

    def _probe_cpu(self):
        v = self.query(self._q_cpu_x('user'))
        return [self._probe_cpu_user(), self._probe_cpu_system(), self._probe_cpu_wait()]

    def probe(self):
        q_fct = getattr(self, '_probe_%s' % self._metric, None)
        return q_fct()

def custom_memory(mem_type, args):
    check = nagiosplugin.Check(
                Memory(args.host, args.port, args.version, args.user,
                    args.password, args.database,
                    node=args.node,
                    time_range=args.time_range,
                    metric=args.metric,
                    )
                )
    check.add(nagiosplugin.ScalarContext('memory', args.warning, args.critical))
    return check

def custom_cpu(cpu_type, args):
    check = nagiosplugin.Check(
                Cpu(cpu_type, args.host, args.port, args.version, args.user,
                    args.password, args.database,
                    node=args.node,
                    time_range=args.time_range,
                    metric=args.metric,
                    )
                )
    check.add(nagiosplugin.ScalarContext('cpu', args.warning, args.critical))
    return check

class UnknownStatus(Exception):
    pass

class StatusContext(nagiosplugin.Context):

    def describe(self, metric, state=None):
        return 'Service status %s' % (metric.name)

    def evaluate(self, metric, resource):
        if metric.value == 0:
            state = Ok
        elif metric.value == 1:
            state = Warn
        elif metric.value == 2:
            state = Critical
        else: # no value or metric.value == 3
            raise UnknownStatus("Unknown state for %s" % metric.name)

        hint = self.describe(metric, state)
        return self.result_cls(state, hint, metric)

class Status(InfluxDB):
    q = "select last(value) from /openstack.%s.status/ where time > now() - 30s group by time(15s) order asc"

    def __init__(self, service, node, *args, **kwargs):
        super(Status, self).__init__(*args, **kwargs)
        self.service = service
        self.node = node

    def probe(self):
        q = self.q % (self.service)
        v = self.query(q)
        return nagiosplugin.Metric(self.service, v, context='status')

def status_check(service, node, args):
    check = nagiosplugin.Check(
        Status(service, node,
               args.host, args.port, args.version, args.user,
               args.password, args.database,))

    check.add(StatusContext('status'))
    return check

re_range = re.compile('^\d+[smhd]$')
def validate_range(time_range):
    if not re_range.match(time_range):
        raise ValueError("Invalid time_range")
    return time_range

@nagiosplugin.runtime.guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-v', '--version',  default='0.8',
                      help='InfluxDB version 0.8 or 0.9')
    argp.add_argument('-H', '--host',  default='', required=True,
                      help='InfluxDB hostname or IP')
    argp.add_argument('-u', '--user',  default='', required=True,
                      help='Username for authentication')
    argp.add_argument('-P', '--port',  default='8086', required=False,
                      help='Password for authentication')
    argp.add_argument('-p', '--password',  default='', required=True,
                      help='Password for authentication')
    argp.add_argument('-d', '--database',  default='', required=True,
                      help='InfluxDB database name')
    # Check options
    argp.add_argument('-w', '--warning', metavar='RANGE', default='', required=False,
                      help='return warning if load is outside RANGE')
    argp.add_argument('-c', '--critical', metavar='RANGE', default='', required=False,
                      help='return critical if load is outside RANGE')

    argp.add_argument('-V', '--verbose', action='count', default=0,
                      help='increase output verbosity (use up to 3 times)')
    argp.add_argument('-t', '--timeout', default=10,
                      help='abort execution after TIMEOUT seconds')

    subparsers = argp.add_subparsers(dest="command", help='sub-command help')
    # command raw query
    parser_raw = subparsers.add_parser('raw', help='InfluxDB raw query')
    parser_raw.add_argument('-q', '--query',  default='', required=True,
                            help='Raw query to run')
    parser_raw.add_argument('-m', '--metric',  default='default', help='Metric name for raw query')

    # command custom checks
    parser_custom = subparsers.add_parser('custom', help='Custom checks')
    parser_custom.add_argument('-m', '--metric',  choices=['cpu', 'cpu_user', 'cpu_system', 'cpu_wait', 'cpu_idle',
                                                           'memory'],
                               required=True,
                               help='Metric name')
    parser_custom.add_argument('-n', '--node', default='', required=True,
                               help='the node name')
    parser_custom.add_argument('-T', '--time-range',  default='1m', required=False,
                               type=validate_range,
                               help='Time range (examples: 1s, 10s, 15m, 2h, 3d, ..)')
    # command custom status checks
    parser_custom = subparsers.add_parser('status', help='Service status checks')
    parser_custom.add_argument('-s', '--service',
                               required=True,
                               help='Service name (nova, ..)')
    parser_custom.add_argument('-n', '--node', default='', required=True,
                               help='the node name')

    args = argp.parse_args()

    if args.command == 'raw':
        check = nagiosplugin.Check(
                    RawQuery(args.host, args.port, args.version, args.user,
                             args.password, args.database, args.query,
                             metric=args.metric),
                    nagiosplugin.ScalarContext(args.metric,
                                               args.warning, args.critical)
        )
    if args.command == 'custom':
        m = args.metric.split('_')
        context = m[0]
        if len(m) > 1:
            metric = m[1]
        else:
            metric = 'all'

        currmodule = sys.modules[__name__]
        fct = getattr(currmodule, 'custom_%s' % context)
        check = fct(metric, args)

    if args.command == 'status':
        check = status_check(args.service, args.node, args)


    check.main(args.verbose, args.timeout)

if __name__ == '__main__':
    main()
