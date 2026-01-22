import json

from operator import attrgetter
from traitlets.config import SingletonConfigurable
from traitlets import Integer

from datetime import datetime
from subprocess import check_output, CalledProcessError

from cachetools import TTLCache, cachedmethod

class SlurmAPI(SingletonConfigurable):
    info_cache_ttl = Integer(300).tag(config=True)
    acct_cache_ttl = Integer(300).tag(config=True)
    acct_cache_size = Integer(100).tag(config=True)
    res_cache_ttl = Integer(300).tag(config=True)

    def __init__(self, config=None):
        super().__init__(config=config)
        self.info_cache = TTLCache(maxsize=1, ttl=self.info_cache_ttl)
        self.acct_cache = TTLCache(maxsize=self.acct_cache_size, ttl=self.acct_cache_ttl)
        self.res_cache = TTLCache(maxsize=1, ttl=self.res_cache_ttl)

    @cachedmethod(attrgetter('info_cache'))
    def get_node_info(self):
        output = {'cpu': [], 'mem': [], 'gres': [], 'partitions': [], 'features': set()}
        try:
            controls = check_output(['scontrol', '--json', 'show', 'node'], encoding='utf-8')
        except CalledProcessError:
            return output
        else:
            nodes = json.loads(controls).get('nodes', [])
            for node in nodes:
                output['cpu'].append(node['cpus'])
                output['mem'].append(node['real_memory'] - node.get('specialized_memory', 0))
                if node['gres']:
                    output['gres'].append(node['gres'])
                output['partitions'].extend(node.get('partitions', []))
                if node.get('active_features', []):
                    output['features'].add(frozenset(node['active_features']))
        return output

    def is_online(self):
        return self.get_node_info()['cpu'] and self.get_node_info()['mem']

    def get_cpus(self):
        cpus = set(self.get_node_info()['cpu'])
        return sorted(cpus)

    def get_mems(self):
        mems = set(self.get_node_info()['mem'])
        return sorted(mems)

    def get_gres(self):
        gres = set(self.get_node_info()['gres']) - set(['gpu:0'])
        return ['gpu:0'] + sorted(gres)

    def get_partitions(self):
        partitions = set(self.get_node_info()['partitions'])
        return sorted(partitions)

    def get_features(self):
        feature_sets = self.get_node_info()['features']
        features = {feature for feature_set in feature_sets for feature in feature_set}
        return sorted(features)

    @cachedmethod(attrgetter('acct_cache'))
    def get_accounts(self, username):
        try:
            string = check_output(['sacctmgr', 'show', 'user', username, 'withassoc',
                                    'format=account', '-P', '--noheader'], encoding='utf-8')
        except CalledProcessError:
            return []
        return string.splitlines()

    @cachedmethod(attrgetter('res_cache'))
    def get_reservations(self):
        try:
            reservations = check_output(['scontrol', 'show', 'res', '--json'], encoding='utf-8')
        except CalledProcessError:
            reservations = []
        else:
            reservations = json.loads(reservations).get('reservations', [])

        filtered_reservations = []
        for res in reservations:
            flags = set(res['flags'])
            if 'MAINT' in flags:
                continue
            current_res = {}
            current_res['ReservationName'] = res['name']
            current_res['Users'] = set(res['users'].split(','))
            current_res['Accounts'] = set(res['accounts'].split(','))
            current_res['StartTime'] = datetime.fromtimestamp(res['start_time']['number'])
            current_res['EndTime'] = datetime.fromtimestamp(res['end_time']['number'])
            filtered_reservations.append(current_res)
        return filtered_reservations

    def get_active_reservations(self, username, accounts):
        reservations = self.get_reservations()
        if not reservations:
            return []

        accounts = set(accounts)
        now = datetime.now()
        return [
            res for res in reservations
            if (
                res['StartTime'] <= now <= res['EndTime'] and
                (
                    username in res['Users'] or
                    bool(accounts.intersection(res['Accounts']))
                )
            )
        ]
