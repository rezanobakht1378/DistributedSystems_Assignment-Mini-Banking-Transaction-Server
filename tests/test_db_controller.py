import copy
import os
import unittest
from unittest import mock

import httpx

from db_controller import (
    Conflict,
    DatabaseController,
    LeaderElection,
    Probe,
    _ANNOTATION_PROMOTING,
    _ANNOTATION_SYNCED_STANDBY,
    _ANNOTATION_TIMELINE,
    _sync_names,
)

CLUSTER = "shard-a-db"
S0, S1, S2 = (f"{CLUSTER}-{index}" for index in range(3))


class FakeApi:
    namespace = "banking"

    def __init__(self, leader, annotations, placement, nodes=("vm2", "vm3")):
        self.lease = {
            "metadata": {"name": f"{CLUSTER}-leader", "resourceVersion": "1",
                         "annotations": dict(annotations)},
            "spec": {"holderIdentity": leader, "leaseTransitions": 0},
        }
        self.pods = {
            pod: {"metadata": {"uid": f"uid-{pod}", "labels": {}}, "spec": {"nodeName": node}}
            for pod, node in placement.items()
        }
        self.nodes = {name: {"ready": True, "schedulable": True} for name in nodes}
        self.claims = {f"data-{pod}": {"spec": {"volumeName": f"pv-{pod}"}} for pod in placement}
        self.volumes = {
            f"pv-{pod}": {"spec": {"nodeAffinity": {"required": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In",
                                       "values": [node]}]}]}}}}
            for pod, node in placement.items()
        }
        self.deleted_pods = []
        self.deleted_claims = []
        self.forgotten_volumes = []

    def get_lease(self, name):
        return copy.deepcopy(self.lease)

    def replace_lease(self, lease):
        if lease["metadata"]["resourceVersion"] != self.lease["metadata"]["resourceVersion"]:
            raise Conflict(lease["metadata"]["name"])
        lease = copy.deepcopy(lease)
        lease["metadata"]["resourceVersion"] = str(int(lease["metadata"]["resourceVersion"]) + 1)
        self.lease = lease
        return copy.deepcopy(lease)

    def get_pod(self, name):
        return copy.deepcopy(self.pods.get(name))

    def label_pod(self, name, role):
        if name in self.pods:
            self.pods[name]["metadata"]["labels"]["db-role"] = role

    def delete_pod(self, name, grace_seconds):
        self.deleted_pods.append(name)
        self.pods.pop(name, None)

    def statefulset_replicas(self, name):
        return 3

    def list_nodes(self):
        return copy.deepcopy(self.nodes)

    def get_pvc(self, name):
        return copy.deepcopy(self.claims.get(name))

    def delete_pvc(self, name):
        self.deleted_claims.append(name)
        self.claims.pop(name, None)

    def get_pv(self, name):
        return copy.deepcopy(self.volumes.get(name))

    def forget_pv(self, name):
        self.forgotten_volumes.append(name)
        self.volumes.pop(name, None)


def primary(sync, replicas, timeline=1):
    return Probe(True, False, sync, [
        {"name": name, "state": state, "sync_state": sync_state, "lag_bytes": 0}
        for name, state, sync_state in replicas
    ], timeline)


STANDBY = Probe(True, True)
DOWN = Probe(False, error="timeout")


class Controller(DatabaseController):
    def __init__(self, api, probes):
        super().__init__(api, [CLUSTER])
        self.probes = probes
        self.promoted = []
        self.sync_set = []

    def probe(self, cluster, pod):
        return self.probes[pod]

    def promote(self, cluster, pod):
        self.promoted.append(pod)
        return True

    def set_synchronous_standby(self, cluster, leader, standby):
        self.sync_set.append(standby)
        return True


@mock.patch.dict(os.environ, {"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "x",
                              "BANK_DB_PLACEMENT": "spread"})
class DatabaseControllerTests(unittest.TestCase):
    def test_parses_synchronous_standby_names(self):
        self.assertEqual(_sync_names('"shard-a-db-1"'), ["shard-a-db-1"])
        self.assertEqual(_sync_names("ANY 1 (a, \"b\")"), ["a", "b"])
        self.assertEqual(_sync_names("*"), ["*"])
        self.assertEqual(_sync_names(""), [])

    def test_prefers_a_synchronous_standby_on_another_node(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S1}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {
            S0: primary('"%s"' % S1, [(S1, "streaming", "sync"), (S2, "streaming", "async")]),
            S1: STANDBY, S2: STANDBY,
        })
        controller.reconcile(CLUSTER)
        # The record is removed before PostgreSQL stops waiting for S1.
        self.assertNotIn(_ANNOTATION_SYNCED_STANDBY, api.lease["metadata"]["annotations"])
        self.assertEqual(controller.sync_set, [S2])

        controller.probes[S0] = primary(
            '"%s"' % S2, [(S1, "streaming", "async"), (S2, "streaming", "sync")])
        controller.reconcile(CLUSTER)
        self.assertEqual(api.lease["metadata"]["annotations"][_ANNOTATION_SYNCED_STANDBY], S2)
        self.assertEqual(api.lease["metadata"]["annotations"][_ANNOTATION_TIMELINE], "1")

    def test_falls_back_to_local_standby_when_the_remote_one_is_gone(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S2}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        api.nodes["vm3"] = {"ready": False, "schedulable": False}
        controller = Controller(api, {
            S0: primary('"%s"' % S2, [(S1, "streaming", "async")]),
            S1: STANDBY, S2: DOWN,
        })
        controller.reconcile(CLUSTER)
        self.assertNotIn(_ANNOTATION_SYNCED_STANDBY, api.lease["metadata"]["annotations"])
        self.assertEqual(controller.sync_set, [S1])

    def _degraded_after_worker_loss(self):
        # S2 on vm3 was promoted; the old primary S0 and its local standby S1
        # are on vm2, which is NotReady, so commits on S2 block.
        api = FakeApi(S2, {_ANNOTATION_TIMELINE: "2"}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        api.nodes["vm2"] = {"ready": False, "schedulable": False}
        controller = Controller(api, {S0: DOWN, S1: DOWN, S2: primary('"%s"' % S0, [], timeline=2)})
        return api, controller

    def test_rebuilds_one_unreachable_standby_once_commits_stay_blocked(self):
        api, controller = self._degraded_after_worker_loss()
        controller.rebuild_unreachable_after = 0.01
        controller.reconcile(CLUSTER)      # starts the timer
        self.assertEqual(api.deleted_claims, [])
        controller.degraded_since[CLUSTER] -= 1
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [f"data-{S0}"])
        self.assertEqual(api.forgotten_volumes, [f"pv-{S0}"])
        self.assertEqual(api.deleted_pods, [S0])
        # S0 is recreated on vm3 and still cloning: S1 keeps its copy.
        api.pods[S0] = {"metadata": {"uid": "uid-new", "labels": {}}, "spec": {"nodeName": "vm3"}}
        controller.degraded_since[CLUSTER] -= 1
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [f"data-{S0}"])

    def test_a_pending_standby_pinned_to_the_lost_node_does_not_block_the_rebuild(self):
        api, controller = self._degraded_after_worker_loss()
        api.pods[S0]["spec"].pop("nodeName")      # Pending; its volume is on vm2
        controller.rebuild_unreachable_after = 0.01
        controller.reconcile(CLUSTER)
        controller.degraded_since[CLUSTER] -= 1
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [f"data-{S0}"])

    def test_does_not_rebuild_standbys_on_a_ready_node_or_when_disabled(self):
        api, controller = self._degraded_after_worker_loss()
        api.nodes["vm2"] = {"ready": True, "schedulable": True}
        controller.rebuild_unreachable_after = 0.01
        controller.reconcile(CLUSTER)
        controller.degraded_since[CLUSTER] -= 1
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [])

        api, controller = self._degraded_after_worker_loss()
        controller.rebuild_unreachable_after = 0
        controller.reconcile(CLUSTER)
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [])

    def test_fails_over_only_to_the_recorded_synchronous_standby(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S2, _ANNOTATION_TIMELINE: "1"},
                      {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {S0: DOWN, S1: STANDBY, S2: STANDBY})
        controller.failover_after = 0
        controller.reconcile(CLUSTER)      # first sighting starts the timer
        controller.reconcile(CLUSTER)
        self.assertEqual(api.lease["spec"]["holderIdentity"], S2)
        self.assertEqual(api.lease["spec"]["leaseTransitions"], 1)
        self.assertEqual(api.lease["metadata"]["annotations"][_ANNOTATION_PROMOTING], "true")
        self.assertEqual(controller.promoted, [S2])
        # Old leader fenced, the other standby re-bootstraps from the new leader.
        self.assertEqual(api.deleted_pods, [S0, S1])

    def test_failover_is_blocked_without_a_recorded_synchronous_standby(self):
        api = FakeApi(S0, {}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {S0: DOWN, S1: STANDBY, S2: STANDBY})
        controller.failover_after = 0
        controller.reconcile(CLUSTER)
        controller.reconcile(CLUSTER)
        self.assertEqual(api.lease["spec"]["holderIdentity"], S0)
        self.assertEqual(controller.promoted, [])

    def test_stale_leader_record_adopts_the_newer_primary(self):
        # e.g. etcd restored from a snapshot taken before a failover to S2
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S2, _ANNOTATION_TIMELINE: "1"},
                      {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {
            S0: STANDBY, S1: STANDBY,
            S2: primary('"%s"' % S1, [(S0, "streaming", "async"), (S1, "streaming", "sync")],
                        timeline=2),
        })
        controller.reconcile(CLUSTER)
        self.assertEqual(api.lease["spec"]["holderIdentity"], S2)
        self.assertEqual(api.lease["metadata"]["annotations"][_ANNOTATION_TIMELINE], "2")
        self.assertNotIn(_ANNOTATION_SYNCED_STANDBY, api.lease["metadata"]["annotations"])
        self.assertEqual(api.deleted_pods, [])
        self.assertEqual(controller.promoted, [])

    def test_old_primary_on_an_older_timeline_is_fenced(self):
        api = FakeApi(S2, {_ANNOTATION_SYNCED_STANDBY: S1, _ANNOTATION_TIMELINE: "2"},
                      {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {
            S0: primary('"%s"' % S2, [], timeline=1),
            S1: STANDBY,
            S2: primary('"%s"' % S1, [(S1, "streaming", "sync")], timeline=2),
        })
        controller.reconcile(CLUSTER)
        self.assertEqual(api.lease["spec"]["holderIdentity"], S2)
        self.assertIn(S0, api.deleted_pods)

    def test_recorded_leader_that_is_a_standby_is_not_promoted_without_a_failover(self):
        api = FakeApi(S0, {_ANNOTATION_TIMELINE: "1"}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        controller = Controller(api, {S0: STANDBY, S1: STANDBY, S2: DOWN})
        controller.reconcile(CLUSTER)
        self.assertEqual(controller.promoted, [])

        api.lease["metadata"]["annotations"][_ANNOTATION_PROMOTING] = "true"
        controller.reconcile(CLUSTER)
        self.assertEqual(controller.promoted, [S0])

    def test_rebuilds_a_standby_whose_node_was_removed(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S1}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        del api.nodes["vm3"]
        controller = Controller(api, {
            S0: primary('"%s"' % S1, [(S1, "streaming", "sync")]),
            S1: STANDBY, S2: DOWN,
        })
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [f"data-{S2}"])
        self.assertEqual(api.forgotten_volumes, [f"pv-{S2}"])
        self.assertIn(S2, api.deleted_pods)

    def test_never_touches_volumes_when_the_node_list_is_empty(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S1}, {S0: "vm2", S1: "vm2", S2: "vm3"})
        api.nodes = {}
        controller = Controller(api, {
            S0: primary('"%s"' % S1, [(S1, "streaming", "sync"), (S2, "streaming", "async")]),
            S1: STANDBY, S2: STANDBY,
        })
        controller.reconcile(CLUSTER)
        self.assertEqual(api.deleted_claims, [])

    def test_moves_a_standby_to_another_node_when_all_members_share_one(self):
        api = FakeApi(S0, {_ANNOTATION_SYNCED_STANDBY: S1}, {S0: "vm2", S1: "vm2", S2: "vm2"},
                      nodes=("vm2", "vm4"))
        controller = Controller(api, {
            S0: primary('"%s"' % S1, [(S1, "streaming", "sync"), (S2, "streaming", "async")]),
            S1: STANDBY, S2: STANDBY,
        })
        controller.reconcile(CLUSTER)
        # Only the standby that is not synchronous is rebuilt.
        self.assertEqual(api.deleted_claims, [f"data-{S2}"])
        self.assertEqual(api.forgotten_volumes, [])     # vm2 cleans it up itself
        self.assertEqual(api.deleted_pods, [S2])


class LeaseStore:
    def __init__(self):
        self.lease = None
        self.reachable = True

    def get_lease(self, name):
        if not self.reachable:
            raise httpx.ConnectError("down")
        return copy.deepcopy(self.lease)

    def create_lease(self, name, holder):
        if self.lease is not None:
            raise Conflict(name)
        self.lease = {"metadata": {"name": name, "resourceVersion": "1"},
                      "spec": {"holderIdentity": holder, "leaseTransitions": 0}}
        return copy.deepcopy(self.lease)

    def replace_lease(self, lease):
        if not self.reachable:
            raise httpx.ConnectError("down")
        if lease["metadata"]["resourceVersion"] != self.lease["metadata"]["resourceVersion"]:
            raise Conflict(lease["metadata"]["name"])
        lease = copy.deepcopy(lease)
        lease["metadata"]["resourceVersion"] = str(int(lease["metadata"]["resourceVersion"]) + 1)
        self.lease = lease
        return copy.deepcopy(lease)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class LeaderElectionTests(unittest.TestCase):
    def setUp(self):
        self.store, self.clock = LeaseStore(), Clock()
        self.a = LeaderElection(self.store, "a", duration=10, renew_deadline=6, clock=self.clock)
        self.b = LeaderElection(self.store, "b", duration=10, renew_deadline=6, clock=self.clock)

    def test_only_one_instance_leads(self):
        self.assertTrue(self.a.step())
        self.assertFalse(self.b.step())
        for _ in range(10):
            self.clock.now += 2
            self.assertTrue(self.a.step())
            self.assertFalse(self.b.step())

    def test_takes_over_only_after_the_lease_stops_changing_for_its_duration(self):
        self.a.step()
        self.b.step()                       # b first sees the record at t=0
        self.clock.now = 9
        self.assertFalse(self.b.step())
        self.clock.now = 10.5
        self.assertTrue(self.b.step())
        self.assertEqual(self.store.lease["spec"]["holderIdentity"], "b")
        self.assertEqual(self.store.lease["spec"]["leaseTransitions"], 1)

    def test_holder_that_cannot_renew_stops_before_anyone_can_take_over(self):
        self.a.step()
        self.b.step()
        self.store.reachable = False
        self.clock.now = 4
        self.assertTrue(self.a.step())      # still within its renew deadline
        self.clock.now = 6.5
        self.assertFalse(self.a.step())     # stopped acting at 6 s ...
        self.store.reachable = True
        self.clock.now = 9
        self.assertFalse(self.b.step())     # ... while b may only take over at 10 s

    def test_former_holder_steps_down_when_it_sees_another_holder(self):
        self.a.step()
        self.b.step()
        self.clock.now = 11
        self.assertTrue(self.b.step())
        self.assertFalse(self.a.step())

    def test_losing_a_compare_and_swap_is_not_leadership(self):
        self.a.step()
        stale = self.store.get_lease("db-controller")
        self.clock.now = 2
        self.a.step()
        with self.assertRaises(Conflict):
            self.store.replace_lease(stale)


if __name__ == "__main__":
    unittest.main()
