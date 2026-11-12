"""Three-VM cluster for the bonus parts: create, inspect, break and repair it.

The VMs are Minikube nodes (docker driver): VM1 = <profile> is the control
plane, VM2 = <profile>-m02 and VM3 = <profile>-m03 are workers. Each node has
its own container runtime, kubelet, IP address and disk; destroying a node
container together with its volume destroys that machine and its disks.

    python scripts/k8s_multivm.py create              # cluster + edge proxy + images
    python scripts/k8s_multivm.py deploy              # kubectl apply -k k8s/overlays/multi-vm
    python scripts/k8s_multivm.py layout              # Pods per VM, DB roles, replication
    python scripts/k8s_multivm.py interrupt <node>    # power cut (docker kill)
    python scripts/k8s_multivm.py resume <node>       # power back on
    python scripts/k8s_multivm.py destroy <node>      # VM and its disks gone for good
    python scripts/k8s_multivm.py forget <node>       # declare a lost worker gone (delete Node)
    python scripts/k8s_multivm.py add-worker          # new worker VM joins
    python scripts/k8s_multivm.py backup-control-plane
    python scripts/k8s_multivm.py restore-control-plane --on <worker>


An edge proxy container (scripts/edge_proxy.py, outside the VMs) is the
external entry point: http://127.0.0.1:18080 for the gateway (NodePort 30080
on every worker) and https://127.0.0.1:18443 for the Kubernetes API (any node
running an API server). The kubectl context <profile>-edge goes through it.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "mini-banking-app:1.3.5"
EXTRA_IMAGES = ("postgres:16", "prom/prometheus:v2.55.1")
# Not in Minikube's preload; pulled from quay.io a new node can take 20+ minutes.
NODE_IMAGES = ("quay.io/calico/node:v3.32.2", "quay.io/calico/cni:v3.32.2",
               "quay.io/calico/kube-controllers:v3.32.2",
               "registry.k8s.io/coredns/coredns:v1.14.6")
EDGE = "banking-edge"
CONTROL_PLANE_ROLE = "node-role.kubernetes.io/control-plane"
RESTORED_ROLE = "node-role.kubernetes.io/control-plane-restored"
NAMESPACE = "banking"


def sh(*command: str, check: bool = True, timeout: float = 900, quiet: bool = False) -> str:
    if not quiet:
        print("+", " ".join(command), flush=True)
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stderr.strip() or result.stdout.strip()}")
    return result.stdout.strip()


def node_exec(node: str, command: str, check: bool = True, timeout: float = 300) -> str:
    return sh("docker", "exec", node, "bash", "-c", command, check=check, timeout=timeout, quiet=True)


class Cluster:
    def __init__(self, profile: str) -> None:
        self.profile = profile
        self.context = f"{profile}-edge"

    def kubectl(self, *arguments: str, check: bool = True, timeout: float = 120) -> str:
        return sh("kubectl", "--context", self.context, *arguments,
                  check=check, timeout=timeout, quiet=True)

    def nodes(self) -> dict[str, str]:
        """Node container name -> IP, for every node container that exists."""
        output = sh("docker", "ps", "-a", "--filter", "label=created_by.minikube.sigs.k8s.io=true",
                    "--format", "{{.Names}}", quiet=True)
        names = [name for name in output.split()
                 if name == self.profile or name.startswith(f"{self.profile}-m")]
        nodes = {}
        for name in sorted(names):
            ip = sh("docker", "inspect", "-f",
                    "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name,
                    check=False, quiet=True)
            try:
                ipaddress.ip_address(ip)
            except ValueError:
                continue        # never started (e.g. a failed node add)
            nodes[name] = ip
        return nodes

    def workers(self) -> list[str]:
        """Node containers that run workloads: all but a dedicated (tainted)
        control plane. Without an API server, all but the original VM1."""
        names = [name for name in self.nodes() if name != self.profile]
        try:
            items = json.loads(self.kubectl("get", "nodes", "-o", "json", timeout=20))["items"]
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            return names
        dedicated = {
            item["metadata"]["name"] for item in items
            if any(taint.get("key") == CONTROL_PLANE_ROLE and taint.get("effect") == "NoSchedule"
                   for taint in item["spec"].get("taints", []))
        }
        return [name for name in names if name not in dedicated]

    # ------------------------------------------------------------------ setup
    def create(self, cpus: int, memory: int) -> None:
        # --wait=apiserver: the nodes become Ready only once Calico runs, and
        # its images are side-loaded below instead of pulled on every node.
        sh("minikube", "start", "-p", self.profile, "--nodes", "3", "--driver", "docker",
           "--container-runtime", "containerd", "--cni", "calico", "--wait", "apiserver",
           "--cpus", str(cpus), "--memory", str(memory), timeout=1800)
        self.edge()
        for worker in self.workers():
            if not self.wait_for_node(worker, 60):
                self.join(worker)
        for node in self.nodes():
            self.side_load(node, [IMAGE, *EXTRA_IMAGES, *NODE_IMAGES])
        sh("kubectl", "--context", self.profile, "delete", "pod", "-A", "--wait=false",
           "--field-selector", "status.phase=Pending", check=False)
        sh("kubectl", "--context", self.profile, "wait", "node", "--all", "--for=condition=Ready",
           "--timeout=600s", timeout=620)
        for addon in ("storage-provisioner-rancher", "metrics-server"):
            sh("minikube", "-p", self.profile, "addons", "enable", addon, timeout=300)
        self.edge()
        kubectl = ["kubectl", "--context", self.profile]
        # VM1 only runs the control plane.
        sh(*kubectl, "taint", "node", self.profile,
           "node-role.kubernetes.io/control-plane=:NoSchedule", "--overwrite")
        for slot, worker in enumerate(self.workers(), start=1):
            sh(*kubectl, "label", "node", worker, "--overwrite",
               "node-role.kubernetes.io/worker=", f"banking.local/backup-slot={slot}")
        # Cluster DNS must not depend on the control-plane VM.
        sh(*kubectl, "-n", "kube-system", "patch", "deployment", "coredns", "--type", "merge",
           "-p", json.dumps({"spec": {"replicas": 2, "template": {"spec": {"affinity": {
               "nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                   "nodeSelectorTerms": [{"matchExpressions": [{
                       "key": "node-role.kubernetes.io/control-plane",
                       "operator": "DoesNotExist"}]}]}},
               "podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                   "topologyKey": "kubernetes.io/hostname",
                   "labelSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}]}}}}}}))
        sh(*kubectl, "-n", "kube-system", "rollout", "status", "deployment/coredns",
           "--timeout=300s")
        self.edge_context()
        self.harden_addons()
        print(f"cluster ready; use: kubectl --context {self.context} ...")

    def harden_addons(self) -> None:
        """Single-replica add-ons leave a lost worker within seconds instead of
        the default five minutes; the volume provisioner matters most, since a
        standby rebuilt on the surviving worker waits for its volume."""
        tolerations = [
            {"key": f"node.kubernetes.io/{key}", "operator": "Exists",
             "effect": "NoExecute", "tolerationSeconds": 10}
            for key in ("unreachable", "not-ready")
        ]
        patch = json.dumps({"spec": {"template": {"spec": {
            "tolerations": tolerations,
            "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{"matchExpressions": [{
                    "key": "node-role.kubernetes.io/control-plane",
                    "operator": "DoesNotExist"}]}]}}}}}}})
        for namespace, deployment in (("local-path-storage", "local-path-provisioner"),
                                      ("kube-system", "metrics-server"),
                                      ("kube-system", "coredns")):
            self.kubectl("-n", namespace, "patch", "deployment", deployment,
                         "--type", "strategic", "-p", patch)
            self.kubectl("-n", namespace, "rollout", "status", f"deployment/{deployment}",
                         "--timeout=300s", timeout=320)

    def load_images(self, nodes: list[str] | None = None) -> None:
        """Side-load local images (no registry needed): into every node, or
        straight into the containerd of the given nodes only."""
        images = [IMAGE, *EXTRA_IMAGES]
        pulled = list(EXTRA_IMAGES)
        if nodes is not None:
            # A StatefulSet recreates Pods at its previous revision until its
            # rollout reaches them, so a new node needs every tag in use.
            in_use = sh("kubectl", "--context", self.context, "get", "pods,statefulsets", "-A",
                        "-o", "jsonpath={..image}", check=False, quiet=True).split()
            images += sorted({image for image in in_use
                              if image.startswith("mini-banking-app:") and image != IMAGE
                              and subprocess.run(["docker", "image", "inspect", image],
                                                 capture_output=True).returncode == 0})
            images += NODE_IMAGES
            pulled += NODE_IMAGES
        for image in pulled:
            if subprocess.run(["docker", "image", "inspect", image],
                              capture_output=True).returncode != 0:
                sh("docker", "pull", image, timeout=900)
        if nodes is None:
            for image in images:
                sh("minikube", "-p", self.profile, "image", "load", image, timeout=900)
            return
        for node in nodes:
            self.side_load(node, images)

    @staticmethod
    def side_load(node: str, images: list[str]) -> None:
        """Copy images from the host's Docker into one node's containerd."""
        for image in images:
            if subprocess.run(["docker", "image", "inspect", image],
                              capture_output=True).returncode != 0:
                sh("docker", "pull", image, timeout=1800)
        archive = ROOT / "backups" / "images.tar"
        archive.parent.mkdir(parents=True, exist_ok=True)
        sh("docker", "save", "-o", str(archive), *images, timeout=900)
        # Not /tmp: it is a tmpfs in the node, and docker cp writes beneath it.
        sh("docker", "cp", str(archive), f"{node}:/var/tmp/images.tar", timeout=900)
        node_exec(node, "ctr -n k8s.io images import /var/tmp/images.tar"
                        " && rm /var/tmp/images.tar", timeout=900)
        archive.unlink()

    def edge(self) -> None:
        """(Re)start the edge proxy with the current node addresses."""
        nodes = self.nodes()
        workers = [ip for name, ip in nodes.items() if name != self.profile and ip]
        api = [ip for ip in nodes.values() if ip]
        sh("docker", "rm", "-f", EDGE, check=False, quiet=True)
        # The last address of the subnet: Minikube hands out node addresses
        # from the bottom and must never find one taken by the proxy.
        subnet = sh("docker", "network", "inspect", self.profile, "-f",
                    "{{(index .IPAM.Config 0).Subnet}}", quiet=True)
        edge_ip = str(list(ipaddress.ip_network(subnet).hosts())[-1])
        sh("docker", "run", "-d", "--name", EDGE, "--restart", "unless-stopped",
           "--network", self.profile, "--ip", edge_ip, "-p", "18080:18080", "-p", "18443:18443",
           "-v", f"{ROOT / 'scripts'}:/edge:ro", "--entrypoint", "python", IMAGE,
           "/edge/edge_proxy.py",
           "--route", "18080=" + ",".join(f"{ip}:30080" for ip in workers),
           "--route", "18443=" + ",".join(f"{ip}:8443" for ip in api))
        print(f"edge proxy: gateway -> {workers}, api -> {api}")

    def edge_context(self) -> None:
        minikube_home = Path.home() / ".minikube"
        sh("kubectl", "config", "set-cluster", self.context, "--server=https://127.0.0.1:18443",
           f"--certificate-authority={minikube_home / 'ca.crt'}", "--embed-certs=true")
        profile_dir = minikube_home / "profiles" / self.profile
        sh("kubectl", "config", "set-credentials", self.context,
           f"--client-certificate={profile_dir / 'client.crt'}",
           f"--client-key={profile_dir / 'client.key'}", "--embed-certs=true")
        sh("kubectl", "config", "set-context", self.context, f"--cluster={self.context}",
           f"--user={self.context}", f"--namespace={NAMESPACE}")
        sh("kubectl", "config", "use-context", self.context)

    def deploy(self) -> None:
        sh("kubectl", "--context", self.context, "apply", "-k",
           str(ROOT / "k8s" / "overlays" / "multi-vm"))
        for name in ("db-controller", "shard-a-app", "shard-b-app", "gateway"):
            self.kubectl("-n", NAMESPACE, "rollout", "status", f"deployment/{name}",
                         "--timeout=600s", timeout=620)

    # ------------------------------------------------------------- inspection
    def layout(self) -> dict:
        pods = json.loads(self.kubectl("-n", NAMESPACE, "get", "pods", "-o", "json"))["items"]
        per_node: dict[str, list[str]] = {}
        for pod in pods:
            node = pod["spec"].get("nodeName") or "<pending>"
            labels = pod["metadata"].get("labels", {})
            role = labels.get("db-role")
            ready = any(c["type"] == "Ready" and c["status"] == "True"
                        for c in pod.get("status", {}).get("conditions", []))
            name = pod["metadata"]["name"] + (f" [{role}]" if role else "") + ("" if ready else " (not ready)")
            per_node.setdefault(node, []).append(name)
        status = json.loads(self.kubectl(
            "-n", NAMESPACE, "exec", "deploy/db-controller", "--",
            "python", "db_controller.py", "status", timeout=60))
        databases = {}
        for cluster in status:
            leader = cluster["pods"].get(cluster["leader"], {})
            databases[cluster["cluster"]] = {
                "leader": cluster["leader"],
                "epoch": cluster["epoch"],
                "synchronous_standby": cluster["annotations"].get("banking.local/synced-standby"),
                "members": {
                    name: f"{pod['role']} on {pod['node']}" for name, pod in cluster["pods"].items()
                },
                "replication": {
                    replica["name"]: f"{replica['state']}/{replica['sync_state']} lag={replica['lag_bytes']}"
                    for replica in leader.get("replicas", [])
                },
            }
        nodes = json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]
        node_state = {
            node["metadata"]["name"]: next(
                (c["status"] for c in node["status"]["conditions"] if c["type"] == "Ready"), "?")
            for node in nodes
        }
        report = {"nodes_ready": node_state, "pods_per_node": per_node, "databases": databases}
        print(json.dumps(report, indent=2))
        return report

    # --------------------------------------------------------------- failures
    def interrupt(self, node: str) -> None:
        sh("docker", "kill", node)

    def resume(self, node: str) -> None:
        # Docker regenerates /etc/hosts when a node container starts, which a
        # rebooted VM would not do; put the API server name back.
        sh("docker", "start", node)
        self.point_at_api(node, self.control_plane_ip())
        node_exec(node, "systemctl restart kubelet")

    def control_plane(self) -> dict | None:
        """The Node running the API server: the original one, or a worker the
        control plane was restored on."""
        for node in json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]:
            labels = node["metadata"].get("labels", {})
            if {CONTROL_PLANE_ROLE, RESTORED_ROLE} & set(labels):
                return node
        return None

    def control_plane_name(self) -> str:
        node = self.control_plane()
        return node["metadata"]["name"] if node else self.profile

    def control_plane_ip(self) -> str:
        node = self.control_plane()
        for address in (node or {}).get("status", {}).get("addresses", []):
            if address["type"] == "InternalIP":
                return address["address"]
        return self.nodes()[self.profile]

    @staticmethod
    def point_at_api(node: str, ip: str) -> None:
        node_exec(node, (
            "grep -v control-plane.minikube.internal /etc/hosts > /tmp/hosts && "
            f"printf '{ip}\\tcontrol-plane.minikube.internal\\n' >> /tmp/hosts && "
            "cat /tmp/hosts > /etc/hosts"))

    def destroy(self, node: str) -> None:
        sh("docker", "rm", "-f", "-v", node)
        sh("docker", "volume", "rm", node, check=False)

    def forget(self, node: str) -> None:
        self.kubectl("delete", "node", node, "--wait=false")

    # ---------------------------------------------------- control-plane DR
    def backup_control_plane(self) -> dict[str, str]:
        """etcd snapshot + cluster PKI + kubeadm config, kept off the control plane.

        The bundle is copied to the host and to /var/lib/banking-dr on every
        worker, so a worker can rebuild the control plane from its own disk.
        """
        cp = self.control_plane_name()
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bundle = "/var/lib/banking-dr"
        etcd = node_exec(cp, "crictl ps --name '^etcd$' -q").splitlines()[0]
        certs = "/var/lib/minikube/certs/etcd"
        node_exec(cp, (
            f"crictl exec {etcd} etcdctl --endpoints=https://127.0.0.1:2379 "
            f"--cacert={certs}/ca.crt --cert={certs}/server.crt --key={certs}/server.key "
            f"snapshot save /var/lib/minikube/etcd/dr-snapshot.db"))
        node_exec(cp, (
            f"mkdir -p {bundle} && mv /var/lib/minikube/etcd/dr-snapshot.db {bundle}/etcd.db && "
            f"tar czf {bundle}/pki.tgz -C /var/lib/minikube certs && "
            # A restored control plane keeps its configuration in minikube-restore.yaml.
            f"cp $(ls /var/tmp/minikube-restore.yaml 2>/dev/null || echo /var/tmp/minikube/kubeadm.yaml) "
            f"{bundle}/kubeadm.yaml && date -u +%FT%TZ > {bundle}/taken_at"))
        # Every worker needs the control-plane images to take over. Minikube
        # preloads them; a node's containerd cannot export them (it drops the
        # compressed layers), so any that are missing come from the host.
        refs = node_exec(cp, (
            "crictl images | awk '/kube-apiserver|kube-controller-manager|kube-scheduler|"
            "registry.k8s.io\\/etcd/ {print $1\":\"$2}'")).split()
        target = ROOT / "backups" / "control-plane" / stamp
        target.mkdir(parents=True, exist_ok=True)
        sh("docker", "cp", f"{cp}:{bundle}/.", str(target))
        for worker in self.workers():
            node_exec(worker, f"mkdir -p {bundle}")
            sh("docker", "cp", f"{target}{os.sep}.", f"{worker}:{bundle}")
            present = node_exec(worker, "crictl images | awk '{print $1\":\"$2}'").split()
            missing = [ref for ref in refs if ref not in present]
            if missing:
                self.side_load(worker, missing)
        print(f"control-plane backup {stamp}: {target} and {bundle} on {self.workers()}")
        return {"taken_at": stamp, "host_copy": str(target), "worker_copies": ", ".join(self.workers())}

    def restore_control_plane(self, target: str, backup: dict[str, str] | None = None,
                              old: str | None = None) -> None:
        """Rebuild the control plane on a surviving worker from its local bundle.

        ``old``: the lost control-plane Node to delete afterwards (default VM1;
        empty when that machine stays on as a worker).
        """
        import yaml

        bundle = "/var/lib/banking-dr"
        ip = self.nodes()[target]
        version = node_exec(target, "ls /var/lib/minikube/binaries").split()[0]
        kubeadm = f"/var/lib/minikube/binaries/{version}/kubeadm"
        print(f"restoring the control plane on {target} ({ip}) from {bundle}", flush=True)

        # kubeadm configuration of the old control plane, re-addressed to this VM.
        documents = list(yaml.safe_load_all(node_exec(target, f"cat {bundle}/kubeadm.yaml")))
        for document in documents:
            if document.get("kind") == "InitConfiguration":
                document["localAPIEndpoint"]["advertiseAddress"] = ip
                document["nodeRegistration"]["name"] = target
                document["nodeRegistration"]["kubeletExtraArgs"] = [
                    {"name": "node-ip", "value": ip}]
                document.pop("bootstrapTokens", None)
            if document.get("kind") == "ClusterConfiguration":
                document["apiServer"]["certSANs"] = [
                    "127.0.0.1", "localhost", ip, "control-plane.minikube.internal"]
        config = yaml.safe_dump_all(documents, sort_keys=False)
        node_exec(target, f"cat > /var/tmp/minikube-restore.yaml <<'EOF'\n{config}\nEOF")

        etcd_image = node_exec(target, (
            "ctr -n k8s.io images ls -q | grep 'registry.k8s.io/etcd:' | grep -v '@' | head -n1"))
        node_exec(target, "systemctl stop kubelet")
        # The cluster CA, service-account keys and front-proxy CA are reused;
        # certificates naming the old machine are issued again.
        node_exec(target, (
            f"rm -rf /var/lib/minikube/certs && tar xzf {bundle}/pki.tgz -C /var/lib/minikube && "
            "rm -f /var/lib/minikube/certs/apiserver.crt /var/lib/minikube/certs/apiserver.key "
            "/var/lib/minikube/certs/etcd/server.* /var/lib/minikube/certs/etcd/peer.* && "
            "rm -f /etc/kubernetes/admin.conf /etc/kubernetes/super-admin.conf "
            "/etc/kubernetes/controller-manager.conf /etc/kubernetes/scheduler.conf"))
        # A single-member etcd from the snapshot, named after this VM.
        node_exec(target, (
            f"rm -rf /var/lib/minikube/etcd && ctr -n k8s.io run --rm "
            f"--mount type=bind,src=/var/lib/minikube,dst=/var/lib/minikube,options=rbind:rw "
            f"--mount type=bind,src={bundle},dst={bundle},options=rbind:ro "
            f"{etcd_image} etcd-restore-{int(time.time())} etcdutl snapshot restore {bundle}/etcd.db "
            f"--data-dir /var/lib/minikube/etcd --name {target} "
            f"--initial-cluster {target}=https://{ip}:2380 "
            f"--initial-advertise-peer-urls https://{ip}:2380"), timeout=300)
        config_flag = "--config /var/tmp/minikube-restore.yaml"
        for phase in ("certs apiserver", "certs etcd-server", "certs etcd-peer",
                      "kubeconfig admin", "kubeconfig super-admin",
                      "kubeconfig controller-manager", "kubeconfig scheduler",
                      "control-plane all", "etcd local"):
            node_exec(target, f"{kubeadm} init phase {phase} {config_flag}", timeout=300)
        # Every node finds the API server through this name.
        for node in self.workers():
            if sh("docker", "inspect", "-f", "{{.State.Running}}", node, check=False, quiet=True) == "true":
                self.point_at_api(node, ip)
        node_exec(target, "systemctl start kubelet")
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            if self.kubectl("get", "--raw", "/readyz", check=False, timeout=15) == "ok":
                break
            time.sleep(5)
        else:
            raise RuntimeError("the restored API server did not become ready")
        for node in self.workers():
            if node != target:
                node_exec(node, "systemctl restart kubelet", check=False)
        old = self.profile if old is None else old
        if old:
            self.kubectl("delete", "node", old, "--wait=false", check=False)
        # A role of its own: the add-ons avoid the original control-plane role,
        # and this worker keeps running workloads.
        self.kubectl("label", "node", target, "--overwrite", f"{RESTORED_ROLE}=")
        self.kubectl("-n", "kube-system", "delete", "pod", "-l", "k8s-app=kube-proxy", "--wait=false",
                     check=False)
        print(f"control plane restored on {target}")

    def add_worker(self) -> str:
        before = set(self.nodes())
        sh("minikube", "-p", self.profile, "node", "add", "--worker", timeout=1200)
        added = sorted(set(self.nodes()) - before)[-1]
        self.prune_profile()
        if not self.wait_for_node(added, 90):
            self.join(added)
            if not self.wait_for_node(added, 180):
                raise RuntimeError(f"{added} did not join the cluster")
        taken = {
            node["metadata"]["labels"].get("banking.local/backup-slot")
            for node in json.loads(self.kubectl("get", "nodes", "-o", "json"))["items"]
        }
        slot = next(slot for slot in ("1", "2", "3", "4") if slot not in taken)
        self.kubectl("label", "node", added, "--overwrite",
                     "node-role.kubernetes.io/worker=", f"banking.local/backup-slot={slot}")
        self.load_images([added])
        # Pods that started pulling before the images arrived keep pulling.
        self.kubectl("delete", "pod", "-A", "--wait=false", "--field-selector",
                     f"spec.nodeName={added},status.phase=Pending", check=False)
        self.edge()
        return added

    def wait_for_node(self, node: str, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.kubectl("get", "node", node, "-o", "name", check=False):
                return True
            time.sleep(5)
        return False

    def join(self, node: str) -> None:
        """kubeadm join with a fresh token (Minikube's own join step sometimes
        does not run when the profile has lost nodes)."""
        control_plane = self.control_plane_name()
        self.point_at_api(node, self.control_plane_ip())
        version = node_exec(node, "ls /var/lib/minikube/binaries").split()[0]
        kubeadm = f"/var/lib/minikube/binaries/{version}/kubeadm"
        command = node_exec(control_plane, f"{kubeadm} token create --print-join-command --ttl 30m")
        node_exec(node, (
            f"{command.replace('kubeadm', kubeadm, 1)} --ignore-preflight-errors=all "
            f"--cri-socket unix:///run/containerd/containerd.sock --node-name={node}"), timeout=300)

    def prune_profile(self) -> None:
        """Drop destroyed workers from the Minikube profile. Done only after a
        replacement was added, so Minikube does not reuse a lost node's name."""
        path = Path.home() / ".minikube" / "profiles" / self.profile / "config.json"
        config = json.loads(path.read_text(encoding="utf-8"))
        existing = set(self.nodes())
        config["Nodes"] = [
            node for node in config["Nodes"]
            if not node["Name"] or f"{self.profile}-{node['Name']}" in existing
        ]
        path.write_text(json.dumps(config, indent=4), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=__doc__.split("\n", 2)[2])
    parser.add_argument("--profile", default="banking-mv")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("--cpus", type=int, default=2)
    create.add_argument("--memory", type=int, default=2200)
    commands.add_parser("edge")
    commands.add_parser("edge-context")
    commands.add_parser("deploy")
    commands.add_parser("layout")
    commands.add_parser("load-images")
    commands.add_parser("harden-addons")
    for name in ("interrupt", "resume", "destroy", "forget"):
        commands.add_parser(name).add_argument("node")
    commands.add_parser("add-worker")
    commands.add_parser("backup-control-plane")
    restore = commands.add_parser("restore-control-plane")
    restore.add_argument("--on", required=True, help="surviving worker that becomes the control plane")
    restore.add_argument("--old", help="lost control-plane Node to delete (default: VM1)")
    args = parser.parse_args()
    cluster = Cluster(args.profile)
    if args.command == "create":
        cluster.create(args.cpus, args.memory)
    elif args.command == "edge":
        cluster.edge()
    elif args.command == "edge-context":
        cluster.edge_context()
    elif args.command == "deploy":
        cluster.deploy()
    elif args.command == "layout":
        cluster.layout()
    elif args.command == "load-images":
        cluster.load_images()
    elif args.command == "harden-addons":
        cluster.harden_addons()
    elif args.command == "add-worker":
        print(cluster.add_worker())
    elif args.command == "backup-control-plane":
        cluster.backup_control_plane()
    elif args.command == "restore-control-plane":
        cluster.restore_control_plane(args.on, old=args.old)
    else:
        getattr(cluster, args.command)(args.node)
    return 0


if __name__ == "__main__":
    sys.exit(main())
