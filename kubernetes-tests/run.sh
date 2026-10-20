#!/usr/bin/env bash
#
# Kubernetes test (plan §1.4) on the current kubectl context (kind in CI): three members form a
# cluster through Kubernetes discovery, survive a rolling restart of the StatefulSet, and a
# client pod connects.
#
# Usage: kubernetes-tests/run.sh IMAGE dns|api
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
dir="$root/kubernetes-tests"
image="$1"
mode="$2"

case "$mode" in
    dns) discovery_name=HC_NETWORK_JOIN_KUBERNETES_SERVICEDNS; discovery_value=hecaton.default.svc.cluster.local ;;
    api) discovery_name=HC_NETWORK_JOIN_KUBERNETES_SERVICENAME; discovery_value=hecaton ;;
    *) echo "mode must be dns or api" >&2; exit 2 ;;
esac

cleanup() {
    echo "--- member logs"
    for pod in hecaton-0 hecaton-1 hecaton-2; do
        kubectl logs "$pod" --tail=30 2>/dev/null || true
    done
    kubectl delete pod hecaton-client --ignore-not-found --wait=true
    kubectl delete -f <(sed -e "s#IMAGE#$image#" "$dir/hecaton.yaml") --ignore-not-found --wait=true
    kubectl delete configmap image-smoke --ignore-not-found
}
trap cleanup EXIT

# all three members report a three-member cluster
check_cluster() {
    for pod in hecaton-0 hecaton-1 hecaton-2; do
        local ok=false
        for _ in $(seq 1 60); do
            if [ "$(kubectl logs "$pod" 2>/dev/null | grep -c 'Members {size:3')" -gt 0 ]; then
                ok=true
                break
            fi
            sleep 2
        done
        $ok || { echo "FAIL: $pod does not see 3 members ($mode)"; exit 1; }
    done
}

[ "$mode" = api ] && kubectl apply -f "$root/kubernetes-rbac.yaml"
sed -e "s#IMAGE#$image#" -e "s#DISCOVERY_NAME#$discovery_name#" -e "s#DISCOVERY_VALUE#$discovery_value#" \
    "$dir/hecaton.yaml" | kubectl apply -f -
kubectl rollout status statefulset/hecaton --timeout=300s
check_cluster
echo "OK cluster formed ($mode)"

kubectl rollout restart statefulset/hecaton
kubectl rollout status statefulset/hecaton --timeout=300s
check_cluster
echo "OK rolling restart ($mode)"

# compiled here: the image has a JRE only, which cannot run a source file
classes="$(mktemp -d)"
javac -d "$classes" -cp "$(ls "$root"/hecaton/target/hecaton-*.jar | grep -vE -- '-(sources|tests|javadoc)\.jar$' | head -n 1)" \
    "$root/tools/ci/ImageSmoke.java"
kubectl create configmap image-smoke --from-file="$classes"
sed -e "s#IMAGE#$image#" "$dir/client.yaml" | kubectl apply -f -
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/hecaton-client --timeout=180s \
    || { kubectl logs hecaton-client; echo "FAIL: client ($mode)"; exit 1; }
kubectl logs hecaton-client | grep "^OK image"
echo "OK client ($mode)"
