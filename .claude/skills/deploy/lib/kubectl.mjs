/**
 * Kubernetes helpers for BitDex deploy operations.
 * Wraps kubectl commands with consistent error handling and JSON output.
 */
import { execSync, spawnSync } from 'child_process';

export const K8S_CONTEXT = 'civit-datapacket';
export const NS = 'bitdex';
export const STS = 'bitdex';
export const NODE = 'talos-fq9-f3k';
export const INDEX_PATH = '/data/indexes/civitai';
export const PG_NS = 'cnpg-database';
export const PG_POD = 'cnpg-cluster-nvme0-1';
export const PG_SVC = 'svc/cnpg-cluster-nvme0-rw';

/**
 * Run a shell command, return trimmed stdout.
 * @param {string} cmd
 * @param {object} opts - { throws: boolean, timeout: number }
 */
export function run(cmd, opts = {}) {
  try {
    return execSync(cmd, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: opts.timeout,
      env: { ...process.env, MSYS_NO_PATHCONV: '1', ...opts.env },
      ...opts,
    }).trim();
  } catch (e) {
    if (opts.throws !== false) throw e;
    return e.stderr?.trim() || e.message;
  }
}

/** Run kubectl in the bitdex namespace. */
export function kubectl(args) {
  return run(`kubectl ${args} -n ${NS} --context ${K8S_CONTEXT} 2>/dev/null`);
}

/** Run a command inside a bitdex pod container. */
export function kubectlExec(cmd, { pod = 'bitdex-0', container = 'bitdex' } = {}) {
  return run(
    `kubectl exec -n ${NS} ${pod} -c ${container} --context ${K8S_CONTEXT} -- ${cmd} 2>/dev/null`,
    { throws: false }
  );
}

/** Run a command inside a named pod (no container flag). */
export function kubectlExecPod(podName, cmd, opts = {}) {
  return run(
    `kubectl exec -n ${NS} --context ${K8S_CONTEXT} ${podName} -- ${cmd} 2>/dev/null`,
    { throws: false, ...opts }
  );
}

/**
 * Create an ephemeral pod with a PVC mount and run a command.
 * Waits for completion, captures logs, and cleans up.
 * @returns {{ logs: string, success: boolean }}
 */
export function runEphemeralPod(name, { image = 'busybox', command, pvcIndex = 0, timeout = 60 } = {}) {
  const overrides = JSON.stringify({
    spec: {
      containers: [{
        name: 'task',
        image,
        command: ['sh', '-c', command],
        volumeMounts: [{ name: 'data', mountPath: '/data' }],
      }],
      volumes: [{
        name: 'data',
        persistentVolumeClaim: { claimName: `data-bitdex-${pvcIndex}` },
      }],
      nodeSelector: { 'kubernetes.io/hostname': NODE },
    },
  });

  // Clean up any leftover pod with the same name
  run(`kubectl delete pod -n ${NS} --context ${K8S_CONTEXT} ${name} --grace-period=0 2>/dev/null`, { throws: false });
  run(`kubectl wait --for=delete pod/${name} -n ${NS} --context ${K8S_CONTEXT} --timeout=30s 2>/dev/null`, { throws: false });

  run(`kubectl run ${name} -n ${NS} --context ${K8S_CONTEXT} --image=${image} --overrides='${overrides}' --restart=Never 2>/dev/null`);

  const waitResult = run(
    `kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/${name} -n ${NS} --context ${K8S_CONTEXT} --timeout=${timeout}s 2>/dev/null`,
    { throws: false }
  );
  const success = waitResult.includes('condition met');
  const logs = run(`kubectl logs -n ${NS} --context ${K8S_CONTEXT} ${name} 2>/dev/null`, { throws: false });

  run(`kubectl delete pod -n ${NS} --context ${K8S_CONTEXT} ${name} --grace-period=0 2>/dev/null`, { throws: false });

  return { logs, success };
}

/**
 * Create a long-lived transfer pod with DB access and PVC mount.
 * Used for CSV dumps and data operations.
 * @returns {string} pod name
 */
export function createTransferPod(name, { pvcIndex = 0, image = 'postgres:16-bookworm', timeout = 120 } = {}) {
  const overrides = JSON.stringify({
    spec: {
      containers: [{
        name: 'transfer',
        image,
        command: ['sleep', '7200'], // 2h max lifetime
        env: [{
          name: 'DATABASE_URL',
          valueFrom: { secretKeyRef: { name: 'bitdex-secrets', key: 'DATABASE_URL' } },
        }],
        volumeMounts: [{ name: 'data', mountPath: '/data' }],
      }],
      volumes: [{
        name: 'data',
        persistentVolumeClaim: { claimName: `data-bitdex-${pvcIndex}` },
      }],
      nodeSelector: { 'kubernetes.io/hostname': NODE },
    },
  });

  // Clean up any existing pod
  run(`kubectl delete pod -n ${NS} --context ${K8S_CONTEXT} ${name} --grace-period=0 2>/dev/null`, { throws: false });
  run(`kubectl wait --for=delete pod/${name} -n ${NS} --context ${K8S_CONTEXT} --timeout=30s 2>/dev/null`, { throws: false });

  run(`kubectl run ${name} -n ${NS} --context ${K8S_CONTEXT} --image=${image} --overrides='${overrides}' --restart=Never 2>/dev/null`);
  run(`kubectl wait --for=condition=Ready pod/${name} -n ${NS} --context ${K8S_CONTEXT} --timeout=${timeout}s 2>/dev/null`);

  return name;
}

/** Delete a pod by name. */
export function deletePod(name) {
  run(`kubectl delete pod -n ${NS} --context ${K8S_CONTEXT} ${name} --grace-period=0 2>/dev/null`, { throws: false });
}

/**
 * Start a kubectl port-forward in the background.
 * Waits up to retries seconds for the tunnel to be ready.
 * @returns {boolean} whether the tunnel is ready
 */
export function portForward({ namespace = NS, target, localPort, remotePort, healthCmd, retries = 10 } = {}) {
  // Check if already running
  if (healthCmd) {
    const check = run(healthCmd, { throws: false });
    if (check && !check.includes('refused') && !check.includes('error')) return true;
  }

  spawnSync('kubectl', [
    '--context', K8S_CONTEXT, 'port-forward', '-n', namespace,
    target, `${localPort}:${remotePort}`,
  ], {
    detached: true, stdio: 'ignore', windowsHide: true,
    shell: process.platform === 'win32',
  });

  for (let i = 0; i < retries; i++) {
    if (healthCmd) {
      const check = run(healthCmd, { throws: false });
      if (check && !check.includes('refused') && !check.includes('error')) return true;
    }
    run('sleep 1', { throws: false });
  }
  return false;
}

/** Kill a port-forward by local port. */
export function killPortForward(localPort, remotePort) {
  const pids = run(
    `ps aux 2>/dev/null | grep "port-forward.*${localPort}:${remotePort}" | grep -v grep | awk '{print $2}'`,
    { throws: false }
  );
  if (pids) {
    for (const pid of pids.split('\n').filter(Boolean)) {
      run(`kill ${pid} 2>/dev/null`, { throws: false });
    }
  }
}
