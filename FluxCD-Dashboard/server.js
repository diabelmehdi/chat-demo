const http = require('http');
const { execFile } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');

const execFileAsync = promisify(execFile);
const PORT = 3777;

// --- In-memory cache ---
const cache = { data: null, timestamp: 0, ttl: 5000 };

function getCached() {
  if (cache.data && Date.now() - cache.timestamp < cache.ttl) return cache.data;
  return null;
}

function setCache(data) {
  cache.data = data;
  cache.timestamp = Date.now();
}

// --- Async kubectl helper ---
async function kubectl(args) {
  try {
    const { stdout } = await execFileAsync('kubectl', [...args.split(/\s+/), '-o', 'json'], { timeout: 10000 });
    return JSON.parse(stdout);
  } catch {
    return null;
  }
}

// Raw kubectl (no -o json, returns plain text)
async function kubectlRaw(args) {
  try {
    const { stdout } = await execFileAsync('kubectl', args.split(/\s+/), { timeout: 10000 });
    return stdout;
  } catch {
    return null;
  }
}

// --- Data fetchers ---

async function getKustomizations(ns) {
  const data = await kubectl(`get kustomizations ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => {
      const ready = item.status?.conditions?.find(c => c.type === 'Ready');
      return {
        name: item.metadata.name,
        namespace: item.metadata.namespace,
        ready: ready?.status === 'True',
        status: ready?.reason || 'Unknown',
        message: ready?.message || '',
        revision: item.status?.lastAppliedRevision || item.status?.lastAttemptedRevision || 'N/A',
        lastTransition: ready?.lastTransitionTime || '',
        suspended: item.spec?.suspend || false,
        sourceRef: item.spec?.sourceRef || {},
        path: item.spec?.path || '',
        interval: item.spec?.interval || '',
        dependsOn: item.spec?.dependsOn || [],
      };
    }),
    error: null,
  };
}

async function getHelmReleases(ns) {
  const data = await kubectl(`get helmreleases ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => {
      const ready = item.status?.conditions?.find(c => c.type === 'Ready');
      return {
        name: item.metadata.name,
        namespace: item.metadata.namespace,
        ready: ready?.status === 'True',
        status: ready?.reason || 'Unknown',
        message: ready?.message || '',
        revision: item.status?.lastAppliedRevision || String(item.status?.lastAttemptedRevision || 'N/A'),
        chart: item.spec?.chart?.spec?.chart || 'N/A',
        version: item.spec?.chart?.spec?.version || '*',
        lastTransition: ready?.lastTransitionTime || '',
        suspended: item.spec?.suspend || false,
        interval: item.spec?.interval || '',
      };
    }),
    error: null,
  };
}

async function getGitRepos(ns) {
  const data = await kubectl(`get gitrepositories ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      url: item.spec?.url || '',
      branch: item.spec?.ref?.branch || 'main',
      ready: item.status?.conditions?.find(c => c.type === 'Ready')?.status === 'True',
      revision: item.status?.artifact?.revision || 'N/A',
      lastTransition: item.status?.conditions?.find(c => c.type === 'Ready')?.lastTransitionTime || '',
      interval: item.spec?.interval || '',
    })),
    error: null,
  };
}

async function getHelmRepos(ns) {
  const data = await kubectl(`get helmrepositories ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      url: item.spec?.url || '',
      ready: item.status?.conditions?.find(c => c.type === 'Ready')?.status === 'True',
      type: item.spec?.type || 'default',
      interval: item.spec?.interval || '',
    })),
    error: null,
  };
}

async function getPods(ns) {
  const data = await kubectl(`get pods ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => {
      const cs = item.status?.containerStatuses || [];
      const readyC = cs.filter(c => c.ready).length;
      const totalC = cs.length || item.spec?.containers?.length || 0;
      const restarts = cs.reduce((s, c) => s + (c.restartCount || 0), 0);
      return {
        name: item.metadata.name,
        namespace: item.metadata.namespace,
        status: item.status?.phase || 'Unknown',
        ready: `${readyC}/${totalC}`,
        restarts,
        node: item.spec?.nodeName || 'N/A',
        age: item.metadata.creationTimestamp,
        containers: cs.map(c => ({
          name: c.name, ready: c.ready, restarts: c.restartCount,
          image: c.image, state: Object.keys(c.state || {})[0] || 'unknown',
        })),
        resources: (item.spec?.containers || []).map(c => ({
          name: c.name,
          requests: c.resources?.requests || {},
          limits: c.resources?.limits || {},
        })),
      };
    }),
    error: null,
  };
}

async function getDeployments(ns) {
  const data = await kubectl(`get deployments ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      ready: `${item.status?.readyReplicas || 0}/${item.spec?.replicas || 0}`,
      upToDate: item.status?.updatedReplicas || 0,
      available: item.status?.availableReplicas || 0,
      age: item.metadata.creationTimestamp,
      images: (item.spec?.template?.spec?.containers || []).map(c => c.image),
    })),
    error: null,
  };
}

async function getNodes() {
  // Fetch node info and usage in parallel
  const [data, topData] = await Promise.all([
    kubectl('get nodes'),
    kubectlRaw('top nodes --no-headers --use-protocol-buffers'),
  ]);
  if (!data?.items) return { data: [], error: null };

  // Parse "kubectl top nodes" output (it returns plain text, not JSON)
  // Format: NAME  CPU(cores)  CPU%  MEMORY(bytes)  MEMORY%
  const usageMap = {};
  if (topData && typeof topData === 'string') {
    topData.trim().split('\n').forEach(line => {
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 5) {
        usageMap[parts[0]] = {
          cpuCores: parts[1],   // e.g. "350m"
          cpuPct: parseInt(parts[2]) || 0,
          memBytes: parts[3],   // e.g. "2100Mi"
          memPct: parseInt(parts[4]) || 0,
        };
      }
    });
  }

  // Also count pods per node
  let podCounts = {};
  try {
    const podsData = await kubectl('get pods -A -o json');
    if (podsData?.items) {
      podsData.items.forEach(p => {
        const node = p.spec?.nodeName;
        if (node) podCounts[node] = (podCounts[node] || 0) + 1;
      });
    }
  } catch (e) { /* ignore */ }

  return {
    data: data.items.map(item => {
      const conditions = item.status?.conditions || [];
      const ready = conditions.find(c => c.type === 'Ready');
      const cap = item.status?.capacity || {};
      const usage = usageMap[item.metadata.name] || {};
      return {
        name: item.metadata.name,
        ready: ready?.status === 'True',
        roles: Object.keys(item.metadata.labels || {})
          .filter(l => l.startsWith('node-role.kubernetes.io/'))
          .map(l => l.replace('node-role.kubernetes.io/', '')) || ['worker'],
        version: item.status?.nodeInfo?.kubeletVersion || 'N/A',
        os: item.status?.nodeInfo?.osImage || 'N/A',
        arch: item.status?.nodeInfo?.architecture || 'N/A',
        containerRuntime: item.status?.nodeInfo?.containerRuntimeVersion || 'N/A',
        capacity: { cpu: cap.cpu || '0', memory: cap.memory || '0', pods: cap.pods || '0' },
        usage: {
          cpu: usage.cpuCores || '0m',
          cpuPct: usage.cpuPct || 0,
          memory: usage.memBytes || '0Mi',
          memPct: usage.memPct || 0,
          pods: podCounts[item.metadata.name] || 0,
        },
        age: item.metadata.creationTimestamp,
        conditions: conditions.map(c => ({ type: c.type, status: c.status, reason: c.reason })),
      };
    }),
    error: null,
  };
}

async function getServices(ns) {
  const data = await kubectl(`get services ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      type: item.spec?.type || 'ClusterIP',
      clusterIP: item.spec?.clusterIP || 'None',
      externalIP: (item.status?.loadBalancer?.ingress || []).map(i => i.ip || i.hostname).join(', ') || item.spec?.externalIPs?.join(', ') || '-',
      ports: (item.spec?.ports || []).map(p => `${p.port}${p.targetPort ? ':' + p.targetPort : ''}/${p.protocol || 'TCP'}`),
    })),
    error: null,
  };
}

async function getIngresses(ns) {
  const data = await kubectl(`get ingresses ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      hosts: (item.spec?.rules || []).map(r => r.host || '*'),
      tls: (item.spec?.tls || []).map(t => ({ hosts: t.hosts, secret: t.secretName })),
      paths: (item.spec?.rules || []).flatMap(r =>
        (r.http?.paths || []).map(p => ({
          path: p.path, pathType: p.pathType,
          service: p.backend?.service?.name,
          port: p.backend?.service?.port?.number || p.backend?.service?.port?.name,
        }))
      ),
    })),
    error: null,
  };
}

async function getCertificates(ns) {
  const data = await kubectl(`get certificates ${ns ? `-n ${ns}` : '-A'}`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.map(item => ({
      name: item.metadata.name,
      namespace: item.metadata.namespace,
      ready: item.status?.conditions?.find(c => c.type === 'Ready')?.status === 'True',
      secret: item.spec?.secretName || '',
      domains: item.spec?.dnsNames || [],
      issuer: item.spec?.issuerRef?.name || '',
      renewalTime: item.status?.renewalTime || '',
      notAfter: item.status?.notAfter || '',
    })),
    error: null,
  };
}

async function getEvents(ns) {
  const data = await kubectl(`get events ${ns ? `-n ${ns}` : '-A'} --sort-by=.lastTimestamp`);
  if (!data?.items) return { data: [], error: null };
  return {
    data: data.items.slice(-100).reverse().map(item => ({
      type: item.type || 'Normal',
      reason: item.reason || '',
      message: item.message || '',
      namespace: item.metadata.namespace,
      object: `${item.involvedObject?.kind || ''}/${item.involvedObject?.name || ''}`,
      count: item.count || 1,
      firstSeen: item.firstTimestamp || item.metadata.creationTimestamp,
      lastSeen: item.lastTimestamp || item.metadata.creationTimestamp,
      source: item.source?.component || '',
    })),
    error: null,
  };
}

async function getNamespaces() {
  const data = await kubectl('get namespaces');
  if (!data?.items) return [];
  return data.items.map(item => item.metadata.name);
}

// --- Aggregate all data (parallel) ---
async function getAllData(ns) {
  const [
    kustomizations, helmReleases, gitRepositories, helmRepositories,
    deployments, pods, services, ingresses, certificates, nodes, events, namespaces,
  ] = await Promise.all([
    getKustomizations(ns), getHelmReleases(ns), getGitRepos(ns), getHelmRepos(ns),
    getDeployments(ns), getPods(ns), getServices(ns), getIngresses(ns),
    getCertificates(ns), getNodes(), getEvents(ns), getNamespaces(),
  ]);

  return {
    timestamp: new Date().toISOString(),
    namespaces,
    flux: {
      kustomizations: kustomizations.data,
      helmReleases: helmReleases.data,
      gitRepositories: gitRepositories.data,
      helmRepositories: helmRepositories.data,
    },
    workloads: {
      deployments: deployments.data,
      pods: pods.data,
    },
    networking: {
      services: services.data,
      ingresses: ingresses.data,
      certificates: certificates.data,
    },
    cluster: {
      nodes: nodes.data,
    },
    events: events.data,
    errors: [
      kustomizations.error, helmReleases.error, gitRepositories.error, helmRepositories.error,
      deployments.error, pods.error, services.error, ingresses.error, certificates.error,
      nodes.error, events.error,
    ].filter(Boolean),
  };
}

// --- Topology: build resource tree from ownerReferences ---
async function getTopology(ns) {
  // Fetch raw resources in parallel (we need ownerReferences)
  const types = [
    'kustomizations', 'helmreleases', 'deployments', 'replicasets', 'statefulsets',
    'pods', 'services', 'ingresses', 'configmaps', 'secrets',
    'persistentvolumeclaims', 'certificates',
  ];
  const fetches = types.map(t => kubectl(`get ${t} ${ns ? `-n ${ns}` : '-A'}`));
  const results = await Promise.all(fetches);

  const allResources = [];
  const kindMap = {
    kustomizations: 'Kustomization', helmreleases: 'HelmRelease',
    deployments: 'Deployment', replicasets: 'ReplicaSet', statefulsets: 'StatefulSet',
    pods: 'Pod', services: 'Service', ingresses: 'Ingress',
    configmaps: 'ConfigMap', secrets: 'Secret',
    persistentvolumeclaims: 'PVC', certificates: 'Certificate',
  };
  const iconMap = {
    Kustomization: 'ks', HelmRelease: 'hr', Deployment: 'deploy', ReplicaSet: 'rs',
    StatefulSet: 'sts', Pod: 'pod', Service: 'svc', Ingress: 'ing',
    ConfigMap: 'cm', Secret: 'secret', PVC: 'pvc', Certificate: 'cert',
  };

  results.forEach((data, i) => {
    if (!data?.items) return;
    const kind = kindMap[types[i]];
    data.items.forEach(item => {
      // Skip any resource scaled to 0 (inactive ReplicaSets, Deployments, StatefulSets, etc.)
      const currentReplicas = item.status?.replicas;
      if (currentReplicas !== undefined && currentReplicas === 0) return;

      const conditions = item.status?.conditions || [];
      const readyCond = conditions.find(c => c.type === 'Ready' || c.type === 'Available');
      const phase = item.status?.phase;
      const cs = item.status?.containerStatuses || [];
      const readyC = cs.filter(c => c.ready).length;
      const totalC = cs.length || item.spec?.containers?.length || 0;

      let health = 'unknown';
      if (kind === 'Pod') {
        health = phase === 'Running' || phase === 'Succeeded' ? 'healthy' : phase === 'Pending' ? 'progressing' : 'degraded';
      } else if (readyCond) {
        health = readyCond.status === 'True' ? 'healthy' : 'degraded';
      } else if (kind === 'ConfigMap' || kind === 'Secret' || kind === 'PVC') {
        health = 'healthy';
      }

      const uid = item.metadata.uid;
      const owners = (item.metadata.ownerReferences || []).map(o => o.uid);

      allResources.push({
        uid,
        kind,
        icon: iconMap[kind] || kind.toLowerCase(),
        name: item.metadata.name,
        namespace: item.metadata.namespace,
        health,
        info: getResourceInfo(kind, item, readyC, totalC),
        age: item.metadata.creationTimestamp,
        owners,
        children: [],
      });
    });
  });

  // Build tree: link children to parents via ownerReferences
  const byUid = {};
  allResources.forEach(r => { byUid[r.uid] = r; });

  const roots = [];
  allResources.forEach(r => {
    let attached = false;
    for (const ownerUid of r.owners) {
      if (byUid[ownerUid]) {
        byUid[ownerUid].children.push(r);
        attached = true;
        break;
      }
    }
    if (!attached) roots.push(r);
  });

  // Group roots by Flux kustomization label or namespace
  return roots;
}

function getResourceInfo(kind, item, readyC, totalC) {
  switch (kind) {
    case 'Deployment': {
      const rr = item.status?.readyReplicas || 0;
      const r = item.spec?.replicas || 0;
      return `${rr}/${r} replicas`;
    }
    case 'ReplicaSet': return `rev:${item.metadata.annotations?.['deployment.kubernetes.io/revision'] || '?'}`;
    case 'StatefulSet': {
      const rr = item.status?.readyReplicas || 0;
      const r = item.spec?.replicas || 0;
      return `${rr}/${r} replicas`;
    }
    case 'Pod': return `${readyC}/${totalC} ready`;
    case 'Service': return item.spec?.type || 'ClusterIP';
    case 'Ingress': return (item.spec?.rules || []).map(r => r.host).filter(Boolean).join(', ') || '';
    case 'Kustomization': return item.spec?.path || '';
    case 'HelmRelease': return item.spec?.chart?.spec?.chart || '';
    case 'Certificate': {
      const ready = item.status?.conditions?.find(c => c.type === 'Ready');
      return ready?.status === 'True' ? 'Valid' : 'Pending';
    }
    default: return '';
  }
}

// --- Health check ---
async function healthCheck() {
  try {
    const { stdout } = await execFileAsync('kubectl', ['cluster-info'], { timeout: 5000 });
    return { status: 'ok', cluster: stdout.trim().split('\n')[0] };
  } catch (e) {
    return { status: 'error', message: e.message };
  }
}

// --- Body parser ---
function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

// --- Apply resource via kubectl ---
async function applyResource(manifest) {
  const tmpFile = path.join(require('os').tmpdir(), `k8s-apply-${Date.now()}.json`);
  try {
    fs.writeFileSync(tmpFile, JSON.stringify(manifest));
    const { stdout, stderr } = await execFileAsync('kubectl', ['apply', '-f', tmpFile], { timeout: 15000 });
    return { success: true, message: stdout.trim() || stderr.trim() };
  } catch (e) {
    return { success: false, message: e.stderr?.trim() || e.message };
  } finally {
    try { fs.unlinkSync(tmpFile); } catch {}
  }
}

// --- CORS ---
function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// --- HTTP Server ---
const server = http.createServer(async (req, res) => {
  setCORS(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);
  const ns = url.searchParams.get('namespace') || null;

  // Serve static files for non-API routes
  if (!url.pathname.startsWith('/api')) {
    const filePath = path.join(__dirname, 'index.html');
    try {
      const html = fs.readFileSync(filePath, 'utf-8');
      res.setHeader('Content-Type', 'text/html');
      res.writeHead(200);
      res.end(html);
    } catch {
      res.writeHead(404);
      res.end('Not found');
    }
    return;
  }

  res.setHeader('Content-Type', 'application/json');
  try {
    switch (url.pathname) {
      case '/api/health': {
        const health = await healthCheck();
        res.writeHead(health.status === 'ok' ? 200 : 503);
        res.end(JSON.stringify(health));
        break;
      }
      case '/api/all': {
        const cached = !ns && getCached();
        if (cached) {
          res.writeHead(200);
          res.end(JSON.stringify(cached));
          break;
        }
        const result = await getAllData(ns);
        if (!ns) setCache(result);
        res.writeHead(200);
        res.end(JSON.stringify(result));
        break;
      }
      case '/api/events': {
        const events = await getEvents(ns);
        res.writeHead(200);
        res.end(JSON.stringify(events));
        break;
      }
      case '/api/namespaces': {
        const namespaces = await getNamespaces();
        res.writeHead(200);
        res.end(JSON.stringify(namespaces));
        break;
      }
      case '/api/flux': {
        const [ks, hr, gr, hrepo] = await Promise.all([
          getKustomizations(ns), getHelmReleases(ns), getGitRepos(ns), getHelmRepos(ns),
        ]);
        res.writeHead(200);
        res.end(JSON.stringify({
          kustomizations: ks.data, helmReleases: hr.data,
          gitRepositories: gr.data, helmRepositories: hrepo.data,
        }));
        break;
      }
      case '/api/workloads': {
        const [deps, pods] = await Promise.all([getDeployments(ns), getPods(ns)]);
        res.writeHead(200);
        res.end(JSON.stringify({ deployments: deps.data, pods: pods.data }));
        break;
      }
      case '/api/networking': {
        const [svcs, ings, certs] = await Promise.all([getServices(ns), getIngresses(ns), getCertificates(ns)]);
        res.writeHead(200);
        res.end(JSON.stringify({ services: svcs.data, ingresses: ings.data, certificates: certs.data }));
        break;
      }
      case '/api/cluster': {
        const nodes = await getNodes();
        res.writeHead(200);
        res.end(JSON.stringify({ nodes: nodes.data }));
        break;
      }
      case '/api/topology': {
        const topo = await getTopology(ns);
        res.writeHead(200);
        res.end(JSON.stringify(topo));
        break;
      }
      case '/api/apply': {
        if (req.method !== 'POST') {
          res.writeHead(405);
          res.end(JSON.stringify({ error: 'Method not allowed' }));
          break;
        }
        const body = await parseBody(req);
        if (!body?.resource) {
          res.writeHead(400);
          res.end(JSON.stringify({ success: false, message: 'Missing resource in body' }));
          break;
        }
        const result = await applyResource(body.resource);
        cache.data = null; // invalidate cache
        res.writeHead(result.success ? 200 : 400);
        res.end(JSON.stringify(result));
        break;
      }
      default:
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not found' }));
    }
  } catch (e) {
    res.writeHead(500);
    res.end(JSON.stringify({ error: e.message }));
  }
});

server.listen(PORT, () => {
  console.log(`FluxCD Dashboard API running on http://localhost:${PORT}`);
  console.log('Endpoints:');
  console.log('  GET /api/health              — Cluster connectivity check');
  console.log('  GET /api/all                 — All data (cached 5s)');
  console.log('  GET /api/all?namespace=X     — Filtered by namespace');
  console.log('  GET /api/namespaces          — List all namespaces');
  console.log('  GET /api/events              — Recent cluster events');
  console.log('  GET /api/flux                — FluxCD resources');
  console.log('  GET /api/workloads           — Deployments & Pods');
  console.log('  GET /api/networking           — Services, Ingresses, Certificates');
  console.log('  GET /api/cluster             — Node info');
});
