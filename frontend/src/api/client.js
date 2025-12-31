const API_URL = import.meta.env.VITE_API_URL || window.location.origin;

async function request(url, options = {}) {
  const { timeout = 30000, ...fetchOptions } = options;
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);

  try {
    const res = await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
    });
    clearTimeout(id);

    if (!res.ok) {
      let errorDetail = '';
      try {
        const errJson = await res.json();
        errorDetail = errJson.detail || errJson.message || JSON.stringify(errJson);
      } catch (e) {
        errorDetail = await res.text().catch(() => '');
      }
      throw new Error(errorDetail || `Request failed: ${res.status}`);
    }

    // Return empty for 204 No Content
    if (res.status === 204) return null;
    
    return res.json();
  } catch (error) {
    clearTimeout(id);
    throw error;
  }
}

export const api = {
  base: API_URL,

  get(endpoint) {
    return request(`${API_URL}${endpoint}`);
  },

  health() {
    return request(`${API_URL}/healthz`, { timeout: 5000 });
  },

  listDocs(limit = 200) {
    return request(`${API_URL}/ui/docs?limit=${limit}`);
  },

  deleteDoc(doc_id) {
    return request(`${API_URL}/ui/docs/${doc_id}`, { method: 'DELETE' });
  },

  linkForDoc(doc_id) {
    return request(`${API_URL}/ui/link/${doc_id}`);
  },

  docStatus(doc_id) {
    return request(`${API_URL}/ui/status/${doc_id}`);
  },

  ingest(files, { source_uri = '', source = '' } = {}) {
    const fd = new FormData();
    files.forEach((f) => fd.append('files', f));
    if (source_uri) fd.append('source_uri', source_uri);
    if (source) fd.append('source', source);
    // Use pipeline endpoint so uploads are processed automatically (normalize→extract→chunk→embed)
    // Default async processing on server keeps UI responsive.
    fd.append('async_process', 'true');
    // Long timeout for ingestion
    return request(`${API_URL}/pipeline/ingest_index`, { method: 'POST', body: fd, timeout: 600000 });
  },

  ingestJob(files, { source_uri = '', source = '' } = {}) {
    const fd = new FormData();
    files.forEach((f) => fd.append('files', f));
    if (source_uri) fd.append('source_uri', source_uri);
    if (source) fd.append('source', source);
    return request(`${API_URL}/pipeline/ingest_job`, { method: 'POST', body: fd, timeout: 600000 });
  },

  jobStatus(job_id) {
    return request(`${API_URL}/jobs/${job_id}`);
  },

  search(q, { k = 8, filters = {} } = {}) {
    return request(`${API_URL}/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ q, k, hybrid: true, filters }),
    });
  },

  answer(q, { k = 8, filters = {} } = {}) {
    return request(`${API_URL}/answer`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ q, k, filters }),
      timeout: 60000, // 60s for LLM generation
    });
  },

  streamAnswer(q, { k = 8, filters = {} } = {}) {
    const ctrl = new AbortController();
    const body = JSON.stringify({ q, k, filters });
    const url = `${API_URL}/answer_stream`;
    async function* parse(stream) {
      const reader = stream.getReader();
      const decoder = new TextDecoder('utf-8');
      let buf = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const raw = buf.slice(0, idx).trim();
          buf = buf.slice(idx + 2);
          if (raw.startsWith('data:')) {
            const json = raw.slice(5).trim();
            try { yield JSON.parse(json); } catch (_) { /* ignore */ }
          }
        }
      }
    }
    const start = async () => {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: ctrl.signal,
      });
      if (!res.ok || !res.body) throw new Error('stream failed');
      return parse(res.body);
    };
    return { start, cancel: () => ctrl.abort() };
  },

  normalize(doc_ids) {
    if (Array.isArray(doc_ids)) {
      return request(`${API_URL}/normalize`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc_ids),
        timeout: 120000
      })
    }
    return request(`${API_URL}/normalize/${doc_ids}`, { method: 'POST', timeout: 60000 })
  },

  extract(doc_ids) {
    if (Array.isArray(doc_ids)) {
      return request(`${API_URL}/extract`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc_ids),
        timeout: 120000
      })
    }
    return request(`${API_URL}/extract/${doc_ids}`, { method: 'POST', timeout: 60000 })
  },

  chunk(doc_ids) {
    if (Array.isArray(doc_ids)) {
      return request(`${API_URL}/chunk`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(doc_ids),
        timeout: 120000
      })
    }
    return request(`${API_URL}/chunk/${doc_ids}`, { method: 'POST', timeout: 60000 })
  },

  embed(doc_id, plan_id) {
    return request(`${API_URL}/embed/${doc_id}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(plan_id ? { plan_id } : {}),
      timeout: 120000
    })
  },

  index(doc_id) {
    return request(`${API_URL}/index/${doc_id}`, { method: 'POST', timeout: 60000 })
  },

  clearAll() {
    return request(`${API_URL}/admin/reset`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confirm: true }),
    })
  },
};