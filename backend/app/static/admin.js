const authUserBadge = document.getElementById('authUserBadge');
const authLogoutBtn = document.getElementById('authLogoutBtn');
const adminCreateUserForm = document.getElementById('adminCreateUserForm');
const adminUserEmail = document.getElementById('adminUserEmail');
const adminUserPassword = document.getElementById('adminUserPassword');
const adminUserRole = document.getElementById('adminUserRole');
const adminUsersTable = document.getElementById('adminUsersTable');
const adminClearDataBtn = document.getElementById('adminClearDataBtn');
const adminClearDataMsg = document.getElementById('adminClearDataMsg');

let authToken = localStorage.getItem('auth_token') || '';
let authRole = localStorage.getItem('auth_role') || '';
let authEmail = localStorage.getItem('auth_email') || '';
let currentUserId = '';

async function safeParseJson(res) {
  try {
    return await res.json();
  } catch (_err) {
    return null;
  }
}

async function fetchAuthed(url, opts = {}) {
  const headers = new Headers(opts.headers || {});
  if (authToken) headers.set('Authorization', `Bearer ${authToken}`);
  return fetch(url, { ...opts, headers });
}

function doLogout() {
  localStorage.removeItem('auth_token');
  localStorage.removeItem('auth_role');
  localStorage.removeItem('auth_email');
  window.location.href = '/login';
}

function formatDate(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '-';
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function roleLabel(role) {
  return String(role || '')
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderUsers(items) {
  if (!adminUsersTable) return;
  if (!items.length) {
    adminUsersTable.innerHTML = '<div class="table-empty">No users found</div>';
    return;
  }

  const rows = items.map((item) => {
    const activeLabel = item.is_active ? 'Active' : 'Inactive';
    const self = item.id === currentUserId;
    const nextActive = item.is_active ? 'false' : 'true';
    const actionLabel = item.is_active ? 'Deactivate' : 'Reactivate';
    const disableToggle = self;
    return `<tr>
      <td>${escapeHtml(formatDate(item.created_at))}</td>
      <td>${escapeHtml(item.email || '-')}</td>
      <td>${escapeHtml(roleLabel(item.role) || '-')}</td>
      <td>${escapeHtml(activeLabel)}</td>
      <td class="workflow-item-actions">
        <button class="preview-nav workflow-mini-btn" data-action="toggle-user-active" data-id="${escapeHtml(item.id)}" data-next-active="${nextActive}" ${disableToggle ? 'disabled' : ''}>${actionLabel}</button>
      </td>
    </tr>`;
  }).join('');

  adminUsersTable.innerHTML = `
    <div class="agent-submissions-table-inner">
      <table class="agent-submissions-grid admin-users-grid">
        <thead>
          <tr>
            <th>Created</th>
            <th>Email</th>
            <th>Role</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

async function loadUsers() {
  const res = await fetchAuthed('/admin/users');
  const body = await safeParseJson(res);
  if (!res.ok) {
    if (res.status === 401 || res.status === 403) {
      doLogout();
      return;
    }
    if (adminUsersTable) {
      adminUsersTable.innerHTML = `<div class="table-empty">${escapeHtml((body && body.detail) || 'Failed to load users')}</div>`;
    }
    return;
  }
  renderUsers((body && body.items) || []);
}

async function createUser() {
  const email = (adminUserEmail && adminUserEmail.value || '').trim();
  const password = (adminUserPassword && adminUserPassword.value || '').trim();
  const role = (adminUserRole && adminUserRole.value || '').trim();
  if (!email || !password || !role) return;

  const res = await fetchAuthed('/admin/users', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password, role }),
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to create user');
    return;
  }
  if (adminCreateUserForm) adminCreateUserForm.reset();
  await loadUsers();
}

async function setUserActive(userId, isActive) {
  const res = await fetchAuthed(`/admin/users/${userId}/active`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ is_active: Boolean(isActive) }),
  });
  const body = await safeParseJson(res);
  if (!res.ok) {
    alert((body && body.detail) || 'Failed to update user status');
    return;
  }
  await loadUsers();
}

async function clearSubmittedData() {
  const ok = window.confirm('Clear all submitted files data? This cannot be undone.');
  if (!ok) return;
  if (!adminClearDataBtn) return;

  adminClearDataBtn.disabled = true;
  if (adminClearDataMsg) adminClearDataMsg.textContent = 'Clearing data...';
  try {
    const res = await fetchAuthed('/admin/clear-submissions', { method: 'POST' });
    const body = await safeParseJson(res);
    if (!res.ok) {
      throw new Error((body && body.detail) || 'Failed to clear submitted data');
    }
    const cleared = body && body.cleared ? body.cleared : {};
    if (adminClearDataMsg) {
      adminClearDataMsg.textContent = `Cleared submissions: ${cleared.submissions || 0}, jobs: ${cleared.jobs || 0}, transactions: ${cleared.transactions || 0}.`;
    }
  } catch (err) {
    if (adminClearDataMsg) adminClearDataMsg.textContent = err.message || 'Failed to clear submitted data';
  } finally {
    adminClearDataBtn.disabled = false;
  }
}

async function bootstrap() {
  if (!authToken || !authRole) {
    window.location.href = '/login';
    return;
  }
  if (authRole !== 'admin') {
    if (authRole === 'agent') {
      window.location.href = '/agent';
      return;
    }
    if (authRole === 'credit_evaluator') {
      window.location.href = '/evaluator';
      return;
    }
    window.location.href = '/login';
    return;
  }

  const meRes = await fetchAuthed('/auth/me');
  const meBody = await safeParseJson(meRes);
  if (!meRes.ok || !meBody || meBody.role !== 'admin') {
    doLogout();
    return;
  }
  currentUserId = meBody.id || '';
  authEmail = meBody.email || authEmail;
  if (authUserBadge) authUserBadge.textContent = `${authEmail} (Admin)`;
  if (authLogoutBtn) authLogoutBtn.style.display = '';
  await loadUsers();
}

if (authLogoutBtn) {
  authLogoutBtn.addEventListener('click', doLogout);
}
if (adminCreateUserForm) {
  adminCreateUserForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    await createUser();
  });
}
if (adminUsersTable) {
  adminUsersTable.addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-action="toggle-user-active"]');
    if (!btn) return;
    const id = btn.dataset.id || '';
    const nextActive = String(btn.dataset.nextActive || '').toLowerCase() === 'true';
    if (!id) return;
    await setUserActive(id, nextActive);
  });
}
if (adminClearDataBtn) {
  adminClearDataBtn.addEventListener('click', clearSubmittedData);
}

bootstrap();
