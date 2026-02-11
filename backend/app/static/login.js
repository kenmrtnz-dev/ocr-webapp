async function safeParseJson(res) {
  try {
    return await res.json();
  } catch (_err) {
    return null;
  }
}

function redirectForRole(role) {
  if (role === 'agent') {
    window.location.href = '/agent';
    return;
  }
  if (role === 'credit_evaluator') {
    window.location.href = '/evaluator';
    return;
  }
  if (role === 'admin') {
    window.location.href = '/admin';
    return;
  }
  window.location.href = '/app';
}

// Login page is an explicit entry point: always allow role switching here.
localStorage.removeItem('auth_token');
localStorage.removeItem('auth_role');
localStorage.removeItem('auth_email');

const loginForm = document.getElementById('loginForm');
const loginEmail = document.getElementById('loginEmail');
const loginPassword = document.getElementById('loginPassword');
const loginPasswordToggle = document.getElementById('loginPasswordToggle');
const loginMessage = document.getElementById('loginMessage');
const loginSubmit = document.getElementById('loginSubmit');

function setLoginMessage(text, kind = 'neutral') {
  if (!loginMessage) return;
  loginMessage.textContent = text;
  loginMessage.classList.remove('is-error', 'is-info');
  if (kind === 'error') loginMessage.classList.add('is-error');
  if (kind === 'info') loginMessage.classList.add('is-info');
}

function setLoginSubmitting(isSubmitting) {
  if (!loginSubmit) return;
  loginSubmit.disabled = isSubmitting;
  loginSubmit.textContent = isSubmitting ? 'Signing in...' : 'Login';
}

if (loginPasswordToggle && loginPassword) {
  loginPasswordToggle.addEventListener('click', () => {
    const nextType = loginPassword.type === 'password' ? 'text' : 'password';
    loginPassword.type = nextType;
    loginPasswordToggle.textContent = nextType === 'password' ? 'Show' : 'Hide';
    loginPasswordToggle.setAttribute('aria-label', nextType === 'password' ? 'Show password' : 'Hide password');
  });
}

if (loginForm) {
  loginForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const email = (loginEmail && loginEmail.value || '').trim();
    const password = (loginPassword && loginPassword.value || '').trim();
    if (!email || !password) {
      setLoginMessage('Enter email and password.', 'error');
      return;
    }

    setLoginSubmitting(true);
    setLoginMessage('Signing in...', 'info');
    try {
      const res = await fetch('/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      const body = await safeParseJson(res);
      if (!res.ok) {
        setLoginMessage((body && body.detail) || 'Login failed.', 'error');
        return;
      }
      localStorage.setItem('auth_token', body.access_token || '');
      localStorage.setItem('auth_role', body.role || '');
      localStorage.setItem('auth_email', email);
      redirectForRole(body.role || '');
    } catch (_err) {
      setLoginMessage('Login request failed.', 'error');
    } finally {
      setLoginSubmitting(false);
    }
  });
}
