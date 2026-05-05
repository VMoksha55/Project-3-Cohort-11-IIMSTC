/**
 * SmartBIZ RAG Dashboard — Single Page Application
 * Enterprise AI-powered Business Intelligence
 */

// ============================================
// CONFIG & STATE
// ============================================
const API_BASE = window.location.origin + '/api';
const APP = {
    token: localStorage.getItem('smartbiz_token'),
    user: JSON.parse(localStorage.getItem('smartbiz_user') || 'null'),
    currentPage: 'analytics',
    sidebarOpen: false,
    chartInstance: null,
    activeJob: null, // { jobId, fileId, progress, message }
    currentFilename: null
};

// ============================================
// ROUTER
// ============================================
function navigate(page) {
    APP.currentPage = page;
    APP.sidebarOpen = false;
    render();
}

function render() {
    const app = document.getElementById('app');
    if (!APP.token || !APP.user) {
        // Check hash for register
        if (window.location.hash === '#register') {
            app.innerHTML = renderRegisterPage();
            bindRegisterEvents();
        } else {
            app.innerHTML = renderLoginPage();
            bindLoginEvents();
        }
    } else {
        app.innerHTML = renderAppLayout();
        bindAppEvents();
        loadPageContent();
    }
}

// ============================================
// API HELPERS
// ============================================
async function apiFetch(endpoint, options = {}) {
    const headers = { 'Content-Type': 'application/json' };
    if (APP.token) headers['Authorization'] = `Bearer ${APP.token}`;

    try {
        const resp = await fetch(`${API_BASE}${endpoint}`, {
            ...options,
            headers: { ...headers, ...options.headers }
        });

        if (resp.status === 401) {
            logout();
            return { error: 'Session expired. Please login again.' };
        }

        const data = await resp.json();
        if (!resp.ok) return { error: data.error || 'Request failed' };
        return data;
    } catch (err) {
        return { error: 'Network error. Please check your connection.' };
    }
}

async function apiUpload(endpoint, formData) {
    const headers = {};
    if (APP.token) headers['Authorization'] = `Bearer ${APP.token}`;

    try {
        const resp = await fetch(`${API_BASE}${endpoint}`, {
            method: 'POST',
            headers,
            body: formData
        });
        const data = await resp.json();
        if (!resp.ok) return { error: data.error || 'Upload failed' };
        return data;
    } catch (err) {
        return { error: 'Upload failed. Check your connection.' };
    }
}

function logout() {
    APP.token = null;
    APP.user = null;
    localStorage.removeItem('smartbiz_token');
    localStorage.removeItem('smartbiz_user');
    render();
}

function setAuth(token, user) {
    APP.token = token;
    APP.user = user;
    localStorage.setItem('smartbiz_token', token);
    localStorage.setItem('smartbiz_user', JSON.stringify(user));
}

// ============================================
// TOAST NOTIFICATIONS
// ============================================
function showToast(message, type = 'info') {
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }

    const icons = { success: 'check_circle', error: 'error', info: 'info' };
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `<span class="material-icons-outlined" style="font-size:18px">${icons[type]}</span>${message}`;
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(60px)';
        toast.style.transition = 'all 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// ============================================
// FORMAT HELPERS
// ============================================
function formatCurrency(n) {
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
    return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function formatNumber(n) {
    return Number(n).toLocaleString('en-US');
}

function timeAgo(isoStr) {
    const d = new Date(isoStr);
    const now = new Date();
    const s = Math.floor((now - d) / 1000);
    if (s < 60) return 'Just now';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    if (s < 86400) return Math.floor(s / 3600) + 'h ago';
    return Math.floor(s / 86400) + 'd ago';
}

function userInitials() {
    if (!APP.user || !APP.user.name) return '?';
    return APP.user.name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
}

// ============================================
// LOGIN PAGE
// ============================================
function renderLoginPage() {
    return `
    <div class="auth-page">
        <div class="auth-brand">
            <div class="auth-brand-content">
                <div class="auth-logo">
                    <div class="auth-logo-icon"><span class="material-icons-outlined">hub</span></div>
                    <span class="auth-logo-text">SmartBIZ</span>
                </div>
                <p class="auth-tagline">AI-powered enterprise intelligence.<br>Transform raw data into actionable insights.</p>
                <ul class="auth-features">
                    <li><span class="material-icons-outlined">auto_awesome</span> RAG-powered AI analytics on your data</li>
                    <li><span class="material-icons-outlined">query_stats</span> Real-time revenue & trend forecasting</li>
                    <li><span class="material-icons-outlined">shield</span> Enterprise-grade security & privacy</li>
                    <li><span class="material-icons-outlined">cloud_upload</span> CSV upload with instant processing</li>
                </ul>
            </div>
        </div>
        <div class="auth-form-panel">
            <h1 class="auth-form-title">SmartBIZ</h1>
            <p class="auth-form-subtitle">Sign in to your enterprise dashboard</p>
            <div class="auth-error" id="login-error">
                <span class="material-icons-outlined" style="font-size:16px">error</span>
                <span id="login-error-text"></span>
            </div>
            <form class="auth-form" id="login-form">
                <div class="input-group">
                    <label for="login-email">Email address</label>
                    <input type="email" id="login-email" class="input-field" placeholder="name@company.com" required autocomplete="email">
                </div>
                <div class="input-group">
                    <label for="login-password">Password</label>
                    <input type="password" id="login-password" class="input-field" placeholder="Enter your password" required autocomplete="current-password">
                </div>
                <div class="auth-extras">
                    <label class="auth-checkbox">
                        <input type="checkbox" checked> Remember me
                    </label>
                    <a href="#">Forgot password?</a>
                </div>
                <button type="submit" class="btn btn-primary btn-block btn-lg" id="login-btn">
                    Sign In
                </button>
            </form>
            <p class="auth-switch">Don't have an account? <a href="#register" id="goto-register">Create an account</a></p>
        </div>
    </div>`;
}

function bindLoginEvents() {
    const form = document.getElementById('login-form');
    const errBox = document.getElementById('login-error');
    const errText = document.getElementById('login-error-text');
    const btn = document.getElementById('login-btn');

    form?.addEventListener('submit', async (e) => {
        e.preventDefault();
        errBox?.classList.remove('visible');
        btn.disabled = true;
        btn.textContent = 'Signing in...';

        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;

        const data = await apiFetch('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ email, password })
        });

        if (data.error) {
            if (errText) errText.textContent = data.error;
            errBox?.classList.add('visible');
            btn.disabled = false;
            btn.textContent = 'Sign In';
            return;
        }

        setAuth(data.token, data.user);
        navigate('analytics');
    });

    document.getElementById('goto-register')?.addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#register';
        render();
    });
}

// ============================================
// REGISTER PAGE
// ============================================
function renderRegisterPage() {
    return `
    <div class="auth-page">
        <div class="auth-brand">
            <div class="auth-brand-content">
                <div class="auth-logo">
                    <div class="auth-logo-icon"><span class="material-icons-outlined">hub</span></div>
                    <span class="auth-logo-text">SmartBIZ</span>
                </div>
                <p class="auth-tagline">Join the future of business intelligence.<br>Start making data-driven decisions today.</p>
                <ul class="auth-features">
                    <li><span class="material-icons-outlined">rocket_launch</span> Get started in under 60 seconds</li>
                    <li><span class="material-icons-outlined">psychology</span> AI learns your business patterns</li>
                    <li><span class="material-icons-outlined">trending_up</span> Predictive analytics & forecasting</li>
                    <li><span class="material-icons-outlined">group</span> Built for teams of all sizes</li>
                </ul>
            </div>
        </div>
        <div class="auth-form-panel">
            <h1 class="auth-form-title">Create an account</h1>
            <p class="auth-form-subtitle">Enter your details to get started with enterprise BI.</p>
            <div class="auth-error" id="register-error">
                <span class="material-icons-outlined" style="font-size:16px">error</span>
                <span id="register-error-text"></span>
            </div>
            <form class="auth-form" id="register-form">
                <div class="input-group">
                    <label for="reg-name">Full name</label>
                    <input type="text" id="reg-name" class="input-field" placeholder="John Doe" required autocomplete="name">
                </div>
                <div class="input-group">
                    <label for="reg-email">Email address</label>
                    <input type="email" id="reg-email" class="input-field" placeholder="name@company.com" required autocomplete="email">
                </div>
                <div class="input-group">
                    <label for="reg-password">Password</label>
                    <input type="password" id="reg-password" class="input-field" placeholder="Min. 6 characters" required minlength="6" autocomplete="new-password">
                </div>
                <label class="auth-checkbox">
                    <input type="checkbox" id="reg-terms" required> I agree to the <a href="#" style="margin-left:4px">Terms of Service</a>&nbsp;and&nbsp;<a href="#">Privacy Policy</a>
                </label>
                <button type="submit" class="btn btn-primary btn-block btn-lg" id="register-btn">
                    Create Account
                </button>
            </form>
            <p class="auth-switch">Already have an account? <a href="#" id="goto-login">Sign in</a></p>
        </div>
    </div>`;
}

function bindRegisterEvents() {
    const form = document.getElementById('register-form');
    const errBox = document.getElementById('register-error');
    const errText = document.getElementById('register-error-text');
    const btn = document.getElementById('register-btn');

    form?.addEventListener('submit', async (e) => {
        e.preventDefault();
        errBox?.classList.remove('visible');
        btn.disabled = true;
        btn.textContent = 'Creating account...';

        const name = document.getElementById('reg-name').value;
        const email = document.getElementById('reg-email').value;
        const password = document.getElementById('reg-password').value;

        const data = await apiFetch('/auth/register', {
            method: 'POST',
            body: JSON.stringify({ name, email, password })
        });

        if (data.error) {
            if (errText) errText.textContent = data.error;
            errBox?.classList.add('visible');
            btn.disabled = false;
            btn.textContent = 'Create Account';
            return;
        }

        setAuth(data.token, data.user);
        showToast('Account created successfully!', 'success');
        navigate('analytics');
    });

    document.getElementById('goto-login')?.addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '';
        render();
    });
}

// ============================================
// APP LAYOUT (Sidebar + Main)
// ============================================
function renderAppLayout() {
    const navItems = [
        { id: 'analytics', icon: 'query_stats', label: 'Dashboard' },
        { id: 'history', icon: 'history', label: 'History' },
    ];

    const navHTML = navItems.map(n => `
        <button class="nav-item ${APP.currentPage === n.id ? 'active' : ''}" data-page="${n.id}" id="nav-${n.id}">
            <span class="material-icons-outlined">${n.icon}</span>
            ${n.label}
        </button>
    `).join('');

    const mobileNavHTML = navItems.map(n => `
        <button class="mobile-nav-item ${APP.currentPage === n.id ? 'active' : ''}" data-page="${n.id}">
            <span class="material-icons-outlined">${n.icon}</span>
            ${n.label}
        </button>
    `).join('');

    return `
    <button class="mobile-hamburger" id="hamburger-btn">
        <span class="material-icons-outlined">menu</span>
    </button>
    <div class="sidebar-overlay" id="sidebar-overlay"></div>

    <div class="app-layout">
        <aside class="sidebar" id="sidebar">
            <div class="sidebar-header">
                <div class="sidebar-logo">
                    <div class="sidebar-logo-icon"><span class="material-icons-outlined">hub</span></div>
                    <div>
                        <div class="sidebar-logo-name">SmartBIZ</div>
                        <span class="sidebar-logo-badge">AI BI</span>
                    </div>
                </div>
            </div>
            <nav class="sidebar-nav">
                ${navHTML}
                <div class="nav-spacer"></div>
                <button class="nav-item" data-page="account" id="nav-account">
                    <span class="material-icons-outlined">account_circle</span>
                    Account
                </button>
                <button class="nav-item" id="nav-logout">
                    <span class="material-icons-outlined">logout</span>
                    Logout
                </button>
            </nav>
            <div class="sidebar-footer">
                <div class="user-info">
                    <div class="user-avatar">${userInitials()}</div>
                    <div>
                        <div class="user-name">${APP.user?.name || 'User'}</div>
                        <div class="user-email">${APP.user?.email || ''}</div>
                    </div>
                </div>
            </div>
        </aside>

        <main class="main-content" id="main-content">
            <div id="page-container" class="page-enter"></div>
        </main>
    </div>

    <nav class="mobile-bottom-nav">
        <div class="mobile-nav-items">
            ${mobileNavHTML}
        </div>
    </nav>`;
}

function bindAppEvents() {
    document.querySelectorAll('.nav-item[data-page]').forEach(btn => {
        btn.addEventListener('click', () => navigate(btn.dataset.page));
    });

    document.querySelectorAll('.mobile-nav-item[data-page]').forEach(btn => {
        btn.addEventListener('click', () => navigate(btn.dataset.page));
    });

    document.getElementById('nav-logout')?.addEventListener('click', () => {
        logout();
        showToast('Logged out successfully', 'info');
    });

    const hamburger = document.getElementById('hamburger-btn');
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('sidebar-overlay');

    hamburger?.addEventListener('click', () => {
        sidebar?.classList.toggle('open');
        overlay?.classList.toggle('open');
    });

    overlay?.addEventListener('click', () => {
        sidebar?.classList.remove('open');
        overlay?.classList.remove('open');
    });
}

// ============================================
// PAGE CONTENT LOADER
// ============================================
function loadPageContent() {
    const container = document.getElementById('page-container');
    if (!container) return;

    container.innerHTML = '';
    container.className = 'page-container page-enter';

    if (APP.currentPage === 'analytics') {
        renderAnalyticsPage(container);
    } else if (APP.currentPage === 'history') {
        renderHistoryPage(container);
    } else if (APP.currentPage === 'account') {
        renderAccountPage(container);
    } else {
        renderAnalyticsPage(container);
    }
}

// ============================================
// ANALYTICS PAGE — Full MCP Pipeline UI
// ============================================
function renderAnalyticsPage(container) {
    container.innerHTML = `
    <div class="page-header">
        <div class="page-header-row">
            <div>
                <h1 class="page-title">Dashboard</h1>
                <p class="page-subtitle">Upload a CSV → MCP cleans &amp; analyzes → AI generates charts &amp; insights.</p>
            </div>
            <span class="chip chip-info">
                <span class="material-icons-outlined" style="font-size:14px">auto_awesome</span>
                MCP + RAG + LLM
            </span>
        </div>
    </div>
    <div class="page-body">
        <div class="pipeline-steps">
            <div class="pipeline-step active"><span class="step-num">1</span><span>Upload CSV</span></div>
            <div class="pipeline-arrow">→</div>
            <div class="pipeline-step" id="ps-clean"><span class="step-num">2</span><span>MCP Clean</span></div>
            <div class="pipeline-arrow">→</div>
            <div class="pipeline-step" id="ps-analyze"><span class="step-num">3</span><span>MCP Analyze</span></div>
            <div class="pipeline-arrow">→</div>
            <div class="pipeline-step" id="ps-viz"><span class="step-num">4</span><span>Visualize</span></div>
            <div class="pipeline-arrow">→</div>
            <div class="pipeline-step" id="ps-llm"><span class="step-num">5</span><span>RAG + LLM</span></div>
        </div>

        <div class="card" id="analytics-upload-card">
            <div class="upload-zone" id="analytics-drop-zone">
                <span class="material-icons-outlined upload-icon">upload_file</span>
                <div class="upload-title">Drop your CSV here</div>
                <div class="upload-subtitle">Sales, reviews, orders — any CSV file works</div>
                <button class="btn btn-primary" id="analytics-browse-btn">
                    <span class="material-icons-outlined" style="font-size:18px">folder_open</span>
                    Browse CSV
                </button>
                <input type="file" id="analytics-file-input" accept=".csv" style="display:none">
            </div>
            <div id="pipeline-progress-wrapper" style="display:none; margin-top:20px">
                <div class="pipeline-progress-bar-bg">
                    <div class="pipeline-progress-bar-fill" id="pipeline-progress-fill" style="width:0%"></div>
                </div>
                <div id="pipeline-progress-msg" class="pipeline-progress-msg">Starting...</div>
            </div>
        </div>

        <div id="analytics-results" style="display:none">
            <div class="card mt-md" id="clean-report-card">
                <div class="card-header">
                    <div>
                        <div class="card-title">🔧 MCP: Data Cleaning Report</div>
                        <div class="card-subtitle">Issues detected &amp; fixed automatically</div>
                    </div>
                    <span class="chip chip-success">Complete</span>
                </div>
                <div id="clean-report-body"></div>
            </div>

            <div class="kpi-grid mt-md" id="analytics-kpi-grid"></div>

            <div class="card mt-md">
                <div class="card-header">
                    <div>
                        <div class="card-title">📈 MCP: Visualizations</div>
                        <div class="card-subtitle">Auto-generated from your data</div>
                    </div>
                    <span id="chart-count-badge" class="chip chip-info"></span>
                </div>
                <div class="analytics-charts-grid" id="analytics-charts-grid"></div>
            </div>

            <div class="card mt-md">
                <div class="card-header">
                    <div>
                        <div class="card-title">✨ RAG + LLM: Business Insights</div>
                        <div class="card-subtitle">AI-generated from embedded data context</div>
                    </div>
                    <span class="material-icons-outlined text-primary" style="font-size:22px">psychology</span>
                </div>
                <div class="insights-panel" id="analytics-insights-panel"></div>
            </div>

            <div class="card mt-md">
                <div class="card-header">
                    <div>
                        <div class="card-title">💬 Ask SmartBIZ AI</div>
                        <div class="card-subtitle" id="chat-subtitle">Query your data with natural language</div>
                    </div>
                </div>
                <div class="chat-messages" id="analytics-messages" style="min-height:120px;max-height:400px">
                    <div class="chat-msg ai">Data loaded! Ask me anything about your CSV — trends, top products, anomalies…</div>
                </div>
                <div class="chat-input-row">
                    <input type="text" class="input-field" id="analytics-chat-input" placeholder="e.g. What drives our highest revenue?">
                    <button class="btn btn-primary" id="analytics-chat-send">
                        <span class="material-icons-outlined" style="font-size:18px">send</span>
                    </button>
                </div>
            </div>
        </div>

        <div class="card mt-xl" id="analytics-recent-uploads">
            <div class="card-header">
                <div>
                    <div class="card-title">Recent Data Sources</div>
                    <div class="card-subtitle">Previously ingested files available for RAG search</div>
                </div>
                <span class="material-icons-outlined text-muted" style="font-size:20px">storage</span>
            </div>
            <div id="uploads-list">
                <div class="skeleton skeleton-text" style="height:40px;margin-top:8px"></div>
                <div class="skeleton skeleton-text" style="height:40px;margin-top:8px"></div>
            </div>
        </div>
    </div>
    `;

    bindAnalyticsUploadEvents();
    if (APP.activeJob) {
        document.getElementById('pipeline-progress-wrapper').style.display = 'block';
        setPipelineProgress(APP.activeJob.progress, APP.activeJob.message);
    }
    loadUploads();
}

function bindAnalyticsUploadEvents() {
    const zone = document.getElementById('analytics-drop-zone');
    const input = document.getElementById('analytics-file-input');
    const browseBtn = document.getElementById('analytics-browse-btn');

    browseBtn?.addEventListener('click', (e) => { e.stopPropagation(); input.click(); });
    zone?.addEventListener('click', () => input.click());

    zone?.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('dragover'); });
    zone?.addEventListener('dragleave', () => zone.classList.remove('dragover'));
    zone?.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('dragover');
        const f = e.dataTransfer.files[0];
        if (f) startPipeline(f);
    });

    input?.addEventListener('change', () => {
        if (input.files[0]) startPipeline(input.files[0]);
    });

    const chatInput = document.getElementById('analytics-chat-input');
    const chatSend = document.getElementById('analytics-chat-send');
    const sendChat = () => {
        const q = chatInput?.value.trim();
        if (q) { sendAnalyticsMessage(q); chatInput.value = ''; }
    };
    chatSend?.addEventListener('click', sendChat);
    chatInput?.addEventListener('keydown', (e) => { if (e.key === 'Enter') sendChat(); });
}

async function startPipeline(file) {
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showToast('Only CSV files are supported.', 'error'); return;
    }
    
    APP.currentFilename = file.name;

    document.getElementById('pipeline-progress-wrapper').style.display = 'block';
    document.getElementById('analytics-results').style.display = 'none';
    setPipelineProgress(2, '⏳ Uploading CSV...');

    const formData = new FormData();
    formData.append('file', file);

    const uploadData = await apiUpload('/upload', formData);
    if (uploadData.error) {
        showToast(uploadData.error, 'error');
        setPipelineProgress(0, '❌ Upload failed');
        return;
    }
    
    if (uploadData.file_name) {
        APP.currentFilename = uploadData.file_name;
    }

    const fileId = uploadData.file_id;
    loadUploads();

    setPipelineProgress(5, '⏳ Starting analysis...');
    const startData = await apiFetch('/start-analysis', {
        method: 'POST',
        body: JSON.stringify({ file_id: fileId })
    });

    if (startData.error) {
        showToast(startData.error, 'error');
        setPipelineProgress(0, '❌ Failed to start analysis');
        return;
    }

    pollPipeline(startData.job_id, fileId);
}

function setPipelineProgress(pct, msg) {
    const fill = document.getElementById('pipeline-progress-fill');
    const msgEl = document.getElementById('pipeline-progress-msg');
    if (fill) fill.style.width = pct + '%';
    if (msgEl) msgEl.textContent = msg;

    if (pct >= 20) document.getElementById('ps-clean')?.classList.add('active');
    if (pct >= 50) document.getElementById('ps-analyze')?.classList.add('active');
    if (pct >= 75) document.getElementById('ps-viz')?.classList.add('active');
    if (pct >= 90) document.getElementById('ps-llm')?.classList.add('active');
}

async function pollPipeline(jobId, fileId) {
    APP.activeJob = { jobId, fileId, progress: 0, message: 'Starting...' };

    const interval = setInterval(async () => {
        const data = await apiFetch(`/job/${jobId}`);
        if (data.error && !data.status) {
            clearInterval(interval);
            APP.activeJob = null;
            showToast('Failed to poll job.', 'error'); return;
        }

        const progress = data.progress || 0;
        const message = data.message || '';
        if (APP.activeJob) {
            APP.activeJob.progress = progress;
            APP.activeJob.message = message;
        }

        if (APP.currentPage === 'analytics') {
            setPipelineProgress(progress, message);
        }

        if (data.status === 'done') {
            clearInterval(interval);
            APP.activeJob = null;
            showToast('Pipeline complete! Fetching results...', 'success');
            const reportData = await apiFetch(`/report/${fileId}`);
            if (reportData.error) {
                showToast('Failed to fetch report.', 'error');
            } else if (APP.currentPage === 'analytics') {
                renderPipelineResults(reportData);
            }
            loadUploads();
        } else if (data.status === 'error' || data.status === 'failed') {
            clearInterval(interval);
            APP.activeJob = null;
            if (APP.currentPage === 'analytics') {
                setPipelineProgress(0, '❌ ' + (data.error || 'Pipeline error'));
            }
            showToast('Pipeline failed: ' + (data.error?.slice(0, 80) || ''), 'error');
        }
    }, 1500);
}

function renderPipelineResults(result) {
    const resultsArea = document.getElementById('analytics-results');
    if (resultsArea) resultsArea.style.display = 'block';

    const chatSubtitle = document.getElementById('chat-subtitle');
    if (chatSubtitle && APP.currentFilename) {
        chatSubtitle.innerHTML = `Chatting with: <b style="color:var(--primary)">${escapeHtml(APP.currentFilename)}</b>`;
    }

    const cr = result.clean_report || {};
    const cleanBody = document.getElementById('clean-report-body');
    if (cleanBody) {
        const steps = (cr.steps || []).map(s => {
            const detail = Object.entries(s)
                .filter(([k]) => k !== 'step')
                .map(([k, v]) => `<span class="stat-pill">${k}: <b>${JSON.stringify(v)}</b></span>`)
                .join(' ');
            return `<div class="clean-step">
                <span class="material-icons-outlined" style="font-size:16px;color:var(--secondary)">check</span>
                <strong>${s.step.replace(/_/g,' ')}</strong> ${detail}
            </div>`;
        }).join('');
        cleanBody.innerHTML = `
            <div class="clean-summary">
                <span class="stat-pill">Original: <b>${cr.original_shape?.rows} rows × ${cr.original_shape?.cols} cols</b></span>
                <span class="stat-pill">Clean: <b>${cr.clean_shape?.rows} rows × ${cr.clean_shape?.cols} cols</b></span>
                <span class="stat-pill">Issues fixed: <b>${cr.issues_fixed}</b></span>
            </div>
            <div class="clean-steps">${steps}</div>
        `;
    }

    const kpis = result.analysis?.kpis || {};
    const kpiGrid = document.getElementById('analytics-kpi-grid');
    if (kpiGrid && kpis.total !== undefined) {
        const growth = (kpis.growth_rate_pct !== undefined && kpis.growth_rate_pct !== null)
            ? `<span class="kpi-change ${kpis.growth_rate_pct >= 0 ? 'positive':'negative'}">
               <span class="material-icons-outlined" style="font-size:14px">${kpis.growth_rate_pct >= 0 ? 'trending_up':'trending_down'}</span>
               ${kpis.growth_rate_pct}%</span>` : '';
        kpiGrid.innerHTML = `
            <div class="kpi-card">
                <div class="kpi-label"><span class="material-icons-outlined">payments</span> Total Revenue</div>
                <div class="kpi-value">${formatCurrency(kpis.total || 0)}</div>
                ${growth}
            </div>
            <div class="kpi-card">
                <div class="kpi-label"><span class="material-icons-outlined">receipt_long</span> Total Records</div>
                <div class="kpi-value">${formatNumber(kpis.total_records || 0)}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label"><span class="material-icons-outlined">signal_cellular_alt</span> Avg Transaction</div>
                <div class="kpi-value">${formatCurrency(kpis.mean || 0)}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label"><span class="material-icons-outlined">workspace_premium</span> Peak Value</div>
                <div class="kpi-value">${formatCurrency(kpis.max || 0)}</div>
            </div>
        `;
    }

    const chartsGrid = document.getElementById('analytics-charts-grid');
    const charts = (result.charts || []).filter(c => c.image);
    const badge = document.getElementById('chart-count-badge');
    if (badge) badge.textContent = `${charts.length} charts`;
    if (chartsGrid) {
        chartsGrid.innerHTML = charts.map(c => `
            <div class="analytics-chart-item">
                <div class="analytics-chart-title">${escapeHtml(c.title)}</div>
                <img src="${c.image}" alt="${escapeHtml(c.title)}" class="analytics-chart-img" loading="lazy">
            </div>
        `).join('') || '<p class="text-muted body-md">No charts generated.</p>';
    }

    const insightsPanel = document.getElementById('analytics-insights-panel');
    const insights = result.insights || [];
    const typeIcons = { risk: 'warning', opportunity: 'lightbulb', trend: 'insights' };
    if (insightsPanel) {
        insightsPanel.innerHTML = insights.map(i => `
            <div class="insight-item type-${i.type || 'trend'}">
                <span class="insight-icon material-icons-outlined">${typeIcons[i.type] || 'auto_awesome'}</span>
                <div class="insight-title">${escapeHtml(i.title)}</div>
                <div class="insight-body">${escapeHtml(i.body)}</div>
            </div>
        `).join('') || '<p class="text-muted">No insights generated.</p>';
    }

    resultsArea?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function sendAnalyticsMessage(question) {
    const messages = document.getElementById('analytics-messages');
    if (!messages) return;

    messages.innerHTML += `<div class="chat-msg user">${escapeHtml(question)}</div>`;
    const lid = 'alid-' + Date.now();
    messages.innerHTML += `<div class="chat-msg ai" id="${lid}"><div class="spinner"></div> Searching your data...</div>`;
    messages.scrollTop = messages.scrollHeight;

    const bodyPayload = { question };
    if (APP.currentFilename) {
        bodyPayload.filename = APP.currentFilename;
    }

    const data = await apiFetch('/chat', { method: 'POST', body: JSON.stringify(bodyPayload) });
    const el = document.getElementById(lid);
    if (el) {
        const answer = data.error || data.answer || 'Unable to process.';
        const sources = data.sources?.length
            ? `<div class="sources">📄 Sources: ${data.sources.join(', ')}</div>` : '';
        el.innerHTML = `${escapeHtml(answer)}${sources}`;
    }
    messages.scrollTop = messages.scrollHeight;
}

async function loadUploads() {
    const list = document.getElementById('uploads-list');
    if (!list) return;

    const data = await apiFetch('/uploads');
    const files = data.files || [];

    if (files.length === 0) {
        list.innerHTML = '<p class="text-muted body-md" style="padding:16px 0">No files uploaded yet. Upload a CSV to get started.</p>';
        return;
    }

    list.innerHTML = `
        <table class="uploads-table">
            <thead>
                <tr>
                    <th>Filename</th>
                    <th>Size</th>
                    <th>Uploaded</th>
                    <th>Status</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                ${files.map(f => `
                    <tr>
                        <td><span class="file-icon"><span class="material-icons-outlined">description</span>${escapeHtml(f.filename)}</span></td>
                        <td>${f.size}</td>
                        <td>${timeAgo(f.uploaded_at)}</td>
                        <td><span class="chip chip-success">Ingested</span></td>
                        <td>
                            <div style="display:flex; gap:8px">
                                <button class="btn btn-primary btn-sm btn-load-report" data-id="${f.file_id}" data-filename="${escapeHtml(f.filename)}" style="padding: 4px 12px; font-size: 12px">
                                    View Analysis
                                </button>
                                <button class="btn btn-outline btn-sm btn-delete-upload" data-id="${f.file_id}" style="padding: 4px 8px; font-size: 12px; border-color: #fee2e2; color: #ef4444">
                                    <span class="material-icons-outlined" style="font-size:16px">delete</span>
                                </button>
                            </div>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;

    list.querySelectorAll('.btn-load-report').forEach(btn => {
        btn.addEventListener('click', async () => {
            const fid = btn.getAttribute('data-id');
            const fname = btn.getAttribute('data-filename');
            
            // Temporary hack to decode escaped HTML quotes if any, although escapeHtml handles tags
            APP.currentFilename = fname;
            
            showToast('Fetching analysis results...', 'info');
            const report = await apiFetch(`/report/${fid}`);
            if (report.error) {
                showToast('Report not found. You may need to run analysis first.', 'warning');
            } else {
                const chatSubtitle = document.getElementById('chat-subtitle');
                if (chatSubtitle) {
                    chatSubtitle.innerHTML = `Chatting with: <b style="color:var(--primary)">${escapeHtml(fname)}</b>`;
                }
                renderPipelineResults(report);
            }
        });
    });

    list.querySelectorAll('.btn-delete-upload').forEach(btn => {
        btn.addEventListener('click', async () => {
            const fid = btn.getAttribute('data-id');
            if (!confirm('Are you sure you want to delete this data source and its analysis?')) return;
            showToast('Deleting...', 'info');
            const res = await apiFetch(`/upload/${fid}`, { method: 'DELETE' });
            if (!res.error) {
                showToast('Deleted successfully', 'success');
                loadUploads();
            }
        });
    });
}

// ============================================
// HISTORY PAGE
// ============================================
function renderHistoryPage(container) {
    container.innerHTML = `
        <div class="page-header">
            <div class="page-header-row">
                <div>
                    <h1 class="page-title">History</h1>
                    <p class="page-subtitle">View your data ingestion history and system activity.</p>
                </div>
            </div>
        </div>
        <div class="page-body">
            <div class="card">
                <div class="card-header">
                    <div>
                        <div class="card-title">Ingested Data Sources</div>
                        <div class="card-subtitle">All files processed through the RAG pipeline</div>
                    </div>
                </div>
                <div id="history-list">
                    <div class="skeleton skeleton-text" style="height:40px;margin-top:8px"></div>
                </div>
            </div>
        </div>
    `;
    loadHistoryData();
}

async function loadHistoryData() {
    const list = document.getElementById('history-list');
    if (!list) return;
    const data = await apiFetch('/uploads');
    const files = data.files || [];
    if (files.length === 0) {
        list.innerHTML = '<p class="text-muted body-md" style="padding:16px 0">No history yet.</p>';
        return;
    }
    list.innerHTML = `
        <table class="uploads-table">
            <thead><tr><th>File</th><th>Size</th><th>Processed</th><th>Status</th></tr></thead>
            <tbody>
                ${files.map(f => `
                    <tr>
                        <td><span class="file-icon"><span class="material-icons-outlined">description</span>${escapeHtml(f.filename)}</span></td>
                        <td>${f.size}</td>
                        <td>${timeAgo(f.uploaded_at)}</td>
                        <td><span class="chip chip-success">Complete</span></td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
}

// ============================================
// ACCOUNT PAGE
// ============================================
function renderAccountPage(container) {
    container.innerHTML = `
        <div class="page-header">
            <div class="page-header-row">
                <div>
                    <h1 class="page-title">Account</h1>
                    <p class="page-subtitle">Manage your profile and preferences.</p>
                </div>
            </div>
        </div>
        <div class="page-body">
            <div class="card" style="max-width:600px">
                <div class="card-header">
                    <div class="card-title">Profile Information</div>
                </div>
                <div style="display:flex;align-items:center;gap:20px;margin-bottom:24px">
                    <div class="user-avatar" style="width:64px;height:64px;font-size:24px">${userInitials()}</div>
                    <div>
                        <div style="font-size:20px;font-weight:600">${escapeHtml(APP.user?.name || 'User')}</div>
                        <div class="text-muted">${escapeHtml(APP.user?.email || '')}</div>
                    </div>
                </div>
                <div style="margin-top:24px;padding-top:24px;border-top:1px solid var(--outline-variant)">
                    <button class="btn btn-secondary" id="account-logout">
                        <span class="material-icons-outlined" style="font-size:18px">logout</span>
                        Sign Out
                    </button>
                </div>
            </div>
        </div>
    `;
    document.getElementById('account-logout')?.addEventListener('click', () => {
        logout();
        showToast('Logged out successfully', 'info');
    });
}

// ============================================
// UTILITIES
// ============================================
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ============================================
// INIT
// ============================================
window.addEventListener('hashchange', render);
document.addEventListener('DOMContentLoaded', render);
