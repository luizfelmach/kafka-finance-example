const App = {
  currentPage: 'dashboard',
  stats: { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0 },
  recentAlerts: [],

  init() {
    this.route();
    window.addEventListener('hashchange', () => this.route());
    document.querySelectorAll('.nav-link').forEach(a => {
      a.addEventListener('click', () => document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active')));
    });
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.event-form').forEach(f => f.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab).classList.add('active');
      });
    });
  },

  route() {
    const hash = location.hash.slice(1) || 'dashboard';
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const page = document.getElementById('page-' + hash);
    const link = document.querySelector(`.nav-link[href="#${hash}"]`);
    if (page) page.classList.add('active');
    if (link) link.classList.add('active');
    this.currentPage = hash;
    document.dispatchEvent(new CustomEvent('pagechange', { detail: { page: hash } }));
  },

  updateStats(type) {
    if (this.stats[type] !== undefined) {
      this.stats[type]++;
      document.getElementById('stat-' + type.toLowerCase()).textContent = this.stats[type];
    }
  },

  pushAlert(alert) {
    this.recentAlerts.unshift(alert);
    if (this.recentAlerts.length > 100) this.recentAlerts.pop();
    this.updateStats(alert.severity);
  }
};

document.addEventListener('DOMContentLoaded', () => App.init());
