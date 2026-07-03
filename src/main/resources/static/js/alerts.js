(function() {
  let alertCount = 0;
  let autoScroll = true;
  let eventSource = null;
  let map = null;
  let markers = [];
  let markerColors = { CRITICAL: '#da3633', HIGH: '#d29922', MEDIUM: '#58a6ff', LOW: '#3fb950' };

  function connectSSE() {
    if (eventSource) eventSource.close();
    eventSource = new EventSource('/api/alerts/stream');

    eventSource.addEventListener('alert', e => {
      try {
        const alert = JSON.parse(e.data);
        addAlert(alert);
        App.pushAlert(alert);
      } catch (err) {
        console.error('SSE parse error:', err);
      }
    });

    eventSource.onerror = () => {
      console.warn('SSE disconnected, reconnecting...');
      setTimeout(connectSSE, 3000);
    };
  }

  function addAlert(alert) {
    alertCount++;
    document.getElementById('alertCount').textContent = alertCount;
    document.getElementById('alertBadge').textContent = alertCount;
    document.getElementById('alertBadge').style.display = 'inline';

    const ts = new Date(alert.timestamp).toLocaleTimeString();
    const item = document.createElement('div');
    item.className = 'alert-item';
    item.innerHTML = `
      <span class="alert-severity ${alert.severity}">${alert.severity}</span>
      <div class="alert-body">
        <div class="alert-type">${alert.alertType}</div>
        <div class="alert-desc">${alert.description}</div>
      </div>
      <span class="alert-time">${ts}</span>
    `;

    const feed = document.getElementById('alertFeed');
    feed.insertBefore(item, feed.firstChild);
    if (item === feed.firstChild && feed.children.length > 1 && feed.children[1].classList.contains('empty')) {
      feed.children[1].remove();
    }

    const recent = document.getElementById('recentAlerts');
    if (recent) {
      const clone = item.cloneNode(true);
      recent.insertBefore(clone, recent.firstChild);
      if (clone === recent.firstChild && recent.children.length > 1 && recent.children[1].classList.contains('empty')) {
        recent.children[1].remove();
      }
      while (recent.children.length > 20) recent.lastChild.remove();
    }

    addMarker(alert);

    if (autoScroll) {
      feed.scrollTop = 0;
    }
  }

  function addMarker(alert) {
    if (!map || alert.latitude == null || alert.longitude == null) return;
    const lat = parseFloat(alert.latitude);
    const lng = parseFloat(alert.longitude);
    if (isNaN(lat) || isNaN(lng)) return;

    const color = markerColors[alert.severity] || '#8b949e';
    const marker = L.circleMarker([lat, lng], {
      radius: 8,
      fillColor: color,
      color: '#fff',
      weight: 1.5,
      fillOpacity: 0.8
    }).addTo(map);

    const time = new Date(alert.timestamp).toLocaleString();
    marker.bindPopup(`
      <strong style="color:${color}">${alert.alertType}</strong><br>
      ${alert.description}<br>
      <small>${time}</small>
    `);

    markers.push(marker);
  }

  function initMap() {
    map = L.map('alertMap').setView([-15, -55], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://openstreetmap.org/copyright">OSM</a>',
      maxZoom: 18
    }).addTo(map);
  }

  document.addEventListener('pagechange', e => {
    if (e.detail.page === 'alerts' && map) {
      setTimeout(() => map.invalidateSize(), 100);
    }
  });

  document.addEventListener('DOMContentLoaded', () => {
    initMap();
    connectSSE();

    document.getElementById('clearAlertsBtn').addEventListener('click', () => {
      document.getElementById('alertFeed').innerHTML = '<p class="empty">Waiting for alerts...</p>';
      markers.forEach(m => map.removeLayer(m));
      markers = [];
      alertCount = 0;
      document.getElementById('alertCount').textContent = '0';
      document.getElementById('alertBadge').textContent = '0';
      document.getElementById('alertBadge').style.display = 'none';
    });

    document.getElementById('autoScroll').addEventListener('change', e => {
      autoScroll = e.target.checked;
    });
  });
})();
