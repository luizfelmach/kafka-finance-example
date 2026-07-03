(function() {
  const BASE = '';

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('q-faraway-btn').addEventListener('click', async () => {
      const userId = document.getElementById('q-userId').value.trim();
      if (!userId) { document.getElementById('q-faraway-result').textContent = 'Enter a userId'; return; }
      try {
        const r = await fetch(BASE + '/api/queries/faraway-logins/' + encodeURIComponent(userId));
        if (!r.ok) throw new Error(await r.text());
        const data = await r.json();
        document.getElementById('q-faraway-result').textContent = JSON.stringify(data, null, 2);
      } catch (err) {
        document.getElementById('q-faraway-result').textContent = 'Error: ' + err.message;
      }
    });

    document.getElementById('q-obs-btn').addEventListener('click', async () => {
      try {
        const r = await fetch(BASE + '/api/queries/observations');
        if (!r.ok) throw new Error(await r.text());
        const data = await r.json();
        document.getElementById('q-obs-result').textContent = data.length ? JSON.stringify(data, null, 2) : 'No accounts under observation.';
      } catch (err) {
        document.getElementById('q-obs-result').textContent = 'Error: ' + err.message;
      }
    });
  });
})();
