function randomRoom() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  if (!globalThis.crypto?.getRandomValues) {
    throw new Error('Secure random generator unavailable');
  }
  const output = [];
  const rejectionLimit = Math.floor(256 / alphabet.length) * alphabet.length;
  while (output.length < 6) {
    const bytes = new Uint8Array(12);
    globalThis.crypto.getRandomValues(bytes);
    for (const value of bytes) {
      if (value >= rejectionLimit) continue;
      output.push(alphabet[value % alphabet.length]);
      if (output.length === 6) break;
    }
  }
  return output.join('');
}
function qs(sel){ return document.querySelector(sel); }
function getRoomFromQuery() {
  const params = new URLSearchParams(location.search);
  return params.get('room');
}
