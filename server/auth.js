const admin = require('firebase-admin');

async function requireAuth(req, res, next) {
  try {
    const header = req.headers.authorization || '';
    const token = header.startsWith('Bearer ') ? header.slice(7) : null;

    if (!token) {
      return res.status(401).json({ error: 'missing_token' });
    }

    const decoded = await admin.auth().verifyIdToken(token);
    req.user = decoded;
    return next();
  } catch (_error) {
    return res.status(401).json({ error: 'invalid_token' });
  }
}

module.exports = { requireAuth };
