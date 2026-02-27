const admin = require('firebase-admin');

const ensureArray = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return [value];
};

const normalizeRole = (value) => {
  if (typeof value !== 'string') return 'user';
  const normalized = value.trim().toLowerCase();
  return normalized || 'user';
};

const extractBearerToken = (authorizationHeader) => {
  if (typeof authorizationHeader !== 'string') return null;
  if (!authorizationHeader.startsWith('Bearer ')) return null;
  const token = authorizationHeader.slice(7).trim();
  return token || null;
};

const requireAuth = (allowedRoles = null) => async (req, res, next) => {
  try {
    const token = extractBearerToken(req.headers.authorization || '');
    if (!token) {
      return res.status(401).json({ error: 'missing_token' });
    }

    const decoded = await admin.auth().verifyIdToken(token);
    const userRole = normalizeRole(decoded.role);
    req.user = decoded;
    req.userRole = userRole;

    const allowed = ensureArray(allowedRoles).map(normalizeRole).filter(Boolean);
    if (allowed.length > 0 && !allowed.includes(userRole)) {
      return res.status(403).json({ error: 'insufficient_role' });
    }

    return next();
  } catch (_error) {
    return res.status(401).json({ error: 'invalid_token' });
  }
};

module.exports = { requireAuth, extractBearerToken, normalizeRole };
