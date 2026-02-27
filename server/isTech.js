const admin = require('firebase-admin');

async function isTechUid(uid) {
  if (!uid) return false;
  const snapshot = await admin.firestore().doc(`techs/${uid}`).get();
  return snapshot.exists && snapshot.data()?.active === true;
}

module.exports = { isTechUid };
