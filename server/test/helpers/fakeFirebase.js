'use strict';

class FakeFirestore {
  constructor(seedDocs = {}, { events = [], failSet = null } = {}) {
    this.docs = new Map(
      Object.entries(seedDocs).map(([path, value]) => [path, clone(value)])
    );
    this.events = events;
    this.failSet = failSet;
    this.transactionQueue = Promise.resolve();
  }

  collection(name) {
    return new FakeCollectionRef(this, [name]);
  }

  async recursiveDelete(ref) {
    const prefix = `${ref.path}/`;
    this.events.push(`firestore:recursiveDelete:${ref.path}`);
    for (const path of [...this.docs.keys()]) {
      if (path === ref.path || path.startsWith(prefix)) {
        this.docs.delete(path);
      }
    }
  }

  async runTransaction(handler) {
    let release;
    const previous = this.transactionQueue;
    this.transactionQueue = new Promise((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await handler({
        get: (ref) => ref.get(),
        set: (ref, payload, options) => ref.set(payload, options),
        delete: (ref) => ref.delete(),
      });
    } finally {
      release();
    }
  }
}

class FakeCollectionRef {
  constructor(db, segments) {
    this.db = db;
    this.segments = segments;
  }

  get id() {
    return this.segments[this.segments.length - 1];
  }

  get path() {
    return this.segments.join('/');
  }

  doc(id) {
    return new FakeDocumentRef(this.db, [...this.segments, String(id)]);
  }

  where(field, operator, value) {
    if (operator !== '==') {
      throw new Error(`FakeFirestore only supports == queries, received ${operator}`);
    }
    return new FakeQuery(this.db, this.segments, field, value);
  }
}

class FakeDocumentRef {
  constructor(db, segments) {
    this.db = db;
    this.segments = segments;
  }

  get id() {
    return this.segments[this.segments.length - 1];
  }

  get path() {
    return this.segments.join('/');
  }

  get parent() {
    return new FakeCollectionRef(this.db, this.segments.slice(0, -1));
  }

  collection(name) {
    return new FakeCollectionRef(this.db, [...this.segments, String(name)]);
  }

  async get() {
    return createSnapshot(this);
  }

  async set(payload, options = {}) {
    if (typeof this.db.failSet === 'function') {
      const failure = this.db.failSet(this.path, payload, options, this.db);
      if (failure) throw failure instanceof Error ? failure : new Error(String(failure));
    }

    const nextPayload = clone(payload);
    if (options?.merge === true) {
      const current = this.db.docs.get(this.path);
      const base = current && typeof current === 'object' ? clone(current) : {};
      this.db.docs.set(this.path, { ...base, ...nextPayload });
    } else {
      this.db.docs.set(this.path, nextPayload);
    }
    this.db.events.push(`firestore:set:${this.path}`);
  }

  async delete() {
    this.db.docs.delete(this.path);
    this.db.events.push(`firestore:delete:${this.path}`);
  }
}

class FakeQuery {
  constructor(db, collectionSegments, field, value) {
    this.db = db;
    this.collectionSegments = collectionSegments;
    this.field = field;
    this.value = value;
    this.maximum = Number.POSITIVE_INFINITY;
  }

  limit(value) {
    this.maximum = Math.max(0, Number(value) || 0);
    return this;
  }

  async get() {
    const collectionPath = this.collectionSegments.join('/');
    const expectedSegments = this.collectionSegments.length + 1;
    const docs = [];

    for (const path of [...this.db.docs.keys()].sort()) {
      const segments = path.split('/');
      if (
        segments.length !== expectedSegments ||
        segments.slice(0, -1).join('/') !== collectionPath
      ) {
        continue;
      }
      const data = this.db.docs.get(path);
      if (readNestedField(data, this.field) !== this.value) continue;
      docs.push(createSnapshot(new FakeDocumentRef(this.db, segments)));
      if (docs.length >= this.maximum) break;
    }

    return {
      docs,
      empty: docs.length === 0,
      size: docs.length,
    };
  }
}

class FakeBucket {
  constructor({ events = [], failPrefixes = [], delayMs = 0 } = {}) {
    this.events = events;
    this.failPrefixes = new Set(failPrefixes);
    this.delayMs = Math.max(0, Number(delayMs) || 0);
    this.deletedPrefixes = [];
  }

  async deleteFiles({ prefix } = {}) {
    this.events.push(`storage:deleteFiles:${prefix}`);
    this.deletedPrefixes.push(prefix);
    if (this.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.delayMs));
    }
    if (this.failPrefixes.has(prefix)) {
      throw new Error(`storage failure for ${prefix}`);
    }
  }
}

class FakeAuth {
  constructor({
    tokens = {},
    events = [],
    deleteErrors = {},
  } = {}) {
    this.tokens = new Map(Object.entries(tokens));
    this.events = events;
    this.deleteErrors = new Map(Object.entries(deleteErrors));
    this.deletedUids = [];
    this.verifiedTokens = [];
  }

  async verifyIdToken(token, checkRevoked) {
    this.events.push(`auth:verify:${token}`);
    this.verifiedTokens.push({ token, checkRevoked });
    if (!this.tokens.has(token)) {
      const error = new Error('invalid token');
      error.code = 'auth/invalid-id-token';
      throw error;
    }
    return clone(this.tokens.get(token));
  }

  async deleteUser(uid) {
    this.events.push(`auth:delete:${uid}`);
    this.deletedUids.push(uid);
    if (this.deleteErrors.has(uid)) {
      const configured = this.deleteErrors.get(uid);
      throw configured instanceof Error ? configured : Object.assign(
        new Error(String(configured)),
        { code: String(configured) }
      );
    }
  }
}

function createSnapshot(ref) {
  const value = ref.db.docs.get(ref.path);
  return {
    id: ref.id,
    ref,
    exists: value !== undefined,
    data: () => (value === undefined ? undefined : clone(value)),
  };
}

function readNestedField(value, dottedPath) {
  return String(dottedPath)
    .split('.')
    .reduce(
      (current, segment) =>
        current && typeof current === 'object' ? current[segment] : undefined,
      value
    );
}

function clone(value) {
  return value === undefined ? undefined : structuredClone(value);
}

module.exports = {
  FakeAuth,
  FakeBucket,
  FakeFirestore,
};
