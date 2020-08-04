const {
  AbstractLevelDOWN,
  AbstractIterator
} = require('abstract-leveldown')
const HyperBTree = require('hyper-btree')
const inherits = require('inherits')

const EMPTY = Buffer.alloc(0)

module.exports = HyperDown

function HyperDown (storage) {
  if (!(this instanceof HyperDown)) return new HyperDown(storage)
  this.storage = storage

  // Set in _open.
  this._db = null

  AbstractLevelDOWN.call(this, storage)
}
inherits(HyperDown, AbstractLevelDOWN)

HyperDown.prototype._serializeKey = function (key) {
  return Buffer.isBuffer(key) ? key : String(key)
}

HyperDown.prototype._open = function (opts, cb) {
  if (this._db) return process.nextTick(cb, null)
  this._db = new HyperBTree(this.storage, opts.key, opts)
  this._db.ready(cb)
}

HyperDown.prototype._get = function (key, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    this._db.get(key, opts, function (err, value) {
      if (err) return cb(err)

      if (!value) {
        err = new Error('NotFound')
        err.notFound = true
        return cb(err)
      }

      if (!opts.asBuffer) value = value.toString('utf-8')
      return cb(null, value)
    })
  })
}

HyperDown.prototype._put = function (key, value, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    return this._db.put(key, value, cb)
  })
}

HyperDown.prototype._del = function (key, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    return this._db.del(key, cb)
  })
}

HyperDown.prototype._batch = function (array, opts, cb) {
  return this._db.batch(array, cb)
}

HyperDown.prototype._iterator = function (opts) {
  if (opts.start) {
    if (opts.reverse) opts.lte = opts.start
    else opts.gte = opts.start
  }
  if (opts.end) {
    if (opts.reverse) opts.gte = opts.end
    else opts.lte = opts.end
  }
  return new HyperIterator(this, opts)
}

function HyperIterator (db, opts) {
  this.db = db
  this.opts = opts

  this._keyAsBuffer = opts.keyAsBuffer
  this._valueAsBuffer = opts.valueAsBuffer

  // Set when first opened
  this.ite = null
  this.opened = false

  AbstractIterator.call(this, db)
}
inherits(HyperIterator, AbstractIterator)

HyperIterator.prototype._next = function (cb) {
  if (!cb) throw new Error('next() requires a callback argument')
  if (!this.opened) {
    return this.db._db.snapshot((err, snapshot) => {
      if (err) return cb(err)
      this.ite = snapshot.iterator(this.opts)
      this.opened = true
      return this._next(cb)
    })
  }
  this.ite.next((err, node) => {
    if (err) return cb(err)
    if (!node) return cb()
    // TODO: Better buffer conversion for key?
    return cb(null, node.key, node.value)
    return cb(null,
      !this._keyAsBuffer ? node.key : Buffer.from(node.key),
      !this._valueAsBuffer ? node.value.toString('utf-8') : Buffer.from(node.value))
  })
}

function ensureValidKey (key, cb) {
  if (key === null) return cb(new Error('key cannot be `null` or `undefined`'))
  if (key === undefined) return cb(new Error('key cannot be `null` or `undefined`'))
  if (key === '') return cb(new Error('key cannot be an empty string'))
  // Quick check that will pass for valid keys.
  if (typeof key === 'string') return cb(null, key)

  if (Buffer.isBuffer(key)) {
    if (EMPTY.equals(key)) return cb(new Error('key cannot be an empty Buffer'))
    key = key.toString('utf-8')
  } else if (typeof key === 'number') {
    key = String(key)
  }
  if ((key instanceof Array) && key.length === 0) return cb(new Error('key cannot be an empty string'))
  return cb(null, key)
}
