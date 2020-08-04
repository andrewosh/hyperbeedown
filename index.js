const {
  AbstractLevelDOWN,
  AbstractIterator
} = require('abstract-leveldown')
const inherits = require('inherits')

const EMPTY = Buffer.alloc(0)

module.exports = HyperDown

function HyperDown () {
  if (!(this instanceof HyperDown)) return new HyperDown()
  // Set in _open.
  this.tree = null
  AbstractLevelDOWN.call(this, storage)
}
inherits(HyperDown, AbstractLevelDOWN)

HyperDown.prototype._serializeKey = function (key) {
  return Buffer.isBuffer(key) ? key : String(key)
}

HyperDown.prototype._open = function (tree, cb) {
  if (this.tree) return process.nextTick(cb, null)
  this.tree = tree
  this.tree.ready(cb)
}

HyperDown.prototype._get = function (key, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    this.tree.get(key, opts, function (err, node) {
      if (err) return cb(err)

      if (!node) {
        err = new Error('NotFound')
        err.notFound = true
        return cb(err)
      }

      const value = node.value
      if (!opts.asBuffer) value = value.toString('utf-8')
      return cb(null, value)
    })
  })
}

HyperDown.prototype._put = function (key, value, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    return this.tree.put(key, value, cb)
  })
}

HyperDown.prototype._del = function (key, opts, cb) {
  ensureValidKey(key, (err, key) => {
    if (err) return cb(err)
    return this.tree.del(key, cb)
  })
}

HyperDown.prototype._batch = function (array, opts, cb) {
  return this.tree.batch(array, cb)
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
    const snapshot = this.db.tree.snapshot()
    this.ite = snapshot.createReadStream(this.opts)[Symbol.asyncIterable]
    this.opened = true
    return this._next(cb)
  }
  this.ite.next().then(node => {
    if (!node) return cb()
    // TODO: Better buffer conversion for key?
    return cb(null, node.key, node.value)
    return cb(null,
      !this._keyAsBuffer ? node.key : Buffer.from(node.key),
      !this._valueAsBuffer ? node.value.toString('utf-8') : Buffer.from(node.value))
  }, err => cb(err))
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
