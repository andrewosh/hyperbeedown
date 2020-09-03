const {
  AbstractLevelDOWN,
  AbstractIterator
} = require('abstract-leveldown')
const inherits = require('inherits')

const EMPTY = Buffer.alloc(0)

module.exports = HyperDown

function HyperDown (tree) {
  if (!(this instanceof HyperDown)) return new HyperDown()
  // Set in _open.
  this.tree = tree
  AbstractLevelDOWN.call(this)
}
inherits(HyperDown, AbstractLevelDOWN)

HyperDown.prototype._serializeKey = function (key) {
  if (key === null) throw new Error('key cannot be `null` or `undefined`')
  else if (key === undefined) throw new Error('key cannot be `null` or `undefined`')
  else if (key === '') throw new Error('key cannot be an empty string')
  // Quick check that will pass for valid keys.
  else if (typeof key === 'string') return key
  else if (Buffer.isBuffer(key) && EMPTY.equals(key)) throw new Error('key cannot be an empty Buffer')
  else if ((key instanceof Array) && key.length === 0) throw new Error('key cannot be an empty string')
  else if (typeof key === 'number' || typeof key === 'boolean') key = String(key)
  else if (Array.isArray(key)) key = key.join(',')
  return key
}

HyperDown.prototype._serializeValue = function (value) {
  if (value && Array.isArray(value)) return value.join(',')
  else if (typeof value === 'number' || typeof value === 'boolean') value = String(value)
  return value
}

HyperDown.prototype._open = function (opts, cb) {
  // Open options are not handled, because a Hyperbee must be passed in as a constructor arg.
  if (!this.tree) return process.nextTick(cb, new Error('A Hyperbee must be provided as a constructor argument'))
  return this.tree.ready().then(() => process.nextTick(cb, null), err => process.nextTick(cb, err))
}

HyperDown.prototype._get = function (key, opts, cb) {
  this.tree.get(key, opts).then(node => {
    if (!node || !node.value) {
      err = new Error('NotFound')
      err.notFound = true
      return cb(err)
    }
    return cb(null, !opts.asBuffer ? node.value.toString('utf-8') : node.value)
  }, err => cb(err))
}

HyperDown.prototype._put = function (key, value, opts, cb) {
  return this.tree.put(key, value).then(() => cb(null), err => cb(err))
}

HyperDown.prototype._del = function (key, opts, cb) {
  return this.tree.del(key).then(() => cb(null), err => cb(err))
}

HyperDown.prototype._batchPromise = async function (array, opts) {
  const batch = this.tree.batch(opts)
  for (let { type, key, value } of array) {
    key = this._serializeKey(key)
    value = value && this._serializeValue(value)
    if (type === 'put') await batch.put(key, value)
    else if (type === 'del') await batch.del(key)
  }
  return batch.flush()
}

HyperDown.prototype._batch = function (array, opts, cb) {
  return this._batchPromise(array, opts).then(() => cb(null), err => cb(err))
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
  this.tree = db.tree.snapshot()
  this.opts = opts

  // Set in first next
  
  this._keyAsBuffer = opts.keyAsBuffer
  this._valueAsBuffer = opts.valueAsBuffer

  AbstractIterator.call(this, db)
}
inherits(HyperIterator, AbstractIterator)

HyperIterator.prototype._next = function (cb) {
  if (!cb) throw new Error('next() requires a callback argument')
  if (!this.ite) {
    const stream = this.tree.createReadStream(this.opts)
    this.ite = stream[Symbol.asyncIterator]() 
  }
  this.ite.next().then(({ value: node, done }) => {
    if (done) return cb(null)
    if (!node || !node.value) return this._next(cb)
    // TODO: Better buffer conversion for key?
    return cb(null,
      !this._keyAsBuffer ? node.key : Buffer.from(node.key),
      !this._valueAsBuffer ? node.value.toString('utf-8') : Buffer.from(node.value))
  }, err => cb(err))
}

function ensureValidKey (key) {
}
