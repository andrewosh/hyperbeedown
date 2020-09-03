const {
  AbstractLevelDOWN,
  AbstractIterator
} = require('abstract-leveldown')

const EMPTY = Buffer.alloc(0)

module.exports = class HyperDown extends AbstractLevelDOWN {
  constructor (tree) {
    super()
    // Set in _open.
    this.tree = tree
  }

  _serializeKey (key) {
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

  _serializeValue (value) {
    if (value && Array.isArray(value)) return value.join(',')
    else if (typeof value === 'number' || typeof value === 'boolean') value = String(value)
    return value
  }

  _open (_, cb) {
    // Open options are not handled, because a Hyperbee must be passed in as a constructor arg.
    if (!this.tree) return process.nextTick(cb, new Error('A Hyperbee must be provided as a constructor argument'))

    return this.tree.ready().then(cb, err => cb(err))
  }

  _get (key, opts, cb) {
    this.tree.get(key, opts).then(node => {
      if (!node || !node.value) {
        const err = new Error('NotFound')
        err.notFound = true
        return cb(err)
      }
      return cb(null, !opts.asBuffer ? node.value.toString('utf-8') : node.value)
    }, err => cb(err))
  }

  _put (key, value, _, cb) {
    return this.tree.put(key, value).then(() => cb(null), err => cb(err))
  }

  _del (key, _, cb) {
    return this.tree.del(key).then(() => cb(null), err => cb(err))
  }

  async _batchPromise (array, opts) {
    const batch = this.tree.batch(opts)
    for (let { type, key, value } of array) {
      key = this._serializeKey(key)
      value = value && this._serializeValue(value)
      if (type === 'put') await batch.put(key, value)
      else if (type === 'del') await batch.del(key)
    }
    return batch.flush()
  }

  _batch (array, opts, cb) {
    return this._batchPromise(array, opts).then(() => cb(null), err => cb(err))
  }

  _iterator (opts) {
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
}

class HyperIterator extends AbstractIterator {
  constructor (db, opts) {
    super(db)
    this.tree = db.tree.snapshot()
    this.opts = opts

    // Set in first next

    this._keyAsBuffer = opts.keyAsBuffer
    this._valueAsBuffer = opts.valueAsBuffer
  }

  _next (cb) {
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
}
