const length = 26
const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
// IDs are minted per streamed event, so draw random bytes from a refilled pool instead of one
// crypto call per ID.
const random = { pool: new Uint8Array(4096), offset: 4096 }
let lastTimestamp = 0
let counter = 0

export function ascending() {
  return create(false)
}

export function descending() {
  return create(true)
}

export function create(descending: boolean, timestamp = Date.now()) {
  if (timestamp !== lastTimestamp) {
    lastTimestamp = timestamp
    counter = 0
  }
  counter++

  // Low 48 bits of timestamp * 0x1000 + counter, kept in exact double range.
  const current = ((timestamp % 2 ** 36) * 0x1000 + counter) % 2 ** 48
  const value = descending ? 2 ** 48 - 1 - current : current
  if (random.offset + length - 12 > random.pool.length) {
    crypto.getRandomValues(random.pool)
    random.offset = 0
  }
  const end = random.offset + length - 12
  let id = value.toString(16).padStart(12, "0")
  for (; random.offset < end; random.offset++) id += chars[random.pool[random.offset] % 62]
  return id
}
