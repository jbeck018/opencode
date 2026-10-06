export * as KeyOrder from "./key-order"

// Effect 4.0.1 removed the `propertyOrder: "original"` parse option and no longer guarantees that decoded
// objects keep the input key order. Permission precedence depends on user key order, so this restores it:
// at every plain-object level, input keys come first, followed by keys that only exist in the output.
export function preserve<A>(input: unknown, output: A): A {
  if (Array.isArray(input) && Array.isArray(output))
    return output.map((item, index) => preserve(input[index], item)) as A
  if (!isPlainObject(input) || !isPlainObject(output)) return output
  return Object.fromEntries(
    [...Reflect.ownKeys(input), ...Reflect.ownKeys(output)]
      .filter((key, index, keys) => keys.indexOf(key) === index && Object.hasOwn(output, key))
      .map((key) => [key, preserve(input[key], output[key])]),
  ) as A
}

function isPlainObject(value: unknown): value is Record<PropertyKey, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}
