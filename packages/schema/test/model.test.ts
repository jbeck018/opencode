import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { Model } from "../src/model"
import { Provider } from "../src/provider"

describe("Model.Info.empty", () => {
  test("produces a value the schema accepts", () => {
    const empty = Model.Info.empty(Provider.ID.make("acme"), Model.ID.make("acme-1"))
    expect(Schema.decodeUnknownSync(Model.Info)(empty)).toEqual(empty)
  })
})
