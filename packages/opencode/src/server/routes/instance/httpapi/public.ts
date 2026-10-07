import { Context, Schema, SchemaAST } from "effect"
import type { SchemaRepresentation } from "effect"
import { HttpApi, HttpApiMiddleware, OpenApi } from "effect/http-api"
import { OpenCodeHttpApi, ServerApi } from "./api"
import { QueryBooleanOpenApi } from "./groups/query"

type OpenApiParameter = {
  name: string
  in: string
  required?: boolean
  schema?: OpenApiSchema
}

type OpenApiOperation = {
  parameters?: OpenApiParameter[]
  responses?: Record<string, OpenApiResponse>
  requestBody?: {
    required?: boolean
    content?: Record<string, { schema?: OpenApiSchema }>
  }
  security?: unknown
}

type OpenApiPathItem = Partial<Record<"get" | "post" | "put" | "delete" | "patch", OpenApiOperation>>

type OpenApiSpec = {
  components?: {
    schemas?: Record<string, OpenApiSchema>
    securitySchemes?: Record<string, unknown>
  }
  paths?: Record<string, OpenApiPathItem>
}

type OpenApiSchema = {
  $ref?: string
  additionalProperties?: OpenApiSchema | boolean
  allOf?: OpenApiSchema[]
  anyOf?: OpenApiSchema[]
  contentMediaType?: string
  contentSchema?: OpenApiSchema
  description?: string
  enum?: Array<string | boolean>
  items?: OpenApiSchema
  maximum?: number
  minimum?: number
  not?: OpenApiSchema
  oneOf?: OpenApiSchema[]
  pattern?: string
  patternProperties?: Record<string, OpenApiSchema>
  prefixItems?: OpenApiSchema[]
  properties?: Record<string, OpenApiSchema>
  required?: string[]
  type?: string
}

type OpenApiResponse = {
  description?: string
  content?: Record<string, { schema?: OpenApiSchema }>
}

// Query schemas describe decoded Effect values, but the generated SDK needs the
// public call shape. These keep SDK callers passing numbers/booleans while the
// server still decodes string query params at runtime.
const QueryParameterSchemas: Record<string, OpenApiSchema> = {
  "GET /experimental/session start": { type: "number" },
  "GET /experimental/session roots": QueryBooleanOpenApi,
  "GET /experimental/session archived": QueryBooleanOpenApi,
  "GET /find/file limit": { type: "integer", minimum: 1, maximum: 200 },
  "GET /experimental/session cursor": { type: "number" },
  "GET /experimental/session limit": { type: "number" },
  "GET /session start": { type: "number" },
  "GET /session roots": QueryBooleanOpenApi,
  "GET /session limit": { type: "number" },
  "GET /session/{sessionID}/message limit": { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
  "GET /vcs/diff context": { type: "integer", minimum: 0 },
  "GET /api/session limit": { type: "number" },
  "GET /api/session start": { type: "number" },
  "GET /api/session roots": QueryBooleanOpenApi,
  "GET /api/session/{sessionID}/message limit": { type: "number" },
}

const LegacyComponentDescriptions: Record<string, string> = {
  LogLevel: "Log level",
  ServerConfig: "Server configuration for opencode serve and web commands",
  LayoutConfig: "@deprecated Always uses stretch layout.",
}

function matchLegacyOpenApi(input: Record<string, unknown>) {
  const spec = input as OpenApiSpec

  // Effect's multi-document JSON Schema deduplicator can produce self-referencing
  // component schemas (e.g. `{"$ref":"#/components/schemas/X"}` as the definition
  // of X itself) when the same AST node appears both as a standalone endpoint
  // payload and inside an annotated union arm. Resolve these by inlining the
  // actual schema from any parent union that references them.
  fixSelfReferencingComponents(spec)
  restoreStreamContentSchemas(spec)
  orderComponentsLikeLegacy(spec)

  // Effect's Schema.optional emits `anyOf: [T, {type:"null"}]` in OpenAPI,
  // but the legacy SDK expected plain `T` for optional fields. Strip null
  // from all component schemas so both request and response types match.
  for (const [name, schema] of Object.entries(spec.components?.schemas ?? {})) {
    spec.components!.schemas![name] = stripOptionalNull(structuredClone(schema))
  }
  normalizeComponentNames(spec)
  collapseDuplicateComponents(spec)
  applyLegacySchemaOverrides(spec)
  normalizeComponentDescriptions(spec)
  addLegacyOutputFormatCopy(spec)
  addLegacyErrorSchemas(spec)
  delete spec.components?.securitySchemes

  for (const [path, item] of Object.entries(spec.paths ?? {})) {
    for (const method of ["get", "post", "put", "delete", "patch"] as const) {
      const operation = item[method]
      if (!operation) continue
      const isV2Api = isV2ApiPath(path)
      if (operation.requestBody) {
        // The legacy OpenAPI surface never marked request bodies as required.
        // Keep that SDK surface stable while the HttpApi spec is tightened.
        if (!isV2Api) delete operation.requestBody.required
        const body = operation.requestBody.content?.["application/json"]
        if (body?.schema) body.schema = stripOptionalNull(structuredClone(body.schema))
        if (path === "/experimental/workspace" && method === "post") {
          // Workspace creation fields `branch` and `extra` are Schema.NullOr —
          // genuinely nullable, not just optional. Re-add the null that the
          // component-level strip above removed.
          const ref = operation.requestBody.content?.["application/json"]?.schema?.$ref?.replace(
            "#/components/schemas/",
            "",
          )
          const properties = ref
            ? spec.components?.schemas?.[ref]?.properties
            : operation.requestBody.content?.["application/json"]?.schema?.properties
          if (properties?.branch) properties.branch = { anyOf: [properties.branch, { type: "null" }] }
          if (properties?.extra) properties.extra = { anyOf: [properties.extra, { type: "null" }] }
        }
        if (path === "/experimental/workspace/warp" && method === "post") {
          const ref = operation.requestBody.content?.["application/json"]?.schema?.$ref?.replace(
            "#/components/schemas/",
            "",
          )
          const properties = ref
            ? spec.components?.schemas?.[ref]?.properties
            : operation.requestBody.content?.["application/json"]?.schema?.properties
          if (properties?.id) properties.id = { anyOf: [properties.id, { type: "null" }] }
        }
      }
      for (const response of Object.values(operation.responses ?? {})) {
        for (const [type, content] of Object.entries(response.content ?? {})) {
          if (content.schema) content.schema = stripOptionalNull(structuredClone(content.schema))
          if (type === "text/event-stream") requireSseEventId(content.schema)
        }
      }
      if (!isV2Api) {
        // Auth is still runtime middleware outside the legacy public OpenAPI
        // metadata, so the legacy SDK should not expose auth schemes or
        // generated 401 error unions.
        delete operation.security
        delete operation.responses?.["401"]
        normalizeLegacyErrorResponses(operation)
      }
      if (isV2Api) orderMiddlewareErrorsLast(operation)
      normalizeLegacyOperation(operation, path, method)
      if ((path === "/event" || path === "/global/event" || path === "/api/event") && method === "get") {
        // HttpApi has no first-class SSE response schema, and these handlers are
        // raw/streaming routes. Document the actual wire protocol explicitly.
        operation.responses!["200"] = {
          description: "Event stream",
          content: {
            "text/event-stream": {
              schema:
                path === "/event"
                  ? { $ref: "#/components/schemas/Event" }
                  : path === "/global/event"
                    ? { $ref: "#/components/schemas/GlobalEvent" }
                    : { $ref: "#/components/schemas/V2Event" },
            },
          },
        }
      }
      const route = `${method.toUpperCase()} ${path}`
      for (const param of operation.parameters ?? []) normalizeParameter(param, route)
    }
  }
  deleteUnusedLegacyErrorComponents(spec)
  return input
}

// Legacy Effect required the SSE `id` field.
function requireSseEventId(schema: OpenApiSchema | undefined) {
  if (!schema?.properties?.id || !schema.properties.event || schema.required?.includes("id")) return
  schema.required = ["id", ...(schema.required ?? [])]
}

// Legacy Effect listed an endpoint's own errors before the errors its
// middleware adds, and the generated SDK keeps that union order.
function orderMiddlewareErrorsLast(operation: OpenApiOperation) {
  for (const [status, response] of Object.entries(operation.responses ?? {})) {
    const schema = response.content?.["application/json"]?.schema
    if (Number(status) < 400 || !schema?.anyOf) continue
    const fromMiddleware = (item: OpenApiSchema) =>
      MiddlewareErrorNames.has(item.$ref?.replace("#/components/schemas/", "") ?? "")
    schema.anyOf = [...schema.anyOf.filter((item) => !fromMiddleware(item)), ...schema.anyOf.filter(fromMiddleware)]
  }
}

function isV2ApiPath(path: string) {
  return path === "/api" || path.startsWith("/api/")
}

// Effect no longer links an SSE JSON data string to the schema it decodes.
// The legacy spec named that schema with `contentSchema` on `<Data>Stream`.
function restoreStreamContentSchemas(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  for (const [name, schema] of Object.entries(schemas)) {
    const data = name.replace(/Stream$/, "")
    if (data === name || !schemas[data] || schema.contentMediaType !== "application/json" || schema.contentSchema)
      continue
    schemas[name] = { type: schema.type, contentSchema: { $ref: `#/components/schemas/${data}` }, ...schema }
  }
}

// The generated SDK declares types in component order. Legacy Effect emitted
// the API's additional schemas first, then every other component after its
// dependencies, in the order operations first referenced them.
function orderComponentsLikeLegacy(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  const order = new Set(LegacyAdditionalSchemaNames.filter((name) => schemas[name]))
  const visited = new Set<string>()
  const visit = (input: unknown): void => {
    if (Array.isArray(input)) return input.forEach(visit)
    if (!input || typeof input !== "object") return
    const name = (input as OpenApiSchema).$ref?.replace("#/components/schemas/", "")
    if (name === undefined) return Object.values(input).forEach(visit)
    if (visited.has(name) || !schemas[name]) return
    visited.add(name)
    visit(schemas[name])
    order.add(name)
  }
  Object.values(spec.paths ?? {})
    .flatMap((item) => Object.values(item))
    .forEach((operation) => {
      visit(operation.requestBody)
      visit(operation.parameters)
      visit(operation.responses)
    })
  LegacyAdditionalSchemaNames.forEach((name) => visit({ $ref: `#/components/schemas/${name}` }))
  spec.components!.schemas = Object.fromEntries(
    [...order, ...Object.keys(schemas).filter((name) => !order.has(name))].map((name) => [name, schemas[name]]),
  )
}

// Legacy Effect emitted an unreferenced copy of OutputFormat with inline arms
// while walking the v2 event union, and the legacy SDK exports it.
function addLegacyOutputFormatCopy(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  const options = schemas?.OutputFormat?.anyOf
  if (!schemas || !options || schemas.OutputFormat1) return
  const copy = {
    anyOf: options.map((item) =>
      structuredClone(schemas[item.$ref?.replace("#/components/schemas/", "") ?? ""] ?? item),
    ),
  }
  spec.components!.schemas = Object.fromEntries(
    Object.entries(schemas).flatMap(([name, schema]) =>
      name === "ProviderNotFoundError"
        ? [
            [name, schema],
            ["OutputFormat1", copy],
          ]
        : [[name, schema]],
    ),
  )
}

function addLegacyErrorSchemas(spec: OpenApiSpec) {
  if (!spec.components?.schemas) return
  spec.components.schemas.BadRequestError = {
    type: "object",
    required: ["name", "data"],
    properties: {
      name: { type: "string", enum: ["BadRequest"] },
      data: {
        type: "object",
        required: ["message"],
        properties: {
          message: { type: "string" },
          kind: {
            type: "string",
            enum: ["Params", "Headers", "Query", "Body", "Payload"],
          },
        },
      },
    },
  }
  spec.components.schemas.NotFoundError = {
    type: "object",
    required: ["name", "data"],
    properties: {
      name: { type: "string", enum: ["NotFoundError"] },
      data: {
        type: "object",
        required: ["message"],
        properties: {
          message: { type: "string" },
        },
      },
    },
  }
}

function collapseDuplicateComponents(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  for (const name of Object.keys(schemas)) {
    const base = name.replace(/_?\d+$/, "")
    if (base === name || !schemas[base]) continue
    if (stableSchema(schemas[name], schemas) !== stableSchema(schemas[base], schemas)) continue
    rewriteRefs(spec, name, base)
    delete schemas[name]
  }
  // Effect suffixes distinct schemas that share an identifier as `X_1`, while
  // the legacy spec used `X1` and reused `X` when no schema claimed it.
  for (const name of Object.keys(schemas)) {
    const match = /^(.*)_(\d+)$/.exec(name)
    if (!match) continue
    renameComponent(spec, name, schemas[match[1]] ? `${match[1]}${match[2]}` : match[1])
  }
}

function renameComponent(spec: OpenApiSpec, from: string, to: string) {
  const schemas = spec.components!.schemas!
  if (schemas[to]) return
  // Rebuild the record so the renamed component keeps its position.
  spec.components!.schemas = Object.fromEntries(
    Object.entries(schemas).map(([name, schema]) => [name === from ? to : name, schema]),
  )
  rewriteRefs(spec, from, to)
}

function normalizeComponentNames(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  for (const name of Object.keys(schemas)) {
    const next = componentTypeName(name)
    if (next === name) continue
    if (schemas[next]) {
      if (stableSchema(schemas[name], schemas) === stableSchema(schemas[next], schemas)) {
        rewriteRefs(spec, name, next)
        delete schemas[name]
      }
      continue
    }
    schemas[next] = schemas[name]
    rewriteRefs(spec, name, next)
    delete schemas[name]
  }
}

function componentTypeName(name: string) {
  if (!name.includes(".")) return name
  return name
    .split(".")
    .filter((part) => !/^\d+$/.test(part))
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join("")
}

function applyLegacySchemaOverrides(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  if (schemas.AgentConfig) schemas.AgentConfig.additionalProperties = {}
  if (schemas.Command?.properties?.template) schemas.Command.properties.template = { type: "string" }
  if (schemas.Workspace?.properties) {
    schemas.Workspace.properties.branch = nullable(schemas.Workspace.properties.branch)
    schemas.Workspace.properties.directory = nullable(schemas.Workspace.properties.directory)
    schemas.Workspace.properties.extra = nullable(schemas.Workspace.properties.extra)
  }
  if (schemas.GlobalSession?.properties?.project)
    schemas.GlobalSession.properties.project = nullable(schemas.GlobalSession.properties.project)
  const providerOptions = schemas.ProviderConfig?.properties?.options
  if (providerOptions) providerOptions.additionalProperties = {}
  const model = schemas.ProviderConfig?.properties?.models?.additionalProperties
  const variants = typeof model === "object" ? model.properties?.variants?.additionalProperties : undefined
  if (variants && typeof variants === "object") variants.additionalProperties = {}
  const syncInfo = schemas.SyncEventSessionUpdated?.properties?.data?.properties?.info
  if (syncInfo?.properties) makePropertiesNullable(syncInfo.properties)
  // Effect only exports regex patterns compiled with the `u` flag.
  for (const color of [schemas.AgentColor, schemas.AgentConfig?.properties?.color]) {
    const hex = color?.anyOf?.[0]
    if (hex?.type === "string" && !hex.pattern) hex.pattern = "^#[0-9a-fA-F]{6}$"
  }
  // Effect only keeps `oneOf` when it can prove the union arms are exclusive.
  const durable = schemas.SessionDurableEvent
  if (durable?.anyOf) {
    durable.oneOf = durable.anyOf
    delete durable.anyOf
  }
}

function normalizeComponentDescriptions(spec: OpenApiSpec) {
  for (const [name, schema] of Object.entries(spec.components?.schemas ?? {})) {
    const description = LegacyComponentDescriptions[name]
    if (description) {
      schema.description = description
      continue
    }
    delete schema.description
  }
}

function makePropertiesNullable(properties: Record<string, OpenApiSchema>) {
  for (const [key, value] of Object.entries(properties)) {
    if (key === "share" && value.properties?.url) {
      value.properties.url = nullable(value.properties.url)
      continue
    }
    if (key === "time" && value.properties) {
      makePropertiesNullable(value.properties)
      continue
    }
    properties[key] = nullable(value)
  }
}

function nullable(schema: OpenApiSchema): OpenApiSchema {
  if (flattenOptions(schema.anyOf ?? schema.oneOf)?.some((item) => item.type === "null")) return schema
  return { anyOf: [schema, { type: "null" }] }
}

function stableSchema(input: unknown, schemas: Record<string, OpenApiSchema>): string {
  return JSON.stringify(canonicalizeSchema(input, schemas))
}

function canonicalizeSchema(input: unknown, schemas: Record<string, OpenApiSchema>): unknown {
  if (Array.isArray(input)) return input.map((item) => canonicalizeSchema(item, schemas))
  if (!input || typeof input !== "object") return input
  const schema = input as OpenApiSchema
  if (schema.$ref) return { $ref: canonicalRef(schema.$ref, schemas) }
  return Object.fromEntries(
    Object.entries(input)
      .filter(([key]) => key !== "description")
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => [key, canonicalizeSchema(value, schemas)]),
  )
}

function canonicalRef(ref: string, schemas: Record<string, OpenApiSchema>) {
  const name = ref.replace("#/components/schemas/", "")
  const base = name.replace(/_?\d+$/, "")
  if (base !== name && schemas[base]) return `#/components/schemas/${base}`
  return ref
}

function rewriteRefs(input: unknown, from: string, to: string): void {
  if (Array.isArray(input)) {
    for (const item of input) rewriteRefs(item, from, to)
    return
  }
  if (!input || typeof input !== "object") return
  const schema = input as OpenApiSchema
  if (schema.$ref === `#/components/schemas/${from}`) schema.$ref = `#/components/schemas/${to}`
  for (const value of Object.values(input)) rewriteRefs(value, from, to)
}

function normalizeLegacyErrorResponses(operation: OpenApiOperation) {
  if (operation.responses?.["400"] && isLegacyBadRequestResponse(operation.responses["400"])) {
    operation.responses["400"] = legacyErrorResponse("Bad request", "BadRequestError")
  }
  if (operation.responses?.["404"] && isBuiltInErrorResponse(operation.responses["404"], "NotFound")) {
    operation.responses["404"] = legacyErrorResponse("Not found", "NotFoundError")
  }
}

function deleteUnusedLegacyErrorComponents(spec: OpenApiSpec) {
  for (const name of [
    "Unauthorized",
    "EffectHttpApiErrorBadRequest",
    "EffectHttpApiErrorNotFound",
    "effect_HttpApiError_BadRequest",
    "effect_HttpApiError_NotFound",
  ]) {
    if (referencesComponent(spec.paths, name)) continue
    delete spec.components?.schemas?.[name]
  }
}

function referencesComponent(input: unknown, name: string): boolean {
  if (Array.isArray(input)) return input.some((item) => referencesComponent(item, name))
  if (!input || typeof input !== "object") return false
  if ((input as OpenApiSchema).$ref === `#/components/schemas/${name}`) return true
  return Object.values(input).some((value) => referencesComponent(value, name))
}

function normalizeLegacyOperation(operation: OpenApiOperation, path: string, method: string) {
  if (path === "/experimental/console/switch" && method === "post") delete operation.responses?.["400"]
  if ((path !== "/session/{sessionID}/message" && path !== "/session/{sessionID}/command") || method !== "post") return
  const response = operation.responses?.["200"]?.content?.["application/json"]
  if (!response) return
  response.schema = {
    type: "object",
    required: ["info", "parts"],
    properties: {
      info: { $ref: "#/components/schemas/AssistantMessage" },
      parts: {
        type: "array",
        items: { $ref: "#/components/schemas/Part" },
      },
    },
  }
}

function isRefResponse(response: OpenApiResponse, name: string) {
  return response.content?.["application/json"]?.schema?.$ref === `#/components/schemas/${name}`
}

function isBuiltInErrorResponse(response: OpenApiResponse, name: "BadRequest" | "NotFound") {
  return response.description === name || isRefResponse(response, `EffectHttpApiError${name}`)
}

function isLegacyBadRequestResponse(response: OpenApiResponse) {
  return isBuiltInErrorResponse(response, "BadRequest") || isRefResponse(response, "InvalidRequestError")
}

function legacyErrorResponse(description: string, name: "BadRequestError" | "NotFoundError"): OpenApiResponse {
  return {
    description,
    content: {
      "application/json": {
        schema: { $ref: `#/components/schemas/${name}` },
      },
    },
  }
}

/**
 * Fix component schemas that are self-referencing `$ref`s — an Effect OpenAPI
 * generation bug where annotated union arms that share AST nodes with other
 * endpoints produce `{"$ref":"#/components/schemas/X"}` as the definition of X.
 *
 * Resolves by finding the actual schema from a parent union's `anyOf`/`oneOf`
 * that references the broken component, then inlining that schema.
 */
function fixSelfReferencingComponents(spec: OpenApiSpec) {
  const schemas = spec.components?.schemas
  if (!schemas) return
  const selfRefs = new Set<string>()
  for (const [name, schema] of Object.entries(schemas)) {
    if (schema.$ref === `#/components/schemas/${name}`) selfRefs.add(name)
  }
  if (selfRefs.size === 0) return
  // Find a parent union component whose anyOf/oneOf contains a $ref to the
  // broken component — that parent was generated correctly and holds the inline
  // schema we need.
  for (const [, schema] of Object.entries(schemas)) {
    for (const member of schema.anyOf ?? schema.oneOf ?? []) {
      const ref = member.$ref?.replace("#/components/schemas/", "")
      if (!ref || !selfRefs.has(ref)) continue
      // This member's $ref points to a self-referencing component. The member
      // itself is just {$ref:...}, so the actual schema must be resolved from
      // the union. Since the union component was generated before the
      // deduplicator broke things, the inline version lives elsewhere. Generate
      // a fresh spec without the transform to get the correct schema.
      // Simpler approach: look through all paths for an endpoint that uses this
      // schema as a payload (it would have been expanded by the ref-expansion
      // logic above if we ran after that, but we run before). Instead, just
      // delete the broken component — if it's referenced via $ref elsewhere,
      // the ref expansion in the request body loop will inline it anyway.
    }
  }
  // Simplest fix: generate the raw spec (without transform) to get correct schemas
  const raw: OpenApiSpec = OpenApi.fromApi(OpenCodeHttpApi, PublicOpenApiOptions)
  const rawSchemas = raw.components?.schemas
  if (!rawSchemas) return
  for (const name of selfRefs) {
    if (rawSchemas[name]) schemas[name] = rawSchemas[name]
  }
}

/** Strip `{type:"null"}` arms that Effect's `Schema.optional` adds to OpenAPI unions. */
function stripOptionalNull(schema: OpenApiSchema): OpenApiSchema {
  if (schema.allOf?.length === 1) {
    const [constraint] = schema.allOf
    delete schema.allOf
    return stripOptionalNull({ ...schema, ...constraint })
  }
  if (isEmptyObjectUnion(schema) || isNonNullSchema(schema)) return { type: "object", properties: {} }
  if (isNonFiniteNumberUnion(schema))
    return { anyOf: LegacyNonFiniteNumberOptions.map((item) => structuredClone(item)) }
  const options = flattenOptions(schema.anyOf ?? schema.oneOf)
  if (options) {
    const withoutNull = options.filter((item) => item.type !== "null")
    if (withoutNull.length === 1) return stripOptionalNull(withoutNull[0])
    if (schema.anyOf) schema.anyOf = withoutNull.map(stripOptionalNull)
    if (schema.oneOf) schema.oneOf = withoutNull.map(stripOptionalNull)
  }
  if (schema.allOf) {
    const allOf = schema.allOf.map(stripOptionalNull)
    if (schema.type) {
      delete schema.allOf
      for (const item of allOf) Object.assign(schema, item)
    } else {
      schema.allOf = allOf
    }
  }
  if (schema.prefixItems && schema.items) delete schema.prefixItems
  // Legacy Effect left pattern-keyed records open to other keys.
  if (schema.patternProperties && schema.additionalProperties === false) delete schema.additionalProperties
  if (schema.items) schema.items = stripOptionalNull(schema.items)
  if (schema.properties) {
    for (const [key, value] of Object.entries(schema.properties)) {
      schema.properties[key] = stripOptionalNull(value)
    }
  }
  if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
    schema.additionalProperties = stripOptionalNull(schema.additionalProperties)
  }
  return schema
}

function isEmptyObjectUnion(schema: OpenApiSchema) {
  const options = schema.anyOf ?? schema.oneOf
  return options?.length === 2 && options.some(isBareObjectSchema) && options.some(isBareArraySchema)
}

// Effect encodes `Schema.Struct({})` as any non-null value; the legacy SDK
// exposed it as an empty object.
function isNonNullSchema(schema: OpenApiSchema) {
  return Object.keys(schema).length === 1 && schema.not?.type === "null" && Object.keys(schema.not).length === 1
}

const NonFiniteNumberStrings = ["Infinity", "-Infinity", "NaN"]

// Legacy Effect listed each non-finite number string before the combined enum.
const LegacyNonFiniteNumberOptions: OpenApiSchema[] = [
  { type: "number" },
  { type: "string", enum: ["NaN"] },
  { type: "string", enum: ["Infinity"] },
  { type: "string", enum: ["-Infinity"] },
  { type: "string", enum: NonFiniteNumberStrings },
]

function isNonFiniteNumberUnion(schema: OpenApiSchema) {
  return (
    Object.keys(schema).length === 1 &&
    schema.anyOf?.length === 2 &&
    schema.anyOf[0].type === "number" &&
    Object.keys(schema.anyOf[0]).length === 1 &&
    schema.anyOf[1].type === "string" &&
    JSON.stringify(schema.anyOf[1].enum) === JSON.stringify(NonFiniteNumberStrings)
  )
}

function isBareObjectSchema(schema: OpenApiSchema) {
  return schema.type === "object" && !schema.properties && !schema.additionalProperties
}

function isBareArraySchema(schema: OpenApiSchema) {
  return schema.type === "array" && !schema.items && !schema.prefixItems
}

function flattenOptions(options: OpenApiSchema[] | undefined): OpenApiSchema[] | undefined {
  return options?.flatMap((item) => flattenOptions(item.anyOf ?? item.oneOf) ?? [item])
}

function normalizeParameter(param: OpenApiParameter, route: string) {
  if (!param.schema || typeof param.schema !== "object") return
  if (param.in === "path") {
    param.schema = stripOptionalNull(param.schema)
    return
  }
  if (param.in === "query") {
    const override = QueryParameterSchemas[`${route} ${param.name}`]
    if (override) {
      param.schema = override
      return
    }
  }
  param.schema = stripOptionalNull(param.schema)
}

// Effect names component schemas inherited from a decoded identifier as
// `<identifier>Encoded`. The public spec predates that convention, so keep the
// plain identifier and let the transform reconcile the resulting collisions.
export const PublicOpenApiOptions: SchemaRepresentation.ToRepresentationOptions = {
  referencePolicy: (input) => input.identifier?.replace(/Encoded$/, ""),
}

const LegacyAdditionalSchemas = Context.getOrElse(OpenCodeHttpApi.annotations, HttpApi.AdditionalSchemas, () => [])

const MiddlewareErrorNames = new Set(
  Object.values(OpenCodeHttpApi.groups)
    .flatMap((group) => Object.values(group.endpoints))
    .flatMap((endpoint) => [...endpoint.middlewares])
    .flatMap((middleware) => [...(middleware as HttpApiMiddleware.AnyService).error])
    .flatMap((schema) => SchemaAST.resolveIdentifier(schema.ast) ?? []),
)

const LegacyAdditionalSchemaNames = LegacyAdditionalSchemas.flatMap(
  (schema) => SchemaAST.resolveIdentifier(schema.ast) ?? [],
)

// Effect no longer emits `contentSchema` for SSE JSON data, so the v2 event
// union behind `/api/event` would vanish from the spec. Recover it from the
// stream's `data` field, which is the union with a JSON string encoding, and
// rebuild the union without that encoding.
const V2Event = (() => {
  const success = [...ServerApi.groups["server.event"].endpoints["event.subscribe"].success][0]
  const events = "events" in success && Schema.isSchema(success.events) ? success.events.ast : undefined
  const data =
    events && SchemaAST.isObjects(events) ? events.propertySignatures.find((item) => item.name === "data") : undefined
  if (!data || !SchemaAST.isUnion(data.type)) throw new Error("Expected /api/event to stream the v2 event union")
  return Schema.make(new SchemaAST.Union(data.type.types, data.type.options, { identifier: "V2Event" }))
})()

export const PublicApi = OpenCodeHttpApi.annotateMerge(
  OpenApi.annotations({
    title: "opencode",
    version: "1.0.0",
    description: "opencode api",
    transform: matchLegacyOpenApi,
  }),
).annotate(HttpApi.AdditionalSchemas, [...LegacyAdditionalSchemas, V2Event])
