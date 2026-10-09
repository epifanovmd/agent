// Проверка значения по JSON Schema (@cfworker/json-schema: без eval и генерации кода — схема
// приходит от воркера, чужому коду её не доверяем). Замечания — по-русски, с путём к полю.
import { type OutputUnit, Validator } from "@cfworker/json-schema";

type Draft = "4" | "7" | "2019-09" | "2020-12";

/** Сколько замечаний показывать. */
const MAX_PROBLEMS = 3;

/** Версия JSON Schema по $schema; нет — 2020-12. */
const draftOf = (schema: Record<string, unknown>): Draft => {
  const id = typeof schema.$schema === "string" ? schema.$schema : "";

  if (id.includes("draft-04")) return "4";
  if (id.includes("draft-06") || id.includes("draft-07")) return "7";
  if (id.includes("2019-09")) return "2019-09";

  return "2020-12";
};

/** Ключевые слова-обёртки: их замечание повторяет замечания вложенных схем. */
const WRAPPERS = new Set([
  "properties",
  "patternProperties",
  "additionalProperties",
  "unevaluatedProperties",
  "items",
  "prefixItems",
  "additionalItems",
  "unevaluatedItems",
  "contains",
  "propertyNames",
  "dependentSchemas",
  "allOf",
  "$ref",
  "$recursiveRef",
  "$dynamicRef",
  "if",
  "then",
  "else",
]);

const TYPE_NAMES: Record<string, string> = {
  string: "строка",
  number: "число",
  integer: "целое число",
  boolean: "true или false",
  object: "объект",
  array: "массив",
  null: "null",
};

/** Значение ключевого слова в схеме по keywordLocation (#/properties/a/maxLength). */
const keywordValue = (
  schema: Record<string, unknown>,
  location: string,
): unknown =>
  location
    .replace(/^#\/?/, "")
    .split("/")
    .filter(Boolean)
    .map(p => decodeURIComponent(p).replace(/~1/g, "/").replace(/~0/g, "~"))
    .reduce<unknown>(
      (v, p) =>
        v && typeof v === "object"
          ? (v as Record<string, unknown>)[p]
          : undefined,
      schema,
    );

const show = (v: unknown) => JSON.stringify(v);

/** Текст замечания по ключевому слову; незнакомое — текст библиотеки. */
const problemText = (
  schema: Record<string, unknown>,
  unit: OutputUnit,
): string => {
  const v = keywordValue(schema, unit.keywordLocation);
  const quoted = /"([^"]*)"/.exec(unit.error)?.[1] ?? "";

  switch (unit.keyword) {
    case "type":
      return `ожидается ${[v]
        .flat()
        .map(t => TYPE_NAMES[String(t)] ?? String(t))
        .join(" или ")}`;
    case "required":
      return `нет поля ${quoted}`;
    case "false":
      return "лишнее поле";
    case "enum":
      return `ожидается одно из: ${Array.isArray(v) ? v.map(show).join(", ") : ""}`;
    case "const":
      return `ожидается ${show(v)}`;
    case "minLength":
      return `нужно не меньше ${show(v)} символов`;
    case "maxLength":
      return `нужно не больше ${show(v)} символов`;
    case "minimum":
      return `нужно не меньше ${show(v)}`;
    case "maximum":
      return `нужно не больше ${show(v)}`;
    case "exclusiveMinimum":
      return `нужно больше ${show(v)}`;
    case "exclusiveMaximum":
      return `нужно меньше ${show(v)}`;
    case "multipleOf":
      return `нужно кратное ${show(v)}`;
    case "minItems":
      return `нужно не меньше ${show(v)} элементов`;
    case "maxItems":
      return `нужно не больше ${show(v)} элементов`;
    case "uniqueItems":
      return "элементы повторяются";
    case "minProperties":
      return `нужно не меньше ${show(v)} полей`;
    case "maxProperties":
      return `нужно не больше ${show(v)} полей`;
    case "pattern":
      return `не по правилу ${String(v)}`;
    case "format":
      return `неверный формат ${show(v)}`;
    case "anyOf":
    case "oneOf":
      return "не подходит ни под один вариант";
    case "not":
      return "значение запрещено схемой";
    default:
      return unit.error;
  }
};

/** Путь к полю: #/items/0/name → items[0].name; корень — пусто. */
const fieldPath = (location: string): string =>
  location
    .replace(/^#\/?/, "")
    .split("/")
    .filter(Boolean)
    .map(p => decodeURIComponent(p).replace(/~1/g, "/").replace(/~0/g, "~"))
    .reduce(
      (s, p) => (/^\d+$/.test(p) ? `${s}[${p}]` : s ? `${s}.${p}` : p),
      "",
    );

/**
 * Замечания к значению data по JSON Schema schema (пусто — значение подходит), не больше трёх:
 * «поле: что не так». Схема, которую нельзя применить (например, ссылка на внешний документ), —
 * исключение.
 */
export const jsonSchemaProblems = (
  schema: Record<string, unknown>,
  data: unknown,
): string[] => {
  const result = new Validator(schema, draftOf(schema), false).validate(data);

  if (result.valid) return [];
  const leaves = result.errors.filter(e => !WRAPPERS.has(e.keyword));
  // Поле, отклонённое своей схемой, библиотека отмечает ещё и как лишнее — это замечание лишнее.
  const real = new Set(
    leaves.filter(e => e.keyword !== "false").map(e => e.instanceLocation),
  );
  const problems = leaves
    .filter(e => e.keyword !== "false" || !real.has(e.instanceLocation))
    .map(e => {
      const path = fieldPath(e.instanceLocation);
      const text = problemText(schema, e);

      return path ? `${path}: ${text}` : text;
    });
  const unique = [...new Set(problems)];

  return unique.length
    ? unique.slice(0, MAX_PROBLEMS)
    : ["значение не подходит под схему"];
};
