import { defineConfig } from "tsup";

// Два входа — раскладка dist под "exports" в package.json; зависимости пакета (ws, zod, semver,
// lru-cache, @cfworker/json-schema) — внешние.
export default defineConfig({
  entry: {
    index: "src/index.ts",
    "server/index": "src/server/index.ts",
  },
  format: ["esm"],
  target: "node24",
  platform: "node",
  dts: true,
  clean: true,
  sourcemap: false,
  external: ["ws", "zod", "semver", "lru-cache", "@cfworker/json-schema"],
});
