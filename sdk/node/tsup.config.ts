import { defineConfig } from "tsup";

// Три входа — раскладка dist под "exports" в package.json; ws — внешняя зависимость.
export default defineConfig({
  entry: {
    index: "src/index.ts",
    "worker/index": "src/worker/index.ts",
    "server/index": "src/server/index.ts",
  },
  format: ["esm"],
  target: "node24",
  platform: "node",
  dts: true,
  clean: true,
  sourcemap: false,
  external: ["ws"],
});
