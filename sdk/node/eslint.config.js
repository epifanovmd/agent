// ESLint 9 (flat config): typescript-eslint recommended; правила форматирования
// отключены eslint-config-prettier — форматирование не дело линтера.
import js from "@eslint/js";
import prettier from "eslint-config-prettier";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "node_modules"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      // const { secret, ...rest } = x — отбрасывание полей деструктуризацией.
      "@typescript-eslint/no-unused-vars": ["error", { ignoreRestSiblings: true }],
    },
  },
  prettier,
);
