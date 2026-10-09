// ESLint 9 (flat config): typescript-eslint recommended, порядок импортов, правила логики и
// читаемости. Форматирование — Prettier (.prettierrc.json), правила форматирования отключены
// eslint-config-prettier.
import eslint from "@eslint/js";
import prettier from "eslint-config-prettier";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: ["dist", "node_modules"],
  },
  {
    extends: [eslint.configs.recommended, ...tseslint.configs.recommended],
    files: ["**/*.ts", "**/*.mjs"],
    plugins: {
      "simple-import-sort": simpleImportSort,
    },
    rules: {
      "simple-import-sort/imports": "error",
      "simple-import-sort/exports": "error",

      "no-undef": "off",
      "no-unused-vars": "off",
      "no-redeclare": "off",

      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          // const { secret, ...rest } = x — отбрасывание полей деструктуризацией.
          ignoreRestSiblings: true,
        },
      ],

      // Логика и читаемость (форматирование — Prettier)
      "func-style": ["error", "expression"],
      "prefer-arrow-callback": "error",
      "no-console": "error",
      "no-bitwise": "error",
      "no-plusplus": "error",
      "no-lonely-if": "error",
      "no-multi-assign": "error",
      "no-unneeded-ternary": "error",
      "no-array-constructor": "error",
      "operator-assignment": ["error", "always"],
      "padding-line-between-statements": [
        "error",
        { blankLine: "always", prev: ["const", "let", "var"], next: "*" },
        { blankLine: "always", prev: "*", next: "return" },
        {
          blankLine: "any",
          prev: ["const", "let", "var"],
          next: ["const", "let", "var"],
        },
      ],
    },
  },
  prettier,
);
