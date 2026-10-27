import tseslint from 'typescript-eslint';
import importPlugin from 'eslint-plugin-import';
import react from 'eslint-plugin-react';
import jsxA11y from 'eslint-plugin-jsx-a11y';

export default tseslint.config(
  ...tseslint.configs.recommendedTypeChecked,
  { languageOptions: { parserOptions: { projectService: true } } },
  {
    plugins: { import: importPlugin, react, 'jsx-a11y': jsxA11y },
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/consistent-type-definitions': ['error', 'interface'],
      'no-console': 'error',
      'import/order': ['error', { groups: ['builtin', 'external', 'internal', 'parent', 'sibling'], 'newlines-between': 'always' }],
      'import/no-default-export': 'error',
      'react/jsx-key': 'error',
      'jsx-a11y/alt-text': 'error',
    },
  },
  { files: ['**/pages/**', '**/*.page.tsx'], rules: { 'import/no-default-export': 'off' } },
);
