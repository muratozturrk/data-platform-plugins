export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'scope-enum': [2, 'always', ['control-plane', 'sdk', 'cli', 'api', 'plugins', 'infra', 'docs', 'data-governance', 'platform-engineering', 'naming', 'standards']],
    'subject-case': [2, 'always', 'lower-case'],
    'header-max-length': [2, 'always', 72],
    'body-max-line-length': [2, 'always', 72],
  },
};
