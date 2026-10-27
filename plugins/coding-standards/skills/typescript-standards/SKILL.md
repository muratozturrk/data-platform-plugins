---
name: typescript-standards
description: TypeScript/React coding standard for the platform UI and SDK: Google TypeScript style baseline, strict compiler options, eslint/prettier, module structure, types vs interfaces, error handling, React component rules. Use when writing or reviewing frontend or TS SDK code.
---

# TypeScript Standards

Baseline: Google TypeScript Style Guide (gts) + typescript-eslint recommended-type-checked; formatting by Prettier.

## Core Rules
| # | Rule | Enforced by |
|---|---|---|
| 1 | `strict: true`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, `noImplicitOverride`; no `any` (use `unknown` + narrowing) | tsc, eslint no-explicit-any |
| 2 | Prettier defaults (2 spaces, semicolons, single quotes, trailing commas `all`, width 100) | prettier |
| 3 | ES modules only; named exports; no default exports except React pages/lazy; import order stdlib → third-party → aliases → relative | eslint import/order |
| 4 | `interface` for object shapes that may be extended, `type` for unions/aliases; `readonly` by default; discriminated unions over boolean flags | review |
| 5 | Generated API client from OpenAPI; never hand-written fetch types; snake_case wire fields mapped once at the client boundary | review |
| 6 | Errors: `Result`-style or typed errors from the client; no `throw` of strings; `try/catch` only at boundaries; `unknown` in catch | eslint |
| 7 | Async: `async/await`; no floating promises; cancellation via `AbortController` | eslint no-floating-promises |
| 8 | React: function components + hooks; props typed; no business logic in components (hooks/services); state colocated; lists keyed by stable ids; accessibility lint | eslint react/jsx-a11y |
| 9 | File/folder: `feature/` modules with `components/`, `hooks/`, `api/`, `model/`; file names kebab-case, component files PascalCase | review |
| 10 | Tests: Vitest + Testing Library; test behaviour, not implementation; MSW for API | review |
| 11 | No `console.log`; structured logger; no secrets in bundle (env vars prefixed and allow-listed) | eslint no-console |
| 12 | Comments: TSDoc on exported symbols | eslint tsdoc |

## Anti-patterns
- `as any`, `!` non-null assertions to silence the compiler
- Barrel files re-exporting everything (slow builds, cycles)
- Components > 200 lines; prop drilling > 2 levels where context/store fits
- Mutating state; `useEffect` for derived data

Sources: Google TypeScript Style Guide (https://google.github.io/styleguide/tsguide.html); typescript-eslint; Prettier; React docs "Thinking in React"; TSDoc.
