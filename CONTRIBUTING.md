# Contributing

Read the [documentation index](docs/README.md), [development workflow](docs/development.md), and the module you intend to change. Function contracts live beside the code; configuration/API behavior has dedicated references.

Use Node 22 and `npm ci`. Before submitting a change, run `npm run build`, `npm run check`, `node tests/server-control.mjs`, `node tests/incoming-history.mjs`, and `node tests/ldap-contacts.mjs` from the repository root. Describe multi-browser/PBX/media verification and any tests you could not run.

Keep secrets and generated/private runtime data out of contributions. Update documentation with behavior changes, including defaults, failure behavior, and persistence. Report reproducible problems with sanitized steps/logs; never include upstream PBX secrets, LDAP credentials or real subscription keys.

Contributions of Software should be compatible with the [MIT License](LICENSE). Do not replace or add Envato Elements media under MIT; see [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES). Other third-party materials keep their own licenses.
