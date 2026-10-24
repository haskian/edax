# Changelog
All notable changes to this project will be documented in this file. See [conventional commits](https://www.conventionalcommits.org/) for commit guidelines.

- - -
## [v0.18.0](https://github.com/haskian/edax/compare/1be3f8d28b90818967596d45779344468954466d..v0.18.0) - 2026-10-24
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) store message maps as jsonb, refuse NUL text in every store (#34) - ([438013c](https://github.com/haskian/edax/commit/438013c2b643fe2977b9fd7710325dd3be9e2850)) - Črtomir Majer
#### Documentation
- document dedup-only idempotent commands (#33) - ([61726e6](https://github.com/haskian/edax/commit/61726e6e3172d95f6870c94111644d37b7e8302b)) - Črtomir Majer
#### Continuous Integration
- (**edax-py**) type-check with ruff and ty, not mypy --strict (#32) - ([1be3f8d](https://github.com/haskian/edax/commit/1be3f8d28b90818967596d45779344468954466d)) - Črtomir Majer

- - -

## [v0.17.0](https://github.com/haskian/edax/compare/eb49e5734cc036b31269123b808e6165436b7076..v0.17.0) - 2026-10-24
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) (**edax-postgres**) store message maps and encryption info as jsonb (#31) - ([eb49e57](https://github.com/haskian/edax/commit/eb49e5734cc036b31269123b808e6165436b7076)) - Črtomir Majer

- - -

## [v0.16.0](https://github.com/haskian/edax/compare/6757c83d99b4ef7f296c297d103fa0810d2881c9..v0.16.0) - 2026-10-23
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) (**edax-postgres**) let a host name the erasure and ingest partition-control tables (#30) - ([6757c83](https://github.com/haskian/edax/commit/6757c83d99b4ef7f296c297d103fa0810d2881c9)) - Črtomir Majer

- - -

## [v0.15.0](https://github.com/haskian/edax/compare/20c34a83f07a7d915cf979f78d456d2200c4518c..v0.15.0) - 2026-10-23
#### Features
- add owner_mismatch error code for sealed owner refusals (#29) - ([20c34a8](https://github.com/haskian/edax/commit/20c34a83f07a7d915cf979f78d456d2200c4518c)) - Črtomir Majer

- - -

## [v0.14.0](https://github.com/haskian/edax/compare/f822e138129ae539183c21dbc75d2c76b785b60d..v0.14.0) - 2026-10-20
#### Features
- add opt-in time-partitioned append ingestion (#28) - ([f822e13](https://github.com/haskian/edax/commit/f822e138129ae539183c21dbc75d2c76b785b60d)) - Črtomir Majer

- - -

## [v0.13.0](https://github.com/haskian/edax/compare/1b43560f5f5ead2724b96a42fbf1fd50c83b044c..v0.13.0) - 2026-10-19
#### Features
- add atomic ingestion and ordered projection processing (#24) - ([1b43560](https://github.com/haskian/edax/commit/1b43560f5f5ead2724b96a42fbf1fd50c83b044c)) - Črtomir Majer

- - -

## [v0.12.0](https://github.com/haskian/edax/compare/b0710b6d981d362cc35de70d05762830d70f9a0e..v0.12.0) - 2026-10-16
#### Features
- (**edax-py**) fence sealed writes inside the write's own transaction (#27) - ([b0710b6](https://github.com/haskian/edax/commit/b0710b6d981d362cc35de70d05762830d70f9a0e)) - Črtomir Majer

- - -

## [v0.11.0](https://github.com/haskian/edax/compare/413361ef4337fa865a56d6a86733c03cd935fd83..v0.11.0) - 2026-10-16
#### Features
- add scoped payload encryption and tracked erasure (#26) - ([413361e](https://github.com/haskian/edax/commit/413361ef4337fa865a56d6a86733c03cd935fd83)) - Črtomir Majer

- - -

## [v0.10.0](https://github.com/haskian/edax/compare/7bc3e479a4d45f3c298b4427508ff41f70b4bcf7..v0.10.0) - 2026-10-15
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) add create, Instant.now, read-model names and rejection reasons (#25) - ([7bc3e47](https://github.com/haskian/edax/commit/7bc3e479a4d45f3c298b4427508ff41f70b4bcf7)) - Črtomir Majer

- - -

## [v0.9.0](https://github.com/haskian/edax/compare/d999d6ae9199cd11dc201e304ea37fd864ed6992..v0.9.0) - 2026-10-10
#### Features
- (**postgres**) add the Postgres kernel and the asyncpg backend (#23) - ([d999d6a](https://github.com/haskian/edax/commit/d999d6ae9199cd11dc201e304ea37fd864ed6992)) - Črtomir Majer

- - -

## [v0.8.0](https://github.com/haskian/edax/compare/1f0e7915a51974c9ccac8aedabf8cb0cd51a54b4..v0.8.0) - 2026-10-08
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) enforce SDK contracts and harden message delivery (#21) - ([1f0e791](https://github.com/haskian/edax/commit/1f0e7915a51974c9ccac8aedabf8cb0cd51a54b4)) - Črtomir Majer

- - -

## [v0.7.0](https://github.com/haskian/edax/compare/f1edb34ccd855e0ee6d9a05d3b74888f76709d42..v0.7.0) - 2026-10-07
#### Features
- add the Go SDK on the C interface and move scripts to mise tasks (#20) - ([f1edb34](https://github.com/haskian/edax/commit/f1edb34ccd855e0ee6d9a05d3b74888f76709d42)) - Črtomir Majer

- - -

## [v0.6.0](https://github.com/haskian/edax/compare/cc4df9c7276595cca0b3a491c27244c1fec4a1ef..v0.6.0) - 2026-10-06
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) move duplicated SDK orchestration into kernels and fix drift (#19) - ([cc4df9c](https://github.com/haskian/edax/commit/cc4df9c7276595cca0b3a491c27244c1fec4a1ef)) - Črtomir Majer

- - -

## [v0.5.0](https://github.com/haskian/edax/compare/2dbedc0628df4729c75b69e8d913209fa1a98c80..v0.5.0) - 2026-10-05
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) fix the feature review and compare Decimal and datetime by value (#18) - ([2dbedc0](https://github.com/haskian/edax/commit/2dbedc0628df4729c75b69e8d913209fa1a98c80)) - Črtomir Majer

- - -

## [v0.4.0](https://github.com/haskian/edax/compare/66f280b8e56f1b9d15be3432fec22257af45b043..v0.4.0) - 2026-10-04
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) make the tenant a typed field with a kernel key rule (#17) - ([66f280b](https://github.com/haskian/edax/commit/66f280b8e56f1b9d15be3432fec22257af45b043)) - Črtomir Majer

- - -

## [v0.3.0](https://github.com/haskian/edax/compare/d4d14b10d42050b7eaae2699228e46c9852dedf4..v0.3.0) - 2026-10-04
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) split events and commands into flat typed messages (#16) - ([d4d14b1](https://github.com/haskian/edax/commit/d4d14b10d42050b7eaae2699228e46c9852dedf4)) - Črtomir Majer

- - -

## [v0.2.0](https://github.com/haskian/edax/compare/88e611c13041143e088e174432bbb97742913c19..v0.2.0) - 2026-10-03
#### Features
- ![BREAKING](https://img.shields.io/badge/BREAKING-red) address the nine-crate review (104 findings) (#15) - ([a7896aa](https://github.com/haskian/edax/commit/a7896aa3577d976a32f68b85945c95d8f5486a1f)) - Črtomir Majer
#### Documentation
- fix the command-field rule and document the saga causation hop (#14) - ([882ddb2](https://github.com/haskian/edax/commit/882ddb2f5005bd2987327c0c6e8e4e481c197be3)) - Črtomir Majer
#### Continuous Integration
- let the release smoke tests fetch dependencies from the index (#12) - ([45cc793](https://github.com/haskian/edax/commit/45cc79385243ed8aabf0f8f3e4f795fe1cc2ec9e)) - Črtomir Majer
#### Refactoring
- (**kernels**) move filter and query into edax-cqrs (#13) - ([88e611c](https://github.com/haskian/edax/commit/88e611c13041143e088e174432bbb97742913c19)) - Črtomir Majer

- - -

## [v0.1.0](https://github.com/haskian/edax/compare/1440e10672bbf3f858c178a1e1c796d55158b53c..v0.1.0) - 2026-09-16
#### Features
- (**edax-py**) observability seam — Observer plus timing middleware, no deps (#6) - ([a9fc898](https://github.com/haskian/edax/commit/a9fc898504166d601b7d62874dc18b602b413909)) - Črtomir Majer
- edax, a polyglot toolkit for event-driven systems (#1) - ([1440e10](https://github.com/haskian/edax/commit/1440e10672bbf3f858c178a1e1c796d55158b53c)) - Črtomir Majer
#### Bug Fixes
- (**ci**) drop --locked from the pre-bump hook so a bump can re-lock (#11) - ([419af87](https://github.com/haskian/edax/commit/419af87fe4eda608f8ae1a4650c7fc53af26eb44)) - Črtomir Majer
- (**ci**) make ci.yml reusable so release.yml can call it (#4) - ([f35e90e](https://github.com/haskian/edax/commit/f35e90ee7c86c16d4d52a3f8a39db9256d015b36)) - Črtomir Majer
- (**ci**) grant id-token to the release call from version.yml (#2) - ([e79a325](https://github.com/haskian/edax/commit/e79a325e49edaf3f6fb53d0382b9ad080a2f3ec9)) - Črtomir Majer
- (**edax-py**) resolve event_store lazily so a split ledger skips the rollback clause (#7) - ([5c095a0](https://github.com/haskian/edax/commit/5c095a07057a0e2b5dbb84c5416e01d70ceba088)) - Črtomir Majer
- act on the adopter review — nine defects, the doc gaps, and the 0.1.0 gate (#5) - ([834fcd1](https://github.com/haskian/edax/commit/834fcd1028324beff50cc841f612cd3e79dfa0bb)) - Črtomir Majer
#### Documentation
- early-alpha warning + install steps; fix CI example segfault gate (#3) - ([2899343](https://github.com/haskian/edax/commit/28993438fc329a82ff4d17bf7113baaf2d71eae3)) - Črtomir Majer
#### Continuous Integration
- enforce conventional PR titles and gate majors on versionbump! (#10) - ([f45f9f7](https://github.com/haskian/edax/commit/f45f9f7909b0a009c5067403fe321fdacd3120b3)) - Črtomir Majer

- - -

Changelog generated by [cocogitto](https://github.com/cocogitto/cocogitto).