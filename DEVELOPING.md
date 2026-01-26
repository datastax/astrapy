# Development and maintenance

First install `uv` (e.g. `pipx install uv`), then set up a dev environment with `make venv`, or equivalently:

```
uv venv
uv sync --dev
```

Linter, style and typecheck should all pass for a PR:

```bash
make format
```

With `make format-fix` the style and imports are autofixed (by `ruff`)

Features must be thoroughly covered in tests (have a look at `tests/*` to infer
naming convention and module structure).

## astrapy-repl

Astrapy comes with a REPL, a customized Python interactive shell, which pre-loads
all relevant imports and comes with `client` and `database` objects instantiated and
ready-to-use.

Just ensure you have defined the environment variable as you would need for testing (see below),
or set them as command-line parameters. Start the shell with:

```
astrapy-repl
```

Try `-h` for more guidance on passing options. In a local development environment, you will
have to launch it as `uv run astrapy-repl`.

![AstraPy, REPL screenshot](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_repl.png)

## Running tests

### Typical testing

In most cases you want to run the "base" test suite (the one in the CI/CD automation) against either Astra DB or a local Data API + HCD.

Steps:

- Export variables as in one of the `tests/env_templates/*.base.template` examples.
- Export variables as in the `tests/env_templates/env.vectorize-minimal.template` example.
- Run: `uv venv --python ">=3.9,<3.13" && uv run pytest tests/base`

### All available tests/targets

Tests are grouped in:
- "base", covering general-purpose astrapy functionality. Divided in unit/integration;
- "vectorize", extensively running a base workload on all provider/integration choices;
- "admin", doing a whole sweep of admin operations. Very slow on Astra DB.

Astrapy's CI only runs "base". The others are to be checked manually when it's needed.

Tests can be run on three types of Data API _targets_ (with slight differences in what is applicable):

- **Astra**: an Astra DB target account (or two, as some tests are specific to dev environment)
- **Local**: a ready-to-use (user-supplied) local Data API on top of a DSE/HCD (e.g. using `tests/hcd_compose`).
- **DockerCompose**: HCD+Data API, started by the test itself on startup. This is used in the Github action `local` workflow. _Note that in this case the containers created will not be automatically destroyed._

Depending on the target chosen, different environment variables are needed: refer to
the `tests/env_templates/*.base.template` examples.
Note that the variables defined in the desired "base" template **must** be set to run test, even for unit tests.

Additionally, you will need to define the environment variables in `tests/env_templates/env.vectorize-minimal.template`,
which are needed by the minimal set of "vectorize" testing belonging to the "base" test group.

For Astra DB, you can include "shared secret" vectorize tests (i.e. KMS-based authentication).
To run those tests, you must scope an OpenAI API key
to the target Astra DB with secret name `"SHARED_SECRET_EMBEDDING_API_KEY_OPENAI"`
and comment the environment flag that suppresses them (see the base Astra env template).

For non-Astra, the reranking-related tests run only if one sets
`HEADER_RERANKING_API_KEY_NVIDIA="AstraCS:<dev token...>` (as shown in the Local/DockerCompose base env templates).

### Docker vs. Podman

In case you use a different Docker-compatible container runtime (e.g. `podman`) and are running against the
"DockerCompose" target make sure to export the environment variable such as `DOCKER_COMMAND_NAME="podman"`
to maek the test startup logic work properly.

### Keyspaces

You shoud never need to worry about keyspaces. Tests use two keyspaces, which are created if not found, with default names.
The env templates show how to override those names, if you want to.

### Multiple Python versions

If may be useful to run e.g. unit tests with multiple Python versions. You can have `uv`
create more than one venv and specify the version, e.g. for each one:

```
uv venv --python 3.9 .venv-3.9
. .venv-3.9/bin/activate
uv sync --dev --active
```

Then, with the desired virtual env active, you will run e.g. `uv run --active pytest [...]`.

Most make targets will also support running in the named virtual env:
assuming you activated a certain virtual env, you can run e.g.: `make format VENV=true`.

**Warning: Python 3.13+ currently not supported to run integration tests! (but the package itself is all right).**

### Adding/changing dependencies

After editing the `pyproject.toml`, make sure you run

```
uv lock
uv sync --dev
```

and then commit the new `uv.lock` to the repo as well.

### Sample testing commands

Base:

```
# choose one:
uv run pytest tests/base
uv run pytest tests/base/unit
uv run pytest tests/base/integration
```

Admin:

```
# depending on the environment, different 'admin tests' will run:
uv run pytest tests/admin
```

Extended vectorize:

```
# very many env. variables required for this one:
uv run pytest tests/vectorize

# restrict to some combination(s) with e.g.:
EMBEDDING_MODEL_TAGS="openai/text-embedding-3-large/HEADER/0,voyageAI/voyage-finance-2/SHARED_SECRET/f" \
    uv run pytest tests/vectorize/integration/test_vectorize_providers.py \
    -k test_vectorize_usage_auth_type_header_sync
```

All the usual `pytest` ways of restricting the test selection hold
(e.g. `uv run pytest tests/idiomatic/unit` or `[...] -k <test_name_selector>`). Also e.g.:

```
# suppress log noise
uv run pytest [...] -o log_cli=0

# increase log level
uv run pytest [...] -o log_cli=1 --log-cli-level=10
```

## Special tests

The following are special provision to manage features under evolution or not
entirely deployed to all environments. Typically they require manually passing
certain environment variables, otherwise the associated tests are excluded from CI.

### Cutting-edge features on `main`

Prepend tests with a `ASTRAPY_TEST_LATEST_MAIN=y` for features found on `main` that are not released anywhere.
_(Tip: run a code search first to see what is currently marked as such. Chances are nothing is.)_

### Legacy ordered-insert-many behaviour

Generally, [PR 2193](https://github.com/stargate/data-api/pull/2193) is included in modern Data API versions. In case tests are run to Data API 1.0.32 or lower, run the tests with the following additional environment variable to adjust the test expectations: `LEGACY_INSERTMANY_BEHAVIOUR_PRE2193="yes"`.

## Publish-and-release

Releasing a new version happens through the Github `release` workflow, which includes all necessary testing
plus a publish step to the test-PyPI.

The outcome of the workflow is: (1) the new package on PyPI, and (2) a new release on Github (which one would then probably edit manually to refine the message).

The workflow uses PyPI's "trusted publishing" (i.e. OIDC). This requires a Trusted Publishing entry to be set up on both PyPI instances (test and prod) with:

- owner, repo = `datastax`, `astrapy`;
- workflow name = `_test_release.yml` or `release.yml` respectively;
- environment = `pypi` (matching a same-named environment in the repo, used by the workflow's publish job)
