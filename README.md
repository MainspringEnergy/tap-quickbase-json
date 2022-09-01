# tap-quickbase-json

`tap-quickbase-json` is a Singer tap for the [Quickbase JSON API](https://developer.quickbase.com/).

The [alternative Singer tap](https://github.com/singer-io/tap-quickbase) (`tap-quickbase`) uses the older XML-based API and may have some severe performance limitations.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```
pipx install git+https://gitlab.com/etagen-internal/tap-quickbase-json.git
```

## Configuration

Follow [these instructions](https://www.stitchdata.com/docs/integrations/saas/quick-base) to find your Quickbase hostname, app id, and user id.  Fill the `start_date` with the earliest time you want to start syncing data.

```
{
 "qb_hostname": "<yourdomain>.quickbase.com",
 "qb_appid": "<your app id>",
 "qb_user_token": "<your user token>",
 "start_date": "2020-01-01 00:00:00",
 "table_catalog": [],
}
```

## Usage

You can easily run `tap-quickbase-json` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-quickbase-json --version
tap-quickbase-json --help
tap-quickbase-json --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_quickbase_json/tests` subfolder and
  then run:

```bash
poetry run pytest
```

or run all the test and all the linters:

```bash
tox
```

Use black and isort for code formatting:

```bash
tox -e format
```

You can also test the `tap-quickbase-json` CLI interface directly using `poetry run`:

```bash
poetry run tap-quickbase-json --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-quickbase-json
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-quickbase-json --version
# OR run a test `elt` pipeline:
meltano elt tap-quickbase-json target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to develop your own taps and targets.
