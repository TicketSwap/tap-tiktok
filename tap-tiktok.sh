#!/bin/sh

unset VIRTUAL_ENV

exec uv run --project "$(dirname "$0")" tap-tiktok "$@"
