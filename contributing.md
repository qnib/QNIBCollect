# Contributing to fullerite

We welcome all contribution to fullerite, If you have a feature request or you want to improve
existing functionality of fullerite - it is probably best to open a pull request with your changes.

## Adding new dependency

If you want to add new external dependency to fullerite, please make sure it is added to `Gomfile`.
Do not forget to specify `TAG` or `commit_id` of external git repository.  More information about
`Gomfile` can be found from https://github.com/mattn/gom.

## Ensure code is formatted and linted.

Running `make` should do that out of box.

## Building package fails or gom install fails

If you were using older way of vendoring external dependencies you should delete `src/github.com`, `pkg`
and `src/golang.org` before running `gom install` or attempting to build the package.
