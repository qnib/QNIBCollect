# Contributing to fullerite

We welcome all contribution to fullerite, If you have a feature request or you want to improve
existing functionality of fullerite - it is probably best to open a pull request with your changes.

## Adding new dependency

If you want to add new external dependency to fullerite, please make sure it is added to `Gomfile`.
Do not forget to specify `TAG` or `commit_id` of external git repository.  More information about
`Gomfile` can be found from https://github.com/mattn/gom.

## Ensure code is formatted, tested and passes golint.

Running `make` should do all of the above. If you see any failures or errors while running `make`,
please fix them before opening pull request.

## Building and compiling

Running `make` should build fullerite binary and place it in `bin` directory.

## Building package fails or gom install fails

If you were using older way of vendoring external dependencies you should delete `src/github.com`, `pkg`
and `src/golang.org` before running `gom install` or attempting to build the package.
