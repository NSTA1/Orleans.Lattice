# Orleans.Lattice.Auth

Authorization and enforcement add-on for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Auth` is the intended home for the authorization and enforcement surface layered on top of a lattice cluster. It builds on the identity primitives from `Orleans.Lattice.Membership`. This release is **scaffolding only**: the package is empty and inert. It establishes a compiling, packaged home so that later authorization features land into a stable place without churning the solution or the package graph.

## Status

The package currently ships:

- An empty `Orleans.Lattice.Auth` namespace with no public API.
- No dependency on the core read/write path, and therefore zero runtime cost.

There is nothing to register and nothing to call yet. Everything else is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues) and summarised in the [feature index](features.md).

## Reference

- [Auth Feature Index](features.md) - grouped, issue-linked index of the `Orleans.Lattice.Auth` package's tracked features.

Feature planning is managed on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files.
