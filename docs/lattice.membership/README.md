# Orleans.Lattice.Membership

Identity, authorization and enforcement add-on for [Orleans.Lattice](../../README.md).

## What is it?

`Orleans.Lattice.Membership` is the intended home for the identity, authorization, and enforcement surface layered on top of a lattice cluster. This release is **scaffolding only**: the package is empty and inert. It establishes a compiling, packaged home so that later membership features land into a stable place without churning the solution or the package graph.

## Status

The package currently ships:

- An empty `Orleans.Lattice.Membership` namespace with no public API.
- No dependency on the core read/write path, and therefore zero runtime cost.

There is nothing to register and nothing to call yet. Everything else is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues) and summarised in the [feature index](features.md).

## Reference

- [Membership Feature Index](features.md) - grouped, issue-linked index of the `Orleans.Lattice.Membership` package's tracked features.

Feature planning is managed on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files.
