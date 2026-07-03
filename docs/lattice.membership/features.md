# Orleans.Lattice.Membership Feature Index

Feature planning for the `Orleans.Lattice.Membership` package - the identity, authorization, and enforcement add-on for `Orleans.Lattice` - is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files. See the [package overview](./README.md) for the user-facing description. This page is a grouped, human-readable index that links each tracked item to its issue. Keep it in sync whenever an issue is opened, closed, or retitled.

- **Browse all membership issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=Orleans.Lattice.Membership

## Package boundary

Everything tracked here ships in the `Orleans.Lattice.Membership` assembly. Public API lives under the `Orleans.Lattice.Membership` namespace; internals stay internal. The package depends only on `Orleans.Lattice` and adds no cost to the core read/write path.

This release is **scaffolding only**: the package is empty and inert. It exposes no public API beyond the namespace and carries no behaviour. Later sub-issues populate it.

## Features

### Shipped

- (none yet)

### Planned / open

- See the [identity, authorization & enforcement epic](https://github.com/NSTA1/Orleans.Lattice/issues/971) and its linked sub-issues.
