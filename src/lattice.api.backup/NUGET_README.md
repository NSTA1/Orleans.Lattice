# Orleans.Lattice.Api.Backup

Optional, opt-in **backup control-API facade** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

This release is **scaffolding only**: it reserves the package id and the
transport-agnostic control-API serialization-alias prefix that the backup admin
surface (list, create, and restore backups) will build on. There is no runtime
behaviour yet; the admin facade and its gRPC binding are layered by later
releases.
