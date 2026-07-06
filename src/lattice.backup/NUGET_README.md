# Orleans.Lattice.Backup

Optional, opt-in **backup and restore** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

This release is **scaffolding only**: it reserves the package id, the Orleans
serialization-alias prefix, and the reserved `sys-backup-` system-tree namespace
that the backup catalog and manifest engine will build on. There is no runtime
behaviour yet; the backup grains, catalog, and restore pipeline are layered by
later releases.
