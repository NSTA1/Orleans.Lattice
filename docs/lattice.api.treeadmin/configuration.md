# Configuration

`AddLatticeTreeAdminApi(Action<LatticeApiTreeAdminOptions>?)` registers and binds `LatticeApiTreeAdminOptions`. It is a public **empty options type**: there are no package-specific properties or defaults to tune today.

Core tree sizing, retention, WAL and admission settings belong to [core configuration](../lattice/configuration.md). Schema enforcement belongs to the [schema package](../lattice.schema/README.md). Transport credentials, discovery and authorization belong to the chosen binding, for example [TreeAdmin gRPC](../lattice.api.treeadmin.grpc/README.md), rather than to this in-process facade.

The facade checks authority even if an outer transport gate has admitted a request. Choose public registration seams and caller grants instead of treating options as an authorization bypass.

## Source map

- [Options](../../src/lattice.api.treeadmin/LatticeApiTreeAdminOptions.cs)
- [Registration](../../src/lattice.api.treeadmin/LatticeApiTreeAdminServiceCollectionExtensions.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)
