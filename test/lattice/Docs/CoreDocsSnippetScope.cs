namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The registry of every <c>docs/&lt;package&gt;</c> directory that a package
/// test project owns via its own <see cref="Orleans.Lattice.Testing.Docs.DocsSnippetCompilationTestsBase"/>
/// subclass. The core docs-snippet fixture skips these (they are compiled by
/// their owning project, which CI runs whenever that package changes) and
/// compiles everything else under <c>docs/</c> plus the repo-root
/// <c>README.md</c>, so every <c>```csharp verify</c> snippet is compiled
/// exactly once.
/// <para>
/// Adding a new package docs-snippet fixture means adding its
/// <c>docs/&lt;package&gt;</c> root here so the core fixture does not
/// double-compile it.
/// </para>
/// </summary>
internal static class CoreDocsSnippetScope
{
    /// <summary>
    /// Every <c>docs/&lt;package&gt;</c> directory owned by a package project's
    /// docs-snippet fixture. Kept in sync with the per-project subclasses.
    /// <para>
    /// <c>docs/lattice.replication</c> is deliberately absent: its snippets
    /// demonstrate the gRPC replication transport, but the replication test
    /// project intentionally avoids a real <c>Grpc.Core</c> dependency (it
    /// declares a stub <c>Grpc.Core.RpcException</c> for its classifier tests),
    /// so it cannot reference <c>Orleans.Lattice.Replication.Grpc</c> without a
    /// namespace collision. Those snippets therefore stay in the core fixture,
    /// whose full reference closure compiles them.
    /// </para>
    /// </summary>
    internal static readonly string[] ClaimedPackageDocsRoots =
    {
        "docs/lattice.api.abstractions",
        "docs/lattice.api.backup.grpc",
        "docs/lattice.api.data",
        "docs/lattice.api.mcp",
        "docs/lattice.api.mcp.repocontext",
        "docs/lattice.api.mcp.repocontext.replication",
        "docs/lattice.api.mcp.telemetry",
        "docs/lattice.api.mcp.telemetry.azure",
        "docs/lattice.api.state",
        "docs/lattice.api.state.grpc",
        "docs/lattice.api.telemetry",
        "docs/lattice.api.telemetry.grpc",
        "docs/lattice.auth",
        "docs/lattice.backup",
        "docs/lattice.backup.azureblob",
        "docs/lattice.caching.azureblob",
        "docs/lattice.explorer",
        "docs/lattice.explorer.entra.web",
        "docs/lattice.grainindex",
        "docs/lattice.membership",
        "docs/lattice.replication.grpc",
        "docs/lattice.scaling",
        "docs/lattice.schema",
        "docs/lattice.storage.azuretable",
        "docs/lattice.storage.file",
        "docs/lattice.tenancy",
    };
}
