using Microsoft.Extensions.Logging.Abstractions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Test-only factories for the vector-plane self-heal collaborators, so unit tests
/// that construct a <see cref="RepoContextVectorWriter"/> do not have to repeat the
/// re-deriver's null-logger wiring at every call site.
/// </summary>
internal static class RepoContextVectorPlaneTestDoubles
{
    /// <summary>
    /// Builds a live <see cref="RepoContextVectorPlaneReDeriver"/> over
    /// <paramref name="grainFactory"/> with a no-op logger, for tests that only need
    /// the writer wired and do not assert on the re-derivation path itself.
    /// </summary>
    /// <param name="grainFactory">The grain factory the re-deriver resets faulting trees through.</param>
    /// <returns>A re-deriver instance suitable for constructing a writer under test.</returns>
    internal static RepoContextVectorPlaneReDeriver ReDeriver(IGrainFactory grainFactory)
        => new(grainFactory, NullLogger<RepoContextVectorPlaneReDeriver>.Instance);
}
