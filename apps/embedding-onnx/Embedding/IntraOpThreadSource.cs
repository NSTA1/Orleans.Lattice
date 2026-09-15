namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// Where a resolved intra-op thread count came from.
/// </summary>
/// <remarks>
/// The source is reported alongside the number because the number alone cannot
/// be audited. An operator reading <c>intra-op threads 16</c> cannot tell
/// whether the deployment asked for sixteen, whether sixteen was derived from a
/// CPU grant, or whether it is a host core count that ignores a quota, and those
/// three call for different responses. Printing a derived value
/// indistinguishably from a declared one is the trap issue #2593 walked into on
/// the repository-context host.
/// </remarks>
internal enum IntraOpThreadSource
{
    /// <summary>
    /// <c>EMBED_INTRA_THREADS</c> supplied the value explicitly.
    /// </summary>
    Declared,

    /// <summary>
    /// Derived from the container's CPU quota, read from the cgroup filesystem.
    /// This is the preferred source because it is the figure the kernel actually
    /// enforces.
    /// </summary>
    ContainerCpuGrant,

    /// <summary>
    /// Derived from <see cref="System.Environment.ProcessorCount"/>, used only
    /// when no CPU quota could be read. Note this is not a synonym for the
    /// quota: <c>DOTNET_PROCESSOR_COUNT</c> overrides it, and on an unconstrained
    /// host it is the host core count.
    /// </summary>
    ProcessorCount,
}
