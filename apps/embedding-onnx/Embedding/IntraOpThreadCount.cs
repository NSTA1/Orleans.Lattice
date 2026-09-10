namespace Orleans.Lattice.Embedding.Onnx;

/// <summary>
/// The resolved ONNX Runtime intra-op thread count, together with where it came
/// from.
/// </summary>
/// <remarks>
/// The two halves travel together on purpose. A count on its own cannot be
/// audited: an operator reading <c>intra-op threads 16</c> in a startup log
/// cannot tell whether the deployment asked for sixteen, whether it was derived
/// from a CPU grant of sixteen, or whether it is a host core count that ignores
/// a four-CPU quota. Those call for different responses, and only the last is a
/// defect. Reporting a derived value indistinguishably from a declared one is
/// the trap issue #2593 walked into on the repository-context host, where it
/// produced a directly self-contradicting pair of log lines.
/// </remarks>
/// <param name="Threads">The count handed to ONNX Runtime. Zero means the
/// runtime chooses for itself, which is only safe when no CPU quota is
/// enforced.</param>
/// <param name="Source">Where <paramref name="Threads"/> came from.</param>
internal readonly record struct IntraOpThreadCount(int Threads, IntraOpThreadSource Source)
{
    /// <summary>
    /// Renders the provenance for an operator-facing log line, in the same
    /// declared-versus-derived vocabulary the repository-context effective
    /// configuration report uses.
    /// </summary>
    /// <returns>A short human-readable provenance phrase.</returns>
    public string DescribeProvenance() => Source switch
    {
        IntraOpThreadSource.Declared =>
            $"DECLARED via {EmbedServerOptions.IntraOpThreadsKey}",
        IntraOpThreadSource.ContainerCpuGrant =>
            $"DERIVED from the enforced container CPU grant, not declared; set {EmbedServerOptions.IntraOpThreadsKey} to override",
        _ =>
            $"DERIVED from Environment.ProcessorCount because no CPU quota was readable, not declared; set {EmbedServerOptions.IntraOpThreadsKey} to override",
    };
}
