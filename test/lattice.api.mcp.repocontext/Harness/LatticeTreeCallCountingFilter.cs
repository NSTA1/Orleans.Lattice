using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Incoming grain-call filter that feeds <see cref="LatticeTreeCallCounter"/>.
/// Counts only calls whose interface method is declared on <see cref="ILattice"/>,
/// so the shard and leaf grains a single facade call fans out to are never
/// double-counted, and restricts them to the counter's tree when it names one.
/// </summary>
/// <param name="counter">The tally the filter records into.</param>
internal sealed class LatticeTreeCallCountingFilter(LatticeTreeCallCounter counter) : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        var method = context.InterfaceMethod;
        if (method?.DeclaringType == typeof(ILattice)
            && (counter.TreeId is null
                || string.Equals(
                    context.TargetContext.GrainId.Key.ToString(),
                    counter.TreeId,
                    StringComparison.Ordinal)))
        {
            counter.Record(method.Name);
        }

        return context.Invoke();
    }
}
