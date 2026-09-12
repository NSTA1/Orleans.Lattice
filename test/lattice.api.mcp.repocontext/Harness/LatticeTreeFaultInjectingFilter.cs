using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Incoming grain-call filter that throws a <see cref="TimeoutException"/> for
/// the calls <see cref="LatticeTreeFaultInjector"/> selects, reproducing the
/// production fault shape (an <see cref="ILattice"/> write to the vector
/// membership tree exceeding its response deadline) without any timing
/// dependence, so the test is deterministic.
/// </summary>
/// <param name="injectors">The selectors deciding which calls fail.</param>
internal sealed class LatticeTreeFaultInjectingFilter(IEnumerable<LatticeTreeFaultInjector> injectors)
    : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        var method = context.InterfaceMethod;
        if (method is not null)
        {
            var treeId = context.TargetContext.GrainId.Key.ToString();

            // Short-circuits on the first injector that claims the call, so a test
            // registering exactly one behaves precisely as it did when this filter
            // took a single instance. A second injector lets a test fault two seams
            // in one pass - faulting the coverage tree to force the digest unbuilt,
            // so the membership probe underneath it is reachable at all.
            foreach (var injector in injectors)
            {
                if (injector.ShouldFail(method.Name, treeId))
                {
                    throw new TimeoutException(
                        $"Injected fault: {method.Name} on '{context.TargetContext.GrainId.Key}' "
                        + "did not respond in time.");
                }
            }
        }

        return context.Invoke();
    }
}
