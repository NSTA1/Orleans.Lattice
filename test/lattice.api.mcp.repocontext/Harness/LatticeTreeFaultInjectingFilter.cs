using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Incoming grain-call filter that throws a <see cref="TimeoutException"/> for
/// the calls <see cref="LatticeTreeFaultInjector"/> selects, reproducing the
/// production fault shape (an <see cref="ILattice"/> write to the vector
/// membership tree exceeding its response deadline) without any timing
/// dependence, so the test is deterministic.
/// </summary>
/// <param name="injector">The selector deciding which calls fail.</param>
internal sealed class LatticeTreeFaultInjectingFilter(LatticeTreeFaultInjector injector) : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public Task Invoke(IIncomingGrainCallContext context)
    {
        var method = context.InterfaceMethod;
        if (method?.DeclaringType == typeof(ILattice)
            && injector.ShouldFail(method.Name, context.TargetContext.GrainId.Key.ToString()))
        {
            throw new TimeoutException(
                $"Injected fault: {method.Name} on '{context.TargetContext.GrainId.Key}' did not respond in time.");
        }

        return context.Invoke();
    }
}
