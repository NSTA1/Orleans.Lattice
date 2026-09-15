using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Incoming grain-call filter that suspends the calls
/// <see cref="LatticeTreeCallGate"/> selects until the test releases them,
/// holding a multi-step store operation at a chosen step so the test can sample
/// what an outside reader sees while it runs.
/// </summary>
/// <param name="gates">The selectors deciding which calls park.</param>
internal sealed class LatticeTreeGatingFilter(IEnumerable<LatticeTreeCallGate> gates)
    : IIncomingGrainCallFilter
{
    /// <inheritdoc />
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        var method = context.InterfaceMethod;
        if (method is not null)
        {
            var treeId = context.TargetContext.GrainId.Key.ToString();
            foreach (var gate in gates)
            {
                if (gate.ShouldHold(method.Name, treeId))
                {
                    await gate.HoldAsync().ConfigureAwait(false);
                    break;
                }
            }
        }

        await context.Invoke().ConfigureAwait(false);
    }
}
