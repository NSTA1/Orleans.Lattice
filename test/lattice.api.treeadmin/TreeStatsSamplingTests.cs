using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

[TestFixture]
public sealed class TreeStatsSamplingTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetTreeStatsAsync_overlaps_reads_and_settles_storage_even_when_diagnostics_fails(bool fail)
    {
        var diagnostics = new TaskCompletionSource<TreeDiagnosticReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var storage = new TaskCompletionSource<TreeStorageUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tree = Substitute.For<ILattice>();
        tree.TreeExistsAsync(Arg.Any<CancellationToken>()).Returns(true);
        tree.DiagnoseAsync(false, Arg.Any<CancellationToken>()).Returns(diagnostics.Task);
        tree.GetStorageUsageAsync(Arg.Any<CancellationToken>()).Returns(storage.Task);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>("orders").Returns(tree);
        var facade = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(), factory,
            new TreeAdminAccessAuthorizer(new AllowGate()),
            Options.Create(new LatticeApiTreeAdminOptions()), new NullTenantContextResolver());

        var pending = facade.GetTreeStatsAsync("orders");
        try
        {
            _ = tree.Received(1).GetStorageUsageAsync(Arg.Any<CancellationToken>());
            if (fail)
                diagnostics.SetException(new InvalidOperationException("diagnostics unavailable"));
            else
                diagnostics.SetResult(new TreeDiagnosticReport { TotalLiveKeys = 9 });
            Assert.That(pending.IsCompleted, Is.False, "Every issued read must settle before returning.");
        }
        finally
        {
            diagnostics.TrySetResult(new TreeDiagnosticReport());
            storage.TrySetResult(new TreeStorageUsageReport { TotalBytes = 42 });
        }

        if (fail)
            Assert.That(async () => await pending, Throws.TypeOf<InvalidOperationException>());
        else
        {
            var result = await pending;
            Assert.That(result.TotalLiveKeys, Is.EqualTo(9));
            Assert.That(result.TotalBytes, Is.EqualTo(42));
        }
    }

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }
}
