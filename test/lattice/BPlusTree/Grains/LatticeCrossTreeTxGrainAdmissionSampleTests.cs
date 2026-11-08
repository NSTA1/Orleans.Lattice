using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6017, cross-tree admission: a partial storage-usage report is a lower
/// bound, so it must never displace a complete sample the coordinator already
/// read for that tree - otherwise a refused WAL scan admits a cross-tree write
/// past a participating tree's cap.
/// </summary>
[TestFixture]
public sealed class LatticeCrossTreeTxGrainAdmissionSampleTests
{
    private const string OperationId = "xop-adm";
    private const string Tree = "orders";

    [Test]
    public void CommitAsync_after_a_partial_report_keeps_enforcing_the_previous_complete_sample()
    {
        var reports = new Queue<TreeStorageUsageReport>(
        [
            new TreeStorageUsageReport { TreeId = Tree, LiveKeys = 10, TotalBytes = 1_000 },
            new TreeStorageUsageReport { TreeId = Tree, LiveKeys = 0, TotalBytes = 0, Partial = true },
        ]);
        var grain = CreateGrain(reports);

        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => grain.CommitAsync(Batch()),
            "the complete sample is over the cap");
        Assert.ThrowsAsync<LatticeQuotaExceededException>(() => grain.CommitAsync(Batch()),
            "a partial report must not lower the sample the cap is compared against");
        Assert.That(reports, Is.Empty, "both reports were read");
    }

    [Test]
    public async Task CommitAsync_with_only_a_partial_report_uses_it_as_before()
    {
        var reports = new Queue<TreeStorageUsageReport>(
        [
            new TreeStorageUsageReport { TreeId = Tree, LiveKeys = 0, Partial = true },
        ]);
        var grain = CreateGrain(reports);

        var outcome = await grain.CommitAsync(Batch());

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
    }

    private static List<LatticeTreeBatch> Batch()
        => [new LatticeTreeBatch(Tree, [new KeyValuePair<string, byte[]>("k", [1])])];

    private static LatticeCrossTreeTxGrain CreateGrain(Queue<TreeStorageUsageReport> reports)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cross-tree-tx", OperationId));
        context.ActivationServices.Returns(Substitute.For<IServiceProvider>());

        var grainFactory = Substitute.For<IGrainFactory>();
        var participant = Substitute.For<IAtomicWriteGrain>();
        participant.PrepareForCoordinatorAsync(
                Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>(),
                Arg.Any<LatticePredicateNode?>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>())
            .Returns(CrossTreePrepareVote.Prepared);
        participant.FinalizeAsync(Arg.Any<bool>()).Returns(Task.CompletedTask);
        grainFactory.GetGrain<IAtomicWriteGrain>($"{Tree}/{OperationId}").Returns(participant);

        var usage = Substitute.For<ILatticeStorageUsage>();
        usage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(reports.Dequeue()));
        grainFactory.GetGrain<ILatticeStorageUsage>(Arg.Is(Tree), Arg.Any<string>()).Returns(usage);

        var reminderRegistry = Substitute.For<IReminderRegistry>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var options = new LatticeOptions { MaxLiveKeys = 5 };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(options);
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(string.Empty);

        return new LatticeCrossTreeTxGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, clusterIdResolver,
            new LoggerFactory().CreateLogger<LatticeCrossTreeTxGrain>(), new FakePersistentState<CrossTreeTxState>());
    }
}
