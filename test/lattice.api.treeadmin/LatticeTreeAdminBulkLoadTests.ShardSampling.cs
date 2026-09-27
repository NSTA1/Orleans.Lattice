using System.Collections.Immutable;
using NSubstitute;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Regression coverage for the bulk-load emptiness probe on a tree whose
/// diagnostics fan-out could not sample every shard. The aggregator contains a
/// faulting shard as an all-zero placeholder rather than failing the report, so
/// zero totals alone used to admit a session onto a tree whose unreachable shard
/// held data.
/// </summary>
public sealed partial class LatticeTreeAdminBulkLoadTests
{
    private static void StubDiagnoseShards(ILattice lattice, params ShardDiagnosticReport[] shards)
        => lattice.DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeDiagnosticReport
            {
                ShardCount = shards.Length,
                TotalLiveKeys = shards.Sum(s => s.LiveKeys),
                TotalTombstones = shards.Sum(s => s.Tombstones),
                Shards = [.. shards],
            });

    [Test]
    public void BeginBulkLoadAsync_on_a_tree_with_an_unsampled_shard_fails_closed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnoseShards(
            lattice,
            new ShardDiagnosticReport { ShardIndex = 0, Depth = 1, RootIsLeaf = true },
            new ShardDiagnosticReport { ShardIndex = 1, SampleFailed = true });
        var facade = Create(factory);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await facade.BeginBulkLoadAsync(Tree, Op));
        Assert.That(ex!.Message, Does.Contain("shard 1"));
    }

    [Test]
    public void BeginBulkLoadAsync_reports_TreeNotEmpty_when_a_sampled_shard_holds_data_even_if_another_failed()
    {
        // A definite answer from a healthy shard wins over an unknown one: the tree
        // is provably non-empty, which is the typed, non-retryable outcome.
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnoseShards(
            lattice,
            new ShardDiagnosticReport { ShardIndex = 0, SampleFailed = true },
            new ShardDiagnosticReport { ShardIndex = 1, Depth = 1, RootIsLeaf = true, LiveKeys = 5 });
        var facade = Create(factory);

        Assert.That(async () => await facade.BeginBulkLoadAsync(Tree, Op),
            Throws.TypeOf<TreeNotEmptyException>());
    }

    [Test]
    public async Task BeginBulkLoadAsync_admits_a_tree_whose_every_shard_was_sampled_empty()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        StubDiagnoseShards(
            lattice,
            new ShardDiagnosticReport { ShardIndex = 0, Depth = 1, RootIsLeaf = true },
            new ShardDiagnosticReport { ShardIndex = 1, Depth = 1, RootIsLeaf = true });
        var facade = Create(factory);

        var session = await facade.BeginBulkLoadAsync(Tree, Op);

        Assert.That(session.OperationId, Is.EqualTo(Op));
    }

    [Test]
    public async Task BeginBulkLoadAsync_tolerates_a_report_carrying_no_shard_entries()
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Wire(factory);
        lattice.DiagnoseAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeDiagnosticReport { Shards = ImmutableArray<ShardDiagnosticReport>.Empty });
        var facade = Create(factory);

        var session = await facade.BeginBulkLoadAsync(Tree, Op);

        Assert.That(session.TreeId, Is.EqualTo(Tree));
    }
}
