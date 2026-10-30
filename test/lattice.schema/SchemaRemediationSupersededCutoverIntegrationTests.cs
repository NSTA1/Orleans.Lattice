using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #5289: a schema remediation published its transformed copy of tree T over an
/// alias change an administrator made while it ran, so the admin's publication was
/// lost and readers got a tree that no single physical tree ever served. An explicit
/// alias change is now refused while the remediation holds T's alias reservation, and
/// the cutover publishes only while T still resolves to the physical tree the
/// remediation captured (or, on a resume after its swap, to its own destination);
/// otherwise it refuses with no swap, discards its destination, releases the
/// reservation and ends with a terminal abort naming the supersession.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationSupersededCutoverIntegrationTests
{
    private const int KeyCount = 600;
    private const string OperationId = "op";

    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeRegistry Registry => Grains.GetLatticeRegistry();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string Key(int i) => $"k-{i:D4}";

    private static LatticeSchemaPolicy JsonPolicy() => new([LatticeSchemaRule.Json()]);

    [Test]
    public async Task Cutover_after_the_alias_moved_to_another_tree_is_refused_with_no_swap_and_the_new_alias_intact()
    {
        var (treeId, other) = await SeedAsync("superseded");
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        var destination = await DriveThroughFirstBuildSliceAsync(remediation);

        // The publication an explicit alias change makes (the swap
        // AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync performs), landing after
        // the remediation captured its source - as one that passed its reservation
        // check before the remediation reserved, or one from a silo that predates it.
        await MoveAliasAsync(treeId, other);

        var terminal = await DriveToTerminalAsync(remediation);
        var counts = await CountSourcesAsync(treeId);
        var policy = await _fixture.SiloServices.GetRequiredService<ILatticeSchemaPolicyStore>().GetPolicyAsync(treeId);
        var destinationDeleted = await Grains.GetGrain<ITreeDeletionGrain>(destination).IsDeletedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(terminal.InProgress, Is.False, "the remediation is terminal");
            Assert.That(terminal.Succeeded, Is.False, "a superseded publication must not report a cutover");
            Assert.That(terminal.DidAbort, Is.True, "the refusal is an abort");
            Assert.That(terminal.OffendingKey, Is.Null, "no value is at fault");
            Assert.That(terminal.Reason ?? string.Empty, Does.Contain("superseded").And.Contain(other), terminal.Reason);
            Assert.That(counts, Is.EqualTo(("X", KeyCount)), "the tree serves the tree the administrator aliased, whole");
            Assert.That(policy, Is.Null, "no target policy was installed over the administrator's tree");
            Assert.That(destinationDeleted, Is.True, "the unpublished destination is discarded");
        });
        Assert.That(await Registry.ResolveAsync(treeId), Is.EqualTo(other), "the administrator's alias is intact");
        await AssertReservationReleasedAsync(treeId);
    }

    [Test]
    public async Task An_explicit_alias_while_a_remediation_holds_the_reservation_is_refused_naming_it()
    {
        var (treeId, other) = await SeedAsync("reserved");
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        await DriveThroughFirstBuildSliceAsync(remediation);
        var lineage = (await Registry.GetEntryAsync(treeId))?.Lineage;

        // The exact call LatticeTreeAdmin.SetTreeAliasAsync makes (the #5289 repro).
        var refused = Assert.ThrowsAsync<InvalidOperationException>(() =>
            AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, treeId, other));

        Assert.Multiple(async () =>
        {
            Assert.That(refused!.Message, Does.Contain(treeId).And.Contain("remediation:"), refused.Message);
            Assert.That(await Registry.ResolveAsync(treeId), Is.EqualTo(treeId), "the refused alias moved nothing");
            Assert.That((await Registry.GetEntryAsync(treeId))?.Lineage, Is.EqualTo(lineage),
                "the refusal precedes the lineage re-stamp");
        });

        var terminal = await DriveToTerminalAsync(remediation);
        Assert.That(terminal.Succeeded, Is.True, "the remediation goes on to cut over the tree it captured");
        Assert.That(await CountSourcesAsync(treeId), Is.EqualTo(("T", KeyCount)));
        await AssertReservationReleasedAsync(treeId);
    }

    [Test]
    public async Task Cutover_resumed_after_its_swap_still_completes()
    {
        var (treeId, _) = await SeedAsync("resumed");
        var remediation = Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
        await DriveThroughFirstBuildSliceAsync(remediation);
        LatticeSchemaRemediationReport status;
        do
        {
            status = (await remediation.RunSliceAsync(OperationId)).Report;
        }
        while (status.Phase != LatticeSchemaRemediationPhase.Cutover);

        // The swap a cutover performs before it records completion: a silo lost
        // after it resumes the remediation with the alias already on the destination.
        var destination = status.DestinationTreeId!;
        await AliasCutoverShardMaps.PrepareCutoverResolvingAsync(Grains, treeId, destination);
        await AliasCutoverShardMaps.SwapCutoverAsync(Grains, treeId, destination);

        var terminal = await DriveToTerminalAsync(remediation);

        Assert.Multiple(async () =>
        {
            Assert.That(terminal.Succeeded, Is.True, terminal.Reason);
            Assert.That(await Registry.ResolveAsync(treeId), Is.EqualTo(destination));
            Assert.That(await CountSourcesAsync(treeId), Is.EqualTo(("T", KeyCount)));
        });
        await AssertReservationReleasedAsync(treeId);
    }

    /// <summary>
    /// Writes <see cref="KeyCount"/> keys to a tree T and to a second tree X, each
    /// value naming its tree.
    /// </summary>
    private async Task<(string TreeId, string Other)> SeedAsync(string prefix)
    {
        var treeId = $"{prefix}-t-{Guid.NewGuid():N}";
        var other = $"{prefix}-x-{Guid.NewGuid():N}";
        await WriteAsync(treeId, "T");
        await WriteAsync(other, "X");
        return (treeId, other);
    }

    private async Task WriteAsync(string treeId, string source)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
        var value = Encoding.UTF8.GetBytes($"{{\"src\":\"{source}\"}}");
        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync(Key(i), value);
        }
    }

    /// <summary>
    /// Accepts a passthrough remediation and slices it through the dry run and the
    /// first build slice, returning its destination tree id.
    /// </summary>
    private static async Task<string> DriveThroughFirstBuildSliceAsync(ILatticeSchemaRemediationGrain remediation)
    {
        await remediation.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), OperationId);
        LatticeSchemaRemediationReport status;
        do
        {
            status = (await remediation.RunSliceAsync(OperationId)).Report;
        }
        while (status.Phase == LatticeSchemaRemediationPhase.DryRun);

        Assert.That(status.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build), "precondition: the dry run passed");
        status = (await remediation.RunSliceAsync(OperationId)).Report;
        Assert.That(status.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Build),
            "precondition: the build is part way through");
        return status.DestinationTreeId!;
    }

    private static async Task<LatticeSchemaRemediationReport> DriveToTerminalAsync(ILatticeSchemaRemediationGrain remediation)
    {
        LatticeSchemaRemediationReport status;
        do
        {
            status = (await remediation.RunSliceAsync(OperationId)).Report;
        }
        while (status.InProgress);

        return status;
    }

    private async Task MoveAliasAsync(string treeId, string other)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var target = await Registry.GetEntryAsync(other);
            await Registry.SwapAliasAsync(
                treeId,
                other,
                AliasCutoverShardMaps.EffectiveMap(target),
                target?.NextShardIndex,
                expectedPhysicalTreeId: treeId);
        }
    }

    /// <summary>
    /// Reads every key through the logical tree and returns the one tree every value
    /// came from with the number read, or a mixed marker when the values disagree.
    /// </summary>
    private async Task<(string Source, int Count)> CountSourcesAsync(string treeId)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var sources = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < KeyCount; i++)
        {
            var value = await tree.GetAsync(Key(i));
            var source = value is null ? "absent" : Encoding.UTF8.GetString(value).Contains("\"X\"") ? "X" : "T";
            sources[source] = sources.GetValueOrDefault(source) + 1;
        }

        return sources.Count == 1
            ? (sources.Keys.Single(), sources.Values.Single())
            : (string.Join(",", sources.Select(p => $"{p.Key}:{p.Value}")), KeyCount);
    }

    /// <summary>
    /// The reservation is released when another alias operation can take it.
    /// </summary>
    private async Task AssertReservationReleasedAsync(string treeId)
    {
        var deletion = Grains.GetGrain<ITreeDeletionGrain>(treeId);
        await deletion.BeginAliasChangeAsync("probe");
        await deletion.EndAliasChangeAsync("probe");
    }
}
