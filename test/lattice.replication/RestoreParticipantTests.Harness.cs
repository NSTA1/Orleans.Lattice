using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.Core;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Shared substitute-based harness for the focused <see cref="RestoreParticipant"/>
/// coverage fixtures split across the <c>RestoreParticipantTests.*</c> partials.
/// The existing <see cref="RestoreParticipantTests"/> primary file drives the happy
/// single-tree paths through the in-memory <c>FakeCoordinatedRestoreEngine</c>; the
/// partials below inject faults (cancellation, engine-side throws, an unwired
/// engine, and the group-atomic set path) that the fake cannot express, so an
/// NSubstitute engine and restore service give per-call control. These helpers keep
/// each test terse while wiring the eight-seam constructor consistently.
/// </summary>
public partial class RestoreParticipantTests
{
    /// <summary>A fully wired, always-succeeding coordinated restore engine substitute.</summary>
    private static ILatticeCoordinatedRestoreEngine HealthyEngine(string defaultTree = TargetTree)
    {
        var engine = Substitute.For<ILatticeCoordinatedRestoreEngine>();
        engine.ProbeAdmissionAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(ReportFor(TreeOf(ci, defaultTree))));
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(ResultFor(TreeOf(ci, defaultTree))));
        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        engine.DeleteShadowAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        engine.ResolveShadowTreeId(Arg.Any<LatticeRestoreRequest>())
            .Returns(ci => TreeOf(ci, defaultTree) + "-shadow");
        return engine;
    }

    /// <summary>A capacity probe that always answers with <paramref name="canHost"/>.</summary>
    private static IRestoreCapacityProbe Capacity(bool canHost)
    {
        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(canHost));
        return capacity;
    }

    /// <summary>A capacity probe that admits every tree except <paramref name="refusedTree"/>.</summary>
    private static IRestoreCapacityProbe CapacityExcept(string refusedTree)
    {
        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(((RestoreAdmissionReport)ci[0]).TargetTreeId != refusedTree));
        return capacity;
    }

    /// <summary>A grain factory that resolves the saga write-fence grain to <paramref name="fence"/>.</summary>
    private static IGrainFactory FactoryFor(ISagaWriteFenceGrain fence)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(fence);
        return factory;
    }

    /// <summary>A set resolver that expands any set id into <paramref name="members"/>.</summary>
    private static ILatticeBackupSetResolver ResolverFor(params BackupSetMember[] members)
    {
        var resolver = Substitute.For<ILatticeBackupSetResolver>();
        resolver.ResolveMembersAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<BackupSetMember>>([.. members]));
        return resolver;
    }

    /// <summary>An options monitor whose current value reports <paramref name="clusterId"/>.</summary>
    private static IOptionsMonitor<LatticeReplicationOptions> OptionsFor(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    /// <summary>A membership seam reporting exactly <paramref name="replicated"/> as replicated trees.</summary>
    private static IReplicatedTreeMembership MembershipFor(params string[] replicated)
    {
        var membership = Substitute.For<IReplicatedTreeMembership>();
        var set = new HashSet<string>(replicated, StringComparer.Ordinal);
        membership.IsReplicated(Arg.Any<string>()).Returns(ci => set.Contains((string)ci[0]));
        return membership;
    }

    private static RestoreParticipant Participant(
        ILatticeCoordinatedRestoreEngine? engine,
        IGrainFactory factory,
        IRestoreCapacityProbe? capacity = null,
        ILatticeBackupRestoreService? restoreService = null,
        ILatticeBackupSetResolver? setResolver = null,
        IReplicatedTreeMembership? membership = null,
        IOptionsMonitor<LatticeReplicationOptions>? options = null) =>
        new(
            engine,
            restoreService,
            capacity ?? Capacity(true),
            factory,
            NullLogger<RestoreParticipant>.Instance,
            setResolver,
            membership,
            options);

    private static RestoreAdmissionReport ReportFor(string treeId) =>
        new(
            backupId: "bkp-" + treeId,
            targetTreeId: treeId,
            totalByteLength: 1024,
            totalChunkCount: 1,
            shardCount: 1,
            manifestChain: ["bkp-" + treeId]);

    private static LatticeRestoreResult ResultFor(string treeId, string? shadowId = null) =>
        new(
            backupId: "bkp-" + treeId,
            targetTreeId: treeId,
            mode: LatticeRestoreMode.ShadowCutover,
            operationId: "op-" + treeId,
            manifestChain: ["bkp-" + treeId],
            entriesApplied: 0,
            shadowPhysicalTreeId: shadowId ?? treeId + "-shadow",
            previousPhysicalTreeId: treeId);

    private static string TreeOf(CallInfo ci, string fallback) =>
        ((LatticeRestoreRequest)ci[0]).TargetTreeId ?? fallback;

    private static SagaControlRequest SetRequestFor(
        string setId, string coordinator = CoordinatorCluster) =>
        new()
        {
            SagaId = setId,
            TargetTree = setId,
            ManifestId = setId,
            CoordinatorClusterId = coordinator,
            SetId = setId,
        };
}
