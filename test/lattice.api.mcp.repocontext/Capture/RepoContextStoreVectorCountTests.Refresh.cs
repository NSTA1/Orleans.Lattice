using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

public sealed partial class RepoContextStoreVectorCountTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task An_older_background_scan_cannot_replace_a_newer_completed_snapshot(bool membershipChanges)
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        using var gate = new MembershipScanGate(harness.GrainFactory);
        var writer = CreateWriter(harness, gate.Factory);
        var store = ActivatorUtilities.CreateInstance<RepoContextStore>(harness.Services, writer);
        await SeedRepoMarkerAsync(harness, RepoId, Ct);
        await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/A.cs") }, Ct);

        // This is the startup reporter's public call path: it discovers a marker
        // and schedules a scan before the fixture's explicit priming scan.
        gate.Arm();
        var first = (await store.ListReposAsync(Ct)).Repos.Single(r => r.RepoId == RepoId);
        await gate.Entered.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        var pending = writer.PendingEmbeddedCountRefresh(RepoId);
        Assert.That(pending, Is.Not.Null, "The listing must have started the held background scan.");
        Assert.That(first.EmbeddedVectorCountPending, Is.True);

        if (membershipChanges)
        {
            await writer.AddMembersAsync(RepoId, new[] { RepoContextKeys.File(RepoId, "src/B.cs") }, Ct);
        }

        await writer.ScanEmbeddedAsync(RepoId, Ct);
        var newer = writer.ReadEmbeddedCount(RepoId);
        gate.Release();
        await pending!.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        var served = (await store.ListReposAsync(Ct)).Repos.Single(r => r.RepoId == RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(newer.Pending, Is.False);
            Assert.That(served.EmbeddedVectorCount, Is.EqualTo(membershipChanges ? 2 : 1),
                "Completing an older walk must not roll the served count backwards.");
            Assert.That(served.EmbeddedVectorCountPending, Is.False,
                "An older generation must not overwrite the current completed snapshot.");
            Assert.That(served.EmbeddedVectorCountAsOf, Is.EqualTo(newer.AsOf),
                "Even at the same generation, a late old walk must not roll snapshot age backwards.");
        });
    }

    private static RepoContextVectorWriter CreateWriter(
        RepoContextMcpHarness harness, IGrainFactory factory, Func<Func<Task>, Task>? scheduleCountRefresh = null)
        => new(
            factory,
            harness.Services.GetRequiredService<Serializer>(),
            harness.Services.GetRequiredService<ILatticeReplicationContext>(),
            new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
            harness.Services.GetRequiredService<RepoContextVectorPlaneReDeriver>(),
            scheduleCountRefresh: scheduleCountRefresh);

    private sealed class MembershipScanGate : IDisposable
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public MembershipScanGate(IGrainFactory real)
        {
            var membership = real.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
            var held = Substitute.For<ILattice>();
            // raw-enumeration-ok: substitute the raw primitive used by ScanEntriesAsync;
            // the backing read still goes through the real resilient scan.
            held.EntriesAsync(default, default, default, default, default).ReturnsForAnyArgs(call =>
                HoldAsync(membership.ScanEntriesAsync(
                    call.ArgAt<string?>(0), call.ArgAt<string?>(1), call.ArgAt<bool>(2),
                    call.ArgAt<bool?>(3), cancellationToken: call.ArgAt<CancellationToken>(4)),
                    call.ArgAt<CancellationToken>(4)));

            Factory = Substitute.For<IGrainFactory>();
            Factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
                call.ArgAt<string>(0) == RepoContextTrees.VectorMembership
                && Interlocked.Exchange(ref _armed, 0) == 1
                    ? held
                    : real.GetGrain<ILattice>(call.ArgAt<string>(0)));
        }

        public IGrainFactory Factory { get; }
        public Task Entered => _entered.Task;
        public void Arm() => Volatile.Write(ref _armed, 1);
        public void Release() => _release.TrySetResult();
        public void Dispose() => Release();

        private async IAsyncEnumerable<KeyValuePair<string, byte[]>> HoldAsync(
            IAsyncEnumerable<KeyValuePair<string, byte[]>> source,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var entries = new List<KeyValuePair<string, byte[]>>();
            await foreach (var entry in source.WithCancellation(ct))
            {
                entries.Add(entry);
            }

            _entered.TrySetResult();
            await _release.Task.WaitAsync(ct);
            foreach (var entry in entries)
            {
                yield return entry;
            }
        }
    }
}
