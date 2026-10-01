using System.Collections.Concurrent;
using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue 3923, end to end on a real silo scheduler: an undo must be admitted and
/// acknowledged while a resize phase holds the coordinator's turn, and the resize
/// must then unwind to its pre-resize state.
/// <para>
/// The phase is pinned deterministically: an incoming-call filter holds the
/// snapshot slice the resize's phase timer drives, so the non-reentrant resize
/// coordinator sits inside <c>WaitForSnapshotAsync</c> for as long as the test
/// chooses - the exact shape of the incident, where a slow snapshot pass held the
/// turn and every undo timed out behind it. Before the fix the undo, and every
/// status read, queued behind that turn.
/// </para>
/// <para>
/// Each answer is proven with <see cref="InterleaveProbe"/>: the slice is released
/// only after the undo and the status reads have answered, so the claim is never
/// decided by elapsed time (issue #4142).
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TreeResizeUndoInFlightIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        SliceGate.ReleaseAll();
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task An_undo_issued_while_a_snapshot_slice_holds_the_coordinator_is_accepted_while_held_and_unwinds()
    {
        var treeId = $"undo-in-flight-{Guid.NewGuid():N}";
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var expected = new Dictionary<string, string>();
        for (var i = 0; i < 24; i++)
        {
            var key = $"k{i:D3}";
            expected[key] = $"v{i}";
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(expected[key]));
        }

        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var sizingBefore = await registry.GetEntryAsync(treeId);

        var hold = SliceGate.Arm(treeId);
        try
        {
            await tree.ResizeAsync(64, 64);
            await hold.Entered.Task.WaitAsync(InterleaveProbe.HangBound);

            // The phase loop is now inside a snapshot slice it cannot leave, and it
            // stays there until the finally below. The undo accepts the intent and
            // then waits out its own budget for an unwind that cannot start while
            // the slice is held, so it returns - still unwinding - only if both the
            // accept and its progress reads are admitted past the held turn.
            await InterleaveProbe.AnswersWhileHeldAsync(tree.UndoResizeAsync(), hold.Release.Task,
                "the undo");

            var accepted = await InterleaveProbe.AnswersWhileHeldAsync(tree.IsResizeUndoPendingAsync(),
                hold.Release.Task, "the undo-requested status read");
            var complete = await InterleaveProbe.AnswersWhileHeldAsync(tree.IsResizeCompleteAsync(),
                hold.Release.Task, "the resize status read");
            Assert.Multiple(() =>
            {
                Assert.That(accepted, Is.True,
                    "the undo must be admitted and recorded while the phase still holds the coordinator's turn");
                Assert.That(complete, Is.False, "an accepted, unwinding undo is not a finished resize");
            });
        }
        finally
        {
            hold.Release.TrySetResult();
        }

        // Once the slice returns, the loop observes the intent at the slice
        // boundary and runs the phase-aware unwind.
        await TestPoll.UntilAsync(
            async () => await tree.IsResizeCompleteAsync() && !await tree.IsResizeUndoPendingAsync(),
            "the accepted undo to finish unwinding",
            timeout: TimeSpan.FromSeconds(30));

        var sizingAfter = await registry.GetEntryAsync(treeId);
        var resolved = await registry.ResolveAsync(treeId);
        var reads = new Dictionary<string, string?>();
        foreach (var key in expected.Keys)
        {
            var read = await tree.GetAsync(key);
            reads[key] = read is null ? null : Encoding.UTF8.GetString(read);
        }

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.EqualTo(treeId),
                "the logical tree must map back to its pre-resize physical tree");
            Assert.That(sizingAfter?.MaxLeafKeys, Is.EqualTo(sizingBefore?.MaxLeafKeys));
            Assert.That(sizingAfter?.MaxInternalChildren, Is.EqualTo(sizingBefore?.MaxInternalChildren));
            Assert.That(reads, Is.EquivalentTo(expected.Select(e => new KeyValuePair<string, string?>(e.Key, e.Value))));
        });

        // A retry after the unwind is told the undo already succeeded.
        var retry = Assert.ThrowsAsync<InvalidOperationException>(() => tree.UndoResizeAsync());
        Assert.That(retry!.Message, Does.Contain("was already undone at"));
    }

    /// <summary>
    /// Holds <see cref="ITreeSnapshotGrain.RunSnapshotSliceAsync"/> for an armed
    /// tree until the test releases it, keeping the resize coordinator inside the
    /// phase turn that awaits the slice.
    /// </summary>
    private sealed class SliceGate : IIncomingGrainCallFilter
    {
        private static readonly ConcurrentDictionary<string, Hold> Holds = new(StringComparer.Ordinal);

        internal static Hold Arm(string treeId) => Holds.GetOrAdd(treeId, static _ => new Hold());

        internal static void ReleaseAll()
        {
            foreach (var hold in Holds.Values) hold.Release.TrySetResult();
        }

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.MethodName == nameof(ITreeSnapshotGrain.RunSnapshotSliceAsync)
                && Holds.TryGetValue(context.TargetId.Key.ToString()!, out var hold))
            {
                hold.Entered.TrySetResult();
                await hold.Release.Task;
            }

            await context.Invoke();
        }
    }

    private sealed class Hold
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddIncomingGrainCallFilter<SliceGate>();
        }
    }
}
