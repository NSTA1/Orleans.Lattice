using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The coordinator half of the durable WAL move fence (issue #4525): the move
/// raises its fence before it quiesces anything, renews it before every
/// convergence re-quiesce, flips only through the fenced compare-and-swap under
/// its own move id, and on every abort releases the fence before it deactivates
/// the source. The registry half is covered by
/// <c>LatticeRegistryGrainTests.WalMoveFence</c> and the end-to-end interleaving by
/// <c>WalMoveDurableFenceIntegrationTests</c>.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    private static List<string> RecordFenceEvents(Harness harness)
    {
        var events = new List<string>();
        harness.Registry
            .When(r => r.RaiseWalMoveFencesAsync(TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<int>>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<bool>()))
            .Do(ci => events.Add(ci.ArgAt<bool>(5) ? "renew" : $"raise@{harness.QuiesceCalls}"));
        harness.Registry
            .When(r => r.ReleaseWalMoveFenceAsync(TreeId, Arg.Any<int>(), Arg.Any<string>(), Arg.Any<bool>()))
            .Do(_ => events.Add("release"));
        harness.Wal
            .When(w => w.DeactivateForMoveAsync(Arg.Any<CancellationToken>()))
            .Do(_ => events.Add("deactivate"));
        return events;
    }

    private static string RaisedMoveId(Harness harness) =>
        (string)harness.Registry.ReceivedCalls()
            .First(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.RaiseWalMoveFencesAsync))
            .GetArguments()[3]!;

    [Test]
    public async Task A_move_raises_its_fence_before_the_first_quiesce_renews_it_and_flips_under_the_same_move_id()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        var events = RecordFenceEvents(harness);

        await Admin(harness).ExecuteWalMoveAsync(TreeId, 0, SecondaryKey);

        var moveId = RaisedMoveId(harness);
        Assert.Multiple(() =>
        {
            Assert.That(events[0], Is.EqualTo("raise@0"), "the durable fence is raised before any quiesce");
            Assert.That(events, Does.Contain("renew"), "the fence is renewed before the convergence re-quiesce");
            Assert.That(events, Does.Not.Contain("release"), "a successful move leaves the release to the flip");
        });
        await harness.Registry.Received(1).FlipFencedWalPlacementAsync(
            TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<(int Partition, string ProviderKey)>>(), moveId);
    }

    [Test]
    public void A_move_whose_fence_was_released_before_the_renewal_aborts_without_flipping()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Registry
            .RaiseWalMoveFencesAsync(TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<int>>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), true)
            .ThrowsAsync(new InvalidOperationException("no longer holds its fence"));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, 0, SecondaryKey),
            Throws.InvalidOperationException.With.Message.Contains("no longer holds its fence"));
        Assert.That(PinWasFlipped(harness), Is.False);
        Assert.That(harness.DeactivateCalls, Is.GreaterThan(0));
    }

    [Test]
    public async Task A_refused_flip_releases_the_fence_and_the_source_and_surfaces_the_refusal()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Registry
            .FlipFencedWalPlacementAsync(TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<(int Partition, string ProviderKey)>>(), Arg.Any<string>())
            .ThrowsAsync(new InvalidOperationException("refused to flip"));
        var events = RecordFenceEvents(harness);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, 0, SecondaryKey),
            Throws.InvalidOperationException.With.Message.Contains("refused to flip"));

        await harness.Registry.Received().ReleaseWalMoveFenceAsync(TreeId, 0, RaisedMoveId(harness), false);
        Assert.That(events.SkipWhile(e => e != "release"), Does.Contain("deactivate"),
            "the source is deactivated after its fence is released");
    }

    [Test]
    public void An_aborted_move_releases_its_fence_before_it_deactivates_the_source()
    {
        var harness = CreateHarness();
        harness.QuiesceScript.Add(() => NotQuiesced(observedVersion: 42));
        var events = RecordFenceEvents(harness);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(events, Is.EqualTo(new[] { "raise@0", "release", "deactivate", "release", "deactivate" }),
            "the copy helper and the batch both release before deactivating; an activation that came up between "
            + "would otherwise stay fenced for the whole lease");
    }

    [Test]
    public void A_move_aborts_when_the_source_reports_its_drain_incomplete()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => new WalMoveQuiesceResult(
            false, -1, 0, IWalStorageProviderCatalog.DefaultProviderKey, DrainIncomplete: true));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, 0, SecondaryKey),
            Throws.InvalidOperationException.With.Message.Contains("drain budget"));
        Assert.That(PinWasFlipped(harness), Is.False);
    }

    [Test]
    public void A_move_aborts_when_the_source_durable_tail_passed_the_quiesced_tail()
    {
        var harness = CreateHarness();
        // The source holds offset 3, but every quiesce reports 2: an append the
        // final quiesce never saw. The direct re-read before the flip catches it.
        harness.Source.Seed(0, 1, 2, 3);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, 0, SecondaryKey, new WalMoveOptions { CopyPageSize = 1, VerifyAfterCopy = true }),
            Throws.InvalidOperationException.With.Message.Contains("after the final quiesce"));
        Assert.That(PinWasFlipped(harness), Is.False);
    }

    [Test]
    public async Task A_batch_move_fences_every_moved_partition_in_one_write_and_flips_under_its_move_id()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1);
        harness.QuiesceScript.Add(() => Quiesced(highest: 1));

        await Admin(harness).ExecuteWalMoveAsync(TreeId, [(0, SecondaryKey), (1, SecondaryKey)]);

        await harness.Registry.Received(1).RaiseWalMoveFencesAsync(
            TreeId, Arg.Any<long>(), Arg.Is<IReadOnlyCollection<int>>(p => p.Count == 2 && p.Contains(0) && p.Contains(1)),
            Arg.Any<string>(), Arg.Any<TimeSpan>(), false);
        await harness.Registry.Received(1).FlipFencedWalPlacementAsync(
            TreeId, Arg.Any<long>(), Arg.Any<IReadOnlyCollection<(int Partition, string ProviderKey)>>(), RaisedMoveId(harness));
    }
}
