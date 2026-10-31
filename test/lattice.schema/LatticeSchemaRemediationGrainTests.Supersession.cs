using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Schema.Tests;

public partial class LatticeSchemaRemediationGrainTests
{
    // Issue #5289: the cutover publishes only over the physical tree it captured.

    [Test]
    public async Task Cutover_superseded_before_it_starts_installs_no_policy_swaps_nothing_and_aborts_naming_it()
    {
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") });
        h.Registry.ResolveAsync(TreeId).Returns(TreeId);
        await h.Grain.AcceptAsync(LatticeValueTransform.Passthrough(), JsonPolicy(), "op");
        while (h.State.State.Phase != LatticeSchemaRemediationPhase.Cutover)
        {
            await h.Grain.RunSliceAsync("op");
        }

        h.Registry.ResolveAsync(TreeId).Returns("admin-copy");
        var report = (await h.Grain.RunSliceAsync("op")).Report;

        Assert.Multiple(async () =>
        {
            Assert.That(report.DidAbort, Is.True);
            Assert.That(report.OffendingKey, Is.Null);
            Assert.That(report.Reason, Does.Contain("'admin-copy'").And.Contain("superseded"));
            Assert.That(h.State.State.InProgress, Is.False);
            Assert.That(h.State.State.AliasReservationId, Is.Null, "the reservation is released");
            await h.PolicyStore.DidNotReceive().SetPolicyAsync(
                Arg.Any<string>(), Arg.Any<LatticeSchemaPolicy>(), Arg.Any<CancellationToken>());
            await h.Registry.DidNotReceive().SwapAliasAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
            await h.Destination.Received(1).DeleteTreeAsync();
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Cutover_superseded_at_its_swap_restores_the_prior_policy_and_aborts(bool hadPolicy)
    {
        var prior = hadPolicy ? MaxLenPolicy(100) : null;
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") }, existingPolicy: prior);
        var current = TreeId;
        h.Registry.ResolveAsync(TreeId).Returns(_ => Task.FromResult(current));
        h.Registry.SwapAliasAsync(TreeId, Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>())
            .Returns(_ =>
            {
                // A change that landed between the cutover's check and its swap: the
                // registry's expected-identity fence refuses the swap.
                current = "admin-copy";
                return Task.FromException<TreeRegistryEntry?>(new InvalidOperationException("moved"));
            });

        var report = await h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy());

        Assert.Multiple(async () =>
        {
            Assert.That(report.DidAbort, Is.True);
            Assert.That(report.OffendingKey, Is.Null);
            Assert.That(report.Reason, Does.Contain("'admin-copy'"));
            Assert.That(h.State.State.AliasReservationId, Is.Null, "the reservation is released");
            if (hadPolicy)
            {
                await h.PolicyStore.Received(1).SetPolicyAsync(TreeId, prior!, Arg.Any<CancellationToken>());
            }
            else
            {
                await h.PolicyStore.Received(1).ClearPolicyAsync(TreeId, Arg.Any<CancellationToken>());
            }

            await h.Destination.Received(1).DeleteTreeAsync();
        });
    }

    [Test]
    public void Cutover_swap_refused_while_still_on_the_captured_source_stays_in_cutover_for_retry()
    {
        // A refusal that is not a supersession - the tree still resolves to the
        // source - keeps the documented retry contract: in flight, reservation held.
        var h = CreateGrain(new[] { ("k1", "{\"a\":1}") });
        h.Registry.SwapAliasAsync(TreeId, Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>())
            .Returns(Task.FromException<TreeRegistryEntry?>(new InvalidOperationException("refused")));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Grain.StartAsync(LatticeValueTransform.Passthrough(), JsonPolicy()));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.InProgress, Is.True);
            Assert.That(h.State.State.Phase, Is.EqualTo(LatticeSchemaRemediationPhase.Cutover));
            Assert.That(h.State.State.AliasReservationId, Is.Not.Null);
        });
    }
}
