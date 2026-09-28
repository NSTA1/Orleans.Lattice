using System.Text.Json;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    [TestCase("logical")]
    [TestCase("physical")]
    [TestCase("owner")]
    public async Task SetAliasAsync_lifecycle_refusal_precedes_ownership_lookup(string endpoint)
    {
        var factory = Substitute.For<IGrainFactory>();
        var tree = Substitute.For<ISystemLattice>();
        tree.GetAsync("physical").Returns(JsonSerializer.SerializeToUtf8Bytes(
            new TreeRegistryEntry { DerivedFrom = "owner" }));
        factory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(tree);
        var deleted = Substitute.For<ITreeDeletionGrain>();
        var refusal = new InvalidOperationException("deleted endpoint");
        deleted.EnsureAliasWritableAsync().ThrowsAsync(refusal);
        factory.GetGrain<ITreeDeletionGrain>(endpoint).Returns(deleted);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var guard = Substitute.For<ITreeOwnershipGuard>();
        var grain = new LatticeRegistryGrain(factory, options, ownershipGuard: guard);

        Assert.That(Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SetAliasAsync("logical", "physical")), Is.SameAs(refusal));

        Assert.That(guard.ReceivedCalls(), Is.Empty);
        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task SetAliasAsync_denied_ownership_writes_and_publishes_nothing_even_for_system_origin(bool systemOrigin)
    {
        var guard = Substitute.For<ITreeOwnershipGuard>();
        guard.AuthorizeAliasAsync("logical", "physical", "owner", Arg.Any<CancellationToken>())
            .Returns(new ValueTask<TreeOwnershipDecision>(TreeOwnershipDecision.Deny("different owner")));
        var (grain, tree, observer) = CreateGrainWithAliasObserver(ownershipGuard: guard);
        tree.GetAsync("physical").Returns(JsonSerializer.SerializeToUtf8Bytes(
            new TreeRegistryEntry { DerivedFrom = "owner" }));
        using var scope = systemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;

        var error = Assert.ThrowsAsync<LatticeTreeOwnershipDeniedException>(
            () => grain.SetAliasAsync("logical", "physical"));

        Assert.That(error!.Reason, Is.EqualTo("different owner"));
        await guard.Received(1).AuthorizeAliasAsync("logical", "physical", "owner", Arg.Any<CancellationToken>());
        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        Assert.That(observer.Changes, Is.Empty);
    }

    [TestCase(null)]
    [TestCase("logical")]
    public async Task SetAliasAsync_allow_receives_authoritative_derivation_then_writes_and_publishes(string? derivedFrom)
    {
        var guard = Substitute.For<ITreeOwnershipGuard>();
        guard.AuthorizeAliasAsync("logical", "physical", derivedFrom, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<TreeOwnershipDecision>(TreeOwnershipDecision.Allow()));
        var (grain, tree, observer) = CreateGrainWithAliasObserver(ownershipGuard: guard);
        tree.GetAsync("logical").Returns((byte[]?)null);
        tree.GetAsync("physical").Returns(JsonSerializer.SerializeToUtf8Bytes(
            new TreeRegistryEntry { DerivedFrom = derivedFrom }));

        await grain.SetAliasAsync("logical", "physical");

        await guard.Received(1).AuthorizeAliasAsync("logical", "physical", derivedFrom, Arg.Any<CancellationToken>());
        await tree.Received(1).SetAsync("logical", Arg.Any<byte[]>());
        Assert.That(observer.Changes, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task SetAliasAsync_default_decision_denies_an_unregistered_target()
    {
        var guard = Substitute.For<ITreeOwnershipGuard>();
        var (grain, tree, observer) = CreateGrainWithAliasObserver(ownershipGuard: guard);
        tree.GetAsync("physical").Returns((byte[]?)null);

        var error = Assert.ThrowsAsync<LatticeTreeOwnershipDeniedException>(
            () => grain.SetAliasAsync("logical", "physical"));

        Assert.That(error!.Reason, Does.Contain("did not allow"));
        await guard.Received(1).AuthorizeAliasAsync("logical", "physical", null, Arg.Any<CancellationToken>());
        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        Assert.That(observer.Changes, Is.Empty);
    }

    [Test]
    public async Task SetAliasAsync_guard_failure_propagates_without_writes_or_notifications()
    {
        var guard = Substitute.For<ITreeOwnershipGuard>();
        var failure = new InvalidOperationException("ownership store unavailable");
        guard.AuthorizeAliasAsync("logical", "physical", null, Arg.Any<CancellationToken>())
            .Returns(new ValueTask<TreeOwnershipDecision>(Task.FromException<TreeOwnershipDecision>(failure)));
        var (grain, tree, observer) = CreateGrainWithAliasObserver(ownershipGuard: guard);
        tree.GetAsync("physical").Returns((byte[]?)null);

        Assert.That(Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SetAliasAsync("logical", "physical")), Is.SameAs(failure));

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
        Assert.That(observer.Changes, Is.Empty);
    }

    [Test]
    public void SetAliasAsync_namespace_refusal_precedes_ownership_lookup()
    {
        var guard = Substitute.For<ITreeOwnershipGuard>();
        var (grain, _, _) = CreateGrainWithAliasObserver(ownershipGuard: guard);

        Assert.ThrowsAsync<ArgumentException>(() => grain.SetAliasAsync("logical", "sys-private"));

        Assert.That(guard.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void SetAliasAsync_target_control_refusal_precedes_ownership_lookup()
    {
        var gate = Substitute.For<ILatticeAccessGate>();
        gate.AuthorizeAsync(Arg.Any<LatticeAccessRequest>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Deny("not controlled")));
        var guard = Substitute.For<ITreeOwnershipGuard>();
        var (grain, _, _) = CreateGrainWithAliasObserver(ownershipGuard: guard, accessGate: gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(() => grain.SetAliasAsync("logical", "physical"));

        Assert.That(guard.ReceivedCalls(), Is.Empty);
    }
}
