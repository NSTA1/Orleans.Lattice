using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaAdmin"/>: delegation to the policy /
/// dead-letter stores, eager provider-cache eviction on policy change, and
/// parameter guards.
/// </summary>
public class LatticeSchemaAdminTests
{
    private static (LatticeSchemaAdmin Admin, ILatticeSchemaPolicyStore PolicyStore, ILatticeSchemaDeadLetterStore Dlq, ILatticeSchemaPolicyProvider Provider) Create()
    {
        var policyStore = Substitute.For<ILatticeSchemaPolicyStore>();
        var dlq = Substitute.For<ILatticeSchemaDeadLetterStore>();
        var provider = Substitute.For<ILatticeSchemaPolicyProvider>();
        return (new LatticeSchemaAdmin(policyStore, dlq, provider), policyStore, dlq, provider);
    }

    private static async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> Entries(params LatticeSchemaDeadLetterEntry[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }

    [Test]
    public async Task SetPolicyAsync_delegates_and_invalidates_cache()
    {
        var (admin, policyStore, _, provider) = Create();
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());

        await admin.SetPolicyAsync("orders", policy);

        await policyStore.Received(1).SetPolicyAsync("orders", policy, Arg.Any<CancellationToken>());
        provider.Received(1).Invalidate("orders");
    }

    [Test]
    public async Task ClearPolicyAsync_delegates_and_invalidates_cache()
    {
        var (admin, policyStore, _, provider) = Create();
        policyStore.ClearPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(true);

        var removed = await admin.ClearPolicyAsync("orders");

        Assert.That(removed, Is.True);
        await policyStore.Received(1).ClearPolicyAsync("orders", Arg.Any<CancellationToken>());
        provider.Received(1).Invalidate("orders");
    }

    [Test]
    public async Task GetPolicyAsync_delegates_to_store()
    {
        var (admin, policyStore, _, provider) = Create();
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());
        policyStore.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(policy);

        Assert.That(await admin.GetPolicyAsync("orders"), Is.SameAs(policy));
        provider.DidNotReceive().Invalidate(Arg.Any<string>());
    }

    [Test]
    public async Task ListDeadLettersAsync_delegates_to_store()
    {
        var (admin, _, dlq, _) = Create();
        var entry = new LatticeSchemaDeadLetterEntry(
            "k1", Array.Empty<byte>(), 0, "r", LatticeSchemaDeadLetterSource.Replication, DateTimeOffset.UtcNow);
        dlq.ListAsync("orders", Arg.Any<CancellationToken>()).Returns(Entries(entry));

        var list = new List<LatticeSchemaDeadLetterEntry>();
        await foreach (var e in admin.ListDeadLettersAsync("orders"))
        {
            list.Add(e);
        }

        Assert.That(list, Has.Count.EqualTo(1));
        Assert.That(list[0].Key, Is.EqualTo("k1"));
    }

    [Test]
    public async Task CountDeadLettersAsync_delegates_to_store()
    {
        var (admin, _, dlq, _) = Create();
        dlq.CountAsync("orders", Arg.Any<CancellationToken>()).Returns(3);

        Assert.That(await admin.CountDeadLettersAsync("orders"), Is.EqualTo(3));
    }

    [Test]
    public void SetPolicyAsync_null_or_empty_arguments_throw()
    {
        var (admin, _, _, _) = Create();
        var policy = new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>());

        Assert.ThrowsAsync<ArgumentException>(() => admin.SetPolicyAsync(string.Empty, policy));
        Assert.ThrowsAsync<ArgumentNullException>(() => admin.SetPolicyAsync("orders", null!));
    }

    [Test]
    public void Read_verbs_reject_empty_tree_id()
    {
        var (admin, _, _, _) = Create();
        Assert.ThrowsAsync<ArgumentException>(() => admin.ClearPolicyAsync(string.Empty));
        Assert.That(() => admin.GetPolicyAsync(string.Empty), Throws.ArgumentException);
        Assert.That(() => admin.CountDeadLettersAsync(string.Empty), Throws.ArgumentException);
        Assert.That(() => admin.ListDeadLettersAsync(string.Empty), Throws.ArgumentException);
    }
}
