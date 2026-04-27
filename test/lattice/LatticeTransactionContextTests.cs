using NUnit.Framework;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTransactionContext"/> covering the
/// default-empty contract, <c>EnsureCurrent</c> mint-or-preserve semantics,
/// explicit <c>Set</c> overwrite, and the empty-Guid clear-on-set behaviour.
/// </summary>
[TestFixture]
public sealed class LatticeTransactionContextTests
{
    [SetUp]
    public void SetUp() => LatticeTransactionContext.Set(Guid.Empty);

    [TearDown]
    public void TearDown() => LatticeTransactionContext.Set(Guid.Empty);

    [Test]
    public void Current_returns_empty_when_context_unset()
    {
        Assert.That(LatticeTransactionContext.Current, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void EnsureCurrent_mints_fresh_guid_when_context_unset()
    {
        var minted = LatticeTransactionContext.EnsureCurrent();
        Assert.That(minted, Is.Not.EqualTo(Guid.Empty));
        Assert.That(LatticeTransactionContext.Current, Is.EqualTo(minted));
    }

    [Test]
    public void EnsureCurrent_preserves_existing_non_empty_guid()
    {
        var seeded = Guid.NewGuid();
        LatticeTransactionContext.Set(seeded);

        var observed = LatticeTransactionContext.EnsureCurrent();
        Assert.That(observed, Is.EqualTo(seeded));
    }

    [Test]
    public void EnsureCurrent_replaces_explicitly_empty_guid()
    {
        // An explicit Guid.Empty entry on RequestContext (which Set treats
        // as "remove") should still result in a freshly minted id.
        RequestContext.Set("ol.txid", Guid.Empty);
        var minted = LatticeTransactionContext.EnsureCurrent();
        Assert.That(minted, Is.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public void Set_overwrites_existing_value()
    {
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        LatticeTransactionContext.Set(first);
        LatticeTransactionContext.Set(second);
        Assert.That(LatticeTransactionContext.Current, Is.EqualTo(second));
    }

    [Test]
    public void Set_with_empty_guid_clears_context()
    {
        LatticeTransactionContext.Set(Guid.NewGuid());
        LatticeTransactionContext.Set(Guid.Empty);
        Assert.That(LatticeTransactionContext.Current, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void Default_LatticeMutation_has_empty_TransactionId()
    {
        // Wire-compat: legacy persisted observer payloads (and any caller
        // that constructs the struct without setting TransactionId) must
        // round-trip with Guid.Empty as the default.
        var mutation = new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
        };
        Assert.That(mutation.TransactionId, Is.EqualTo(Guid.Empty));
    }
}
