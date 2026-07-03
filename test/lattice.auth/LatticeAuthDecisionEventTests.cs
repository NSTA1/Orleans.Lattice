using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeAuthDecisionEvent"/> value type: its
/// public constructor validates the required identity fields and preserves every
/// supplied field.
/// </summary>
[TestFixture]
public sealed class LatticeAuthDecisionEventTests
{
    [Test]
    public void Constructor_preserves_all_supplied_fields()
    {
        var ts = DateTimeOffset.UtcNow;
        var evt = new LatticeAuthDecisionEvent(
            "alice",
            LatticeOperation.Write,
            "orders",
            LatticeEffect.Deny,
            policyEpoch: 9,
            timestampUtc: ts,
            key: "k1",
            rangeStart: "a",
            rangeEnd: "z",
            matchedRuleId: "r1",
            matchedScopeKind: LatticeScopeKind.Prefix,
            matchedScopeValue: "a",
            reason: "blocked");

        Assert.Multiple(() =>
        {
            Assert.That(evt.SubjectId, Is.EqualTo("alice"));
            Assert.That(evt.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(evt.TreeId, Is.EqualTo("orders"));
            Assert.That(evt.Effect, Is.EqualTo(LatticeEffect.Deny));
            Assert.That(evt.PolicyEpoch, Is.EqualTo(9));
            Assert.That(evt.TimestampUtc, Is.EqualTo(ts));
            Assert.That(evt.Key, Is.EqualTo("k1"));
            Assert.That(evt.RangeStart, Is.EqualTo("a"));
            Assert.That(evt.RangeEnd, Is.EqualTo("z"));
            Assert.That(evt.MatchedRuleId, Is.EqualTo("r1"));
            Assert.That(evt.MatchedScopeKind, Is.EqualTo(LatticeScopeKind.Prefix));
            Assert.That(evt.MatchedScopeValue, Is.EqualTo("a"));
            Assert.That(evt.Reason, Is.EqualTo("blocked"));
        });
    }

    [Test]
    public void Constructor_defaults_the_optional_fields_to_null()
    {
        var evt = new LatticeAuthDecisionEvent(
            "bob", LatticeOperation.Read, "app", LatticeEffect.Allow, 1, DateTimeOffset.UtcNow);

        Assert.Multiple(() =>
        {
            Assert.That(evt.Key, Is.Null);
            Assert.That(evt.RangeStart, Is.Null);
            Assert.That(evt.RangeEnd, Is.Null);
            Assert.That(evt.MatchedRuleId, Is.Null);
            Assert.That(evt.MatchedScopeKind, Is.Null);
            Assert.That(evt.MatchedScopeValue, Is.Null);
            Assert.That(evt.Reason, Is.Null);
        });
    }

    [Test]
    public void Constructor_rejects_a_null_subject_id()
    {
        Assert.That(
            () => new LatticeAuthDecisionEvent(null!, LatticeOperation.Read, "app", LatticeEffect.Allow, 1, DateTimeOffset.UtcNow),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_rejects_a_null_or_empty_tree_id()
    {
        Assert.That(
            () => new LatticeAuthDecisionEvent("alice", LatticeOperation.Read, string.Empty, LatticeEffect.Allow, 1, DateTimeOffset.UtcNow),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(
            () => new LatticeAuthDecisionEvent("alice", LatticeOperation.Read, null!, LatticeEffect.Allow, 1, DateTimeOffset.UtcNow),
            Throws.InstanceOf<ArgumentException>());
    }
}
