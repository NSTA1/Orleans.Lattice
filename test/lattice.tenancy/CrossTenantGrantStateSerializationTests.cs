using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Serialization tests for the additive <see cref="CrossTenantGrant.State"/> and
/// <see cref="TenantGrantSlot.Generation"/> fields, and for the documented value a
/// grant persisted <em>before</em> those fields existed reads back as.
/// </summary>
/// <remarks>
/// <para>
/// The decision under test is that <see cref="TenantGrantState.Active"/> is the
/// zero value. A payload written by a build that had no <c>[Id(4)]</c> on the
/// grant simply carries no such field, so it deserializes to the enum's default.
/// Before the lifecycle existed every live grant authorized on an operation and
/// scope match, so <see cref="TenantGrantState.Active"/> is the only default that
/// leaves an upgraded cluster behaving exactly as it did - choosing
/// <see cref="TenantGrantState.Pending"/> would silently sever every cross-tenant
/// authorization a host had deliberately configured, with no diagnostic.
/// </para>
/// <para>
/// <see cref="LegacyCrossTenantGrant"/> reproduces that old payload exactly: the
/// same four field ids with the same types and no state field. Round-tripping it
/// through the current type is therefore a real wire-format compatibility test,
/// not a restatement of the enum's default.
/// </para>
/// </remarks>
public sealed class CrossTenantGrantStateSerializationTests
{
    /// <summary>
    /// The <see cref="CrossTenantGrant"/> wire shape as it shipped before the
    /// lifecycle state was added: ids 0 to 3 only. Used to synthesise a genuinely
    /// old payload for the compatibility round-trip.
    /// </summary>
    [GenerateSerializer]
    [Immutable]
    internal readonly record struct LegacyCrossTenantGrant
    {
        [Id(0)]
        public string Grantee { get; init; }

        [Id(1)]
        public TenantGranteeKind GranteeKind { get; init; }

        [Id(2)]
        public string Scope { get; init; }

        [Id(3)]
        public TenantGrantOperations Operations { get; init; }
    }

    [Test]
    public void A_grant_persisted_with_no_state_field_reads_back_as_active()
    {
        var legacy = new LegacyCrossTenantGrant
        {
            Grantee = "beta",
            GranteeKind = TenantGranteeKind.Tenant,
            Scope = "orders",
            Operations = TenantGrantOperations.ReadWrite,
        };

        var payload = TestSerializers.For<LegacyCrossTenantGrant>().Serialize(legacy);
        var recovered = TestSerializers.For<CrossTenantGrant>().Deserialize(payload);

        Assert.Multiple(() =>
        {
            Assert.That(recovered.Grantee, Is.EqualTo("beta"));
            Assert.That(recovered.Scope, Is.EqualTo("orders"));
            Assert.That(recovered.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
            Assert.That(
                recovered.State,
                Is.EqualTo(TenantGrantState.Active),
                "a grant written before the lifecycle existed must keep authorizing after an upgrade");
            Assert.That(TenantGrantLifecycle.Authorizes(recovered.State), Is.True);
        });
    }

    [Test]
    public void The_default_grant_state_is_active()
    {
        Assert.That(default(TenantGrantState), Is.EqualTo(TenantGrantState.Active));
    }

    [Test]
    public void The_pre_existing_create_overload_still_yields_an_active_grant()
    {
        var grant = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.Read);

        Assert.That(grant.State, Is.EqualTo(TenantGrantState.Active));
    }

    [Test]
    public void Create_with_an_explicit_state_carries_it()
    {
        var grant = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.Read, TenantGrantState.Pending);

        Assert.That(grant.State, Is.EqualTo(TenantGrantState.Pending));
    }

    [Test]
    public void Create_with_an_explicit_state_rejects_a_null_grantee()
    {
        Assert.That(
            () => CrossTenantGrant.Create(
                null!, TenantGranteeKind.Tenant, "orders", TenantGrantOperations.Read, TenantGrantState.Pending),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Create_with_an_explicit_state_rejects_a_null_scope()
    {
        Assert.That(
            () => CrossTenantGrant.Create(
                "beta", TenantGranteeKind.Tenant, null!, TenantGrantOperations.Read, TenantGrantState.Pending),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GrantId_is_independent_of_the_lifecycle_state()
    {
        // The state must not be part of the identity, or a transition would move
        // the grant to a different slot instead of updating it in place.
        var pending = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.Read, TenantGrantState.Pending);
        var revoked = pending with { State = TenantGrantState.Revoked };

        Assert.That(pending.GrantId, Is.EqualTo(revoked.GrantId));
    }

    [Test]
    public void A_pending_grant_round_trips_through_the_record_serializer()
    {
        var record = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");
        var offered = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.ReadWrite);
        record.OfferGrant(offered, Clock(10), "granter");

        var serializer = TestSerializers.TenantRecords;
        var recovered = serializer.Deserialize(serializer.Serialize(record));

        Assert.Multiple(() =>
        {
            Assert.That(recovered.TryGetGrant(offered.GrantId, out var grant), Is.True);
            Assert.That(grant.State, Is.EqualTo(TenantGrantState.Pending));
            Assert.That(grant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite));
        });
    }

    [Test]
    public void A_revoked_grants_agreement_generation_survives_a_round_trip()
    {
        var record = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");
        var offered = CrossTenantGrant.Create(
            "beta", TenantGranteeKind.Tenant, "orders", TenantGrantOperations.Read);
        record.OfferGrant(offered, Clock(10), "granter");
        record.TransitionGrant(offered.GrantId, TenantGrantState.Rejected, Clock(20), "grantee");
        record.OfferGrant(offered, Clock(30), "granter");

        var serializer = TestSerializers.TenantRecords;
        var recovered = serializer.Deserialize(serializer.Serialize(record));

        // A stale approval of the closed first agreement must still lose after the
        // record has been through storage, which it only can if the generation was
        // persisted alongside the state.
        var stale = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(1),
            "test");
        stale.OfferGrant(offered, Clock(10), "granter");
        stale.TransitionGrant(offered.GrantId, TenantGrantState.Active, Clock(99), "grantee");

        recovered.MergeFrom(stale);
        recovered.TryGetGrant(offered.GrantId, out var merged);

        Assert.That(merged.State, Is.EqualTo(TenantGrantState.Pending));
    }
}
