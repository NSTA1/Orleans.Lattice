using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantRegionResidencyAuthorizer"/>, the two-tier
/// fail-closed authorization seam for the region-residency facade. The operator
/// tier is granted only by a whole-scope Admin allow on the reserved auth policy
/// tree; the tenant-admin tier is granted by that operator authority <b>or</b> a
/// live admin subject on the tenant record (a CRDT membership check that is
/// inherently independent of the data-plane default effect). Every path is proven
/// fail-closed with hand-written gate doubles - no cluster, no timing.
/// </summary>
[TestFixture]
public sealed class TenantRegionResidencyAuthorizerTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static TenantRecord Record(string tenantId = "acme", string? adminSubject = null)
    {
        var record = TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            writerId: "seed");
        if (adminSubject is not null)
        {
            record.AddAdminSubject(adminSubject, HybridLogicalClock.Tick(HybridLogicalClock.Zero), "seed");
        }

        return record;
    }

    private static TenantRegionResidencyAuthorizer Authorizer(
        ILatticeAccessGate gate, FakeTenantRegistry registry, LatticeSubject? caller = null) =>
        new(
            gate,
            registry,
            caller is { } subject ? new FixedMembershipContext(subject) : null);

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_gate_throws() =>
        Assert.That(
            () => new TenantRegionResidencyAuthorizer(null!, new FakeTenantRegistry()),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_registry_throws() =>
        Assert.That(
            () => new TenantRegionResidencyAuthorizer(new FixedGate(allow: true), null!),
            Throws.ArgumentNullException);

    // ---- operator tier ---------------------------------------------------

    [Test]
    public void AuthorizeOperatorAsync_a_whole_scope_admin_allow_passes()
    {
        var authorizer = Authorizer(new FixedGate(allow: true), new FakeTenantRegistry(), new LatticeSubject("op"));

        Assert.That(
            async () => await authorizer.AuthorizeOperatorAsync(),
            Throws.Nothing);
    }

    [Test]
    public void AuthorizeOperatorAsync_a_gate_denial_is_refused()
    {
        var authorizer = Authorizer(new FixedGate(allow: false), new FakeTenantRegistry(), new LatticeSubject("mallory"));

        Assert.That(
            async () => await authorizer.AuthorizeOperatorAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeOperatorAsync_an_anonymous_caller_is_refused_without_consulting_the_gate()
    {
        var gate = new RecordingGate();
        // No membership context -> the caller resolves to anonymous.
        var authorizer = new TenantRegionResidencyAuthorizer(gate, new FakeTenantRegistry());

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await authorizer.AuthorizeOperatorAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(gate.Calls, Is.Zero, "an anonymous caller is denied before the gate is consulted");
        });
    }

    [Test]
    public void AuthorizeOperatorAsync_a_key_filtered_allow_is_refused_fail_closed()
    {
        // A whole-scope operator capability can never be narrowed to a key subset.
        var authorizer = Authorizer(new FilteredGate(), new FakeTenantRegistry(), new LatticeSubject("op"));

        Assert.That(
            async () => await authorizer.AuthorizeOperatorAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeOperatorAsync_authorizes_admin_on_the_reserved_policy_tree()
    {
        var gate = new RecordingGate();
        var authorizer = Authorizer(gate, new FakeTenantRegistry(), new LatticeSubject("op"));

        await authorizer.AuthorizeOperatorAsync();

        Assert.Multiple(() =>
        {
            Assert.That(gate.LastOperation, Is.EqualTo(LatticeOperation.Admin));
            Assert.That(gate.LastScope, Is.EqualTo(LatticeAuthReservedTrees.PolicyTreeId));
        });
    }

    [Test]
    public async Task AuthorizeOperatorAsync_a_system_origin_caller_bypasses_the_gate()
    {
        var gate = new RecordingGate();
        var authorizer = Authorizer(gate, new FakeTenantRegistry(), new LatticeSubject("op"));

        using (LatticeSystemOrigin.Enter())
        {
            await authorizer.AuthorizeOperatorAsync();
        }

        Assert.That(gate.Calls, Is.Zero, "trusted co-hosted infrastructure bypasses the gate");
    }

    // ---- tenant-admin tier ----------------------------------------------

    [Test]
    public async Task AuthorizeTenantAdminAsync_an_operator_is_authorized_and_reads_the_record()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record());
        var authorizer = Authorizer(new FixedGate(allow: true), registry, new LatticeSubject("op"));

        var record = await authorizer.AuthorizeTenantAdminAsync(Acme);

        Assert.That(record.Id, Is.EqualTo(Acme));
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_a_tenant_admin_subject_is_authorized_even_when_the_gate_denies()
    {
        // The gate denies (models a caller with no data-plane / operator grant, exactly
        // as the isolated reserved-tree gate does under DefaultEffect=Allow), yet the
        // caller is a live admin subject on the record: tenant-admin authority derives
        // from the CRDT membership, independent of any gate default effect.
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(adminSubject: "tenant-admin"));
        var authorizer = Authorizer(new FixedGate(allow: false), registry, new LatticeSubject("tenant-admin"));

        var record = await authorizer.AuthorizeTenantAdminAsync(Acme);

        Assert.That(record.Id, Is.EqualTo(Acme));
    }

    [Test]
    public void AuthorizeTenantAdminAsync_a_non_admin_non_operator_is_denied_not_reported_missing()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(adminSubject: "someone-else"));
        var authorizer = Authorizer(new FixedGate(allow: false), registry, new LatticeSubject("stranger"));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(Acme),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantAdminAsync_a_non_operator_probing_a_missing_tenant_gets_a_denial_not_a_not_found()
    {
        // A non-admin caller must not be able to probe tenant existence: a missing
        // record is reported as a denial, never a not-found.
        var authorizer = Authorizer(new FixedGate(allow: false), new FakeTenantRegistry(), new LatticeSubject("stranger"));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(Acme),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeTenantAdminAsync_an_operator_on_a_missing_tenant_gets_a_not_found()
    {
        var authorizer = Authorizer(new FixedGate(allow: true), new FakeTenantRegistry(), new LatticeSubject("op"));

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(Acme),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void AuthorizeTenantAdminAsync_an_anonymous_caller_is_denied()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record());
        // No membership context -> anonymous, and the gate denies.
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(allow: false), registry);

        Assert.That(
            async () => await authorizer.AuthorizeTenantAdminAsync(Acme),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeTenantAdminAsync_a_system_origin_caller_reads_the_record()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record());
        var authorizer = Authorizer(new FixedGate(allow: false), registry, new LatticeSubject("ignored"));

        TenantRecord record;
        using (LatticeSystemOrigin.Enter())
        {
            record = await authorizer.AuthorizeTenantAdminAsync(Acme);
        }

        Assert.That(record.Id, Is.EqualTo(Acme));
    }

    [Test]
    public void AuthorizeTenantAdminAsync_a_system_origin_caller_on_a_missing_tenant_gets_a_not_found()
    {
        var authorizer = Authorizer(new FixedGate(allow: false), new FakeTenantRegistry(), new LatticeSubject("ignored"));

        Assert.That(
            async () =>
            {
                using (LatticeSystemOrigin.Enter())
                {
                    await authorizer.AuthorizeTenantAdminAsync(Acme);
                }
            },
            Throws.TypeOf<TenantNotFoundException>());
    }
}
