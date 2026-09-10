using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Claims;

/// <summary>
/// Coverage for how a claim and a renew resolve their lease length, and for the
/// signal a renew raises when it <b>shortens</b> the lease it replaced.
/// <para>
/// <b>Read the symmetry test's green carefully; it is not evidence of a fixed
/// bug.</b> <see cref="Characterisation_a_claim_and_a_renew_grant_the_same_lease_when_leaseSeconds_is_omitted"/>
/// is a <i>characterisation</i> test, not a regression test. It passed on its
/// first run and was expected to. It was written after a reported divergence
/// between the two defaults was measured on a live deployment and found not to
/// exist: both surfaces defer to the same value. The test exists to pin that
/// measured symmetry as an enforced invariant, so a future change to either side
/// fails loudly rather than silently reintroducing the divergence this was once
/// believed to have. Nothing here was ever broken.
/// </para>
/// <para>
/// The short default is deliberate rather than accidental: a caller that named no
/// lease length is exactly the caller that should not be granted a long one. What
/// was genuinely wrong, and is covered below, is that a renew which cuts a long
/// lease down to that short default reported nothing but success.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextClaimLeaseTests
{
    private const string RepoId = "lattice";
    private const string Topic = "backlog";
    private const string ItemId = "item-1";
    private const string Key = $"repo/{RepoId}/mem/{Topic}/{ItemId}";

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private SubstitutedClaimSurface _surface = null!;
    private RepoContextStore _store = null!;

    [SetUp]
    public void CreateSurface()
    {
        _surface = new SubstitutedClaimSurface(Serializer);
        _store = _surface.Store();
    }

    private Task SeedAsync() => _store.RememberAsync(
        RepoId, Topic, ItemId, MemoryKind.Note, "Item", "seed", "author", null, null, null, null, null,
        CancellationToken.None);

    private Task<RepoContextClaimResult> ClaimAsync(long? leaseSeconds)
        => _store.ClaimAsync(Key, "agent-a", leaseSeconds, maxWaitSeconds: null, CancellationToken.None);

    private Task<RepoContextClaimResult> RenewAsync(long fencingToken, long? leaseSeconds)
        => _store.RenewClaimAsync(Key, fencingToken, leaseSeconds, CancellationToken.None);

    /// <summary>
    /// Pins the measured symmetry: omitting <c>leaseSeconds</c> resolves to the same
    /// configured default on both surfaces. See the fixture remarks - this passed on
    /// first run and is a characterisation of correct current behaviour, not proof
    /// that a divergence ever existed.
    /// </summary>
    [Test]
    public async Task Characterisation_a_claim_and_a_renew_grant_the_same_lease_when_leaseSeconds_is_omitted()
    {
        await SeedAsync();

        var claim = await ClaimAsync(leaseSeconds: null);
        var renew = await RenewAsync(claim.FencingToken!.Value, leaseSeconds: null);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.True);
            Assert.That(renew.Granted, Is.True);
            Assert.That(
                renew.LeaseSeconds,
                Is.EqualTo(claim.LeaseSeconds),
                "A renew that omits leaseSeconds must defer to the same default a claim does. "
                + "This has always held; the test exists so a change to either side cannot break it quietly.");
        });
    }

    [Test]
    public async Task A_renew_that_shortens_the_lease_says_so_rather_than_reporting_a_plain_grant()
    {
        await SeedAsync();

        // Take a long lease explicitly, then renew without naming one: the renew falls
        // back to the short default and cuts the holder's own deadline dramatically.
        var claim = await ClaimAsync(leaseSeconds: 1800);
        var renew = await RenewAsync(claim.FencingToken!.Value, leaseSeconds: null);

        Assert.Multiple(() =>
        {
            Assert.That(renew.Granted, Is.True);
            Assert.That(renew.LeaseShortened, Is.True);
            Assert.That(renew.PreviousLeaseExpiresAtUtc, Is.EqualTo(claim.LeaseExpiresAtUtc));
            Assert.That(renew.LeaseSeconds, Is.LessThan(claim.LeaseSeconds!.Value));
        });
    }

    [Test]
    public async Task A_renew_that_extends_the_lease_is_not_flagged_as_shortening()
    {
        await SeedAsync();

        var claim = await ClaimAsync(leaseSeconds: 30);
        var renew = await RenewAsync(claim.FencingToken!.Value, leaseSeconds: 1800);

        Assert.Multiple(() =>
        {
            Assert.That(renew.Granted, Is.True);
            Assert.That(renew.LeaseShortened, Is.False);
            Assert.That(renew.PreviousLeaseExpiresAtUtc, Is.EqualTo(claim.LeaseExpiresAtUtc));
            Assert.That(renew.LeaseSeconds, Is.GreaterThan(claim.LeaseSeconds!.Value));
        });
    }

    [Test]
    public async Task A_claim_reports_no_shortening_verdict_because_the_question_does_not_apply()
    {
        await SeedAsync();

        var claim = await ClaimAsync(leaseSeconds: 1800);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.True);
            Assert.That(claim.LeaseShortened, Is.Null);
            Assert.That(claim.PreviousLeaseExpiresAtUtc, Is.Null);
        });
    }

    [Test]
    public async Task A_superseded_renew_reports_no_shortening_verdict_rather_than_a_reassuring_false()
    {
        await SeedAsync();

        var claim = await ClaimAsync(leaseSeconds: 1800);
        _surface.LockFor(Key).ExpireLease();
        await ClaimAsync(leaseSeconds: 1800);

        var stale = await RenewAsync(claim.FencingToken!.Value, leaseSeconds: null);

        Assert.Multiple(() =>
        {
            Assert.That(stale.Granted, Is.False);
            Assert.That(stale.Reason, Is.EqualTo("superseded"));

            // Null is "no verdict", not "nothing shrank". A refusal must never carry a
            // negative that reads as an assurance.
            Assert.That(stale.LeaseShortened, Is.Null);
            Assert.That(stale.PreviousLeaseExpiresAtUtc, Is.Null);
        });
    }
}
