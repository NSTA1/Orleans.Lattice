namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeCredentialVerification"/> and the default
/// <see cref="ILatticeMembershipContext.VerifyCurrentCredentialAsync"/>: a host
/// with no real membership context never reports a refusal, so every caller there
/// resolves exactly as it did before the verdict existed.
/// </summary>
[TestFixture]
public sealed class LatticeCredentialVerificationTests
{
    [Test]
    public async Task VerifyCurrentAsync_without_a_credential_reports_absent()
    {
        var verdict = await LatticeCredentialVerification.VerifyCurrentAsync(new NullLatticeMembershipContext());

        Assert.That(verdict, Is.EqualTo(LatticeCredentialVerdict.Absent));
    }

    [Test]
    public async Task VerifyCurrentAsync_with_no_membership_context_reports_unverified()
    {
        using (LatticeCredentialContext.Use("token", scheme: "Basic"))
        {
            Assert.That(await LatticeCredentialVerification.VerifyCurrentAsync(null), Is.EqualTo(LatticeCredentialVerdict.Unverified));
        }
    }

    [Test]
    public async Task VerifyCurrentAsync_through_the_null_context_never_reports_a_refusal()
    {
        using (LatticeCredentialContext.Use("token", scheme: "Basic"))
        {
            var verdict = await LatticeCredentialVerification.VerifyCurrentAsync(new NullLatticeMembershipContext());

            Assert.That(verdict, Is.EqualTo(LatticeCredentialVerdict.Unverified));
        }
    }

    [Test]
    public async Task The_default_VerifyCurrentCredentialAsync_reports_absent_or_unverified()
    {
        ILatticeMembershipContext context = new DefaultOnlyMembershipContext();

        var absent = await context.VerifyCurrentCredentialAsync();
        LatticeCredentialVerdict presented;
        using (LatticeCredentialContext.Use("token", scheme: "Basic"))
        {
            presented = await context.VerifyCurrentCredentialAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(absent, Is.EqualTo(LatticeCredentialVerdict.Absent));
            Assert.That(presented, Is.EqualTo(LatticeCredentialVerdict.Unverified));
        });
    }

    [Test]
    public async Task VerifyCurrentAsync_on_a_cold_context_runs_the_verification_system_origin()
    {
        var context = new ColdOnlyMembershipContext();

        using (LatticeCredentialContext.Use("token", scheme: "Basic"))
        {
            await LatticeCredentialVerification.VerifyCurrentAsync(context);
        }

        Assert.Multiple(() =>
        {
            Assert.That(context.VerifiedSystemOrigin, Is.True, "a cache-miss verification reads the directory, so it bypasses the gate");
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the scope ends with the verification");
        });
    }

    /// <summary>A context that implements only the required member, so the defaults apply.</summary>
    private sealed class DefaultOnlyMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);
    }

    /// <summary>A context with no synchronous path, recording the scope it was verified under.</summary>
    private sealed class ColdOnlyMembershipContext : ILatticeMembershipContext
    {
        public bool VerifiedSystemOrigin { get; private set; }

        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public ValueTask<LatticeCredentialVerdict> VerifyCurrentCredentialAsync(CancellationToken cancellationToken = default)
        {
            VerifiedSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
            return new(LatticeCredentialVerdict.Refused);
        }
    }
}
