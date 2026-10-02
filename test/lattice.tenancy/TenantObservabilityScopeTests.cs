namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilityScope"/>: the explicit visibility
/// assertion a caller passes to <see cref="ITenantObservabilityView.ListAsync"/>.
/// Covers that the default scope is the per-tenant (not cluster-wide) channel with
/// the anonymous subject, and that the cluster-wide factory carries the asserting
/// operator subject. Pure value construction, so there is no timing dependency.
/// </summary>
[TestFixture]
public sealed class TenantObservabilityScopeTests
{
    [Test]
    public void ActiveTenant_is_the_per_tenant_default_with_the_anonymous_subject()
    {
        var scope = TenantObservabilityScope.ActiveTenant;

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsClusterWide, Is.False, "the default never crosses tenants");
            Assert.That(scope.Subject, Is.EqualTo(LatticeSubject.Anonymous), "the default carries no operator subject");
        });
    }

    [Test]
    public void ClusterWide_carries_the_asserting_operator_subject()
    {
        var subject = new LatticeSubject("op-1");

        var scope = TenantObservabilityScope.ClusterWide(subject);

        Assert.Multiple(() =>
        {
            Assert.That(scope.IsClusterWide, Is.True);
            Assert.That(scope.Subject, Is.EqualTo(subject));
        });
    }

    [Test]
    public void ActiveTenant_is_stable_across_reads()
    {
        // Bound to distinct locals so the assertion is visibly between two
        // independent reads of the cached property rather than reading as a
        // self-comparison, and so a failure reports both observed values.
        var first = TenantObservabilityScope.ActiveTenant;
        var second = TenantObservabilityScope.ActiveTenant;

        Assert.That(second, Is.EqualTo(first));
    }
}
