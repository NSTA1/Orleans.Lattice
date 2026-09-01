using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the per-circuit scope-notice slot: the handover between whatever
/// produced an outcome and the control that announces it.
/// </summary>
/// <remarks>
/// Direct assertions against the type - no cluster, no timing, no ordering, no
/// wall clock.
/// </remarks>
[TestFixture]
public class ExplorerTenantScopeNoticesTests
{
    [Test]
    public void Current_beforeAnythingIsPublished_isNull()
    {
        Assert.That(new ExplorerTenantScopeNotices().Current, Is.Null);
    }

    [Test]
    public void Publish_makesTheNoticeCurrent()
    {
        var notices = new ExplorerTenantScopeNotices();
        var notice = ExplorerTenantScopeNotice.Applied(new ExplorerTenantId(SampleTenant.TenantId));

        notices.Publish(notice);

        Assert.That(notices.Current, Is.SameAs(notice));
    }

    [Test]
    public void Publish_replacesAnEarlierNotice()
    {
        // Only the latest outcome is worth announcing.
        var notices = new ExplorerTenantScopeNotices();
        notices.Publish(ExplorerTenantScopeNotice.Refused());
        var latest = ExplorerTenantScopeNotice.Applied(new ExplorerTenantId(SampleTenant.TenantId));

        notices.Publish(latest);

        Assert.That(notices.Current, Is.SameAs(latest));
    }

    [Test]
    public void Publish_null_throws()
    {
        Assert.That(() => new ExplorerTenantScopeNotices().Publish(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clear_dropsTheCurrentNotice()
    {
        var notices = new ExplorerTenantScopeNotices();
        notices.Publish(ExplorerTenantScopeNotice.Refused());

        notices.Clear();

        Assert.That(notices.Current, Is.Null);
    }

    [Test]
    public void Clear_withNothingPublished_isHarmless()
    {
        var notices = new ExplorerTenantScopeNotices();

        notices.Clear();

        Assert.That(notices.Current, Is.Null);
    }
}
