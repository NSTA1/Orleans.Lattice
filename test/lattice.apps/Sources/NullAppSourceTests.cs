namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class NullAppSourceTests
{
    [Test]
    public async Task ResolveAsync_always_returns_not_found()
    {
        var slug = AppSlug.Parse("demo-app");

        var unversioned = await NullAppSource.Instance.ResolveAsync(slug);
        var versioned = await NullAppSource.Instance.ResolveAsync(slug, AppVersion.Parse("1.0.0"));

        Assert.That(unversioned.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(versioned.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(unversioned.Slug, Is.EqualTo(slug));
        Assert.That(unversioned.Manifest, Is.Null);
    }

    [Test]
    public void Instance_is_shared()
    {
        Assert.That(NullAppSource.Instance, Is.SameAs(NullAppSource.Instance));
        Assert.That(NullAppSource.Instance, Is.InstanceOf<IAppSource>());
    }
}
