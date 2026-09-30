using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public class AppSourceDescriptorTests
{
    [Test]
    public void Constructor_sets_every_member()
    {
        var descriptor = new AppSourceDescriptor("feed-1", "Feed one", AppSourceKind.Dynamic,
            AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search);

        Assert.That(descriptor.Key, Is.EqualTo("feed-1"));
        Assert.That(descriptor.DisplayName, Is.EqualTo("Feed one"));
        Assert.That(descriptor.Kind, Is.EqualTo(AppSourceKind.Dynamic));
        Assert.That(descriptor.Capabilities, Is.EqualTo(AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search));
    }

    [Test]
    public void Constructor_accepts_no_capabilities_and_the_key_length_bounds()
    {
        Assert.That(new AppSourceDescriptor("ab", "Two", AppSourceKind.Static, AppSourceCapabilities.None).Key, Is.EqualTo("ab"));
        var longest = "a" + new string('b', 30);
        Assert.That(new AppSourceDescriptor(longest, "Long", AppSourceKind.Static, AppSourceCapabilities.None).Key, Is.EqualTo(longest));
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => new AppSourceDescriptor(null!, "Name", AppSourceKind.Static, AppSourceCapabilities.None));
        Assert.Throws<ArgumentNullException>(() => new AppSourceDescriptor("feed", null!, AppSourceKind.Static, AppSourceCapabilities.None));
    }

    [TestCase("")]
    [TestCase("a")]
    [TestCase("1feed")]
    [TestCase("-feed")]
    [TestCase("Feed")]
    [TestCase("feed_one")]
    [TestCase("feed one")]
    [TestCase("feed.one")]
    [TestCase("abcdefghijklmnopqrstuvwxyzabcdef")]
    public void Constructor_rejects_an_invalid_key(string key)
    {
        Assert.Throws<ArgumentException>(() => new AppSourceDescriptor(key, "Name", AppSourceKind.Static, AppSourceCapabilities.None));
        Assert.That(AppSourceDescriptor.IsValidKey(key), Is.False);
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Constructor_rejects_an_empty_display_name(string displayName)
    {
        Assert.Throws<ArgumentException>(() => new AppSourceDescriptor("feed", displayName, AppSourceKind.Static, AppSourceCapabilities.None));
    }

    [Test]
    public void Constructor_rejects_an_undefined_kind_or_unknown_capability()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new AppSourceDescriptor("feed", "Name", (AppSourceKind)2, AppSourceCapabilities.None));
        Assert.Throws<ArgumentException>(() => new AppSourceDescriptor("feed", "Name", AppSourceKind.Static, (AppSourceCapabilities)16));
    }

    [Test]
    public void IsValidKey_accepts_valid_keys_and_rejects_null()
    {
        Assert.That(AppSourceDescriptor.IsValidKey("in-image"), Is.True);
        Assert.That(AppSourceDescriptor.IsValidKey("feed-2"), Is.True);
        Assert.That(AppSourceDescriptor.IsValidKey(null), Is.False);
    }

    [Test]
    public void Supports_requires_every_requested_flag()
    {
        var descriptor = new AppSourceDescriptor("feed", "Name", AppSourceKind.Dynamic,
            AppSourceCapabilities.Enumerate | AppSourceCapabilities.MultipleVersions);

        Assert.That(descriptor.Supports(AppSourceCapabilities.Enumerate), Is.True);
        Assert.That(descriptor.Supports(AppSourceCapabilities.Enumerate | AppSourceCapabilities.MultipleVersions), Is.True);
        Assert.That(descriptor.Supports(AppSourceCapabilities.Search), Is.False);
        Assert.That(descriptor.Supports(AppSourceCapabilities.Enumerate | AppSourceCapabilities.Search), Is.False);
        Assert.That(descriptor.Supports(AppSourceCapabilities.None), Is.True);
    }

    [Test]
    public void Descriptors_with_equal_members_are_equal()
    {
        var left = new AppSourceDescriptor("feed", "Name", AppSourceKind.Static, AppSourceCapabilities.Enumerate);
        var right = new AppSourceDescriptor("feed", "Name", AppSourceKind.Static, AppSourceCapabilities.Enumerate);

        Assert.That(left, Is.EqualTo(right));
        Assert.That(left, Is.Not.EqualTo(new AppSourceDescriptor("other", "Name", AppSourceKind.Static, AppSourceCapabilities.Enumerate)));
    }

    [Test]
    public void Enum_values_are_stable()
    {
        Assert.That((int)AppSourceKind.Static, Is.EqualTo(0));
        Assert.That((int)AppSourceKind.Dynamic, Is.EqualTo(1));
        Assert.That((int)AppSourceCapabilities.None, Is.EqualTo(0));
        Assert.That((int)AppSourceCapabilities.Enumerate, Is.EqualTo(1));
        Assert.That((int)AppSourceCapabilities.Search, Is.EqualTo(2));
        Assert.That((int)AppSourceCapabilities.MultipleVersions, Is.EqualTo(4));
        Assert.That((int)AppSourceCapabilities.RequiresAcquisition, Is.EqualTo(8));
    }
}
