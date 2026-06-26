using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

[TestFixture]
public class UiSessionStoreTests
{
    [Test]
    public void GetOrDefault_UnknownKey_ReturnsFallback()
    {
        var store = new UiSessionStore();

        Assert.That(store.GetOrDefault("k", "fallback"), Is.EqualTo("fallback"));
    }

    [Test]
    public void Set_ThenGetOrDefault_ReturnsStoredValue()
    {
        var store = new UiSessionStore();

        store.Set("k", "abc");

        Assert.That(store.GetOrDefault("k", string.Empty), Is.EqualTo("abc"));
    }

    [Test]
    public void TryGet_WrongType_ReturnsFalse()
    {
        var store = new UiSessionStore();
        store.Set("k", "abc");

        var found = store.TryGet<int>("k", out var value);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(value, Is.EqualTo(0));
        });
    }

    [Test]
    public void Set_SupportsNonStringValueTypes()
    {
        var store = new UiSessionStore();

        store.Set("count", 42);
        store.Set("flag", true);

        Assert.Multiple(() =>
        {
            Assert.That(store.GetOrDefault("count", 0), Is.EqualTo(42));
            Assert.That(store.GetOrDefault("flag", false), Is.True);
        });
    }

    [Test]
    public void Set_IsPerKey()
    {
        var store = new UiSessionStore();

        store.Set("a", "1");
        store.Set("b", "2");

        Assert.Multiple(() =>
        {
            Assert.That(store.GetOrDefault("a", string.Empty), Is.EqualTo("1"));
            Assert.That(store.GetOrDefault("b", string.Empty), Is.EqualTo("2"));
        });
    }

    [Test]
    public void Set_OverwritesPriorValue()
    {
        var store = new UiSessionStore();
        store.Set("k", "abc");

        store.Set("k", "abcd");

        Assert.That(store.GetOrDefault("k", string.Empty), Is.EqualTo("abcd"));
    }

    [Test]
    public void Remove_DeletesEntry()
    {
        var store = new UiSessionStore();
        store.Set("k", "abc");

        store.Remove("k");

        Assert.That(store.TryGet<string>("k", out _), Is.False);
    }

    [Test]
    public void Remove_UnknownKey_IsNoOp()
    {
        var store = new UiSessionStore();

        Assert.DoesNotThrow(() => store.Remove("missing"));
    }
}
