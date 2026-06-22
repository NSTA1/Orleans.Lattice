using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class InMemoryCredentialStoreTests
{
    [Test]
    public async Task GetAsync_whenEmpty_returnsNull()
    {
        var store = new InMemoryCredentialStore();

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public async Task SetAsync_thenGetAsync_returnsStoredCredential()
    {
        var store = new InMemoryCredentialStore();
        var credential = new StoredCredential("alice", "Password1");

        await store.SetAsync(credential);

        Assert.That(await store.GetAsync(), Is.EqualTo(credential));
    }

    [Test]
    public async Task ClearAsync_removesStoredCredential()
    {
        var store = new InMemoryCredentialStore();
        await store.SetAsync(new StoredCredential("alice", "Password1"));

        await store.ClearAsync();

        Assert.That(await store.GetAsync(), Is.Null);
    }

    [Test]
    public void SetAsync_nullCredential_throws()
    {
        var store = new InMemoryCredentialStore();

        Assert.That(async () => await store.SetAsync(null!), Throws.ArgumentNullException);
    }
}
