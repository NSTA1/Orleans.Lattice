using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Claims;

/// <summary>
/// Coverage for the four claim handlers on <see cref="RepoContextToolHandlers"/>:
/// the argument validation each performs before it reaches the store, and the
/// dispatch that proves each one is wired to the store the request context
/// carries. The behaviour past dispatch is covered by
/// <see cref="RepoContextStoreClaimTests"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextClaimToolHandlerTests
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

    private Task<RequestContext<CallToolRequestParams>> ContextAsync()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_store);
        return RepoContextRequestContexts.CreateAsync(services.BuildServiceProvider());
    }

    private Task SeedAsync() => _store.RememberAsync(
        RepoId, Topic, ItemId, MemoryKind.Note, "Item", "seed", "author", null, null, null, null, null,
        CancellationToken.None);

    // ---- argument validation ----------------------------------------------

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public async Task ClaimAsync_rejects_a_missing_key(string? key)
    {
        var context = await ContextAsync();

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.ClaimAsync(context, key!, "agent-a"));

        Assert.That(error!.Message, Does.Contain("'key'"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public async Task ClaimAsync_rejects_a_missing_owner(string? owner)
    {
        var context = await ContextAsync();

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.ClaimAsync(context, Key, owner!));

        Assert.That(error!.Message, Does.Contain("'owner'"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public async Task RenewClaimAsync_rejects_a_missing_key(string? key)
    {
        var context = await ContextAsync();

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.RenewClaimAsync(context, key!, 1L));

        Assert.That(error!.Message, Does.Contain("'key'"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public async Task ReleaseClaimAsync_rejects_a_missing_key(string? key)
    {
        var context = await ContextAsync();

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.ReleaseClaimAsync(context, key!, 1L));

        Assert.That(error!.Message, Does.Contain("'key'"));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public async Task ClaimStatusAsync_rejects_a_missing_key(string? key)
    {
        var context = await ContextAsync();

        var error = Assert.ThrowsAsync<McpException>(
            () => RepoContextToolHandlers.ClaimStatusAsync(context, key!));

        Assert.That(error!.Message, Does.Contain("'key'"));
    }

    // ---- dispatch ---------------------------------------------------------

    [Test]
    public async Task ClaimAsync_dispatches_to_the_store_on_the_request_context()
    {
        await SeedAsync();
        var context = await ContextAsync();

        var claim = await RepoContextToolHandlers.ClaimAsync(context, Key, "agent-a");

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.True);
            Assert.That(claim.Key, Is.EqualTo(Key));
            Assert.That(claim.FencingToken, Is.EqualTo(1L));
            Assert.That(claim.Owner, Is.EqualTo("agent-a"));
        });
    }

    [Test]
    public async Task ClaimAsync_passes_the_lease_and_wait_through_to_the_store()
    {
        await SeedAsync();
        var context = await ContextAsync();

        var claim = await RepoContextToolHandlers.ClaimAsync(
            context, Key, "agent-a", leaseSeconds: 12L, maxWaitSeconds: 3L);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Granted, Is.True);
            Assert.That(claim.LeaseSeconds, Is.EqualTo(12d));
        });
    }

    [Test]
    public async Task RenewClaimAsync_dispatches_to_the_store_on_the_request_context()
    {
        await SeedAsync();
        var context = await ContextAsync();
        var claim = await RepoContextToolHandlers.ClaimAsync(context, Key, "agent-a");

        var renewed = await RepoContextToolHandlers.RenewClaimAsync(
            context, Key, claim.FencingToken!.Value, leaseSeconds: 20L);

        Assert.Multiple(() =>
        {
            Assert.That(renewed.Granted, Is.True);
            Assert.That(renewed.FencingToken, Is.EqualTo(claim.FencingToken));
            Assert.That(renewed.LeaseSeconds, Is.EqualTo(20d));
        });
    }

    [Test]
    public async Task ReleaseClaimAsync_dispatches_to_the_store_on_the_request_context()
    {
        await SeedAsync();
        var context = await ContextAsync();
        var claim = await RepoContextToolHandlers.ClaimAsync(context, Key, "agent-a");

        var released = await RepoContextToolHandlers.ReleaseClaimAsync(
            context, Key, claim.FencingToken!.Value);

        Assert.Multiple(() =>
        {
            Assert.That(released.Released, Is.True);
            Assert.That(released.FencingToken, Is.EqualTo(claim.FencingToken!.Value));
            Assert.That(released.Reason, Is.Null);
        });
    }

    [Test]
    public async Task ClaimStatusAsync_dispatches_to_the_store_on_the_request_context()
    {
        await SeedAsync();
        var context = await ContextAsync();
        await RepoContextToolHandlers.ClaimAsync(context, Key, "agent-a");

        var status = await RepoContextToolHandlers.ClaimStatusAsync(context, Key);

        Assert.Multiple(() =>
        {
            Assert.That(status.Exists, Is.True);
            Assert.That(status.Claimed, Is.True);
            Assert.That(status.IsHeld, Is.True);
            Assert.That(status.FencingToken, Is.EqualTo(1L));
            Assert.That(status.Owner, Is.EqualTo("agent-a"));
            Assert.That(status.Authoritative, Is.False);
        });
    }
}
