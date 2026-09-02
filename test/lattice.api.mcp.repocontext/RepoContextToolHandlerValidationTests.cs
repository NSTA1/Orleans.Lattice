using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit tests for the argument validation every <see cref="RepoContextToolHandlers"/>
/// entry point performs before it touches the request context or resolves any
/// service, and for the clear failure each service-resolution helper raises when
/// the MCP request carries no service provider.
/// </summary>
/// <remarks>
/// <para>
/// These guards are the surface an agent actually hits: a wire caller that omits
/// a required argument, or passes whitespace for it, must get a clean
/// self-contained <see cref="McpException"/> naming the parameter - not a
/// <see cref="NullReferenceException"/> from deep inside the store, and not a
/// silent read against an empty repository id that returns a confidently wrong
/// empty result. Because validation runs first, each can be asserted with a
/// <c>null</c> context, which is what makes them cheap deterministic unit tests.
/// </para>
/// <para>
/// The service-resolution helpers are asserted separately with a real request
/// context whose provider is absent, proving the "no service provider" path
/// fails with an actionable message rather than a null dereference.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextToolHandlerValidationTests
{
    private static void AssertRejects(Action call, string parameterName)
        => Assert.That(
            call,
            Throws.InstanceOf<McpException>().With.Message.Contains($"'{parameterName}'"),
            $"A missing or blank {parameterName} must fault fast with a message naming the parameter.");

    // ---- required-identifier guards ----------------------------------------

    [TestCase("")]
    [TestCase("   ")]
    public void BootstrapAsync_rejects_a_blank_repo_root(string repoRoot)
        => AssertRejects(() => RepoContextToolHandlers.BootstrapAsync(null!, repoRoot, "acme"), "repoRoot");

    [TestCase("")]
    [TestCase("   ")]
    public void BootstrapAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.BootstrapAsync(null!, "/workspace/acme", repoId), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void RecallAsync_rejects_a_blank_key(string key)
        => AssertRejects(() => RepoContextToolHandlers.RecallAsync(null!, key), "key");

    [TestCase("")]
    [TestCase("   ")]
    public void ScanAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.ScanAsync(null!, repoId, "Files"), "repoId");

    [Test]
    public void ScanAsync_rejects_an_unknown_scope()
        => Assert.That(
            () => RepoContextToolHandlers.ScanAsync(null!, "acme", "NotAScope"),
            Throws.InstanceOf<McpException>(),
            "An unrecognised scope must be rejected rather than silently walking the wrong range.");

    [TestCase("")]
    [TestCase("   ")]
    public void ListTopicsAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.ListTopicsAsync(null!, repoId), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void RememberAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.RememberAsync(null!, repoId, "decisions"), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void RememberAsync_rejects_a_blank_topic(string topic)
        => AssertRejects(() => RepoContextToolHandlers.RememberAsync(null!, "acme", topic), "topic");

    [Test]
    public void RememberAsync_rejects_an_unknown_memory_kind()
        => Assert.That(
            () => RepoContextToolHandlers.RememberAsync(null!, "acme", "decisions", kind: "NotAKind"),
            Throws.InstanceOf<McpException>(),
            "An unrecognised kind must be rejected so an entry is never filed under a kind the store cannot honour.");

    [TestCase("")]
    [TestCase("   ")]
    public void UpdateAsync_rejects_a_blank_key(string key)
        => AssertRejects(() => RepoContextToolHandlers.UpdateAsync(null!, key), "key");

    [TestCase("")]
    [TestCase("   ")]
    public void NeighborsAsync_rejects_a_blank_key(string key)
        => AssertRejects(() => RepoContextToolHandlers.NeighborsAsync(null!, key), "key");

    [TestCase("")]
    [TestCase("   ")]
    public void ForgetAsync_rejects_a_blank_key(string key)
        => AssertRejects(() => RepoContextToolHandlers.ForgetAsync(null!, key), "key");

    [TestCase("")]
    [TestCase("   ")]
    public void SearchAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.SearchAsync(null!, repoId, "where is X"), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void SearchAsync_rejects_a_blank_query(string query)
        => AssertRejects(() => RepoContextToolHandlers.SearchAsync(null!, "acme", query), "query");

    [TestCase("")]
    [TestCase("   ")]
    public void OutlineAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.OutlineAsync(null!, repoId, "src/Foo.cs"), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void OutlineAsync_rejects_a_blank_path(string path)
        => AssertRejects(() => RepoContextToolHandlers.OutlineAsync(null!, "acme", path), "path");

    [TestCase("")]
    [TestCase("   ")]
    public void RelatedAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.RelatedAsync(null!, repoId, "src/Foo.cs"), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void RelatedAsync_rejects_a_blank_path(string path)
        => AssertRejects(() => RepoContextToolHandlers.RelatedAsync(null!, "acme", path), "path");

    [TestCase("")]
    [TestCase("   ")]
    public void ChangedAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(
            () => RepoContextToolHandlers.ChangedAsync(null!, repoId, "/workspace/acme").GetAwaiter().GetResult(),
            "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void ChangedAsync_rejects_a_blank_path(string path)
        => AssertRejects(
            () => RepoContextToolHandlers.ChangedAsync(null!, "acme", path).GetAwaiter().GetResult(),
            "path");

    [TestCase("")]
    [TestCase("   ")]
    public void AddRepoAsync_rejects_a_blank_path(string path)
        => AssertRejects(() => RepoContextToolHandlers.AddRepoAsync(null!, path), "path");

    [TestCase("")]
    [TestCase("   ")]
    public void RemoveRepoAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.RemoveRepoAsync(null!, repoId), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void IndexStatusAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.IndexStatusAsync(null!, repoId), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void ContextAsync_rejects_a_blank_repo_id(string repoId)
        => AssertRejects(() => RepoContextToolHandlers.ContextAsync(null!, repoId, "add a widget"), "repoId");

    [TestCase("")]
    [TestCase("   ")]
    public void ContextAsync_rejects_a_blank_task(string task)
        => AssertRejects(() => RepoContextToolHandlers.ContextAsync(null!, "acme", task), "task");

    // ---- health is unconditional -------------------------------------------

    [Test]
    public void Health_reports_the_surface_available_without_touching_the_request()
    {
        var health = RepoContextToolHandlers.Health();

        Assert.That(health.Available, Is.True,
            "Reaching the handler means the caller cleared the authorization gate, so it always reports ready.");
    }

    // ---- service-resolution failures ---------------------------------------

    [Test]
    public async Task ContextAsync_without_a_request_service_provider_fails_with_an_actionable_message()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.ContextAsync(context, "acme", "add a widget"),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"),
            "A request with no service provider must name the missing provider, not null-dereference.");
    }

    [Test]
    public async Task RelatedAsync_without_a_request_service_provider_fails_with_an_actionable_message()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.RelatedAsync(context, "acme", "src/Foo.cs"),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }

    [Test]
    public async Task OutlineAsync_without_a_request_service_provider_fails_with_an_actionable_message()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.OutlineAsync(context, "acme", "src/Foo.cs"),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }

    [Test]
    public async Task Stats_without_a_request_service_provider_fails_with_an_actionable_message()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.Stats(context),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }

    [Test]
    public async Task SearchAsync_without_a_request_service_provider_fails_with_an_actionable_message()
    {
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        Assert.That(
            () => RepoContextToolHandlers.SearchAsync(context, "acme", "where is X"),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("no service provider"));
    }
}
