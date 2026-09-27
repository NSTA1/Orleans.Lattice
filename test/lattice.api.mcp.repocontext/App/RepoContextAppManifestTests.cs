using Orleans.Lattice.Api.Mcp.Apps;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// The repository-context app manifest: it is embedded under its pinned resource name,
/// loads and validates without loading any app code, uses a slug whose namespaced tool
/// names cannot collide with the group's, and declares exactly the configuration-independent
/// tool surface under roles that reflect the group's operation needs.
/// </summary>
[TestFixture]
public sealed class RepoContextAppManifestTests
{
    private static AppManifest Manifest()
    {
        var result = RepoContextAppManifest.Load();
        Assert.That(result.IsValid, Is.True, string.Join("; ", result.Errors.Select(e => $"{e.Code} {e.Path}: {e.Message}")));
        return result.Manifest!;
    }

    private static IEnumerable<RepoContextToolGroup> EveryConfiguration()
    {
        foreach (var writes in new[] { false, true })
        foreach (var workspace in new[] { false, true })
        foreach (var guarded in new[] { false, true })
            yield return new RepoContextToolGroup(writes, workspace, guarded);
    }

    [Test]
    public void Manifest_is_embedded_under_the_pinned_resource_name()
        => Assert.That(
            typeof(RepoContextAppManifest).Assembly.GetManifestResourceNames(),
            Does.Contain(RepoContextAppManifest.ResourceName));

    [Test]
    public void Load_returns_a_valid_manifest_with_no_diagnostics()
    {
        var result = RepoContextAppManifest.Load();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsValid, Is.True);
            Assert.That(result.Errors, Is.Empty);
            Assert.That(result.Manifest, Is.Not.Null);
        });
    }

    [Test]
    public void Manifest_loads_through_the_generic_resource_loader_without_app_code()
    {
        var result = AppManifestResources.Load(typeof(RepoContextAppManifest).Assembly, RepoContextAppManifest.ResourceName);

        Assert.That(result.IsValid, Is.True);
    }

    [Test]
    public async Task Manifest_resolves_through_the_in_image_app_source()
    {
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(new InImageAppRegistration(
            AppSlug.Parse(RepoContextAppManifest.Slug),
            typeof(RepoContextAppManifest).Assembly,
            RepoContextAppManifest.ResourceName));
        var source = new InImageAppSource(Microsoft.Extensions.Options.Options.Create(options));

        var resolved = await source.ResolveAsync(AppSlug.Parse(RepoContextAppManifest.Slug));

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Status, Is.EqualTo(AppSourceStatus.Resolved));
            Assert.That(resolved.Manifest!.Identity.Slug.Value, Is.EqualTo(RepoContextAppManifest.Slug));
        });
    }

    [Test]
    public void Manifest_identity_carries_the_hyphenated_slug_and_a_first_party_in_image_provenance()
    {
        var identity = Manifest().Identity;

        Assert.Multiple(() =>
        {
            Assert.That(identity.Slug.Value, Is.EqualTo("repo-context"));
            Assert.That(identity.Slug.Value, Is.EqualTo(RepoContextAppManifest.Slug));
            Assert.That(identity.Provenance.Source, Is.EqualTo("in-image"));
            Assert.That(identity.Provenance.Publisher, Is.EqualTo("first-party"));
        });
    }

    [Test]
    public void App_tool_names_never_collide_with_any_group_tool_name()
    {
        var slug = AppSlug.Parse(RepoContextAppManifest.Slug);
        var groupNames = EveryConfiguration()
            .SelectMany(g => g.Tools)
            .Select(t => t.ProtocolTool.Name)
            .ToHashSet(StringComparer.Ordinal);

        var colliding = RepoContextAppManifest.AppToolNames
            .Select(name => AppMcpToolName.Compose(slug, name))
            .Where(groupNames.Contains)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(colliding, Is.Empty);
            Assert.That(AppMcpToolName.Compose(slug, "search"), Is.EqualTo("repo-context_search"));
        });
    }

    [Test]
    public void The_unhyphenated_slug_would_collide_which_is_why_it_is_not_used()
    {
        var composed = AppMcpToolName.Compose(AppSlug.Parse("repocontext"), "search");

        Assert.That(new RepoContextToolGroup().Tools.Select(t => t.ProtocolTool.Name), Does.Contain(composed));
    }

    [Test]
    public void Declared_mcp_tools_match_the_app_tool_names_in_order()
        => Assert.That(Manifest().McpTools.Select(t => t.Name), Is.EqualTo(RepoContextAppManifest.AppToolNames));

    [Test]
    public void App_tool_names_are_exactly_the_tools_every_host_configuration_contributes()
    {
        var alwaysOn = EveryConfiguration()
            .Select(g => g.Tools.Select(t => t.ProtocolTool.Name).ToHashSet(StringComparer.Ordinal))
            .Aggregate((acc, next) => { acc.IntersectWith(next); return acc; });

        var expected = RepoContextAppManifest.AppToolNames
            .Select(n => RepoContextAppManifest.GroupToolPrefix + n)
            .Order(StringComparer.Ordinal);

        Assert.That(alwaysOn.Order(StringComparer.Ordinal), Is.EqualTo(expected));
    }

    [Test]
    public void Every_declared_tool_requires_the_reader_role()
        => Assert.That(Manifest().McpTools.Select(t => t.Role).Distinct(), Is.EqualTo(new[] { "reader" }));

    [Test]
    public void Reader_role_reads_every_declared_tree()
    {
        var manifest = Manifest();
        var reader = manifest.Roles.Single(r => r.Name == "reader");

        Assert.Multiple(() =>
        {
            Assert.That(reader.Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead));
            Assert.That(reader.Scopes.Select(s => s.Tree), Is.EqualTo(manifest.Trees.Select(t => t.Name)));
            Assert.That(reader.Scopes.All(s => s.Kind == LatticeScopeKind.Tree && s.App is null), Is.True);
        });
    }

    [Test]
    public void Curator_role_carries_the_data_plane_operations_the_mutating_tools_use_over_every_tree()
    {
        var manifest = Manifest();
        var curator = manifest.Roles.Single(r => r.Name == "curator");

        Assert.Multiple(() =>
        {
            Assert.That(
                curator.Operations,
                Is.EqualTo(LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write
                    | LatticeOperation.Delete | LatticeOperation.RangeDelete));
            Assert.That(curator.Scopes.Select(s => s.Tree), Is.EqualTo(manifest.Trees.Select(t => t.Name)));
            Assert.That(curator.Scopes.All(s => s.Kind == LatticeScopeKind.Tree && s.App is null), Is.True);
        });
    }

    [Test]
    public void Manifest_declares_no_subscriptions_replication_or_schema()
    {
        var manifest = Manifest();

        Assert.Multiple(() =>
        {
            Assert.That(manifest.Subscriptions, Is.Empty);
            Assert.That(manifest.Replication, Is.Null, "replication stays driven by the repocontext replication companion");
            Assert.That(manifest.Schema, Is.Null);
        });
    }

    [TestCase("repocontext_search", "search")]
    [TestCase("repocontext_list_topics", "list_topics")]
    [TestCase("repocontext_claim_status", "claim_status")]
    public void LocalNameFor_maps_an_app_surface_tool_to_its_local_name(string groupName, string expected)
        => Assert.That(RepoContextAppManifest.LocalNameFor(groupName), Is.EqualTo(expected));

    [TestCase(null)]
    [TestCase("")]
    [TestCase("search")]
    [TestCase("repocontext_")]
    [TestCase("repocontext_remember")]
    [TestCase("repocontext_changed")]
    [TestCase("repocontext_list_repos")]
    [TestCase("repocontext_searchx")]
    [TestCase("other_search")]
    public void LocalNameFor_returns_null_for_a_tool_outside_the_app_surface(string? groupName)
        => Assert.That(RepoContextAppManifest.LocalNameFor(groupName), Is.Null);
}
