using NSubstitute;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema plugin's controlled domain model: the one contract the host
/// resolves for the plugin. It projects governable trees into the plugin's own
/// shape, publishes the per-tree probe as scoped access decisions, and forwards
/// every policy, versioning, remediation, compliance, and dead-letter call to
/// the feature services.
/// </summary>
[TestFixture]
public sealed class SchemaPluginDomainTests
{
    private ICatalogReader _catalog = null!;
    private ISchemaPolicyService _policy = null!;
    private ISchemaVersioningService _versioning = null!;
    private ISchemaComplianceService _compliance = null!;
    private ISchemaAdminCapabilityService _capabilities = null!;
    private ExplorerPluginAccessStore _access = null!;
    private SchemaPluginDomain _domain = null!;

    [SetUp]
    public void SetUp()
    {
        _catalog = Substitute.For<ICatalogReader>();
        _policy = Substitute.For<ISchemaPolicyService>();
        _versioning = Substitute.For<ISchemaVersioningService>();
        _compliance = Substitute.For<ISchemaComplianceService>();
        _capabilities = Substitute.For<ISchemaAdminCapabilityService>();
        _access = new ExplorerPluginAccessStore();
        _domain = new SchemaPluginDomain(_catalog, _policy, _versioning, _compliance, _capabilities, _access);
    }

    [Test]
    public void The_domain_rejects_every_null_dependency()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new SchemaPluginDomain(null!, _policy, _versioning, _compliance, _capabilities, _access),
                Throws.ArgumentNullException);
            Assert.That(
                () => new SchemaPluginDomain(_catalog, null!, _versioning, _compliance, _capabilities, _access),
                Throws.ArgumentNullException);
            Assert.That(
                () => new SchemaPluginDomain(_catalog, _policy, null!, _compliance, _capabilities, _access),
                Throws.ArgumentNullException);
            Assert.That(
                () => new SchemaPluginDomain(_catalog, _policy, _versioning, null!, _capabilities, _access),
                Throws.ArgumentNullException);
            Assert.That(
                () => new SchemaPluginDomain(_catalog, _policy, _versioning, _compliance, null!, _access),
                Throws.ArgumentNullException);
            Assert.That(
                () => new SchemaPluginDomain(_catalog, _policy, _versioning, _compliance, _capabilities, null!),
                Throws.ArgumentNullException);
        });
    }

    // ---- tree discovery ----------------------------------------------------

    [Test]
    public async Task Listing_trees_pages_to_completion_and_projects_the_plugins_own_shape()
    {
        _catalog.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Page("orders", next: "cursor"));
        _catalog.LoadAsync(CatalogKind.Trees, "cursor", Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Page("invoices", next: null));

        var result = await _domain.ListGovernableTreesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.True);
            Assert.That(result.Trees.Select(t => t.Id), Is.EqualTo(new[] { "orders", "invoices" }));
            Assert.That(result.Trees[0].Label, Is.EqualTo("orders"));
            Assert.That(result.Trees[0].Lifecycle, Is.EqualTo("active"));
            Assert.That(result.Trees[0].ShardCount, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task Listing_trees_excludes_restore_shadows()
    {
        _catalog.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new CatalogPage
            {
                Items =
                [
                    new CatalogItem { Id = "orders", Kind = CatalogKind.Trees },
                    new CatalogItem { Id = "orders-shadow", Kind = CatalogKind.Trees, RestoreShadowOfTreeId = "orders" },
                ],
            });

        var result = await _domain.ListGovernableTreesAsync();

        Assert.That(
            result.Trees.Select(t => t.Id),
            Is.EqualTo(new[] { "orders" }),
            "a restore shadow is an internal restore artifact, never a governance target");
    }

    [Test]
    public async Task A_discovery_failure_folds_into_a_retryable_message()
    {
        _catalog.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<CatalogPage>(_ => throw new InvalidOperationException("no endpoint"));

        var result = await _domain.ListGovernableTreesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.IsSuccess, Is.False);
            Assert.That(result.Error, Is.EqualTo("no endpoint"));
            Assert.That(result.Trees, Is.Empty);
        });
    }

    // ---- the scoped per-tree probe ----------------------------------------

    [Test]
    public async Task Probing_a_tree_files_one_scoped_decision_per_capability()
    {
        _capabilities.ProbeTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new SchemaCapabilitySnapshot { CanViewPolicy = true, CanScanCompliance = true });

        var grants = await _domain.ProbeTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(grants.TreeId, Is.EqualTo("orders"));
            Assert.That(grants.IsAllowed(SchemaCapability.ViewPolicy), Is.True);
            Assert.That(grants.IsAllowed(SchemaCapability.ScanCompliance), Is.True);
            Assert.That(grants.IsAllowed(SchemaCapability.ManagePolicy), Is.False);

            foreach (var capability in SchemaTreeGrants.Capabilities)
            {
                Assert.That(
                    _access.Snapshot().ContainsKey(SchemaTreeGrants.KeyFor("orders", capability)),
                    Is.True,
                    $"{capability} must be filed, so a later re-probe can revoke it");
            }
        });
    }

    [Test]
    public async Task A_re_probe_that_loses_a_capability_revokes_the_scoped_decision()
    {
        _capabilities.ProbeTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new SchemaCapabilitySnapshot { CanManagePolicy = true });
        var grants = await _domain.ProbeTreeAsync("orders");
        var opened = grants.IsAllowed(SchemaCapability.ManagePolicy);

        _capabilities.ProbeTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(SchemaCapabilitySnapshot.None);
        await _domain.ProbeTreeAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(opened, Is.True);
            Assert.That(grants.IsAllowed(SchemaCapability.ManagePolicy), Is.False);
        });
    }

    [Test]
    public async Task A_probe_never_touches_the_plugin_level_decision()
    {
        _capabilities.ProbeTreeAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new SchemaCapabilitySnapshot { CanManagePolicy = true });

        await _domain.ProbeTreeAsync("orders");

        Assert.That(
            _access.Get(SchemaPluginKeys.PluginId).IsAllowed,
            Is.False,
            "the coarse gate is the host refresher's to file, not the per-tree probe's");
    }

    [Test]
    public void Probing_rejects_an_empty_tree_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _domain.ProbeTreeAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _domain.ProbeTreeAsync(string.Empty), Throws.ArgumentException);
        });
    }

    // ---- forwarding --------------------------------------------------------

    [Test]
    public async Task Policy_calls_forward_to_the_policy_service()
    {
        var read = SchemaReadView<LatticeSchemaPolicy>.Succeeded(null);
        var result = SchemaOperationResult.Success("ok");
        var policy = new LatticeSchemaPolicy([LatticeSchemaRule.Utf8(null)], strictIngest: false);
        _policy.GetPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(read);
        _policy.SetPolicyAsync("orders", policy, Arg.Any<CancellationToken>()).Returns(result);
        _policy.ClearPolicyAsync("orders", Arg.Any<CancellationToken>()).Returns(result);

        Assert.Multiple(async () =>
        {
            Assert.That(await _domain.GetPolicyAsync("orders"), Is.SameAs(read));
            Assert.That(await _domain.SetPolicyAsync("orders", policy), Is.SameAs(result));
            Assert.That(await _domain.ClearPolicyAsync("orders"), Is.SameAs(result));
        });
    }

    [Test]
    public async Task Versioning_calls_forward_to_the_versioning_service()
    {
        var config = new LatticeSchemaVersionConfig(1, 2);
        var read = SchemaReadView<LatticeSchemaVersionConfig>.Succeeded(config);
        var remediation = SchemaReadView<LatticeSchemaRemediationReport>.Succeeded(LatticeSchemaRemediationReport.Idle);
        var result = SchemaOperationResult.Success("ok");

        _versioning.GetVersionConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(read);
        _versioning.SetVersionConfigAsync("orders", config, Arg.Any<CancellationToken>()).Returns(result);
        _versioning.AdvanceTargetVersionAsync("orders", 3u, Arg.Any<CancellationToken>()).Returns(result);
        _versioning.AdvanceAndMigrateAsync("orders", 3u, Arg.Any<CancellationToken>()).Returns(result);
        _versioning.MigrateToTargetVersionAsync("orders", Arg.Any<CancellationToken>()).Returns(result);
        _versioning.ClearVersionConfigAsync("orders", Arg.Any<CancellationToken>()).Returns(result);
        _versioning.GetRemediationStatusAsync("orders", Arg.Any<CancellationToken>()).Returns(remediation);

        Assert.Multiple(async () =>
        {
            Assert.That(await _domain.GetVersionConfigAsync("orders"), Is.SameAs(read));
            Assert.That(await _domain.SetVersionConfigAsync("orders", config), Is.SameAs(result));
            Assert.That(await _domain.AdvanceTargetVersionAsync("orders", 3u), Is.SameAs(result));
            Assert.That(await _domain.AdvanceAndMigrateAsync("orders", 3u), Is.SameAs(result));
            Assert.That(await _domain.MigrateToTargetVersionAsync("orders"), Is.SameAs(result));
            Assert.That(await _domain.ClearVersionConfigAsync("orders"), Is.SameAs(result));
            Assert.That(await _domain.GetRemediationStatusAsync("orders"), Is.SameAs(remediation));
        });
    }

    [Test]
    public async Task Compliance_and_dead_letter_calls_forward_to_the_compliance_service()
    {
        var scan = SchemaReadView<LatticeSchemaComplianceReport>.Succeeded(
            LatticeSchemaComplianceReport.Ungoverned("orders"));
        var deadLetters = new SchemaDeadLetterView { Status = SchemaOperationStatus.Succeeded };

        _compliance.ScanComplianceAsync("orders", Arg.Any<CancellationToken>()).Returns(scan);
        _compliance.ListDeadLettersAsync("orders", 100, Arg.Any<CancellationToken>()).Returns(deadLetters);

        Assert.Multiple(async () =>
        {
            Assert.That(await _domain.ScanComplianceAsync("orders"), Is.SameAs(scan));
            Assert.That(await _domain.ListDeadLettersAsync("orders", 100), Is.SameAs(deadLetters));
        });
    }

    private static CatalogPage Page(string id, string? next) => new()
    {
        Items =
        [
            new CatalogItem { Id = id, Kind = CatalogKind.Trees, Lifecycle = "active", ShardCount = 4 },
        ],
        NextPageToken = next,
    };
}
