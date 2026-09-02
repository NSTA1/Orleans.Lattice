using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests that drive every <see cref="BackupToolGroup"/> tool's own
/// invocation delegate through <see cref="McpToolInvocation"/>: the body that
/// stamps the caller credential, resolves <see cref="ILatticeBackupControl"/> from
/// the request service provider, and forwards the bound arguments to
/// <c>BackupToolInvocations</c>. The sibling <see cref="BackupToolGroupTests"/>
/// covers only the advertised metadata, which never reaches these bodies.
/// </summary>
/// <remarks>
/// The delegates are what wire an advertised tool to a facade call, so a
/// mis-wired argument (a swapped <c>treeId</c>/<c>name</c>, a dropped optional)
/// is invisible to a metadata-only assertion. Each test therefore asserts on the
/// value that reached the facade, not merely that the call succeeded. All
/// deterministic - a stateful in-memory facade fake, no cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class BackupToolGroupInvocationTests
{
    private FakeLatticeBackupControl _control = null!;

    [SetUp]
    public void SetUp() => _control = new FakeLatticeBackupControl();

    private static LatticeBackupCaptureRequest Capture(string name, string treeId)
        => new(name, BackupScopeSelector.WholeTree(treeId));

    private ServiceProvider Services(bool withHttpContext = false, LatticeCredential? credential = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeBackupControl>(_control);
        if (withHttpContext)
        {
            services.AddSingleton<IHttpContextAccessor>(
                new HttpContextAccessor { HttpContext = new DefaultHttpContext() });
            services.AddSingleton<ILatticeApiMcpCredentialBridge>(new StubBridge(credential));
        }

        return services.BuildServiceProvider();
    }

    private static McpServerTool Tool(string name)
        => new BackupToolGroup(
                Options.Create(new LatticeApiMcpOptions { EnableBackupControlTools = true }))
            .Tools.Single(t => t.ProtocolTool.Name == name);

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(
            Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    // ---- read-only inspect tools -------------------------------------------

    [Test]
    public async Task List_tool_delegate_returns_the_catalog_page()
    {
        await _control.CreateBackupAsync(Capture("nightly", "orders"));

        var page = await CallAsync<McpBackupCatalogPage>(
            "lattice_backup_list",
            ("pageSize", 10),
            ("pageToken", null),
            ("orderByCreatedDescending", true));

        Assert.That(page.Entries.Select(e => e.Name), Is.EqualTo(new[] { "nightly" }),
            "The list delegate must return the facade's catalog page.");
    }

    [Test]
    public async Task List_tool_delegate_binds_its_defaults_when_no_arguments_are_supplied()
    {
        await _control.CreateBackupAsync(Capture("nightly", "orders"));

        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(Tool("lattice_backup_list"), services);

        Assert.That(result.Structured<McpBackupCatalogPage>().Entries, Has.Count.EqualTo(1),
            "Every list argument is optional, so a no-argument call must bind the defaults and still succeed.");
    }

    [Test]
    public async Task Describe_tool_delegate_forwards_the_backup_id()
    {
        var created = await _control.CreateBackupAsync(Capture("nightly", "orders"));

        var chain = await CallAsync<McpBackupChain>("lattice_backup_describe", ("backupId", created.BackupId));

        Assert.That(chain.Found, Is.True);
        Assert.That(chain.Manifest!.Id, Is.EqualTo(created.BackupId),
            "The delegate must forward the bound backupId to the facade.");
    }

    [Test]
    public async Task Describe_tool_delegate_reports_not_found_for_an_unknown_id()
    {
        var chain = await CallAsync<McpBackupChain>("lattice_backup_describe", ("backupId", "missing"));

        Assert.That(chain.Found, Is.False);
    }

    [Test]
    public async Task Inventory_tool_delegate_returns_the_facade_report()
    {
        _control.Inventory = new BackupInventoryReport(7, 3, 2, 1024, null, null, 2, 1, 512);

        var inventory = await CallAsync<McpBackupInventory>("lattice_backup_inventory");

        Assert.That(inventory.TotalBackupCount, Is.EqualTo(7),
            "The inventory delegate must project the facade's report.");
    }

    [Test]
    public async Task Scope_status_tool_delegate_forwards_the_scope_selector()
    {
        _control.ScopeStatus = new BackupScopeStatus(
            scope: BackupScopeSelector.Prefix("orders", "eu/"),
            fullScheduleRegistered: true,
            incrementalScheduleRegistered: false,
            lastFullRunUtc: DateTimeOffset.UnixEpoch,
            lastFullSuccessUtc: DateTimeOffset.UnixEpoch,
            lastIncrementalRunUtc: null,
            lastIncrementalSuccessUtc: null,
            lastRunOutcome: BackupScopeRunOutcome.Success,
            chainDepth: 2);

        var status = await CallAsync<McpBackupScopeStatus>(
            "lattice_backup_scope_status",
            ("treeId", "orders"),
            ("scopeKind", "Prefix"),
            ("keyOrPrefix", "eu/"));

        Assert.Multiple(() =>
        {
            Assert.That(status.Found, Is.True,
                "A scope the facade knows must be reported found through the delegate.");
            Assert.That(status.KeyOrPrefix, Is.EqualTo("eu/"));
        });
    }

    [Test]
    public async Task Scope_status_tool_delegate_reports_not_found_for_an_unknown_scope()
    {
        _control.ScopeStatus = null;

        var status = await CallAsync<McpBackupScopeStatus>(
            "lattice_backup_scope_status", ("treeId", "orders"));

        Assert.That(status.Found, Is.False);
    }

    [Test]
    public async Task Export_artifact_tool_delegate_forwards_the_page_cursor()
    {
        _control.SeedArtifact("bk-0", "art-0", Enumerable.Range(0, 48).Select(i => (byte)i).ToArray());

        var page = await CallAsync<McpBackupArtifactPage>(
            "lattice_backup_export_artifact",
            ("backupId", "bk-0"),
            ("artifactId", "art-0"),
            ("chunkOffset", 1),
            ("maxBytes", 16));

        Assert.Multiple(() =>
        {
            Assert.That(page.ByteCount, Is.EqualTo(16),
                "The delegate must forward the byte budget, so exactly one 16-byte chunk is returned.");
            Assert.That(page.NextChunkOffset, Is.EqualTo(2),
                "The delegate must forward the resume cursor so a caller can drain a large artifact.");
            Assert.That(page.EndOfStream, Is.False);
        });
    }

    // ---- mutating control tools --------------------------------------------

    [Test]
    public async Task Create_tool_delegate_forwards_the_name_and_scope()
    {
        var result = await CallAsync<McpBackupCaptureResult>(
            "lattice_backup_create",
            ("name", "nightly"),
            ("treeId", "orders"),
            ("scopeKind", "Prefix"),
            ("keyOrPrefix", "eu/"),
            ("pageSize", 64));

        Assert.Multiple(() =>
        {
            Assert.That(result.Manifest.Name, Is.EqualTo("nightly"),
                "The delegate must forward the bound name, not the tree id.");
            Assert.That(result.Manifest.TreeId, Is.EqualTo("orders"));
            Assert.That(result.Manifest.KeyOrPrefix, Is.EqualTo("eu/"),
                "The delegate must forward the scope's key prefix.");
        });
    }

    [Test]
    public async Task Create_incremental_tool_delegate_forwards_the_base_backup_id()
    {
        var baseBackup = await _control.CreateBackupAsync(Capture("full", "orders"));

        var result = await CallAsync<McpBackupCaptureResult>(
            "lattice_backup_create_incremental",
            ("name", "delta"),
            ("treeId", "orders"),
            ("baseBackupId", baseBackup.BackupId));

        Assert.Multiple(() =>
        {
            Assert.That(result.Manifest.Name, Is.EqualTo("delta"));
            Assert.That(result.Manifest.BaseBackupId, Is.EqualTo(baseBackup.BackupId),
                "The delegate must forward the base backup id the increment layers on.");
        });
    }

    [Test]
    public async Task Restore_tool_delegate_forwards_the_mode_and_operation_id()
    {
        var result = await CallAsync<McpRestoreResult>(
            "lattice_backup_restore",
            ("backupId", "bk-0"),
            ("targetTreeId", "orders-copy"),
            ("mode", "ShadowCutover"),
            ("operationId", "op-42"));

        Assert.Multiple(() =>
        {
            Assert.That(result.TargetTreeId, Is.EqualTo("orders-copy"));
            Assert.That(result.Mode, Is.EqualTo(nameof(LatticeRestoreMode.ShadowCutover)),
                "The delegate must forward the requested restore mode.");
            Assert.That(result.OperationId, Is.EqualTo("op-42"));
        });
    }

    [Test]
    public async Task Revert_restore_tool_delegate_reconstructs_the_restore_result()
    {
        var result = await CallAsync<McpBackupRevertResult>(
            "lattice_backup_revert_restore",
            ("backupId", "bk-0"),
            ("targetTreeId", "orders"),
            ("operationId", "op-7"),
            ("mode", "ShadowCutover"),
            ("manifestChain", new[] { "bk-0" }),
            ("entriesApplied", 3L),
            ("shadowPhysicalTreeId", "phys-new"),
            ("previousPhysicalTreeId", "phys-old"));

        Assert.That(result.Reverted, Is.True);
        Assert.That(_control.LastReverted, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(_control.LastReverted!.OperationId, Is.EqualTo("op-7"),
                "The delegate must rebuild the restore result from the bound arguments.");
            Assert.That(_control.LastReverted.ShadowPhysicalTreeId, Is.EqualTo("phys-new"));
            Assert.That(_control.LastReverted.PreviousPhysicalTreeId, Is.EqualTo("phys-old"));
            Assert.That(_control.LastReverted.EntriesApplied, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task Delete_tool_delegate_forwards_the_backup_id()
    {
        var created = await _control.CreateBackupAsync(Capture("nightly", "orders"));

        var deleted = await CallAsync<McpBackupDeleteResult>(
            "lattice_backup_delete", ("backupId", created.BackupId));
        var again = await CallAsync<McpBackupDeleteResult>(
            "lattice_backup_delete", ("backupId", created.BackupId));

        Assert.Multiple(() =>
        {
            Assert.That(deleted.Deleted, Is.True);
            Assert.That(again.Deleted, Is.False, "Deleting an absent backup reports deleted=false.");
        });
    }

    // ---- the credential-stamping seam --------------------------------------

    [Test]
    public async Task Delegate_stamps_the_bridged_credential_for_the_facade_call()
    {
        var credential = new LatticeCredential("agent", scheme: "demo", principalId: "agent");
        await using var services = Services(withHttpContext: true, credential);

        LatticeCredential? observed = null;
        _control.OnOperation = () => observed = LatticeCredentialContext.Current;

        await McpToolInvocation.CallAsync(Tool("lattice_backup_inventory"), services);

        Assert.Multiple(() =>
        {
            Assert.That(observed, Is.EqualTo(credential),
                "The delegate must lift the bridged credential onto the ambient context for the facade call, "
                + "so the facade's own access gate resolves the real caller.");
            Assert.That(LatticeCredentialContext.Current, Is.Null,
                "The scope must be disposed once the delegate returns.");
        });
    }

    [Test]
    public async Task Delegate_leaves_the_ambient_credential_clear_when_there_is_no_http_context()
    {
        await using var services = Services();

        LatticeCredential? observed = new("stale");
        _control.OnOperation = () => observed = LatticeCredentialContext.Current;

        await McpToolInvocation.CallAsync(Tool("lattice_backup_inventory"), services);

        Assert.That(observed, Is.Null,
            "With no HTTP context the stamping seam is a no-op, so the facade sees no ambient credential.");
    }

    [Test]
    public void Delegate_surfaces_the_facades_fail_closed_denial()
    {
        _control.Authorized = false;

        Assert.That(
            async () =>
            {
                await using var services = Services();
                await McpToolInvocation.CallAsync(Tool("lattice_backup_inventory"), services);
            },
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path: the facade's denial must surface unchanged.");
    }

    private sealed class StubBridge(LatticeCredential? credential) : ILatticeApiMcpCredentialBridge
    {
        public LatticeCredential? Resolve(HttpContext context) => credential;
    }
}
