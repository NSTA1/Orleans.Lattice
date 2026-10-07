using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the arms of <c>LatticeBackupControl</c> that only a genuinely
/// asynchronous tenant resolution, or a tenancy refusal on the restore verb,
/// can reach.
/// </summary>
/// <remarks>
/// <para>
/// <c>ResolveEffectiveScopeAsync</c> is split into a synchronous warm path and an
/// asynchronous fallback. Every other fixture in this project resolves the tenant
/// through <c>FixedTenantResolver</c>, whose <c>ResolveCurrentAsync</c> hands back
/// an <i>already-completed</i> <see cref="ValueTask{TResult}"/> - so even its
/// <c>resolvesSynchronously: false</c> mode leaves
/// <c>pending.IsCompletedSuccessfully</c> true and the awaiting continuation
/// <c>AwaitEffectiveScopeAsync</c> is never entered. Forcing that continuation
/// needs a resolver that actually yields, which is what
/// <see cref="YieldingTenantResolver"/> supplies.
/// </para>
/// <para>
/// The same split hides the back-fill loop in <c>ResolveEffectiveSetRequestAsync</c>:
/// it copies the already-composed prefix of a set request only when an
/// <i>earlier</i> scope was returned unchanged and a <i>later</i> one was
/// rebuilt. A request whose every scope composes never runs it, which is why a
/// mixed request - one already-qualified id followed by a tenant-local name - is
/// needed here.
/// </para>
/// </remarks>
public sealed partial class LatticeBackupControlTenancyTests
{
    // ---- The asynchronous tenant-resolution fallback ---------------------

    /// <summary>
    /// An <see cref="ITenantContextResolver"/> whose asynchronous resolution
    /// genuinely suspends, so the <see cref="ValueTask{TResult}"/> the facade
    /// inspects is incomplete and the awaiting continuation must run.
    /// </summary>
    private sealed class YieldingTenantResolver(TenantId tenant) : ITenantContextResolver
    {
        public async ValueTask<TenantId> ResolveCurrentAsync(
            CancellationToken cancellationToken = default)
        {
            // Task.Yield always suspends, so the ValueTask handed back to
            // ResolveEffectiveScopeAsync cannot be already-completed.
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            return tenant;
        }

        // Declining the synchronous fast path is what routes resolution through
        // ResolveCurrentAsync at all.
        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = default;
            return false;
        }
    }

    private ILatticeBackupControl AsyncResolvedControlFor(string tenant) =>
        _fixture.CreateControlForTenant(new YieldingTenantResolver(TenantId.Parse(tenant)));

    [Test]
    public async Task CreateBackupAsync_composes_the_scope_when_the_tenant_resolves_asynchronously()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "async-secret");

        var captured = await CaptureAsync(AsyncResolvedControlFor(Acme),
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        // The composed id proves the awaited continuation rebuilt the scope; a
        // dropped continuation would capture the bare, uncomposed name.
        Assert.That(captured.Manifest.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
    }

    [Test]
    public async Task An_asynchronously_resolved_tenant_reaches_the_same_tree_as_a_synchronous_one()
    {
        await _fixture.InitializeAsync();
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        // The two resolution paths must be indistinguishable in their result:
        // the fast path and the awaited continuation compose the same id.
        var viaAsync = await CaptureAsync(AsyncResolvedControlFor(Acme),
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));
        var viaSync = await CaptureAsync(ControlFor(Acme),
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(LocalName)));

        Assert.That(
            viaAsync.Manifest.Scope.TreeId,
            Is.EqualTo(viaSync.Manifest.Scope.TreeId),
            "The asynchronous fallback must compose exactly what the warm path composes.");
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_composes_the_scope_when_the_tenant_resolves_asynchronously()
    {
        await _fixture.InitializeAsync();

        var caps = await AsyncResolvedControlFor(Acme)
            .ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(LocalName));

        // ProbeCapabilitiesAsync reports the scope it probed, so the composed id
        // is observable directly rather than only through a captured manifest.
        Assert.That(caps.Scope.TreeId, Is.EqualTo(Effective(Acme, LocalName)));
    }

    // ---- The set-request back-fill loop ----------------------------------

    [Test]
    public async Task CaptureSetAsync_backfills_the_unchanged_scopes_preceding_the_first_composed_one()
    {
        await _fixture.InitializeAsync();

        // An already-qualified id is passed through unchanged (same reference),
        // so scope 0 leaves the composed list null; the tenant-local name at
        // scope 1 is rebuilt, which is what forces the preceding scope to be
        // copied across into the new list.
        var qualified = Effective(Acme, "ledger");
        await SeedAsync(qualified, "k", "v");
        await SeedAsync(Effective(Acme, LocalName), "k", "v");

        var control = ControlFor(Acme);
        var operations = (ILatticeBackupOperations)control;
        var handle = await operations.StartBackupSetAsync(
            new LatticeBackupSetCaptureRequest(
                "set",
                [
                    BackupScopeSelector.WholeTree(qualified),
                    BackupScopeSelector.WholeTree(LocalName),
                ]));
        var status = await UntilTerminalAsync(operations, handle.OperationId);
        var treeIds = BackupOperationResults.ReadMemberBackupIds(status.Result)
            .Select(async id => (await control.DescribeBackupAsync(id))!.Manifest.Scope.TreeId)
            .Select(t => t.GetAwaiter().GetResult())
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Has.Length.EqualTo(2),
                "Both members must survive the back-fill; dropping the prefix would lose the first.");
            Assert.That(treeIds, Does.Contain(qualified),
                "The already-qualified scope must be carried across unchanged, not discarded.");
            Assert.That(treeIds, Does.Contain(Effective(Acme, LocalName)),
                "The tenant-local scope must be composed.");
        });
    }

    // ---- A tenancy refusal on the restore verb is an answer, not a fault --

    [Test]
    public async Task ProbeCapabilitiesAsync_reports_a_foreign_scope_as_unrestorable_without_failing()
    {
        // The sibling cross-tenant fixture pins this translation for the capture
        // verb (IsBackupAuthorizedAsync). The restore verb has its own probe and
        // its own pair of catch arms, and a tenancy refusal there must likewise
        // be reported as a false capability rather than escaping as a fault.
        await _fixture.InitializeAsync();

        var caps = await ScopedControlFor(Acme)
            .ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(Effective(Globex, LocalName)));

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanRestore, Is.False,
                "A tenancy refusal on the restore verb must read as 'cannot restore'.");
            Assert.That(caps.CanCapture, Is.False,
                "The capture verb is refused for the same foreign tree.");
        });
    }

    [Test]
    public async Task ProbeCapabilitiesAsync_still_reports_the_callers_own_scope_as_restorable()
    {
        // The negative control for the test above: the refusal must follow the
        // tenant scope rather than being returned unconditionally, or the probe
        // would report false for everything and the assertion above would pass
        // without the translation working.
        await _fixture.InitializeAsync();

        var caps = await ScopedControlFor(Acme)
            .ProbeCapabilitiesAsync(BackupScopeSelector.WholeTree(Effective(Acme, LocalName)));

        Assert.Multiple(() =>
        {
            Assert.That(caps.CanRestore, Is.True);
            Assert.That(caps.CanCapture, Is.True);
        });
    }

    [Test]
    public async Task StreamBackupsAsync_prunes_another_tenants_rows_instead_of_failing()
    {
        // The paged listing already pins this pruning; the streaming surface
        // carries its own copy of the read check and so is a separate claim.
        // Without it, a drained enumeration could still fault - or disclose -
        // on a row belonging to another tenant.
        await _fixture.InitializeAsync();
        var globex = await CaptureAsAsync(Globex, LocalName);
        var acme = await CaptureAsAsync(Acme, LocalName);

        var streamed = new List<string>();
        await foreach (var manifest in ScopedControlFor(Acme).StreamBackupsAsync())
        {
            streamed.Add(manifest.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Does.Contain(acme.BackupId),
                "The caller must still see its own backup.");
            Assert.That(streamed, Does.Not.Contain(globex.BackupId),
                "Another tenant's backup must be pruned from the stream, not faulted on.");
        });
    }

    [Test]
    public async Task StreamBackupsAsync_drains_empty_rather_than_disclosing_a_foreign_tree()
    {
        // Every row belongs to someone else - the case that would otherwise
        // throw, naming the foreign tree in the message.
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);

        var streamed = new List<BackupManifest>();
        await foreach (var manifest in ScopedControlFor(Acme).StreamBackupsAsync())
        {
            streamed.Add(manifest);
        }

        Assert.Multiple(() =>
        {
            Assert.That(streamed, Is.Empty,
                "A caller entitled to nothing must drain empty, not error.");
            Assert.That(
                streamed.Select(m => m.Scope.TreeId),
                Has.None.EqualTo(Effective(Globex, LocalName)));
        });
    }
}
