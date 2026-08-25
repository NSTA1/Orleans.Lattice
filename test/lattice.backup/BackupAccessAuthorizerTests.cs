using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupAccessAuthorizer"/>: the backup / restore
/// authorization seam consults the registered <see cref="ILatticeAccessGate"/>
/// for the dedicated <see cref="LatticeOperation.Backup"/> /
/// <see cref="LatticeOperation.Restore"/> capability at tree / prefix / key
/// granularity, allows on an allow decision, and fails closed (throwing
/// <see cref="LatticeAuthorizationDeniedException"/>) on a deny or a partial
/// (filtered) allow. Driven by a capturing gate double so the exact request the
/// seam issues is asserted without a cluster.
/// </summary>
[TestFixture]
public sealed class BackupAccessAuthorizerTests
{
    private const string Tree = "orders";

    private static BackupAccessAuthorizer Create(
        CapturingAccessGate gate,
        ILatticeMembershipContext? membership = null) =>
        new(gate, membership);

    // ---- Allow: correct operation and request shape ---------------------

    [Test]
    public async Task AuthorizeBackup_tree_scope_issues_a_whole_tree_backup_request()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Backup));
            Assert.That(gate.Last.TreeId, Is.EqualTo(Tree));
            Assert.That(gate.Last.Key, Is.Null);
            Assert.That(gate.Last.RangeStart, Is.Null);
            Assert.That(gate.Last.RangeEnd, Is.Null);
        });
    }

    [Test]
    public async Task AuthorizeRestore_tree_scope_issues_a_whole_tree_restore_request()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Restore));
            Assert.That(gate.Last.TreeId, Is.EqualTo(Tree));
            Assert.That(gate.Last.Key, Is.Null);
        });
    }

    [Test]
    public async Task AuthorizeBackup_key_scope_issues_a_point_request_at_the_key()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeBackupAsync(BackupScopeSelector.Key(Tree, "k-42"));

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Backup));
            Assert.That(gate.Last.Key, Is.EqualTo("k-42"));
            Assert.That(gate.Last.RangeStart, Is.Null);
        });
    }

    [Test]
    public async Task AuthorizeRestore_prefix_scope_issues_a_point_request_at_the_prefix_root()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.Prefix(Tree, "tenant-a/"));

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Restore));
            Assert.That(gate.Last.Key, Is.EqualTo("tenant-a/"),
                "a prefix scope is authorized at its root as a point so a prefix-scoped grant matches cleanly");
            Assert.That(gate.Last.RangeStart, Is.Null);
        });
    }

    [Test]
    public void AuthorizeBackup_allow_does_not_throw_for_any_scope()
    {
        var authorizer = Create(new CapturingAccessGate());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)), Throws.Nothing);
            Assert.That(async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.Prefix(Tree, "p/")), Throws.Nothing);
            Assert.That(async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.Key(Tree, "k")), Throws.Nothing);
        });
    }

    // ---- Deny: fail closed ----------------------------------------------

    [Test]
    public void AuthorizeBackup_deny_throws_with_backup_operation_and_reason()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no backup grant"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.Backup));
            Assert.That(ex.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Reason, Is.EqualTo("no backup grant"));
        });
    }

    [Test]
    public void AuthorizeRestore_deny_throws_with_restore_operation()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no restore grant"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.Key(Tree, "k")));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.Restore));
    }

    [Test]
    public void AuthorizeBackup_whole_tree_partial_allow_fails_closed()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Filtered(static _ => true, "partial"));
        var authorizer = Create(gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)),
            "a whole-tree backup cannot be narrowed and a filtered allow is refused");
    }

    [Test]
    public void AuthorizeBackup_point_scope_excluded_by_filter_fails_closed()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Filtered(static _ => false, "excluded"));
        var authorizer = Create(gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.Key(Tree, "k")),
            "a point-scope grant whose per-key filter excludes the key fails closed");
    }

    // ---- Subject flows through to the denial ----------------------------

    [Test]
    public void AuthorizeBackup_deny_carries_the_resolved_caller_subject()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("denied"));
        var membership = new FixedMembershipContext(new LatticeSubject("alice"));
        var authorizer = Create(gate, membership);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SubjectId, Is.EqualTo("alice"));
            Assert.That(gate.Last.Subject.SubjectId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void AuthorizeBackup_with_no_membership_resolves_the_anonymous_subject()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("denied"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)));

        Assert.That(ex!.SubjectId, Is.EqualTo(LatticeSubject.AnonymousSubjectId));
    }

    // ---- Tenant isolation seam ------------------------------------------

    [Test]
    public async Task AuthorizeBackup_active_tenant_scope_authorizes_capture()
    {
        var gate = new CapturingAccessGate();
        var scope = new RecordingTenantScope();
        var authorizer = new BackupAccessAuthorizer(gate, membership: null, tenantScope: scope);

        await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(scope.CapturedTreeId, Is.EqualTo(Tree), "a capture consults AuthorizeCapture with the tree id");
            Assert.That(scope.RestoreTargetTreeId, Is.Null, "a capture never consults the restore-target check");
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Backup),
                "the capability gate still runs after the tenant check");
        });
    }

    [Test]
    public async Task AuthorizeRestore_active_tenant_scope_authorizes_restore_target()
    {
        var gate = new CapturingAccessGate();
        var scope = new RecordingTenantScope();
        var authorizer = new BackupAccessAuthorizer(gate, membership: null, tenantScope: scope);

        await authorizer.AuthorizeRestoreAsync(BackupScopeSelector.WholeTree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RestoreTargetTreeId, Is.EqualTo(Tree),
                "a restore consults AuthorizeRestoreTarget with the target tree id");
            Assert.That(scope.CapturedTreeId, Is.Null, "a restore never consults the capture check");
        });
    }

    [Test]
    public void AuthorizeBackup_tenant_scope_refusal_propagates_before_the_gate()
    {
        var gate = new CapturingAccessGate();
        var scope = new RecordingTenantScope { Refuse = true };
        var authorizer = new BackupAccessAuthorizer(gate, membership: null, tenantScope: scope);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)),
                Throws.InstanceOf<LatticeBackupTenantIsolationException>(),
                "a cross-tenant capture is refused by the tenant scope");
            Assert.That(gate.Consulted, Is.False,
                "the tenant check runs before the capability gate, so a refusal short-circuits it");
        });
    }

    [Test]
    public async Task AuthorizeBackup_inactive_tenant_scope_runs_no_tenant_check()
    {
        var gate = new CapturingAccessGate();
        var scope = new RecordingTenantScope { Active = false };
        var authorizer = new BackupAccessAuthorizer(gate, membership: null, tenantScope: scope);

        await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(scope.CapturedTreeId, Is.Null, "an inactive tenant scope is never consulted");
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Backup), "the capability gate still runs");
        });
    }

    [Test]
    public void Constructor_null_tenant_scope_defaults_to_the_inert_scope()
    {
        var authorizer = new BackupAccessAuthorizer(new CapturingAccessGate(), membership: null, tenantScope: null);

        Assert.That(
            async () => await authorizer.AuthorizeBackupAsync(BackupScopeSelector.WholeTree(Tree)),
            Throws.Nothing,
            "with no tenancy add-on the null scope is inert and runs no tenant check");
    }

    // ---- Construction guards --------------------------------------------

    [Test]
    public void Constructor_null_gate_throws()
    {
        Assert.That(() => new BackupAccessAuthorizer(null!), Throws.ArgumentNullException);
    }

    // ---- Scope factory guards -------------------------------------------

    [Test]
    public void BackupScopeSelector_factories_reject_null_or_empty_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => BackupScopeSelector.WholeTree(null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupScopeSelector.WholeTree(""), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupScopeSelector.Prefix(Tree, null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupScopeSelector.Prefix(Tree, ""), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupScopeSelector.Key(Tree, null!), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => BackupScopeSelector.Key("", "k"), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void BackupScopeSelector_factories_populate_the_expected_shape()
    {
        Assert.Multiple(() =>
        {
            var tree = BackupScopeSelector.WholeTree(Tree);
            Assert.That(tree.Kind, Is.EqualTo(BackupScopeKind.WholeTree));
            Assert.That(tree.TreeId, Is.EqualTo(Tree));
            Assert.That(tree.KeyOrPrefix, Is.Null);

            var prefix = BackupScopeSelector.Prefix(Tree, "p/");
            Assert.That(prefix.Kind, Is.EqualTo(BackupScopeKind.Prefix));
            Assert.That(prefix.KeyOrPrefix, Is.EqualTo("p/"));

            var key = BackupScopeSelector.Key(Tree, "k");
            Assert.That(key.Kind, Is.EqualTo(BackupScopeKind.Key));
            Assert.That(key.KeyOrPrefix, Is.EqualTo("k"));
        });
    }

    /// <summary>
    /// A minimal capturing <see cref="ILatticeAccessGate"/> double: records the
    /// last request and returns a configurable decision (allow by default).
    /// </summary>
    private sealed class CapturingAccessGate(Func<LatticeAccessRequest, LatticeAccessDecision>? decide = null)
        : ILatticeAccessGate
    {
        private readonly Func<LatticeAccessRequest, LatticeAccessDecision> _decide =
            decide ?? (_ => LatticeAccessDecision.Allow());

        public LatticeAccessRequest Last { get; private set; }

        public bool Consulted { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            Last = request;
            Consulted = true;
            return new ValueTask<LatticeAccessDecision>(_decide(request));
        }
    }

    /// <summary>
    /// A recording <see cref="ILatticeBackupTenantScope"/> double: captures which
    /// authorization method the seam invoked and the tree id it passed, can be made
    /// inactive, and can be made to refuse (throw) so the fail-fast ordering is
    /// asserted without a real tenant registry.
    /// </summary>
    private sealed class RecordingTenantScope : ILatticeBackupTenantScope
    {
        public bool Active { get; init; } = true;

        public bool Refuse { get; init; }

        public string? CapturedTreeId { get; private set; }

        public string? RestoreTargetTreeId { get; private set; }

        public bool IsActive => Active;

        public void AuthorizeCapture(string treeId)
        {
            CapturedTreeId = treeId;
            if (Refuse)
            {
                throw new LatticeBackupTenantIsolationException("refused");
            }
        }

        public void AuthorizeRestoreTarget(string treeId)
        {
            RestoreTargetTreeId = treeId;
            if (Refuse)
            {
                throw new LatticeBackupTenantIsolationException("refused");
            }
        }

        public ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
            string targetTreeId,
            CancellationToken cancellationToken = default) =>
            new(PermissiveBackupRestoreAdmission.Instance);
    }

    /// <summary>
    /// A membership context double that resolves a fixed subject synchronously.
    /// </summary>
    private sealed class FixedMembershipContext(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);

        public bool TryResolveCurrent(out LatticeSubject resolved)
        {
            resolved = subject;
            return true;
        }
    }
}
