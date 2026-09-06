using Orleans.Lattice;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaAccessAuthorizer"/>: the schema-management
/// authorization seam consults the registered <see cref="ILatticeAccessGate"/>
/// for the <see cref="LatticeOperation.SchemaAdmin"/> capability (mutations) or
/// ordinary <see cref="LatticeOperation.Read"/> authority (inspect verbs and the
/// compliance audit), allows on an allow decision, and fails closed (throwing
/// <see cref="LatticeAuthorizationDeniedException"/>) on a deny or a partial
/// (filtered) allow. The non-throwing probes report the same decision as a bool.
/// Driven by a capturing gate double so the exact request the seam issues is
/// asserted without a cluster.
/// </summary>
[TestFixture]
public sealed class SchemaAccessAuthorizerTests
{
    private const string Tree = "orders";

    private static SchemaAccessAuthorizer Create(
        CapturingAccessGate gate,
        ILatticeMembershipContext? membership = null) =>
        new(gate, membership);

    // ---- Manage: SchemaAdmin whole-tree request -------------------------

    [Test]
    public async Task AuthorizeManage_issues_a_whole_tree_schema_admin_request()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeManageAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.SchemaAdmin));
            Assert.That(gate.Last.TreeId, Is.EqualTo(Tree));
            Assert.That(gate.Last.Key, Is.Null);
            Assert.That(gate.Last.RangeStart, Is.Null);
            Assert.That(gate.Last.RangeEnd, Is.Null);
        });
    }

    [Test]
    public async Task AuthorizeRead_issues_a_whole_tree_read_request()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        await authorizer.AuthorizeReadAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
            Assert.That(gate.Last.TreeId, Is.EqualTo(Tree));
            Assert.That(gate.Last.Key, Is.Null);
        });
    }

    [Test]
    public void AuthorizeManage_allow_does_not_throw()
    {
        var authorizer = Create(new CapturingAccessGate());

        Assert.That(async () => await authorizer.AuthorizeManageAsync(Tree), Throws.Nothing);
    }

    // ---- Deny: fail closed ----------------------------------------------

    [Test]
    public void AuthorizeManage_deny_throws_with_schema_admin_operation_and_reason()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no schema grant"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeManageAsync(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.SchemaAdmin));
            Assert.That(ex.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Reason, Is.EqualTo("no schema grant"));
        });
    }

    [Test]
    public void AuthorizeRead_deny_throws_with_read_operation()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no read grant"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeReadAsync(Tree));

        Assert.That(ex!.Operation, Is.EqualTo(LatticeOperation.Read));
    }

    [Test]
    public void AuthorizeManage_partial_allow_fails_closed()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Filtered(static _ => true, "partial"));
        var authorizer = Create(gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeManageAsync(Tree),
            "a whole-tree schema mutation cannot be narrowed and a filtered allow is refused");
    }

    [Test]
    public void AuthorizeRead_partial_allow_fails_closed()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Filtered(static _ => true, "partial"));
        var authorizer = Create(gate);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeReadAsync(Tree));
    }

    // ---- Probes: report the decision as a bool, never throw on denial ----

    [Test]
    public async Task IsManageAuthorized_returns_true_on_allow_and_false_on_deny()
    {
        var allow = Create(new CapturingAccessGate());
        var deny = Create(new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no")));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await allow.IsManageAuthorizedAsync(Tree), Is.True);
            Assert.That(await deny.IsManageAuthorizedAsync(Tree), Is.False);
        });
    }

    [Test]
    public async Task IsReadAuthorized_returns_true_on_allow_and_false_on_deny()
    {
        var allow = Create(new CapturingAccessGate());
        var deny = Create(new CapturingAccessGate(_ => LatticeAccessDecision.Deny("no")));

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await allow.IsReadAuthorizedAsync(Tree), Is.True);
            Assert.That(await deny.IsReadAuthorizedAsync(Tree), Is.False);
        });
    }

    [Test]
    public async Task IsReadAuthorized_probe_has_no_side_effects_beyond_the_gate_query()
    {
        var gate = new CapturingAccessGate();
        var authorizer = Create(gate);

        _ = await authorizer.IsReadAuthorizedAsync(Tree);

        Assert.That(gate.Last.Operation, Is.EqualTo(LatticeOperation.Read));
    }

    // ---- Subject flows through to the denial ----------------------------

    [Test]
    public void AuthorizeManage_deny_carries_the_resolved_caller_subject()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("denied"));
        var membership = new FixedMembershipContext(new LatticeSubject("alice"));
        var authorizer = Create(gate, membership);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeManageAsync(Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SubjectId, Is.EqualTo("alice"));
            Assert.That(gate.Last.Subject.SubjectId, Is.EqualTo("alice"));
        });
    }

    [Test]
    public void AuthorizeManage_with_no_membership_resolves_the_anonymous_subject()
    {
        var gate = new CapturingAccessGate(_ => LatticeAccessDecision.Deny("denied"));
        var authorizer = Create(gate);

        var ex = Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await authorizer.AuthorizeManageAsync(Tree));

        Assert.That(ex!.SubjectId, Is.EqualTo(LatticeSubject.AnonymousSubjectId));
    }

    // ---- Construction guards --------------------------------------------

    [Test]
    public void Constructor_null_gate_throws()
    {
        Assert.That(() => new SchemaAccessAuthorizer(null!), Throws.ArgumentNullException);
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

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            Last = request;
            return new ValueTask<LatticeAccessDecision>(_decide(request));
        }
    }

    /// <summary>A membership context double that resolves a fixed subject synchronously.</summary>
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
