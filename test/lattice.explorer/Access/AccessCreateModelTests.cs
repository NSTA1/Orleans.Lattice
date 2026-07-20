using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="AccessCreateModel"/>, the extracted create-form /
/// access-state logic: the resolve-and-block decision for a new principal
/// (block-unknown, allow-known, kind-mismatch, and the honest no-directory
/// fallback), the provider explanation surfaced in the form, and the auth-mode +
/// flat-authorizer enforcement banner shown in the Access area. Every case is
/// deterministic - no wall-clock, ordering, or GC dependence.
/// </summary>
[TestFixture]
public sealed class AccessCreateModelTests
{
    private static AccessCreateModel Create(FakeDirectory directory) => new(directory);

    private static AccessModelView Model(
        AccessAuthenticationMode mode = AccessAuthenticationMode.Claims,
        bool rulesEnforced = true,
        bool directoryAvailable = true,
        string explanation = "",
        bool localMembershipEffective = true) =>
        AccessModelView.FromDescriptor(new AccessModelDescriptor
        {
            AuthenticationMode = mode,
            RulesEnforced = rulesEnforced,
            DirectoryAvailable = directoryAvailable,
            DirectoryProviderId = "provider",
            DirectoryExplanation = explanation,
            LocalMembershipEffective = localMembershipEffective,
        });

    private static DirectoryPrincipalDescriptor Principal(string id, DirectoryPrincipalKind kind) =>
        new() { Id = id, DisplayName = id, Kind = kind };

    // ----- Construction / defaults -----

    [Test]
    public void Constructor_null_membership_throws()
    {
        Assert.That(() => new AccessCreateModel(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Default_model_is_the_unavailable_snapshot()
    {
        var model = Create(new FakeDirectory());

        Assert.Multiple(() =>
        {
            Assert.That(model.DirectoryAvailable, Is.False);
            Assert.That(model.DirectoryExplanation, Is.Empty);
            Assert.That(model.AuthenticationMode, Is.EqualTo(AccessAuthenticationMode.Unknown));
            Assert.That(model.AuthenticationModeLabel, Is.EqualTo("Unknown"));
            Assert.That(model.ShowEnforcementNotice, Is.False, "an unread model must not be presented as unenforced");
        });
    }

    [Test]
    public void Apply_null_throws()
    {
        var model = Create(new FakeDirectory());
        Assert.That(() => model.Apply(null!), Throws.ArgumentNullException);
    }

    // ----- Access-state accuracy (auth mode + enforcement banner) -----

    [Test]
    public void ShowEnforcementNotice_true_when_read_succeeds_and_rules_not_enforced()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(mode: AccessAuthenticationMode.Basic, rulesEnforced: false));

        Assert.Multiple(() =>
        {
            Assert.That(model.ShowEnforcementNotice, Is.True);
            Assert.That(model.AuthenticationModeLabel, Is.EqualTo("Basic"));
        });
    }

    [Test]
    public void ShowEnforcementNotice_false_when_rules_are_enforced()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(rulesEnforced: true));

        Assert.That(model.ShowEnforcementNotice, Is.False);
    }

    [Test]
    public void ShowEnforcementNotice_false_when_the_read_failed()
    {
        var model = Create(new FakeDirectory());

        // The unavailable snapshot has RulesEnforced == false but is not a success:
        // an unknown model must never be shown as an unenforced one.
        model.Apply(AccessModelView.Unavailable);

        Assert.That(model.ShowEnforcementNotice, Is.False);
    }

    // ----- GroupMergeMode-aware membership gating -----

    [Test]
    public void Membership_editing_enabled_by_default_before_a_model_is_read()
    {
        var model = Create(new FakeDirectory());

        Assert.Multiple(() =>
        {
            Assert.That(model.MembershipEditingEnabled, Is.True);
            Assert.That(model.ShowMembershipInertNotice, Is.False);
        });
    }

    [Test]
    public void Membership_editing_enabled_when_local_membership_is_effective()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(localMembershipEffective: true));

        Assert.Multiple(() =>
        {
            Assert.That(model.MembershipEditingEnabled, Is.True);
            Assert.That(model.ShowMembershipInertNotice, Is.False);
        });
    }

    [Test]
    public void Membership_editing_disabled_when_read_succeeds_and_membership_is_inert()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(localMembershipEffective: false));

        Assert.Multiple(() =>
        {
            Assert.That(model.ShowMembershipInertNotice, Is.True);
            Assert.That(model.MembershipEditingEnabled, Is.False);
        });
    }

    [Test]
    public void Membership_editing_enabled_when_the_read_failed_even_though_inert_is_the_default()
    {
        var model = Create(new FakeDirectory());

        // The unavailable snapshot has LocalMembershipEffective == false but is not a
        // success: an unknown model must not gate the editing surface.
        model.Apply(AccessModelView.Unavailable);

        Assert.Multiple(() =>
        {
            Assert.That(model.ShowMembershipInertNotice, Is.False);
            Assert.That(model.MembershipEditingEnabled, Is.True);
        });
    }

    [Test]
    public void DirectoryExplanation_is_surfaced_from_the_applied_model()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(explanation: "Pick a directory user configured at deploy time."));

        Assert.That(model.DirectoryExplanation, Is.EqualTo("Pick a directory user configured at deploy time."));
    }

    [TestCase(AccessAuthenticationMode.Unknown, "Unknown")]
    [TestCase(AccessAuthenticationMode.Anonymous, "Anonymous")]
    [TestCase(AccessAuthenticationMode.Claims, "Claims")]
    [TestCase(AccessAuthenticationMode.Basic, "Basic")]
    public void DescribeAuthenticationMode_maps_each_mode(AccessAuthenticationMode mode, string expected)
    {
        Assert.That(AccessCreateModel.DescribeAuthenticationMode(mode), Is.EqualTo(expected));
    }

    // ----- Fail-closed create validation -----

    [Test]
    public void ValidateAsync_null_id_throws()
    {
        var model = Create(new FakeDirectory());
        Assert.That(() => model.ValidateAsync(null!, DirectoryPrincipalKind.User), Throws.ArgumentNullException);
    }

    [Test]
    public async Task ValidateAsync_blank_id_is_blocked()
    {
        var model = Create(new FakeDirectory());
        model.Apply(Model(directoryAvailable: true));

        var decision = await model.ValidateAsync("   ", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.IsBlocked, Is.True);
            Assert.That(decision.CanSave, Is.False);
        });
    }

    [Test]
    public async Task ValidateAsync_no_directory_allows_unvalidated_without_resolving()
    {
        var directory = new FakeDirectory();
        var model = Create(directory);

        // No model applied -> the unavailable snapshot -> no directory to query.
        var decision = await model.ValidateAsync("raw-id", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(CreatePrincipalOutcome.AllowUnvalidated));
            Assert.That(decision.CanSave, Is.True);
            Assert.That(decision.IsUnvalidated, Is.True);
            Assert.That(directory.ResolveCalls, Is.Empty, "the fallback path never queries the directory");
        });
    }

    [Test]
    public async Task ValidateAsync_directory_available_unknown_id_is_blocked()
    {
        var directory = new FakeDirectory { Resolved = null };
        var model = Create(directory);
        model.Apply(Model(directoryAvailable: true));

        var decision = await model.ValidateAsync("ghost", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.IsBlocked, Is.True);
            Assert.That(decision.Reason, Is.EqualTo(AccessCreateModel.NoSuchPrincipalReason));
            Assert.That(directory.ResolveCalls, Is.EqualTo(new[] { "ghost" }));
        });
    }

    [Test]
    public async Task ValidateAsync_directory_available_known_id_is_allowed()
    {
        var directory = new FakeDirectory { Resolved = Principal("alice", DirectoryPrincipalKind.User) };
        var model = Create(directory);
        model.Apply(Model(directoryAvailable: true));

        var decision = await model.ValidateAsync("alice", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(CreatePrincipalOutcome.Allow));
            Assert.That(decision.CanSave, Is.True);
            Assert.That(decision.IsBlocked, Is.False);
        });
    }

    [Test]
    public async Task ValidateAsync_trims_the_id_before_resolving()
    {
        var directory = new FakeDirectory { Resolved = Principal("alice", DirectoryPrincipalKind.User) };
        var model = Create(directory);
        model.Apply(Model(directoryAvailable: true));

        await model.ValidateAsync("  alice  ", DirectoryPrincipalKind.User);

        Assert.That(directory.ResolveCalls, Is.EqualTo(new[] { "alice" }));
    }

    [Test]
    public async Task ValidateAsync_directory_kind_mismatch_is_blocked()
    {
        var directory = new FakeDirectory { Resolved = Principal("payments", DirectoryPrincipalKind.Group) };
        var model = Create(directory);
        model.Apply(Model(directoryAvailable: true));

        var decision = await model.ValidateAsync("payments", DirectoryPrincipalKind.User);

        Assert.Multiple(() =>
        {
            Assert.That(decision.IsBlocked, Is.True);
            Assert.That(decision.Reason, Does.Contain("group"));
            Assert.That(decision.Reason, Does.Contain("user"));
        });
    }

    [Test]
    public async Task ValidateAsync_group_form_resolves_a_real_group()
    {
        var directory = new FakeDirectory { Resolved = Principal("payments", DirectoryPrincipalKind.Group) };
        var model = Create(directory);
        model.Apply(Model(directoryAvailable: true));

        var decision = await model.ValidateAsync("payments", DirectoryPrincipalKind.Group);

        Assert.That(decision.Outcome, Is.EqualTo(CreatePrincipalOutcome.Allow));
    }

    // ----- Decision value type -----

    [Test]
    public void CreatePrincipalDecision_block_null_reason_throws()
    {
        Assert.That(() => CreatePrincipalDecision.Block(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void CreatePrincipalDecision_allow_can_save_and_is_not_unvalidated()
    {
        var decision = CreatePrincipalDecision.Allow();

        Assert.Multiple(() =>
        {
            Assert.That(decision.CanSave, Is.True);
            Assert.That(decision.IsBlocked, Is.False);
            Assert.That(decision.IsUnvalidated, Is.False);
            Assert.That(decision.Reason, Is.Empty);
        });
    }

    /// <summary>
    /// A hand fake of <see cref="IMembershipAdminService"/> that records directory
    /// resolve calls and returns a fixed descriptor (or <see langword="null"/>);
    /// every other member is out of scope for the create model and throws.
    /// </summary>
    private sealed class FakeDirectory : IMembershipAdminService
    {
        public DirectoryPrincipalDescriptor? Resolved { get; set; }

        public List<string> ResolveCalls { get; } = new();

        public Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
        {
            ResolveCalls.Add(principalId);
            return Task.FromResult(Resolved);
        }

        public Task<DirectorySearchView> SearchDirectoryAsync(string term, DirectoryPrincipalKind? kind = null, int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessModelView> GetAccessModelAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<AuthGroup>> ListGroupsAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> DeleteGroupAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<string>> ListDirectMembersAsync(string groupId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
