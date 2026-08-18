using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Unit coverage for the fail-closed identity-directory validation the control
/// facade applies on its membership-reference create paths
/// (<see cref="ILatticeAuthAdmin.AddMemberAsync"/> and
/// <see cref="ILatticeAuthAdmin.UpsertGroupAsync"/>) for issues #1269 and #1519.
/// Instantiates <see cref="LatticeAuthAdmin"/> directly over a configurable fake
/// <see cref="ILatticeIdentityDirectory"/> and a substitute membership directory,
/// so no cluster is involved: it proves that when
/// <see cref="LatticeIdentityDirectoryOptions.ValidationRequired"/> is set and a
/// real provider is active, an unresolvable or wrong-kind reference is rejected
/// before any write, while validation is skipped when it is not required or when
/// the no-op <see cref="NullIdentityDirectory"/> is in force.
/// </summary>
[TestFixture]
public sealed class LatticeAuthAdminValidationTests
{
    private const string GroupId = "g-1";
    private const string MemberId = "u-1";

    private static (LatticeAuthAdmin Admin, ILatticeMembershipDirectory Directory) CreateAdmin(
        ILatticeIdentityDirectory identityDirectory,
        bool validationRequired)
    {
        var membershipDirectory = Substitute.For<ILatticeMembershipDirectory>();
        var authMonitor = Substitute.For<IOptionsMonitor<LatticeAuthOptions>>();
        authMonitor.CurrentValue.Returns(new LatticeAuthOptions());
        var membershipMonitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        membershipMonitor.CurrentValue.Returns(new LatticeMembershipOptions());
        var identityMonitor = Substitute.For<IOptionsMonitor<LatticeIdentityDirectoryOptions>>();
        identityMonitor.CurrentValue.Returns(new LatticeIdentityDirectoryOptions { ValidationRequired = validationRequired });

        var admin = new LatticeAuthAdmin(
            Substitute.For<ILatticeAuthorizationPolicyStore>(),
            membershipDirectory,
            new AllowAllAccessGate(),
            new AnonymousMembershipContext(),
            identityDirectory,
            new ILatticeCredentialAuthenticator[] { new AnonymousCredentialAuthenticator() },
            Options.Create(new LatticeApiAuthOptions()),
            authMonitor,
            membershipMonitor,
            identityMonitor);

        return (admin, membershipDirectory);
    }

    // ----- AddMemberAsync -----

    [Test]
    public void AddMemberAsync_rejects_an_unresolvable_member_when_validation_is_required()
    {
        var directory = new ConfigurableIdentityDirectory();
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.PrincipalId, Is.EqualTo(MemberId));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(ex.ResolvedKind, Is.Null, "an unresolvable id carries no resolved kind");
            Assert.That(directory.ResolveCallCount, Is.EqualTo(1));
        });

        // Fail-closed: nothing is written once validation rejects the reference.
        membershipDirectory.DidNotReceiveWithAnyArgs()
            .AddMemberAsync(default!, default!, default, default);
    }

    [Test]
    public void AddMemberAsync_rejects_a_wrong_kind_member_when_validation_is_required()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            // The id resolves, but to a Group where a User member was supplied.
            [MemberId] = new DirectoryPrincipal(MemberId, "Admins", DirectoryPrincipalKind.Group),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.User));
            Assert.That(ex.ResolvedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
        });

        membershipDirectory.DidNotReceiveWithAnyArgs()
            .AddMemberAsync(default!, default!, default, default);
    }

    [Test]
    public async Task AddMemberAsync_writes_when_a_resolvable_correct_kind_member_is_supplied()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            [MemberId] = new DirectoryPrincipal(MemberId, "Alice", DirectoryPrincipalKind.User),
            [GroupId] = new DirectoryPrincipal(GroupId, "Admins", DirectoryPrincipalKind.Group),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        await admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User);

        Assert.That(directory.ResolveCallCount, Is.EqualTo(2), "both the member id and the target group id are validated");
        await membershipDirectory.Received(1)
            .AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddMemberAsync_validates_a_group_member_against_the_group_kind()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            ["nested-g"] = new DirectoryPrincipal("nested-g", "Nested", DirectoryPrincipalKind.Group),
            [GroupId] = new DirectoryPrincipal(GroupId, "Admins", DirectoryPrincipalKind.Group),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        await admin.AddMemberAsync(GroupId, "nested-g", MembershipMemberKind.Group);

        await membershipDirectory.Received(1)
            .AddMemberAsync(GroupId, "nested-g", MembershipMemberKind.Group, Arg.Any<CancellationToken>());
    }

    [Test]
    public void AddMemberAsync_rejects_an_unresolvable_group_when_validation_is_required()
    {
        // The member resolves, so validation reaches the target group id (issue #1519).
        var directory = new ConfigurableIdentityDirectory
        {
            [MemberId] = new DirectoryPrincipal(MemberId, "Alice", DirectoryPrincipalKind.User),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.PrincipalId, Is.EqualTo(GroupId));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(ex.ResolvedKind, Is.Null, "an unresolvable id carries no resolved kind");
        });

        // Fail-closed: nothing is written once the target group fails validation.
        membershipDirectory.DidNotReceiveWithAnyArgs()
            .AddMemberAsync(default!, default!, default, default);
    }

    [Test]
    public void AddMemberAsync_rejects_a_wrong_kind_group_when_validation_is_required()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            [MemberId] = new DirectoryPrincipal(MemberId, "Alice", DirectoryPrincipalKind.User),
            // The target group id resolves, but to a User where a Group was required.
            [GroupId] = new DirectoryPrincipal(GroupId, "Bob", DirectoryPrincipalKind.User),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.PrincipalId, Is.EqualTo(GroupId));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(ex.ResolvedKind, Is.EqualTo(DirectoryPrincipalKind.User));
        });

        membershipDirectory.DidNotReceiveWithAnyArgs()
            .AddMemberAsync(default!, default!, default, default);
    }

    [Test]
    public async Task AddMemberAsync_skips_validation_when_not_required()
    {
        var directory = new ConfigurableIdentityDirectory();
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: false);

        await admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User);

        Assert.That(directory.ResolveCallCount, Is.Zero, "validation is not required, so the directory is never resolved");
        await membershipDirectory.Received(1)
            .AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AddMemberAsync_skips_validation_under_the_null_directory_even_when_required()
    {
        var (admin, membershipDirectory) = CreateAdmin(new NullIdentityDirectory(), validationRequired: true);

        await admin.AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User);

        await membershipDirectory.Received(1)
            .AddMemberAsync(GroupId, MemberId, MembershipMemberKind.User, Arg.Any<CancellationToken>());
    }

    // ----- UpsertGroupAsync -----

    [Test]
    public void UpsertGroupAsync_rejects_an_unresolvable_group_when_validation_is_required()
    {
        var directory = new ConfigurableIdentityDirectory();
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.UpsertGroupAsync(new AuthGroup { GroupId = GroupId, DisplayName = "Group" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.PrincipalId, Is.EqualTo(GroupId));
            Assert.That(ex.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(ex.ResolvedKind, Is.Null);
        });

        membershipDirectory.DidNotReceiveWithAnyArgs().UpsertGroupAsync(default!, default);
    }

    [Test]
    public void UpsertGroupAsync_rejects_a_wrong_kind_group_when_validation_is_required()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            [GroupId] = new DirectoryPrincipal(GroupId, "Alice", DirectoryPrincipalKind.User),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        var ex = Assert.ThrowsAsync<LatticeDirectoryValidationException>(
            () => admin.UpsertGroupAsync(new AuthGroup { GroupId = GroupId, DisplayName = "Group" }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ExpectedKind, Is.EqualTo(DirectoryPrincipalKind.Group));
            Assert.That(ex.ResolvedKind, Is.EqualTo(DirectoryPrincipalKind.User));
        });

        membershipDirectory.DidNotReceiveWithAnyArgs().UpsertGroupAsync(default!, default);
    }

    [Test]
    public async Task UpsertGroupAsync_writes_when_a_resolvable_group_is_supplied()
    {
        var directory = new ConfigurableIdentityDirectory
        {
            [GroupId] = new DirectoryPrincipal(GroupId, "Admins", DirectoryPrincipalKind.Group),
        };
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: true);

        await admin.UpsertGroupAsync(new AuthGroup { GroupId = GroupId, DisplayName = "Admins" });

        Assert.That(directory.ResolveCallCount, Is.EqualTo(1));
        await membershipDirectory.Received(1).UpsertGroupAsync(
            Arg.Is<MembershipGroup>(g => g.GroupId == GroupId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UpsertGroupAsync_skips_validation_when_not_required()
    {
        var directory = new ConfigurableIdentityDirectory();
        var (admin, membershipDirectory) = CreateAdmin(directory, validationRequired: false);

        await admin.UpsertGroupAsync(new AuthGroup { GroupId = GroupId, DisplayName = "Group" });

        Assert.That(directory.ResolveCallCount, Is.Zero);
        await membershipDirectory.Received(1).UpsertGroupAsync(
            Arg.Is<MembershipGroup>(g => g.GroupId == GroupId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task UpsertGroupAsync_skips_validation_under_the_null_directory_even_when_required()
    {
        var (admin, membershipDirectory) = CreateAdmin(new NullIdentityDirectory(), validationRequired: true);

        await admin.UpsertGroupAsync(new AuthGroup { GroupId = GroupId, DisplayName = "Group" });

        await membershipDirectory.Received(1).UpsertGroupAsync(
            Arg.Is<MembershipGroup>(g => g.GroupId == GroupId), Arg.Any<CancellationToken>());
    }

    private sealed class AllowAllAccessGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Allow());
    }

    private sealed class AnonymousMembershipContext : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(LatticeSubject.Anonymous);

        public bool TryResolveCurrent(out LatticeSubject subject)
        {
            subject = LatticeSubject.Anonymous;
            return true;
        }
    }

    /// <summary>
    /// A real (non-null) identity-directory provider whose resolve answers are
    /// seeded per id via an initializer, tracking how many times it was resolved so
    /// a test can assert validation was or was not invoked.
    /// </summary>
    private sealed class ConfigurableIdentityDirectory : ILatticeIdentityDirectory
    {
        private readonly Dictionary<string, DirectoryPrincipal> _principals = new(StringComparer.Ordinal);

        public int ResolveCallCount { get; private set; }

        public string ProviderId => "configurable";

        public DirectoryPrincipal this[string id]
        {
            set => _principals[id] = value;
        }

        public string DescribeEntry(DirectoryPrincipalKind? kind) => "Enter a principal id.";

        public Task<DirectorySearchPage> SearchAsync(DirectorySearchQuery query, CancellationToken cancellationToken = default) =>
            Task.FromResult(DirectorySearchPage.Empty);

        public Task<DirectoryPrincipal?> ResolveAsync(string principalId, CancellationToken cancellationToken = default)
        {
            ResolveCallCount++;
            return Task.FromResult(_principals.TryGetValue(principalId, out var principal) ? principal : null);
        }
    }
}
