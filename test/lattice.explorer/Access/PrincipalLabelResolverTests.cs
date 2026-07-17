using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Unit coverage for <see cref="PrincipalLabelResolver"/>, the Access area's
/// friendly-name resolver: display-name resolution, the raw-id fallback (no
/// directory, unresolved, blank display name), the per-resolver cache (a second
/// lookup issues no directory call), the allocation-free synchronous cache peek,
/// and the never-throws contract on a directory fault. Every case is deterministic
/// - no wall-clock, ordering, or GC dependence.
/// </summary>
[TestFixture]
public sealed class PrincipalLabelResolverTests
{
    private static PrincipalLabelResolver Create(FakeMembership membership) => new(membership);

    private static DirectoryPrincipalDescriptor Principal(string id, string displayName, DirectoryPrincipalKind kind = DirectoryPrincipalKind.User) =>
        new() { Id = id, DisplayName = displayName, Kind = kind };

    // ----- Construction / guards -----

    [Test]
    public void Constructor_null_membership_throws()
    {
        Assert.That(() => new PrincipalLabelResolver(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Label_null_id_throws()
    {
        var resolver = Create(new FakeMembership());
        Assert.That(() => resolver.Label(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveLabelAsync_null_id_throws()
    {
        var resolver = Create(new FakeMembership());
        Assert.That(() => resolver.ResolveLabelAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveManyAsync_null_ids_throws()
    {
        var resolver = Create(new FakeMembership());
        Assert.That(() => resolver.ResolveManyAsync(null!), Throws.ArgumentNullException);
    }

    // ----- Synchronous cache peek -----

    [Test]
    public void Label_returns_the_id_when_not_yet_resolved()
    {
        var resolver = Create(new FakeMembership());

        Assert.That(resolver.Label("852b"), Is.EqualTo("852b"));
    }

    [Test]
    public async Task Label_returns_the_cached_display_name_after_resolve()
    {
        var membership = new FakeMembership { { "alice", Principal("alice", "Alice Ng") } };
        var resolver = Create(membership);

        await resolver.ResolveLabelAsync("alice");

        Assert.That(resolver.Label("alice"), Is.EqualTo("Alice Ng"));
    }

    // ----- Resolution + fallback -----

    [Test]
    public async Task ResolveLabelAsync_returns_the_display_name_when_resolved()
    {
        var membership = new FakeMembership { { "661", Principal("661", "Lattice Floor Operators", DirectoryPrincipalKind.Group) } };
        var resolver = Create(membership);

        var label = await resolver.ResolveLabelAsync("661");

        Assert.That(label, Is.EqualTo("Lattice Floor Operators"));
    }

    [Test]
    public async Task ResolveLabelAsync_unresolved_id_falls_back_to_the_id()
    {
        // A null descriptor models both a not-found id and a directory that is not
        // configured (the service folds both into null).
        var membership = new FakeMembership();
        var resolver = Create(membership);

        var label = await resolver.ResolveLabelAsync("ghost");

        Assert.That(label, Is.EqualTo("ghost"));
    }

    [Test]
    public async Task ResolveLabelAsync_no_directory_falls_back_to_the_id()
    {
        // No directory configured: every resolve returns null. The label is the raw
        // id, exactly as rendered today when no directory is present.
        var membership = new FakeMembership { AlwaysNull = true };
        var resolver = Create(membership);

        var label = await resolver.ResolveLabelAsync("raw-id");

        Assert.Multiple(() =>
        {
            Assert.That(label, Is.EqualTo("raw-id"));
            Assert.That(resolver.Label("raw-id"), Is.EqualTo("raw-id"));
        });
    }

    [Test]
    public async Task ResolveLabelAsync_blank_display_name_falls_back_to_the_id()
    {
        var membership = new FakeMembership { { "852b", Principal("852b", "   ") } };
        var resolver = Create(membership);

        var label = await resolver.ResolveLabelAsync("852b");

        Assert.That(label, Is.EqualTo("852b"));
    }

    [Test]
    public async Task ResolveLabelAsync_empty_id_returns_empty_without_querying_the_directory()
    {
        var membership = new FakeMembership();
        var resolver = Create(membership);

        var label = await resolver.ResolveLabelAsync(string.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(label, Is.Empty);
            Assert.That(membership.ResolveCallCount, Is.Zero, "an empty id is never queried");
        });
    }

    // ----- Caching -----

    [Test]
    public async Task ResolveLabelAsync_caches_the_result_second_call_issues_no_directory_call()
    {
        var membership = new FakeMembership { { "alice", Principal("alice", "Alice Ng") } };
        var resolver = Create(membership);

        var first = await resolver.ResolveLabelAsync("alice");
        var second = await resolver.ResolveLabelAsync("alice");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("Alice Ng"));
            Assert.That(second, Is.EqualTo("Alice Ng"));
            Assert.That(membership.ResolveCallCount, Is.EqualTo(1), "a cached id is not re-resolved");
        });
    }

    [Test]
    public async Task ResolveLabelAsync_caches_the_id_fallback_second_call_issues_no_directory_call()
    {
        var membership = new FakeMembership();
        var resolver = Create(membership);

        await resolver.ResolveLabelAsync("ghost");
        await resolver.ResolveLabelAsync("ghost");

        Assert.That(membership.ResolveCallCount, Is.EqualTo(1), "an unresolved id is cached as the id fallback");
    }

    // ----- Never-throws -----

    [Test]
    public async Task ResolveLabelAsync_directory_fault_falls_back_to_the_id_and_is_not_cached()
    {
        var membership = new FakeMembership { Throw = true };
        var resolver = Create(membership);

        var first = await resolver.ResolveLabelAsync("alice");
        var second = await resolver.ResolveLabelAsync("alice");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("alice"));
            Assert.That(second, Is.EqualTo("alice"));
            Assert.That(membership.ResolveCallCount, Is.EqualTo(2), "a faulted resolve is left uncached so a later render retries");
        });
    }

    // ----- Batch resolution -----

    [Test]
    public async Task ResolveManyAsync_resolves_each_uncached_id_once()
    {
        var membership = new FakeMembership
        {
            { "alice", Principal("alice", "Alice Ng") },
            { "bob", Principal("bob", "Bob Lee") },
        };
        var resolver = Create(membership);

        await resolver.ResolveManyAsync(new[] { "alice", "bob", "alice" });

        Assert.Multiple(() =>
        {
            Assert.That(resolver.Label("alice"), Is.EqualTo("Alice Ng"));
            Assert.That(resolver.Label("bob"), Is.EqualTo("Bob Lee"));
            Assert.That(membership.ResolveCallCount, Is.EqualTo(2), "the duplicate id is only resolved once");
        });
    }

    [Test]
    public async Task ResolveManyAsync_skips_already_cached_ids()
    {
        var membership = new FakeMembership
        {
            { "alice", Principal("alice", "Alice Ng") },
            { "bob", Principal("bob", "Bob Lee") },
        };
        var resolver = Create(membership);

        await resolver.ResolveLabelAsync("alice");
        await resolver.ResolveManyAsync(new[] { "alice", "bob" });

        Assert.That(membership.ResolveCallCount, Is.EqualTo(2), "the pre-cached id is not resolved again by the batch");
    }

    [Test]
    public async Task ResolveManyAsync_skips_null_ids_without_throwing()
    {
        var membership = new FakeMembership { { "alice", Principal("alice", "Alice Ng") } };
        var resolver = Create(membership);

        await resolver.ResolveManyAsync(new[] { "alice", null! });

        Assert.Multiple(() =>
        {
            Assert.That(resolver.Label("alice"), Is.EqualTo("Alice Ng"));
            Assert.That(membership.ResolveCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ResolveManyAsync_empty_is_a_no_op()
    {
        var membership = new FakeMembership();
        var resolver = Create(membership);

        await resolver.ResolveManyAsync(Array.Empty<string>());

        Assert.That(membership.ResolveCallCount, Is.Zero);
    }

    /// <summary>
    /// A hand fake of <see cref="IMembershipAdminService"/> that records directory
    /// resolve calls and returns a fixed descriptor per id (or <see langword="null"/>);
    /// every other member is out of scope for the resolver and throws.
    /// </summary>
    private sealed class FakeMembership : IMembershipAdminService, IEnumerable<KeyValuePair<string, DirectoryPrincipalDescriptor?>>
    {
        private readonly Dictionary<string, DirectoryPrincipalDescriptor?> _map = new(StringComparer.Ordinal);

        /// <summary>When set, every resolve throws to exercise the never-throws contract.</summary>
        public bool Throw { get; set; }

        /// <summary>When set, every resolve returns <see langword="null"/> (no directory configured).</summary>
        public bool AlwaysNull { get; set; }

        /// <summary>The number of directory resolve calls issued.</summary>
        public int ResolveCallCount { get; private set; }

        public void Add(string id, DirectoryPrincipalDescriptor? descriptor) => _map[id] = descriptor;

        public IEnumerator<KeyValuePair<string, DirectoryPrincipalDescriptor?>> GetEnumerator() => _map.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        public Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
        {
            ResolveCallCount++;
            if (Throw)
            {
                throw new InvalidOperationException("directory fault");
            }

            if (AlwaysNull)
            {
                return Task.FromResult<DirectoryPrincipalDescriptor?>(null);
            }

            return Task.FromResult(_map.TryGetValue(principalId, out var descriptor) ? descriptor : null);
        }

        public Task<DirectorySearchView> SearchDirectoryAsync(string term, DirectoryPrincipalKind? kind = null, int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessModelView> GetAccessModelAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessListView<AuthUser>> ListUsersAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<AccessOperationResult> DeleteUserAsync(string userId, CancellationToken cancellationToken = default) => throw new NotSupportedException();

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
