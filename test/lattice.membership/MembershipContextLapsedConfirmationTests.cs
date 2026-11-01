using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// The refusal path of the cross-silo membership epoch (#5518), end to end through
/// the real <see cref="MembershipContext"/>. A member is removed on another silo
/// while this silo cannot read the cluster-wide epoch, so nothing ever tells it to
/// flush. Once the last successful poll's confirmation lapses, the stale cached
/// groups must never be served again: the context re-reads the directory (the
/// source of truth), and if that read fails the resolution fails rather than fall
/// back to the cached grant.
/// </summary>
[TestFixture]
public sealed class MembershipContextLapsedConfirmationTests
{
    private const string Scheme = "lapse-test";
    private const string Member = "priya";
    private const string Group = "data-stewards";

    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task A_removal_this_silo_was_never_told_about_is_enforced_once_the_confirmation_lapses()
    {
        var rig = await Rig.WarmAsync();
        rig.RemoveMembershipElsewhere();

        rig.Time.Advance(rig.Synchronizer.LeaseDuration);

        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            Assert.That(rig.Context.TryResolveCurrent(out _), Is.False, "the stale cached subject is not served synchronously");
            var subject = await rig.Context.ResolveCurrentAsync();
            Assert.Multiple(() =>
            {
                Assert.That(subject.SubjectId, Is.EqualTo(Member));
                Assert.That(subject.GroupIds, Does.Not.Contain(Group), "the removed group is never granted from the cache");
                Assert.That(rig.Directory.Reads, Is.EqualTo(2), "the directory is re-read as the source of truth");
            });
        }
    }

    [Test]
    public async Task With_the_directory_also_unreachable_resolution_fails_rather_than_serving_the_cached_groups()
    {
        var rig = await Rig.WarmAsync();
        rig.RemoveMembershipElsewhere();
        rig.Directory.Unreachable = true;

        rig.Time.Advance(rig.Synchronizer.LeaseDuration);

        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            Assert.That(rig.Context.TryResolveCurrent(out _), Is.False);
            Assert.That(
                async () => await rig.Context.ResolveCurrentAsync(),
                Throws.InvalidOperationException,
                "with neither the epoch nor the directory readable the caller is refused, never granted the stale groups");
        }
    }

    [Test]
    public async Task Within_the_confirmation_the_warm_entry_is_served_and_the_bound_is_one_lease()
    {
        var rig = await Rig.WarmAsync();
        rig.RemoveMembershipElsewhere();

        rig.Time.Advance(rig.Synchronizer.LeaseDuration - TimeSpan.FromMilliseconds(1));

        using (LatticeCredentialContext.Use("tok", scheme: Scheme))
        {
            Assert.That(rig.Context.TryResolveCurrent(out var cached), Is.True, "precondition: still confirmed");
            Assert.That(cached.GroupIds, Does.Contain(Group));

            rig.Time.Advance(TimeSpan.FromMilliseconds(1));

            Assert.That(rig.Context.TryResolveCurrent(out _), Is.False, "the stale window closes exactly when the lease does");
        }
    }

    private sealed class Rig
    {
        private Rig(MembershipContext context, MembershipEpochSynchronizer synchronizer, IMembershipEpochGrain epoch, MutableDirectory directory, ManualTimeProvider time)
        {
            Context = context;
            Synchronizer = synchronizer;
            Epoch = epoch;
            Directory = directory;
            Time = time;
        }

        public MembershipContext Context { get; }

        public MembershipEpochSynchronizer Synchronizer { get; }

        public IMembershipEpochGrain Epoch { get; }

        public MutableDirectory Directory { get; }

        public ManualTimeProvider Time { get; }

        public static async Task<Rig> WarmAsync()
        {
            var time = new ManualTimeProvider(Start);
            var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
            monitor.CurrentValue.Returns(new LatticeMembershipOptions { ResolutionCacheTtl = TimeSpan.FromMinutes(5) });
            var cache = new MembershipResolutionCache(time, monitor);
            var directory = new MutableDirectory();
            directory.Groups.Add(Group);
            var authenticator = new FakeAuthenticator(c => c.Scheme == Scheme, _ => new LatticePrincipal(Member, Scheme));
            var context = new MembershipContext([authenticator], new DefaultLatticeSubjectMapper(monitor), directory, cache, monitor);

            var epoch = Substitute.For<IMembershipEpochGrain>();
            epoch.GetAsync().Returns(new MembershipEpoch(Guid.NewGuid(), 0));
            var grains = Substitute.For<IGrainFactory>();
            grains.GetGrain<IMembershipEpochGrain>(IMembershipEpochGrain.GrainKey, Arg.Any<string?>()).Returns(epoch);
            var synchronizer = new MembershipEpochSynchronizer(
                grains, cache, NullLogger<MembershipEpochSynchronizer>.Instance, membership: null, time, TimeSpan.FromSeconds(1));

            await synchronizer.PollOnceAsync(default);
            using (LatticeCredentialContext.Use("tok", scheme: Scheme))
            {
                var warm = await context.ResolveCurrentAsync();
                Assert.That(warm.GroupIds, Does.Contain(Group), "precondition: resolved into the group");
                Assert.That(context.TryResolveCurrent(out _), Is.True, "precondition: served warm");
            }

            return new Rig(context, synchronizer, epoch, directory, time);
        }

        /// <summary>
        /// Another silo commits the removal, and this silo cannot read the epoch, so
        /// neither its change feed nor a poll ever flushes its cache.
        /// </summary>
        public void RemoveMembershipElsewhere()
        {
            Directory.Groups.Remove(Group);
            Epoch.GetAsync().Returns(_ => Task.FromException<MembershipEpoch>(new InvalidOperationException("epoch grain unreachable")));
            Assert.That(async () => await Synchronizer.PollOnceAsync(default), Throws.InvalidOperationException);
        }
    }

    /// <summary>A directory whose recorded groups a test can change, and make unreachable.</summary>
    private sealed class MutableDirectory : ILatticeMembershipDirectory
    {
        public HashSet<string> Groups { get; } = new(StringComparer.Ordinal);

        public bool Unreachable { get; set; }

        public int Reads { get; private set; }

        public Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default)
        {
            Reads++;
            return Unreachable
                ? Task.FromException<IReadOnlyCollection<string>>(new InvalidOperationException("directory unreachable"))
                : Task.FromResult<IReadOnlyCollection<string>>(Groups.ToArray());
        }

        public Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default) =>
            Task.FromResult(seedGroups);

        public Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
