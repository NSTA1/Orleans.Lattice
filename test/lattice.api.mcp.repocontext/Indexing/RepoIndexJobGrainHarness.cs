using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// A silo-free unit harness for <see cref="RepoIndexJobGrain"/>: it builds the grain
/// over substituted collaborators so the durable job lifecycle - start, re-attach,
/// progress merge, settle, fail, cancel, and the resume-reminder beat - can be driven
/// as ordinary in-process calls.
/// <para>
/// The grain is reminder-anchored, and every existing fixture that reaches it does so
/// through a cluster, which can call the public entry points but cannot make a real
/// Orleans reminder fire on demand. Because the grain implements
/// <see cref="IRemindable"/> publicly, a beat is just a method call once the registry
/// is substituted, so the resume backstop and its two non-fatal reminder-failure arms
/// become ordinary unit tests instead of timing-dependent integration tests.
/// </para>
/// </summary>
internal sealed class RepoIndexJobGrainHarness
{
    /// <summary>The repository id every harness grain is keyed by.</summary>
    internal const string RepoId = "acme";

    /// <summary>
    /// The stable name of the grain's resume reminder. Mirrored here rather than
    /// referenced because the production constant is private; a rename on either
    /// side must fail a test, which is the point.
    /// </summary>
    internal const string ResumeReminderName = "repo-index-resume";

    internal RepoIndexJobGrainHarness()
    {
        Context = Substitute.For<IGrainContext>();
        Context.GrainId.Returns(GrainId.Create("repoIndexJob", RepoId));

        Reminders = Substitute.For<IReminderRegistry>();
        Reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(_ => Task.FromResult<IGrainReminder?>(Reminder));
        Reminders.RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => Task.FromResult<IGrainReminder>(Reminder));

        Runner = Substitute.For<IRepoIndexRunner>();
        State = new FakeJobState();
        Time = new MutableTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }

    /// <summary>The substituted activation context, keyed by the repository id.</summary>
    internal IGrainContext Context { get; }

    /// <summary>The substituted reminder registry the resume backstop is armed against.</summary>
    internal IReminderRegistry Reminders { get; }

    /// <summary>The reminder handle the registry hands back for the resume reminder.</summary>
    internal IGrainReminder Reminder { get; } = Substitute.For<IGrainReminder>();

    /// <summary>The substituted background runner every enqueue and cancel lands on.</summary>
    internal IRepoIndexRunner Runner { get; }

    /// <summary>The in-memory persisted job state.</summary>
    internal FakeJobState State { get; }

    /// <summary>The controllable clock the job stamps its timestamps from.</summary>
    internal MutableTimeProvider Time { get; }

    /// <summary>Builds the grain under test over this harness's collaborators.</summary>
    /// <returns>A freshly constructed grain.</returns>
    internal RepoIndexJobGrain CreateGrain() => new(
        Context,
        State,
        Reminders,
        Runner,
        Time,
        NullLogger<RepoIndexJobGrain>.Instance);

    /// <summary>A well-formed job request for this harness's repository.</summary>
    /// <param name="allowPrune">Whether the request permits mtime-pruned walking.</param>
    /// <returns>The request the job is started with.</returns>
    internal static RepoIndexJobRequest Request(bool allowPrune = false) =>
        new() { RepoRoot = "/repo", RepoId = RepoId, AllowPrune = allowPrune };

    /// <summary>
    /// A <see cref="TimeProvider"/> whose current instant the test moves by hand, so
    /// the job's start / update / completion stamps are distinguishable without
    /// waiting on the wall clock.
    /// </summary>
    internal sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _now;

        /// <summary>Moves the clock forward.</summary>
        /// <param name="delta">How far to advance.</param>
        internal void Advance(TimeSpan delta) => _now += delta;
    }

    /// <summary>
    /// A minimal in-memory <see cref="IPersistentState{T}"/> that records how often the
    /// grain wrote or cleared its job state, so the durability behaviour is observable
    /// without a storage provider.
    /// </summary>
    internal sealed class FakeJobState : IPersistentState<RepoIndexJobState>
    {
        /// <inheritdoc />
        public RepoIndexJobState State { get; set; } = new();

        /// <inheritdoc />
        public string Etag => string.Empty;

        /// <inheritdoc />
        public bool RecordExists => true;

        /// <summary>How many times the job state was written.</summary>
        internal int WriteCount { get; private set; }

        /// <summary>How many times the job state was cleared.</summary>
        internal int ClearCount { get; private set; }

        /// <inheritdoc />
        public Task ClearStateAsync()
        {
            ClearCount++;
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task ReadStateAsync() => Task.CompletedTask;

        /// <inheritdoc />
        public Task WriteStateAsync()
        {
            WriteCount++;
            return Task.CompletedTask;
        }
    }
}
