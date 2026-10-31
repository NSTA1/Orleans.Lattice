using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Backup.Tests.Coyote;

/// <summary>
/// The rules of the retention fence a run of <see cref="BackupRetentionModel"/>
/// leaves out. Every rule left in is decided by the production
/// <see cref="BackupRetentionPlanner"/>.
/// </summary>
[Flags]
internal enum BackupRetentionRulesRemoved
{
    /// <summary>The production design: every rule in place.</summary>
    None = 0,

    /// <summary>The increment publishes without re-reading its base's catalog row (<see cref="BackupRetentionPlanner.MayPublishIncrement"/> always passes).</summary>
    BaseRecheck = 1,

    /// <summary>The pass keeps nothing it finds built on (<see cref="BackupRetentionPlanner.DependedOn"/> finds nothing).</summary>
    DependentScan = 2,
}

/// <summary>
/// A Coyote model of a backup retention pass racing an incremental and a full
/// capture of the same scope (issue #5357), driving the <b>production</b>
/// <see cref="BackupRetentionPlanner"/> for every decision the pass and the
/// capture take: the keep set and its base closure
/// (<see cref="BackupRetentionPlanner.ComputeRetained"/>), the doomed manifests
/// (<see cref="BackupRetentionPlanner.Doomed"/>), the dependent scan
/// (<see cref="BackupRetentionPlanner.DependedOn"/>), the tip-first delete order
/// (<see cref="BackupRetentionPlanner.TipFirst"/>) and the increment's publish
/// check (<see cref="BackupRetentionPlanner.MayPublishIncrement"/>). It is the
/// implementation-level companion of <c>spec/backup/BackupRetention.tla</c>'s
/// <c>RetainedRestorable</c>.
/// <para>
/// The scope starts with one full backup <c>b0</c>, under keep-last-one. The
/// increment names the newest catalog row as its base, then writes its artifact,
/// its sink manifest, re-reads the base row and publishes or abandons. The full
/// capture writes a newer backup, which dooms the base. The pass lists the
/// catalog, plans, removes every doomed row, scans the sink, restores what it
/// found built on, and deletes the rest tip first, each sink manifest before its
/// artifacts. Every step is its own scheduling point, in any interleaving, and
/// the pass may start before, during or after either capture.
/// </para>
/// <para>
/// The property, asserted after every step: every catalog row is restorable -
/// its manifest resolves, its artifact is in the sink, and so is its whole base
/// chain. The interleaving is drawn from the runtime: which actor moves at every
/// step, and so whether the pass, the newer full capture and the increment start
/// before, during or after one another, and which base the increment names (the
/// newest catalog row when it starts). The retention policy is pinned to
/// keep-last-one at a fixed time, deliberately (#2319): the race under test needs
/// only some newer backup to doom the increment's base, and keep-last-one dooms it
/// exactly when the newer full capture publishes. Which backups each policy keeps
/// - keep-last ordering, the max-age cutoff, the unbounded no-op - is a pure
/// function of the listing, pinned case by case by
/// <c>BackupRetentionPlannerTests</c> and over every policy by the TLA+ module.
/// Every per-iteration value is a local of <see cref="Run"/>.
/// </para>
/// </summary>
internal sealed class BackupRetentionModel(BackupRetentionRulesRemoved removed, bool witnessFallback = false) : ICoyoteModel
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 31, 0, 0, 0, TimeSpan.Zero);

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var catalog = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        var sink = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        var artifacts = new HashSet<string>(StringComparer.Ordinal);

        var b0 = Manifest("b0", T0, baseId: null);
        Publish(b0);

        // The increment's steps: 0 start, 1 artifact, 2 manifest, 3 check, 4 done.
        var incStep = 0;
        BackupManifest? increment = null;
        var incPublished = false;
        var incFellBack = false;

        // The full capture's steps: 0 start, 1 artifact, 2 manifest, 3 register, 4 done.
        var fullStep = 0;
        var full = Manifest("f1", T0.AddMinutes(10), baseId: null);

        // The pass: 0 list, 1 remove rows, 2 scan, 3 restore, 4 delete, 5 done.
        var passStep = 0;
        var doomed = new List<BackupManifest>();
        var removedRows = 0;
        var blocked = new HashSet<string>(StringComparer.Ordinal);
        var deleteQueue = new Queue<(BackupManifest Manifest, bool ManifestGone)>();

        while (incStep < 4 || fullStep < 4 || passStep < 5)
        {
            var actors = new List<int>();
            if (incStep < 4)
            {
                actors.Add(0);
            }

            if (fullStep < 4)
            {
                actors.Add(1);
            }

            if (passStep < 5)
            {
                actors.Add(2);
            }

            switch (actors[runtime.RandomInteger(actors.Count)])
            {
                case 0:
                    StepIncrement();
                    break;
                case 1:
                    StepFull();
                    break;
                default:
                    StepPass();
                    break;
            }

            AssertEveryRowRestorable();
        }

        if (witnessFallback)
        {
            Specification.Assert(!incFellBack, "witness: the increment fell back because a pass had its base");
        }
        else
        {
            Specification.Assert(incPublished || incFellBack || increment is null, "the increment neither published nor fell back");
        }

        void StepIncrement()
        {
            switch (incStep)
            {
                case 0:
                    // The scheduler names the newest catalog row; it reads the base's sink manifest.
                    var newest = catalog.Values
                        .OrderByDescending(m => m.CreatedAtUtc)
                        .ThenByDescending(m => m.Id, StringComparer.Ordinal)
                        .FirstOrDefault();
                    if (newest is null || !sink.ContainsKey(newest.Id))
                    {
                        incStep = 4;
                        return;
                    }

                    increment = Manifest("i1", newest.Id == full.Id ? T0.AddMinutes(15) : T0.AddMinutes(5), newest.Id);
                    incStep = 1;
                    return;
                case 1:
                    artifacts.Add(Artifact(increment!));
                    incStep = 2;
                    return;
                case 2:
                    sink[increment!.Id] = increment;
                    incStep = 3;
                    return;
                default:
                    var baseRow = catalog.GetValueOrDefault(increment!.BaseBackupId!);
                    var mayPublish = removed.HasFlag(BackupRetentionRulesRemoved.BaseRecheck)
                        || BackupRetentionPlanner.MayPublishIncrement(increment, baseRow);
                    if (mayPublish)
                    {
                        catalog[increment.Id] = increment;
                        incPublished = true;
                    }
                    else
                    {
                        sink.Remove(increment.Id);
                        incFellBack = true;
                    }

                    incStep = 4;
                    return;
            }
        }

        void StepFull()
        {
            switch (fullStep)
            {
                case 0:
                    fullStep = 1;
                    return;
                case 1:
                    artifacts.Add(Artifact(full));
                    fullStep = 2;
                    return;
                case 2:
                    sink[full.Id] = full;
                    fullStep = 3;
                    return;
                default:
                    catalog[full.Id] = full;
                    fullStep = 4;
                    return;
            }
        }

        void StepPass()
        {
            switch (passStep)
            {
                case 0:
                    // The policy is pinned to keep-last-one on purpose; see the class remarks (#2319).
                    var listing = catalog.Values.ToList();
                    var keep = BackupRetentionPlanner.ComputeRetained(listing, keepLast: 1, maxAge: null, T0.AddHours(1));
                    doomed = BackupRetentionPlanner.Doomed(listing, keep);
                    passStep = doomed.Count == 0 ? 5 : 1;
                    return;
                case 1:
                    catalog.Remove(doomed[removedRows].Id);
                    if (++removedRows == doomed.Count)
                    {
                        passStep = 2;
                    }

                    return;
                case 2:
                    blocked = removed.HasFlag(BackupRetentionRulesRemoved.DependentScan)
                        ? new HashSet<string>(StringComparer.Ordinal)
                        : BackupRetentionPlanner.DependedOn(doomed, sink.Values.ToList());
                    passStep = 3;
                    return;
                case 3:
                    foreach (var manifest in doomed.Where(m => blocked.Contains(m.Id)))
                    {
                        catalog[manifest.Id] = manifest;
                    }

                    foreach (var manifest in BackupRetentionPlanner.TipFirst(doomed.Where(m => !blocked.Contains(m.Id)).ToList()))
                    {
                        deleteQueue.Enqueue((manifest, false));
                    }

                    passStep = deleteQueue.Count == 0 ? 5 : 4;
                    return;
                default:
                    var (next, manifestGone) = deleteQueue.Dequeue();
                    if (!manifestGone)
                    {
                        sink.Remove(next.Id);
                        deleteQueue = new Queue<(BackupManifest, bool)>(new[] { (next, true) }.Concat(deleteQueue));
                    }
                    else
                    {
                        artifacts.Remove(Artifact(next));
                    }

                    if (deleteQueue.Count == 0)
                    {
                        passStep = 5;
                    }

                    return;
            }
        }

        void AssertEveryRowRestorable()
        {
            foreach (var row in catalog.Values)
            {
                Specification.Assert(Restorable(row.Id, 0), $"catalog row '{row.Id}' is not restorable");
            }
        }

        bool Restorable(string id, int depth)
        {
            var manifest = catalog.GetValueOrDefault(id) ?? sink.GetValueOrDefault(id);
            return depth < 4
                && manifest is not null
                && artifacts.Contains(Artifact(manifest))
                && (manifest.BaseBackupId is not { } baseId || Restorable(baseId, depth + 1));
        }

        void Publish(BackupManifest manifest)
        {
            artifacts.Add(Artifact(manifest));
            sink[manifest.Id] = manifest;
            catalog[manifest.Id] = manifest;
        }
    }

    private static string Artifact(BackupManifest manifest) => manifest.ContentDescriptors[0].ArtifactId;

    private static BackupManifest Manifest(string id, DateTimeOffset createdAt, string? baseId) =>
        BackupManifestModelTests.Sample(id, baseId is null ? BackupKind.Full : BackupKind.Incremental, baseId) with
        {
            CreatedAtUtc = createdAt,
            ContentDescriptors =
            [
                new BackupContentDescriptor("art-" + id, "hash", 10, 1, BackupScopeSelector.WholeTree("orders")),
            ],
        };
}
