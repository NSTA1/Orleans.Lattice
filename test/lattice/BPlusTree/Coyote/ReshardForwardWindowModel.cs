using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Whether a <see cref="ReshardForwardWindowModel"/> run installs the destination
/// shadow marker in the same step that forwards the drain-migrated value, or in a
/// separate, independently scheduled step.
/// </summary>
public enum ForwardMarkerMode
{
    /// <summary>
    /// The value-forward and the shadow-marker install are one indivisible step, so
    /// a destination key is never observable in the migrated-but-unmarked state. No
    /// schedule lets a reader fall through the read gate onto a stale pre-saga
    /// value.
    /// </summary>
    InstalledWithForward,

    /// <summary>
    /// The value-forward and the shadow-marker install are two separately scheduled
    /// steps, mirroring the shipping split path, which forwards the value in one
    /// grain call and marks the saga shadow in a second, later call. Between them a
    /// destination key holds a drain-migrated pre-saga value carrying no marker, so
    /// the read gate is never consulted for it.
    /// </summary>
    InstalledSeparately,
}

/// <summary>
/// A Coyote concurrency model of the <b>forward window</b> of the online-reshard
/// split path: the interval during which a destination leaf already holds a
/// drain-migrated pre-saga value for a key but does not yet carry the shadow marker
/// for the saga that is concurrently rewriting it.
/// <para>
/// This models a different phase from <see cref="ReshardMigrationModel"/>, which
/// begins after migration has fully completed and explores a <b>late</b> orphan
/// prepare arriving at a converged destination. That model cannot represent this
/// window at all: its migration preamble contains no controlled choice, and its
/// per-key value and marker are mutated together in one indivisible step, so a
/// migrated-but-unmarked key has no representation in its state space. The window
/// modelled here is therefore not a gap in that model's assertions - which do
/// include cross-key simultaneity - but a gap in its state space.
/// </para>
/// <para>
/// The reader's per-key visibility decision is the real
/// <see cref="AtomicVisibilityGate.ResolveKey"/> rule fed by a real
/// <see cref="TxRegistryDecisionCore"/>, and the ungated fall-through mirrors the
/// shipping read path, which consults the gate only when a shadow marker is present
/// and otherwise serves the projected value directly. The safety property is the
/// cross-key simultaneity the atomic batch promises: one reader fan-out must not
/// observe two different saga rounds.
/// </para>
/// </summary>
public sealed class ReshardForwardWindowModel : ICoyoteModel
{
    /// <summary>The pre-saga round's value, drain-migrated to the destination.</summary>
    private const int Pre = 1;

    /// <summary>The in-flight saga's value, committed but not yet terminal-applied.</summary>
    private const int Post = 2;

    /// <summary>The destination key has the forwarded value but no shadow marker.</summary>
    private const int PhaseForwarded = 0;

    /// <summary>The shadow marker for the in-flight saga is installed.</summary>
    private const int PhaseMarked = 1;

    /// <summary>The saga terminal has landed and rewritten projected state.</summary>
    private const int PhaseTerminal = 2;

    private readonly int _keyCount;
    private readonly ForwardMarkerMode _mode;

    /// <summary>
    /// Creates the model for a <paramref name="keyCount"/>-key atomic batch landing
    /// on a migrating destination leaf under the chosen <paramref name="mode"/>.
    /// </summary>
    public ReshardForwardWindowModel(int keyCount, ForwardMarkerMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(keyCount, 2);
        _keyCount = keyCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The real recording-side core holds the in-flight saga's decision. The saga
        // has committed; its terminal has not yet been applied to every key.
        var core = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), 0L);
        var saga = Guid.NewGuid();
        core.Apply(saga, TxStatus.Committed);

        // Destination-leaf state: the projected value per key, and how far that key
        // has progressed through the migration handshake. Under InstalledWithForward
        // the marker lands with the value, so no key starts unmarked.
        var projected = new int[_keyCount];
        Array.Fill(projected, Pre);
        var phase = new int[_keyCount];
        Array.Fill(phase, _mode == ForwardMarkerMode.InstalledWithForward ? PhaseMarked : PhaseForwarded);

        // The reader fan-out resolves each key exactly once. Between key
        // resolutions the runtime decides, per key, whether the migration handshake
        // advances one step, so a single fan-out can straddle the window.
        var observed = new int[_keyCount];
        for (var i = 0; i < _keyCount; i++)
        {
            MaybeAdvanceMigration(projected, phase, runtime);
            observed[i] = ObserveKey(i, core, saga, projected, phase);
        }

        AssertNoSplitAcrossKeys(observed);
    }

    /// <summary>
    /// Advances the per-key migration handshake. For each key the runtime decides
    /// whether the next step lands now: an unmarked key installs its shadow marker,
    /// and a marked key applies the saga terminal into projected state. This is the
    /// per-key progress a reader fan-out can interleave with.
    /// </summary>
    private static void MaybeAdvanceMigration(int[] projected, int[] phase, ICoyoteRuntime runtime)
    {
        for (var i = 0; i < phase.Length; i++)
        {
            if (phase[i] == PhaseTerminal || !runtime.RandomBoolean())
            {
                continue;
            }

            if (phase[i] == PhaseForwarded)
            {
                phase[i] = PhaseMarked;
            }
            else
            {
                projected[i] = Post;
                phase[i] = PhaseTerminal;
            }
        }
    }

    /// <summary>
    /// Resolves the value the reader observes for key <paramref name="i"/>, mirroring
    /// the shipping read path. A key carrying a shadow marker consults the production
    /// <see cref="AtomicVisibilityGate.ResolveKey"/>, which surfaces the committed
    /// saga's prepared value while its terminal is outstanding. A key with no marker
    /// takes the ungated fall-through and serves projected state, which during the
    /// forward window is still the stale pre-saga value.
    /// </summary>
    private static int ObserveKey(int i, TxRegistryDecisionCore core, Guid saga, int[] projected, int[] phase)
    {
        if (phase[i] != PhaseMarked)
        {
            return projected[i];
        }

        var outcome = AtomicVisibilityGate.ResolveKey(
            core.Resolve(saga),
            alreadyTerminal: false,
            preparedHiddenByTombstoneOrExpiry: false);

        return outcome == PendingReadOutcome.SurfacePrepared ? Post : projected[i];
    }

    /// <summary>
    /// The safety assertion: one reader fan-out over an atomically written batch must
    /// observe a single saga round across every key. A mixed observation is the torn
    /// read the chaos suite reports as <c>split (pre, post)</c>.
    /// </summary>
    private static void AssertNoSplitAcrossKeys(int[] observed)
    {
        var first = observed[0];
        for (var i = 1; i < observed.Length; i++)
        {
            Specification.Assert(
                observed[i] == first,
                $"reshard forward window split a reader across {observed.Length} keys: " +
                $"key0={first}, key{i}={observed[i]} (one fan-out observed two saga rounds)");
        }
    }
}
