using System.Diagnostics.Metrics;
using System.Reflection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Readiness coverage for <see cref="RepoContextRetrievalPath.KeywordExactFallbackSuppressed"/>,
/// plus the structural guard that stops the next value from repeating this issue's
/// mistake in the other direction.
/// <para>
/// <see cref="RepoContextRetrievalReadinessState.Observe"/> and its cause
/// normalisation both enumerate a closed set and fail closed on anything outside
/// it. That is correct for an arbitrary caller-supplied string and dangerous for a
/// value this package itself emits: a new keyword path that nobody adds to those
/// enumerations makes a wedged box quietly stop reporting unavailable and makes its
/// fault episodes collapse to <c>unknown</c>. Neither shows up as a failure - the
/// box reports ready and the meter reports a bounded tag - which is the shape of a
/// silent regression rather than a loud one. The two behavioural tests below cover
/// the value this change adds; the two structural tests cover every value the
/// vocabulary will ever hold.
/// </para>
/// </summary>
public sealed partial class RepoContextRetrievalReadinessStateTests
{
    /// <summary>
    /// Every public wire value the vocabulary declares. Reflected rather than
    /// listed, so a value added to <see cref="RepoContextRetrievalPath"/> is
    /// enrolled here the moment it exists and cannot be forgotten.
    /// </summary>
    private static IReadOnlyList<string> VocabularyValues()
        => typeof(RepoContextRetrievalPath)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && !f.IsInitOnly && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToArray();

    /// <summary>
    /// Records the fault-episode cause tag observed while <paramref name="observe"/>
    /// runs against a fresh state, or <see langword="null"/> when no episode was
    /// metered at all.
    /// </summary>
    private static string? CaptureFaultCause(Action<RepoContextRetrievalReadinessState> observe)
    {
        var clock = new SettableTimeProvider();
        string? causeTag = null;
        var episodes = 0L;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.UnavailableInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            episodes += measurement;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.CauseTagKey)
                {
                    causeTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        observe(state);

        return episodes == 0 ? null : causeTag;
    }

    [Test]
    public void A_suppressed_exact_fallback_is_observed_as_unavailable()
    {
        using var state = Create(out var clock);
        state.MarkServing();

        state.Observe(RepoContextRetrievalPath.KeywordExactFallbackSuppressed);
        clock.Advance(HoldDown);

        Assert.Multiple(() =>
        {
            Assert.That(state.IsReady, Is.False,
                "A box withholding the only fallback that could answer is not serving semantic retrieval, so "
                + "readiness must revoke. Observe fails closed on an unrecognised value, so a new keyword path "
                + "that nobody wires here does not report a fault - it silently stops reporting one.");
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
        });
    }

    [Test]
    public void A_suppressed_exact_fallback_is_metered_under_its_own_cause()
        => Assert.That(
            CaptureFaultCause(s => s.MarkUnavailable(RepoContextRetrievalPath.KeywordExactFallbackSuppressed)),
            Is.EqualTo(RepoContextRetrievalPath.KeywordExactFallbackSuppressed),
            "An operator separates a withheld fallback from an absent plane by the cause tag. Collapsing it to "
            + "'unknown' would put the two episodes in one bucket and lose the distinction the wire value exists "
            + "to draw.");

    [Test]
    public void Every_vocabulary_value_is_routed_to_a_definite_readiness_disposition()
    {
        var vocabulary = VocabularyValues();
        Assert.That(vocabulary, Is.Not.Empty, "The reflection found no wire values, so this guard proved nothing.");

        var unrouted = new List<string>();
        foreach (var value in vocabulary)
        {
            // A fresh state sits in Building with no fault episode open, so "Observe
            // did nothing at all" is distinguishable from every disposition it can
            // reach: serving and keyword-only move the phase, and an unavailability
            // cause opens (and meters) a fault episode without moving it.
            RepoContextRetrievalReadinessPhase phase = default;
            var cause = CaptureFaultCause(state =>
            {
                state.Observe(value);
                phase = state.Phase;
            });

            if (phase == RepoContextRetrievalReadinessPhase.Building && cause is null)
            {
                unrouted.Add(value);
            }
        }

        Assert.That(unrouted, Is.Empty,
            "Observe enumerates a closed set and does nothing at all with a value outside it. That is right for a "
            + "caller-supplied string and wrong for a value this package emits: the readiness signal keeps "
            + "reporting whatever it last saw, so a wedged box reads as healthy. Wire the value into Observe "
            + "rather than relaxing this guard.");
    }

    [Test]
    public void Every_unavailability_cause_in_the_vocabulary_survives_normalisation()
    {
        var causes = VocabularyValues()
            .Where(v => !RepoContextRetrievalPath.IsSemantic(v))
            .Where(v => !string.Equals(v, RepoContextRetrievalPath.KeywordNoEmbedder, StringComparison.Ordinal))
            .ToArray();

        Assert.That(causes, Is.Not.Empty, "The reflection found no fault causes, so this guard proved nothing.");

        var collapsed = causes
            .Where(c => CaptureFaultCause(s => s.MarkUnavailable(c)) != c)
            .ToArray();

        Assert.That(collapsed, Is.Empty,
            "NormalizeCause bounds the meter's tag cardinality by resolving against a closed set, so a cause it "
            + "does not recognise reaches the meter as 'unknown'. For an arbitrary string that is the point; for "
            + "a value this package emits it silently merges two distinct fault modes into one bucket. Add the "
            + "value to NormalizeCause rather than relaxing this guard.");
    }
}
