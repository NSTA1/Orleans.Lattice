using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Covers <see cref="LatticeMetrics.BuildInfo"/>, the process-scoped deployment
/// liveness instrument.
/// <para>
/// Every other instrument in this assembly reports something a <em>feature</em>
/// did, so each is silent until that feature is exercised. That makes all of them
/// unusable for answering "is the build under test the build that is actually
/// running", because an absent series is ambiguous between "the image did not
/// deploy" and "the code deployed but that path was never reached" - opposite
/// conclusions drawn from byte-identical evidence.
/// </para>
/// <para>
/// The property that closes it is <b>unconditional emission</b>: this gauge
/// reports on every collection from process start, with no registry to populate
/// and no work to wait for. These tests therefore deliberately perform
/// <em>no arrange step at all</em> - the absence of setup is the point, and a
/// test that had to make something happen first would be pinning the wrong
/// property.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class BuildInfoMetricTests
{
    private static List<(long Value, string? Version, string? Sha)> ObserveBuildInfo()
    {
        var captured = new List<(long, string?, string?)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.BuildInfo,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                captured.Add((value, Tag(tags, LatticeMetrics.TagVersion), Tag(tags, LatticeMetrics.TagSha)))));

        listener.RecordObservableInstruments();
        return captured;
    }

    private static string? Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal))
            {
                return tag.Value as string;
            }
        }

        return null;
    }

    /// <summary>
    /// The instrument emits exactly one measurement, valued <c>1</c>, with no
    /// arrange step whatsoever. This is the whole contract: absence of this series
    /// can only mean the process is not running or is not exporting.
    /// </summary>
    [Test]
    public void Build_info_emits_exactly_one_measurement_valued_one_with_no_arrange_step()
    {
        var measurements = ObserveBuildInfo();

        Assert.That(measurements, Has.Count.EqualTo(1),
            "build info must emit exactly one measurement per collection, unconditionally; a count of zero would "
            + "make an absent series ambiguous with an undeployed image, which is the ambiguity this instrument exists to remove");
        Assert.That(measurements[0].Value, Is.EqualTo(1),
            "the value of an info-style metric carries no information and must be a constant 1; all content is in the tags");
    }

    /// <summary>
    /// Emission is idempotent across collections rather than a one-shot at
    /// startup. A gauge that reported once and then fell silent would look
    /// identical to an undeployed image on every scrape after the first.
    /// </summary>
    [Test]
    public void Build_info_emits_again_on_a_second_independent_collection()
    {
        var first = ObserveBuildInfo();
        var second = ObserveBuildInfo();

        Assert.That(first, Has.Count.EqualTo(1), "precondition: the first collection must observe the instrument");
        Assert.That(second, Has.Count.EqualTo(1),
            "a second, independent collection must observe it too; a one-shot instrument would be indistinguishable "
            + "from an undeployed image on every scrape after the first");
        Assert.That(second[0].Version, Is.EqualTo(first[0].Version), "build identity must not vary between collections");
        Assert.That(second[0].Sha, Is.EqualTo(first[0].Sha), "build identity must not vary between collections");
    }

    /// <summary>
    /// Both identity tags are present and non-empty. An info metric whose tags are
    /// missing carries nothing at all, because its value is a constant.
    /// </summary>
    [Test]
    public void Build_info_carries_a_non_empty_version_tag_and_sha_tag()
    {
        var measurement = ObserveBuildInfo().Single();

        Assert.Multiple(() =>
        {
            Assert.That(measurement.Version, Is.Not.Null.And.Not.Empty,
                $"the '{LatticeMetrics.TagVersion}' tag carries half the identity; without it the constant value 1 says nothing");
            Assert.That(measurement.Sha, Is.Not.Null.And.Not.Empty,
                $"the '{LatticeMetrics.TagSha}' tag is what distinguishes two builds of the same version, which is the "
                + "discrimination the instrument exists to provide");
        });
    }

    /// <summary>
    /// The informational version is <b>split</b> on its build-metadata separator
    /// rather than reported whole.
    /// </summary>
    /// <remarks>
    /// This is the arm that falsifies the most likely real bug: emitting the raw
    /// <c>AssemblyInformationalVersionAttribute</c> value, which is
    /// <c>9.6.2+&lt;sha&gt;</c>, into the version tag. Asserting the tag against a
    /// locally recomputed split would be circular - it would pass for any parser
    /// that agreed with itself - so this instead asserts a <em>shape</em> the
    /// unsplit value provably cannot satisfy.
    /// </remarks>
    [Test]
    public void Build_info_splits_the_version_from_the_commit_rather_than_reporting_the_raw_informational_version()
    {
        var measurement = ObserveBuildInfo().Single();

        Assert.Multiple(() =>
        {
            Assert.That(measurement.Version, Does.Not.Contain("+"),
                "the version tag must not carry the '+<sha>' build-metadata suffix; reporting the raw informational "
                + "version would put the whole identity in one tag and leave the commit tag redundant");
            Assert.That(measurement.Sha, Does.Not.Contain("+"),
                "the sha tag must be the build metadata alone, with the separator consumed by the split");
        });
    }

    /// <summary>
    /// The sha tag is a full 40-character lowercase hex sha. Never empty, never a
    /// placeholder, never abbreviated.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This arm deliberately refuses to tolerate the
    /// <see cref="LatticeMetrics.BuildMetadataUnknown"/> sentinel.</b> An info
    /// gauge carrying <c>sha=""</c> or <c>sha="unknown"</c> is worse than a
    /// missing one, because it reads as a working detector while identifying
    /// nothing: a reader sees a present series, concludes the build is known, and
    /// is wrong. Passing this test on a sentinel would make the test itself the
    /// same kind of object - a green that cannot distinguish a stamped build from
    /// an unstamped one.
    /// </para>
    /// <para>
    /// The abbreviation clause is not fussiness either. Resolving a CI run by an
    /// abbreviated head sha returns an empty result set with a success exit code,
    /// which is byte-identical to "this commit has no CI", so a truncated sha here
    /// would silently break the join this series exists to support.
    /// </para>
    /// </remarks>
    [Test]
    public void Build_info_sha_is_a_full_forty_character_sha()
    {
        var sha = ObserveBuildInfo().Single().Sha;

        Assert.Multiple(() =>
        {
            Assert.That(sha, Is.Not.Null.And.Not.Empty,
                "an empty sha is worse than an absent series: it reads as a working detector while identifying nothing");
            Assert.That(sha, Is.Not.EqualTo(LatticeMetrics.BuildMetadataUnknown),
                $"the '{LatticeMetrics.BuildMetadataUnknown}' sentinel must never reach a real build. Reaching it means the "
                + "build stopped stamping SourceRevisionId, which is exactly the regression this arm exists to redden on, "
                + "and tolerating it here would make this test a green that cannot tell a stamped build from an unstamped one");
            Assert.That(sha, Does.Match("^[0-9a-f]{40}$"),
                "the sha must be a full 40-character lowercase hex sha; an abbreviated sha cannot be resolved against a CI "
                + "run by API, where a short sha returns an empty result set indistinguishable from 'no runs'");
        });
    }

    /// <summary>
    /// The reported identity is the one the running assembly actually carries, not
    /// a value read from the surrounding checkout.
    /// </summary>
    /// <remarks>
    /// This is what makes the series describe the <b>image</b>. A liveness signal
    /// sourced from the working tree would report the sha an operator is looking
    /// at rather than the one that is running, which is precisely the confusion it
    /// exists to prevent.
    /// </remarks>
    [Test]
    public void Build_info_reports_the_identity_stamped_into_the_running_assembly()
    {
        var informational = typeof(LatticeMetrics).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        Assume.That(informational, Is.Not.Null.And.Not.Empty,
            "precondition: the running assembly carries an informational version");

        var measurement = ObserveBuildInfo().Single();

        Assert.That(informational, Does.StartWith(measurement.Version!),
            "the version tag must be a prefix of the running assembly's own informational version, so the series "
            + "describes the loaded image rather than the checkout it happens to sit beside");

        if (!string.Equals(measurement.Sha, LatticeMetrics.BuildMetadataUnknown, StringComparison.Ordinal))
        {
            Assert.That(informational, Does.EndWith(measurement.Sha!),
                "the sha tag must be the build metadata of the running assembly's own informational version");
        }
    }

    /// <summary>
    /// The instrument is published on the shared Lattice meter, so a pipeline that
    /// subscribes once receives it alongside everything else.
    /// </summary>
    /// <remarks>
    /// Worth pinning separately: a deployment-liveness signal published on a meter
    /// nobody subscribes to is absent at the scrape for a reason that has nothing
    /// to do with deployment, which would reintroduce the exact ambiguity the
    /// instrument removes.
    /// </remarks>
    [Test]
    public void Build_info_is_published_on_the_shared_lattice_meter_under_its_canonical_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeMetrics.BuildInfo.Meter, Is.SameAs(LatticeMetrics.Meter),
                "a liveness signal on an unsubscribed meter is absent for a reason unrelated to deployment");
            Assert.That(LatticeMetrics.BuildInfo.Name, Is.EqualTo(LatticeMetrics.BuildInfoName),
                "the exported name constant is what the dashboard drift guard resolves a panel token against");
        });
    }
}
