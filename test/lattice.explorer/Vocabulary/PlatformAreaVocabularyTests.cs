using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Schema;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// The four platform areas' own term tables: the jargon each one puts in front
/// of a reader, explained at the point of use.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tables exist beside the shared glossary rather than inside it.</b>
/// The shared glossary names what the whole Explorer shares; an incremental
/// backup, a version config and a metric catalogue are meaningful only inside one
/// area, and the assembly graph puts the glossary in a package these consume
/// rather than own. What must not happen is a <em>second wording</em> for a
/// concept the glossary already names, so the deferral cases below are the point
/// of this fixture as much as the coverage is.
/// </para>
/// <para>
/// Every assertion reads a static table, so nothing here depends on timing,
/// ordering, a wall clock, or garbage collection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class PlatformAreaVocabularyTests
{
    private static IEnumerable<TestCaseData> EveryTerm()
    {
        foreach (var (area, term) in AllTerms())
        {
            yield return new TestCaseData(term).SetArgDisplayNames(area + "/" + term.Id);
        }
    }

    private static IEnumerable<(string Area, ExplorerTerm Term)> AllTerms()
    {
        yield return ("backups", BackupsVocabulary.FullBackup);
        yield return ("backups", BackupsVocabulary.IncrementalBackup);
        yield return ("backups", BackupsVocabulary.BackupSet);
        yield return ("backups", BackupsVocabulary.Restore);
        yield return ("backups", BackupsVocabulary.Revert);
        yield return ("backups", BackupsVocabulary.ScopeStatus);
        yield return ("backups", BackupsVocabulary.Schedule);
        yield return ("backups", BackupsVocabulary.Health);
        yield return ("backups", BackupsVocabulary.Grant);

        yield return ("telemetry", TelemetryVocabulary.MetricCatalog);
        yield return ("telemetry", TelemetryVocabulary.QueryRange);
        yield return ("telemetry", TelemetryVocabulary.QueryStep);
        yield return ("telemetry", TelemetryVocabulary.Backend);
        yield return ("telemetry", TelemetryVocabulary.Scope);

        yield return ("schema", SchemaVocabulary.SchemaPolicy);
        yield return ("schema", SchemaVocabulary.VersionConfig);
        yield return ("schema", SchemaVocabulary.Remediation);
        yield return ("schema", SchemaVocabulary.ComplianceScan);
        yield return ("schema", SchemaVocabulary.StrictSchema);
        yield return ("schema", SchemaVocabulary.DeadLetter);

        yield return ("access", AccessVocabulary.Rule);
        yield return ("access", AccessVocabulary.SubjectSelector);
        yield return ("access", AccessVocabulary.Effect);
        yield return ("access", AccessVocabulary.Scope);
        yield return ("access", AccessVocabulary.Precedence);
        yield return ("access", AccessVocabulary.Grant);
        yield return ("access", AccessVocabulary.AdminSubject);
    }

    [TestCaseSource(nameof(EveryTerm))]
    public void Every_term_is_a_complete_explanation_somebody_could_act_on(ExplorerTerm term)
    {
        Assert.Multiple(() =>
        {
            Assert.That(term.Id, Is.Not.Empty);
            Assert.That(term.Label, Is.Not.Empty);
            Assert.That(term.Explanation, Is.Not.Empty);
            Assert.That(
                term.Explanation,
                Does.EndWith("."),
                "an explanation is a complete sentence, matching the shared glossary's rule");
            Assert.That(term.HasDocsLink, Is.True, "every term says where to read more");
        });
    }

    [Test]
    public void Two_different_terms_never_share_an_id()
    {
        // An id doubles as a help disclosure's element-id prefix. Two distinct
        // terms sharing one would point two aria-describedby references at the
        // same element, so a control would be described by the wrong
        // explanation.
        //
        // The same term appearing in two areas is not a collision but the point:
        // Backups and Access both take "grant" from the shared glossary, so both
        // say the same sentence about it. That is asserted by instance, so a
        // reworded copy would be caught while a shared one is not.
        var collisions = AllTerms()
            .Select(entry => entry.Term)
            .Distinct(ReferenceEqualityComparer.Instance)
            .Cast<ExplorerTerm>()
            .GroupBy(term => term.Id, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray();

        Assert.That(collisions, Is.Empty);
    }

    [Test]
    public void No_area_term_shadows_a_shared_glossary_id_it_did_not_take_from_it()
    {
        // A plugin-owned term whose id collides with a shared one is the drift
        // this vocabulary work exists to remove: two wordings for one concept.
        // A term TAKEN from the glossary is the same instance, so it is exempt.
        var shadowed = AllTerms()
            .Where(entry => ExplorerGlossary.TryGet(entry.Term.Id, out var shared)
                && !ReferenceEquals(shared, entry.Term))
            .Select(entry => entry.Area + "/" + entry.Term.Id)
            .ToArray();

        Assert.That(shadowed, Is.Empty);
    }

    [Test]
    public void A_term_the_shared_glossary_already_names_is_taken_from_it_rather_than_reworded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                BackupsVocabulary.Grant,
                Is.SameAs(ExplorerGlossary.Get(ExplorerTermIds.Grant)));
            Assert.That(
                AccessVocabulary.Grant,
                Is.SameAs(ExplorerGlossary.Get(ExplorerTermIds.Grant)));
            Assert.That(
                AccessVocabulary.AdminSubject,
                Is.SameAs(ExplorerGlossary.Get(ExplorerTermIds.AdminSubject)));
            Assert.That(
                SchemaVocabulary.StrictSchema,
                Is.SameAs(ExplorerGlossary.Get(ExplorerTermIds.StrictSchema)));
            Assert.That(
                SchemaVocabulary.DeadLetter,
                Is.SameAs(ExplorerGlossary.Get(ExplorerTermIds.DeadLetter)));
        });
    }

    [Test]
    public void Every_declared_id_constant_names_the_term_it_is_declared_beside()
    {
        // The constants are what a call site spells; a constant that has drifted
        // from its term would silently describe a control with nothing.
        Assert.Multiple(() =>
        {
            Assert.That(BackupsVocabulary.FullBackupId, Is.EqualTo(BackupsVocabulary.FullBackup.Id));
            Assert.That(
                BackupsVocabulary.IncrementalBackupId,
                Is.EqualTo(BackupsVocabulary.IncrementalBackup.Id));
            Assert.That(BackupsVocabulary.BackupSetId, Is.EqualTo(BackupsVocabulary.BackupSet.Id));
            Assert.That(BackupsVocabulary.RestoreId, Is.EqualTo(BackupsVocabulary.Restore.Id));
            Assert.That(BackupsVocabulary.RevertId, Is.EqualTo(BackupsVocabulary.Revert.Id));
            Assert.That(BackupsVocabulary.ScopeStatusId, Is.EqualTo(BackupsVocabulary.ScopeStatus.Id));
            Assert.That(BackupsVocabulary.ScheduleId, Is.EqualTo(BackupsVocabulary.Schedule.Id));
            Assert.That(BackupsVocabulary.HealthId, Is.EqualTo(BackupsVocabulary.Health.Id));

            Assert.That(
                TelemetryVocabulary.MetricCatalogId,
                Is.EqualTo(TelemetryVocabulary.MetricCatalog.Id));
            Assert.That(TelemetryVocabulary.QueryRangeId, Is.EqualTo(TelemetryVocabulary.QueryRange.Id));
            Assert.That(TelemetryVocabulary.QueryStepId, Is.EqualTo(TelemetryVocabulary.QueryStep.Id));
            Assert.That(TelemetryVocabulary.BackendId, Is.EqualTo(TelemetryVocabulary.Backend.Id));
            Assert.That(TelemetryVocabulary.ScopeId, Is.EqualTo(TelemetryVocabulary.Scope.Id));

            Assert.That(SchemaVocabulary.SchemaPolicyId, Is.EqualTo(SchemaVocabulary.SchemaPolicy.Id));
            Assert.That(SchemaVocabulary.VersionConfigId, Is.EqualTo(SchemaVocabulary.VersionConfig.Id));
            Assert.That(SchemaVocabulary.RemediationId, Is.EqualTo(SchemaVocabulary.Remediation.Id));
            Assert.That(SchemaVocabulary.ComplianceScanId, Is.EqualTo(SchemaVocabulary.ComplianceScan.Id));

            Assert.That(AccessVocabulary.RuleId, Is.EqualTo(AccessVocabulary.Rule.Id));
            Assert.That(
                AccessVocabulary.SubjectSelectorId,
                Is.EqualTo(AccessVocabulary.SubjectSelector.Id));
            Assert.That(AccessVocabulary.EffectId, Is.EqualTo(AccessVocabulary.Effect.Id));
            Assert.That(AccessVocabulary.ScopeId, Is.EqualTo(AccessVocabulary.Scope.Id));
            Assert.That(AccessVocabulary.PrecedenceId, Is.EqualTo(AccessVocabulary.Precedence.Id));
        });
    }

    [Test]
    public void Every_term_is_read_from_a_cached_instance_so_explaining_one_costs_no_allocation()
    {
        // These are read on the render path of a list surface, so a property
        // that composed a fresh record per read would allocate per row per
        // render. Reference equality across two reads is what proves it does
        // not, and it needs no timing and no GC observation to assert.
        Assert.Multiple(() =>
        {
            Assert.That(BackupsVocabulary.IncrementalBackup, Is.SameAs(BackupsVocabulary.IncrementalBackup));
            Assert.That(TelemetryVocabulary.MetricCatalog, Is.SameAs(TelemetryVocabulary.MetricCatalog));
            Assert.That(SchemaVocabulary.ComplianceScan, Is.SameAs(SchemaVocabulary.ComplianceScan));
            Assert.That(AccessVocabulary.Precedence, Is.SameAs(AccessVocabulary.Precedence));
        });
    }
}
