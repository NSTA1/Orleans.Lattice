using System.Reflection;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Tests for the single glossary every Explorer surface takes its wording from.
/// </summary>
/// <remarks>
/// Two things are being defended. The first is that a term is never
/// half-finished: a label or an explanation left empty would render an
/// affordance that explains nothing, which is the defect the glossary exists to
/// close. The second is that the id constants and the table cannot drift apart,
/// because a consumer references a constant and would otherwise get silence.
/// </remarks>
[TestFixture]
public class ExplorerGlossaryTests
{
    private static IReadOnlyList<string> DeclaredTermIds() =>
        typeof(ExplorerTermIds)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(field => field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToArray();

    private static IReadOnlyList<string> DeclaredDocsLinks() =>
        typeof(ExplorerDocsLinks)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(field => field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToArray();

    // ------------------------------------------------------- completeness guard

    [Test]
    public void Every_term_has_a_non_empty_label_and_explanation()
    {
        var halfFinished = ExplorerGlossary.Terms
            .Where(term => string.IsNullOrWhiteSpace(term.Label) || string.IsNullOrWhiteSpace(term.Explanation))
            .Select(term => term.Id)
            .ToArray();

        Assert.That(
            halfFinished,
            Is.Empty,
            "a term with no label or no explanation renders an affordance that explains nothing");
    }

    [Test]
    public void Every_term_id_constant_resolves_to_exactly_one_term()
    {
        var declared = DeclaredTermIds();

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Empty);
            foreach (var id in declared)
            {
                Assert.That(
                    ExplorerGlossary.Terms.Count(term => string.Equals(term.Id, id, StringComparison.Ordinal)),
                    Is.EqualTo(1),
                    "term id '" + id + "' must be defined exactly once");
            }
        });
    }

    [Test]
    public void Every_term_is_reachable_through_a_declared_id_constant()
    {
        var declared = DeclaredTermIds().ToHashSet(StringComparer.Ordinal);
        var orphans = ExplorerGlossary.Terms
            .Select(term => term.Id)
            .Where(id => !declared.Contains(id))
            .ToArray();

        Assert.That(orphans, Is.Empty, "a term with no id constant cannot be referenced by a consumer");
    }

    [Test]
    public void Every_term_explanation_reads_as_a_complete_sentence()
    {
        var truncated = ExplorerGlossary.Terms
            .Where(term => !term.Explanation.EndsWith('.'))
            .Select(term => term.Id)
            .ToArray();

        Assert.That(truncated, Is.Empty, "explanations are prose shown to a user, not fragments");
    }

    [Test]
    public void Every_docs_link_used_by_a_term_is_a_declared_constant()
    {
        var declared = DeclaredDocsLinks().ToHashSet(StringComparer.Ordinal);
        var undeclared = ExplorerGlossary.Terms
            .Where(term => term.HasDocsLink && !declared.Contains(term.DocsLink!))
            .Select(term => term.Id)
            .ToArray();

        Assert.That(undeclared, Is.Empty, "a link target belongs in ExplorerDocsLinks so it is maintained in one place");
    }

    [Test]
    public void Every_term_named_by_the_issue_is_defined()
    {
        // The exact vocabulary the audit found unexplained in the running
        // application, listed so a later edit cannot quietly drop one.
        string[] required =
        [
            ExplorerTermIds.Trees,
            ExplorerTermIds.Views,
            ExplorerTermIds.TagIndexes,
            ExplorerTermIds.LifecycleActive,
            ExplorerTermIds.ShardCount,
            ExplorerTermIds.AggregationView,
            ExplorerTermIds.HistoryView,
            ExplorerTermIds.DeadLetterCount,
            ExplorerTermIds.SourceTree,
            ExplorerTermIds.ProjectionProvider,
            ExplorerTermIds.ProjectionVersion,
            ExplorerTermIds.ActiveTenant,
            ExplorerTermIds.AllTenants,
            ExplorerTermIds.Quota,
            ExplorerTermIds.Residency,
            ExplorerTermIds.Region,
            ExplorerTermIds.Grant,
            ExplorerTermIds.AdminSubject,
            ExplorerTermIds.TenantAdministrationArea,
            ExplorerTermIds.MyTenantArea,
            ExplorerTermIds.Shard,
            ExplorerTermIds.Leaf,
            ExplorerTermIds.Wal,
            ExplorerTermIds.Crdt,
            ExplorerTermIds.DeadLetter,
            ExplorerTermIds.Compaction,
            ExplorerTermIds.Reshard,
        ];

        Assert.Multiple(() =>
        {
            foreach (var id in required)
            {
                Assert.That(ExplorerGlossary.Contains(id), Is.True, "term '" + id + "' is required by the issue");
            }
        });
    }

    // -------------------------------------------------------------- Terms/Count

    [Test]
    public void Terms_exposes_every_definition()
    {
        Assert.That(ExplorerGlossary.Terms, Has.Count.EqualTo(ExplorerGlossary.Count));
        Assert.That(ExplorerGlossary.Count, Is.GreaterThan(0));
    }

    [Test]
    public void Terms_returns_the_same_instance_on_every_read()
    {
        // The table is built once; reading it must not allocate a fresh
        // projection per rendered badge.
        Assert.That(ExplorerGlossary.Terms, Is.SameAs(ExplorerGlossary.Terms));
    }

    // ------------------------------------------------------------------ TryGet

    [Test]
    public void TryGet_known_id_returns_the_term()
    {
        var found = ExplorerGlossary.TryGet(ExplorerTermIds.Shard, out var term);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(term, Is.Not.Null);
            Assert.That(term!.Id, Is.EqualTo(ExplorerTermIds.Shard));
        });
    }

    [Test]
    public void TryGet_unknown_id_returns_false_and_no_term()
    {
        var found = ExplorerGlossary.TryGet("not-a-term", out var term);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(term, Is.Null);
        });
    }

    [Test]
    public void TryGet_null_id_returns_false_rather_than_throwing()
    {
        Assert.That(ExplorerGlossary.TryGet(null, out var term), Is.False);
        Assert.That(term, Is.Null);
    }

    [Test]
    public void TryGet_empty_id_returns_false()
    {
        Assert.That(ExplorerGlossary.TryGet(string.Empty, out _), Is.False);
    }

    [Test]
    public void TryGet_is_case_sensitive()
    {
        Assert.That(ExplorerGlossary.TryGet("SHARD", out _), Is.False);
    }

    // -------------------------------------------------------------------- Find

    [Test]
    public void Find_known_id_returns_the_term()
    {
        Assert.That(ExplorerGlossary.Find(ExplorerTermIds.Wal)!.Label, Is.EqualTo("Write-ahead log (WAL)"));
    }

    [Test]
    public void Find_unknown_or_null_id_returns_null()
    {
        Assert.That(ExplorerGlossary.Find("nope"), Is.Null);
        Assert.That(ExplorerGlossary.Find(null), Is.Null);
    }

    // --------------------------------------------------------------------- Get

    [Test]
    public void Get_known_id_returns_the_term()
    {
        Assert.That(ExplorerGlossary.Get(ExplorerTermIds.Crdt).Id, Is.EqualTo(ExplorerTermIds.Crdt));
    }

    [Test]
    public void Get_unknown_id_throws_key_not_found()
    {
        Assert.That(() => ExplorerGlossary.Get("nope"), Throws.TypeOf<KeyNotFoundException>());
    }

    [Test]
    public void Get_null_id_throws_argument_null()
    {
        Assert.That(() => ExplorerGlossary.Get(null!), Throws.ArgumentNullException);
    }

    // ---------------------------------------------------------------- Contains

    [Test]
    public void Contains_reports_membership()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerGlossary.Contains(ExplorerTermIds.Leaf), Is.True);
            Assert.That(ExplorerGlossary.Contains("nope"), Is.False);
            Assert.That(ExplorerGlossary.Contains(null), Is.False);
        });
    }

    // ---------------------------------------------------------- field accessors

    [Test]
    public void ExplanationFor_returns_the_explanation_or_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerGlossary.ExplanationFor(ExplorerTermIds.Shard),
                Is.EqualTo(ExplorerGlossary.Get(ExplorerTermIds.Shard).Explanation));
            Assert.That(ExplorerGlossary.ExplanationFor("nope"), Is.Null);
            Assert.That(ExplorerGlossary.ExplanationFor(null), Is.Null);
        });
    }

    [Test]
    public void LabelFor_returns_the_label_or_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerGlossary.LabelFor(ExplorerTermIds.Quota), Is.EqualTo("Quota"));
            Assert.That(ExplorerGlossary.LabelFor("nope"), Is.Null);
            Assert.That(ExplorerGlossary.LabelFor(null), Is.Null);
        });
    }

    [Test]
    public void DocsLinkFor_returns_the_link_or_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerGlossary.DocsLinkFor(ExplorerTermIds.Wal), Is.EqualTo(ExplorerDocsLinks.Wal));
            Assert.That(ExplorerGlossary.DocsLinkFor("nope"), Is.Null);
            Assert.That(ExplorerGlossary.DocsLinkFor(null), Is.Null);
        });
    }

    [Test]
    public void A_term_without_a_docs_link_reports_no_link()
    {
        var linkless = new ExplorerTerm
        {
            Id = "x",
            Label = "X",
            Explanation = "An x.",
        };

        Assert.Multiple(() =>
        {
            Assert.That(linkless.DocsLink, Is.Null);
            Assert.That(linkless.HasDocsLink, Is.False);
        });
    }

    [Test]
    public void A_term_with_an_empty_docs_link_reports_no_link()
    {
        var linkless = new ExplorerTerm
        {
            Id = "x",
            Label = "X",
            Explanation = "An x.",
            DocsLink = string.Empty,
        };

        Assert.That(linkless.HasDocsLink, Is.False);
    }

    [Test]
    public void A_term_with_a_docs_link_reports_it()
    {
        Assert.That(ExplorerGlossary.Get(ExplorerTermIds.Shard).HasDocsLink, Is.True);
    }

    // ------------------------------------------------------------ ForLifecycle

    [Test]
    public void ForLifecycle_maps_each_state_the_cluster_reports()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerGlossary.ForLifecycle("Active")!.Id, Is.EqualTo(ExplorerTermIds.LifecycleActive));
            Assert.That(ExplorerGlossary.ForLifecycle("SoftDeleted")!.Id, Is.EqualTo(ExplorerTermIds.LifecycleSoftDeleted));
            Assert.That(ExplorerGlossary.ForLifecycle("Purging")!.Id, Is.EqualTo(ExplorerTermIds.LifecyclePurging));
        });
    }

    [Test]
    public void ForLifecycle_ignores_case()
    {
        Assert.That(ExplorerGlossary.ForLifecycle("active")!.Id, Is.EqualTo(ExplorerTermIds.LifecycleActive));
        Assert.That(ExplorerGlossary.ForLifecycle("softdeleted")!.Id, Is.EqualTo(ExplorerTermIds.LifecycleSoftDeleted));
    }

    [Test]
    public void ForLifecycle_null_empty_or_unknown_returns_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerGlossary.ForLifecycle(null), Is.Null);
            Assert.That(ExplorerGlossary.ForLifecycle(string.Empty), Is.Null);
            Assert.That(ExplorerGlossary.ForLifecycle("Exploded"), Is.Null);
        });
    }
}
