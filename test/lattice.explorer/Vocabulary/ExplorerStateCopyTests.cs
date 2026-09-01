using System.Reflection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Tests for the empty, error and loading copy that replaced "No trees found."
/// </summary>
/// <remarks>
/// The acceptance criterion under test is that the copy distinguishes "there is
/// nothing here" from "a scope is filtering it" from "you may not read it", and
/// says what to do next in each case. Assertions therefore check that the three
/// are actually different and that a refusal carries a remedy, not that a
/// particular sentence was written.
/// </remarks>
[TestFixture]
public class ExplorerStateCopyTests
{
    private static IReadOnlyList<ExplorerSubject> KnownSubjects() =>
        typeof(ExplorerSubjects)
            .GetProperties(BindingFlags.Public | BindingFlags.Static)
            .Where(property => property.PropertyType == typeof(ExplorerSubject))
            .Select(property => (ExplorerSubject)property.GetValue(null)!)
            .ToArray();

    private static ExplorerSubject Custom() => new()
    {
        Id = "widgets",
        Singular = "widget",
        Plural = "widgets",
        CollectionLabel = "Widgets",
    };

    // ------------------------------------------------------ the copy rule

    [Test]
    public void Every_subject_in_every_state_produces_a_headline_and_an_explanation()
    {
        Assert.Multiple(() =>
        {
            foreach (var subject in KnownSubjects())
            {
                foreach (var kind in Enum.GetValues<ExplorerStateKind>())
                {
                    var message = ExplorerStateCopy.For(subject, kind);
                    Assert.That(message.Headline, Is.Not.Empty, subject.Id + "/" + kind);
                    Assert.That(message.Explanation, Is.Not.Empty, subject.Id + "/" + kind);
                    Assert.That(message.Kind, Is.EqualTo(kind), subject.Id + "/" + kind);
                }
            }
        });
    }

    [Test]
    public void Every_state_a_user_can_act_on_carries_a_remedy()
    {
        var actionable = Enum.GetValues<ExplorerStateKind>()
            .Where(kind => kind != ExplorerStateKind.Loading);

        Assert.Multiple(() =>
        {
            foreach (var subject in KnownSubjects())
            {
                foreach (var kind in actionable)
                {
                    Assert.That(
                        ExplorerStateCopy.For(subject, kind).Remedy,
                        Is.Not.Null.And.Not.Empty,
                        subject.Id + "/" + kind + " must say what to do next");
                }
            }
        });
    }

    [Test]
    public void A_load_in_flight_suggests_nothing_to_do()
    {
        Assert.That(ExplorerStateCopy.Loading(ExplorerSubjects.Trees).Remedy, Is.Null);
    }

    [Test]
    public void Nothing_here_scoped_out_and_not_permitted_read_differently()
    {
        var empty = ExplorerStateCopy.Empty(ExplorerSubjects.Trees);
        var scoped = ExplorerStateCopy.ScopedOut(ExplorerSubjects.Trees);
        var denied = ExplorerStateCopy.NotPermitted(ExplorerSubjects.Trees);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Headline, Is.Not.EqualTo(scoped.Headline));
            Assert.That(scoped.Headline, Is.Not.EqualTo(denied.Headline));
            Assert.That(empty.Headline, Is.Not.EqualTo(denied.Headline));
            Assert.That(empty.Explanation, Is.Not.EqualTo(scoped.Explanation));
            Assert.That(scoped.Explanation, Is.Not.EqualTo(denied.Explanation));
            Assert.That(empty.Explanation, Is.Not.EqualTo(denied.Explanation));
        });
    }

    [Test]
    public void An_empty_list_says_it_is_not_being_filtered_or_withheld()
    {
        var empty = ExplorerStateCopy.Empty(ExplorerSubjects.Trees);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Explanation, Does.Contain("hidden"));
            Assert.That(empty.Explanation, Does.Contain("filtered"));
            Assert.That(empty.IsDenial, Is.False);
        });
    }

    [Test]
    public void A_refusal_is_rendered_in_the_denial_tone_and_an_absence_is_not()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerStateCopy.NotPermitted(ExplorerSubjects.Trees).IsDenial, Is.True);
            Assert.That(ExplorerStateCopy.SignInRequired(ExplorerSubjects.Trees).IsDenial, Is.True);
            Assert.That(ExplorerStateCopy.Unavailable(ExplorerSubjects.Trees).IsDenial, Is.False,
                "nothing is being withheld from the caller when a cluster simply does not run the feature");
            Assert.That(ExplorerStateCopy.Empty(ExplorerSubjects.Trees).IsDenial, Is.False);
            Assert.That(ExplorerStateCopy.Failed(ExplorerSubjects.Trees).IsDenial, Is.False);
        });
    }

    [Test]
    public void Only_a_load_in_flight_reports_itself_busy()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerStateCopy.Loading(ExplorerSubjects.Trees).IsBusy, Is.True);
            Assert.That(ExplorerStateCopy.Empty(ExplorerSubjects.Trees).IsBusy, Is.False);
        });
    }

    [Test]
    public void The_copy_names_what_the_surface_lists()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerStateCopy.Empty(ExplorerSubjects.Trees).Headline, Does.Contain("trees"));
            Assert.That(ExplorerStateCopy.Empty(ExplorerSubjects.Views).Headline, Does.Contain("views"));
            Assert.That(ExplorerStateCopy.Empty(ExplorerSubjects.TagIndexes).Headline, Does.Contain("tag indexes"));
        });
    }

    [Test]
    public void The_remedy_label_is_the_settled_wording()
    {
        Assert.That(
            ExplorerStateCopy.Empty(ExplorerSubjects.Trees).RemedyLabel,
            Is.EqualTo(ExplorerVocabulary.RemedyLabel));
    }

    // -------------------------------------------------------------------- For

    [Test]
    public void For_returns_the_same_pre_built_message_every_time()
    {
        // The table is frozen and built once; a per-call composition would put a
        // string allocation on a render path.
        Assert.That(
            ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.Empty),
            Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.Empty)));
    }

    [Test]
    public void For_composes_for_a_subject_a_consumer_declared_itself()
    {
        var message = ExplorerStateCopy.For(Custom(), ExplorerStateKind.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(message.Headline, Does.Contain("widgets"));
            Assert.That(message.Remedy, Does.Contain("widget"));
        });
    }

    [Test]
    public void For_an_undeclared_state_value_falls_back_to_the_failure_wording()
    {
        var message = ExplorerStateCopy.For(ExplorerSubjects.Trees, (ExplorerStateKind)99);

        Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.Failed));
    }

    [Test]
    public void Every_method_rejects_the_uninitialised_subject()
    {
        var empty = default(ExplorerSubject);

        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerStateCopy.For(empty, ExplorerStateKind.Empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.Loading(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.Empty(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.ScopedOut(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.ScopedOut(empty, "acme"), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.NotPermitted(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.NotPermitted(empty, "trees.read"), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.SignInRequired(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.Unavailable(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.Failed(empty), Throws.ArgumentException);
            Assert.That(() => ExplorerStateCopy.Failed(empty, "boom"), Throws.ArgumentException);
        });
    }

    // ------------------------------------------------------------- ScopedOut

    [Test]
    public void ScopedOut_names_the_tenant_when_the_caller_knows_it()
    {
        var message = ExplorerStateCopy.ScopedOut(ExplorerSubjects.Trees, "acme");

        Assert.Multiple(() =>
        {
            Assert.That(message.Explanation, Does.Contain("acme"));
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.ScopedOut));
            Assert.That(message.ActionLabel, Is.EqualTo(ExplorerVocabulary.ClearScopeAction));
        });
    }

    [Test]
    public void ScopedOut_without_a_tenant_uses_the_pre_built_wording()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerStateCopy.ScopedOut(ExplorerSubjects.Trees),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.ScopedOut)));
            Assert.That(
                ExplorerStateCopy.ScopedOut(ExplorerSubjects.Trees, string.Empty),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.ScopedOut)));
        });
    }

    [Test]
    public void ScopedOut_points_at_the_active_tenant_term()
    {
        Assert.That(
            ExplorerStateCopy.ScopedOut(ExplorerSubjects.Trees).TermId,
            Is.EqualTo(ExplorerTermIds.ActiveTenant));
    }

    // ---------------------------------------------------------- NotPermitted

    [Test]
    public void NotPermitted_names_the_grant_when_the_gate_knows_it()
    {
        var message = ExplorerStateCopy.NotPermitted(ExplorerSubjects.Backups, "backups.read");

        Assert.Multiple(() =>
        {
            Assert.That(message.Remedy, Does.Contain("backups.read"));
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.NotPermitted));
        });
    }

    [Test]
    public void NotPermitted_without_a_grant_uses_the_pre_built_wording()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerStateCopy.NotPermitted(ExplorerSubjects.Backups),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Backups, ExplorerStateKind.NotPermitted)));
            Assert.That(
                ExplorerStateCopy.NotPermitted(ExplorerSubjects.Backups, string.Empty),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Backups, ExplorerStateKind.NotPermitted)));
        });
    }

    [Test]
    public void NotPermitted_says_the_list_is_not_merely_empty()
    {
        Assert.That(
            ExplorerStateCopy.NotPermitted(ExplorerSubjects.Trees).Explanation,
            Does.Contain("not an empty list"));
    }

    // ---------------------------------------------------- SignInRequired etc.

    [Test]
    public void SignInRequired_offers_the_sign_in_action()
    {
        var message = ExplorerStateCopy.SignInRequired(ExplorerSubjects.TelemetrySignals);

        Assert.Multiple(() =>
        {
            Assert.That(message.ActionLabel, Is.EqualTo(ExplorerVocabulary.SignInAction));
            Assert.That(message.TermId, Is.EqualTo(ExplorerTermIds.SignInRequired));
        });
    }

    [Test]
    public void Unavailable_points_at_the_cluster_rather_than_the_account()
    {
        var message = ExplorerStateCopy.Unavailable(ExplorerSubjects.TelemetrySignals);

        Assert.Multiple(() =>
        {
            Assert.That(message.Explanation, Does.Contain("cluster"));
            Assert.That(message.TermId, Is.EqualTo(ExplorerTermIds.NotAvailableHere));
        });
    }

    // ----------------------------------------------------------------- Failed

    [Test]
    public void Failed_quotes_what_went_wrong_when_there_is_something_to_quote()
    {
        var message = ExplorerStateCopy.Failed(ExplorerSubjects.Trees, "the channel closed");

        Assert.Multiple(() =>
        {
            Assert.That(message.Explanation, Does.Contain("the channel closed"));
            Assert.That(message.ActionLabel, Is.EqualTo(ExplorerVocabulary.RetryAction));
        });
    }

    [Test]
    public void Failed_without_detail_uses_the_pre_built_wording()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerStateCopy.Failed(ExplorerSubjects.Trees),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.Failed)));
            Assert.That(
                ExplorerStateCopy.Failed(ExplorerSubjects.Trees, string.Empty),
                Is.SameAs(ExplorerStateCopy.For(ExplorerSubjects.Trees, ExplorerStateKind.Failed)));
        });
    }

    // --------------------------------------------------------------- subjects

    [Test]
    public void Every_declared_subject_is_completely_filled_in()
    {
        Assert.Multiple(() =>
        {
            foreach (var subject in KnownSubjects())
            {
                Assert.That(subject.Id, Is.Not.Empty);
                Assert.That(subject.Singular, Is.Not.Empty, subject.Id);
                Assert.That(subject.Plural, Is.Not.Empty, subject.Id);
                Assert.That(subject.CollectionLabel, Is.Not.Empty, subject.Id);
                Assert.That(subject.IsEmpty, Is.False, subject.Id);
            }
        });
    }

    [Test]
    public void Every_subject_id_is_unique()
    {
        var ids = KnownSubjects().Select(subject => subject.Id).ToArray();

        Assert.That(ids, Is.Unique);
    }

    [Test]
    public void Every_subject_term_id_resolves_in_the_glossary()
    {
        var dangling = KnownSubjects()
            .Where(subject => subject.TermId is not null && !ExplorerGlossary.Contains(subject.TermId))
            .Select(subject => subject.Id)
            .ToArray();

        Assert.That(dangling, Is.Empty);
    }

    [Test]
    public void Every_subject_docs_link_is_a_declared_constant()
    {
        var declared = typeof(ExplorerDocsLinks)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(field => field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

        var undeclared = KnownSubjects()
            .Where(subject => subject.DocsLink is not null && !declared.Contains(subject.DocsLink))
            .Select(subject => subject.Id)
            .ToArray();

        Assert.That(undeclared, Is.Empty);
    }

    [Test]
    public void The_uninitialised_subject_reports_itself_empty()
    {
        Assert.That(default(ExplorerSubject).IsEmpty, Is.True);
    }

    [Test]
    public void ForCatalogKind_maps_each_catalog_kind()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerSubjects.ForCatalogKind(CatalogKind.Trees), Is.EqualTo(ExplorerSubjects.Trees));
            Assert.That(ExplorerSubjects.ForCatalogKind(CatalogKind.Views), Is.EqualTo(ExplorerSubjects.Views));
            Assert.That(ExplorerSubjects.ForCatalogKind(CatalogKind.TagIndexes), Is.EqualTo(ExplorerSubjects.TagIndexes));
        });
    }

    [Test]
    public void ForCatalogKind_falls_back_to_trees_for_an_unnamed_kind()
    {
        Assert.That(ExplorerSubjects.ForCatalogKind((CatalogKind)42), Is.EqualTo(ExplorerSubjects.Trees));
    }
}
