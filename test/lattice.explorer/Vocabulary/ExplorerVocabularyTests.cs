using System.Reflection;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Vocabulary;

/// <summary>
/// Tests for the settled words the interface uses, and in particular for the
/// pair the issue exists to disambiguate.
/// </summary>
[TestFixture]
public class ExplorerVocabularyTests
{
    private static IReadOnlyList<(string Name, string Value)> Constants(Type type) =>
        type.GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(field => field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string))
            .Select(field => (field.Name, (string)field.GetRawConstantValue()!))
            .ToArray();

    [Test]
    public void Every_declared_word_is_non_empty()
    {
        Assert.Multiple(() =>
        {
            foreach (var (name, value) in Constants(typeof(ExplorerVocabulary)))
            {
                Assert.That(value, Is.Not.Empty, name);
            }
        });
    }

    [Test]
    public void Tenant_administration_and_my_tenant_cannot_be_confused_for_each_other()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerVocabulary.TenantAdministrationArea,
                Is.Not.EqualTo(ExplorerVocabulary.MyTenantArea));
            Assert.That(
                ExplorerVocabulary.TenantAdministrationAreaShort,
                Is.Not.EqualTo(ExplorerVocabulary.MyTenantArea));
            Assert.That(
                ExplorerVocabulary.TenantAdministrationArea,
                Is.Not.EqualTo("Tenants"),
                "the bare plural was the ambiguous half of the pair and is not used");
        });
    }

    [Test]
    public void Both_halves_of_the_tenancy_pair_are_explained_by_the_glossary()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerGlossary.Get(ExplorerTermIds.TenantAdministrationArea).Label,
                Is.EqualTo(ExplorerVocabulary.TenantAdministrationArea));
            Assert.That(
                ExplorerGlossary.Get(ExplorerTermIds.MyTenantArea).Label,
                Is.EqualTo(ExplorerVocabulary.MyTenantArea));
        });
    }

    [Test]
    public void Every_area_has_a_distinct_name()
    {
        string[] areas =
        [
            ExplorerVocabulary.ExploreArea,
            ExplorerVocabulary.BackupsArea,
            ExplorerVocabulary.AccessArea,
            ExplorerVocabulary.TenantAdministrationArea,
            ExplorerVocabulary.MyTenantArea,
            ExplorerVocabulary.TelemetryArea,
        ];

        Assert.That(areas, Is.Unique);
    }

    [Test]
    public void Every_catalog_kind_has_a_distinct_label_matching_its_glossary_term()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                new[]
                {
                    ExplorerVocabulary.TreesLabel,
                    ExplorerVocabulary.ViewsLabel,
                    ExplorerVocabulary.TagIndexesLabel,
                },
                Is.Unique);
            Assert.That(ExplorerGlossary.LabelFor(ExplorerTermIds.Trees), Is.EqualTo(ExplorerVocabulary.TreesLabel));
            Assert.That(ExplorerGlossary.LabelFor(ExplorerTermIds.Views), Is.EqualTo(ExplorerVocabulary.ViewsLabel));
            Assert.That(
                ExplorerGlossary.LabelFor(ExplorerTermIds.TagIndexes),
                Is.EqualTo(ExplorerVocabulary.TagIndexesLabel));
        });
    }

    [Test]
    public void The_views_long_label_says_what_the_short_one_abbreviates()
    {
        Assert.That(ExplorerVocabulary.ViewsLongLabel, Does.Contain("view"));
        Assert.That(ExplorerVocabulary.ViewsLongLabel, Is.Not.EqualTo(ExplorerVocabulary.ViewsLabel));
    }

    [Test]
    public void The_action_labels_are_settled_distinct_and_name_the_action()
    {
        string[] actions =
        [
            ExplorerVocabulary.ClearScopeAction,
            ExplorerVocabulary.RetryAction,
            ExplorerVocabulary.SignInAction,
        ];

        Assert.Multiple(() =>
        {
            Assert.That(actions, Is.Unique);
            Assert.That(ExplorerVocabulary.RetryAction, Is.EqualTo("Try again"));
            Assert.That(ExplorerVocabulary.SignInAction, Is.EqualTo("Sign in"));
            Assert.That(ExplorerVocabulary.ClearScopeAction, Does.Contain("tenant"));
            Assert.That(ExplorerVocabulary.AllTenantsLabel, Is.EqualTo("All tenants"));
            Assert.That(ExplorerVocabulary.CatalogLabel, Is.Not.Empty);
            Assert.That(ExplorerVocabulary.RemedyLabel, Does.EndWith(":"));
        });
    }

    [Test]
    public void The_active_tenant_label_names_a_concept_rather_than_shouting_a_noun()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerVocabulary.ActiveTenantLabel, Is.EqualTo("Active tenant"));
            Assert.That(
                ExplorerVocabulary.ActiveTenantLabel,
                Is.Not.EqualTo("TENANT"),
                "the header used to shout a bare noun that named no concept");
        });
    }

    [Test]
    public void FormatActiveTenant_names_the_tenant_in_force()
    {
        Assert.That(ExplorerVocabulary.FormatActiveTenant("acme"), Is.EqualTo("Active tenant: acme"));
    }

    [Test]
    public void FormatActiveTenant_accepts_an_empty_tenant_id()
    {
        Assert.That(ExplorerVocabulary.FormatActiveTenant(string.Empty), Is.EqualTo("Active tenant: "));
    }

    [Test]
    public void FormatActiveTenant_rejects_a_null_tenant_id()
    {
        Assert.That(() => ExplorerVocabulary.FormatActiveTenant(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_no_selection_prompt_names_all_three_catalog_kinds()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerVocabulary.NoSelectionHeadline, Is.Not.Empty);
            Assert.That(ExplorerVocabulary.NoSelectionExplanation, Does.Contain("tree"));
            Assert.That(ExplorerVocabulary.NoSelectionExplanation, Does.Contain("view"));
            Assert.That(
                ExplorerVocabulary.NoSelectionExplanation,
                Does.Contain("tag index"),
                "the old prompt named only two of the three kinds the catalog lists");
        });
    }

    [Test]
    public void Every_documentation_link_is_a_repository_relative_markdown_path()
    {
        Assert.Multiple(() =>
        {
            foreach (var (name, value) in Constants(typeof(ExplorerDocsLinks)))
            {
                Assert.That(value, Does.StartWith("docs/"), name);
                Assert.That(value, Does.EndWith(".md"), name);
                Assert.That(value, Does.Not.Contain("\\"), name);
            }
        });
    }

    [Test]
    public void Every_documentation_link_points_at_a_file_that_exists()
    {
        var root = RepositoryRoot();

        Assert.Multiple(() =>
        {
            foreach (var (name, value) in Constants(typeof(ExplorerDocsLinks)))
            {
                var path = Path.Combine(root, value.Replace('/', Path.DirectorySeparatorChar));
                Assert.That(File.Exists(path), Is.True, name + " points at a missing document: " + value);
            }
        });
    }

    // Walks up from the test assembly until the repository marker is found, so
    // the check does not depend on the working directory a runner happens to use.
    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Orleans.Lattice.slnx")))
        {
            directory = directory.Parent;
        }

        Assert.That(directory, Is.Not.Null, "could not locate the repository root from the test assembly");
        return directory!.FullName;
    }
}
