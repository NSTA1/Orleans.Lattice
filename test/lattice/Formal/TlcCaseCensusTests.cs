using NUnit.Framework.Internal;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// The TLC case census (<see cref="TlcCaseCensus"/>, contract C2 of #5148).
/// Toolchain-free: it builds the TLC cases with NUnit and reads <c>spec/</c>,
/// so it runs in the <c>rest</c> shard. Running
/// <see cref="The_census_is_written_to_the_file_LATTICE_TLC_CENSUS_names"/>
/// with <c>LATTICE_TLC_CENSUS</c> set to a fully qualified path exports the
/// census there.
/// </summary>
[TestFixture]
public sealed class TlcCaseCensusTests
{
    [Test]
    public void The_census_is_written_to_the_file_LATTICE_TLC_CENSUS_names()
    {
        var expected = TlcCaseCensus.Build();
        var requested = TlcCaseCensus.ExportIfRequested();
        var categoriesPath = TlcCaseCensus.ExportCategoriesIfRequested();
        if (categoriesPath is not null)
        {
            Assert.That(TlcCaseCensus.Read(categoriesPath).Keys,
                Is.EquivalentTo(expected.Values.SelectMany(names => names)),
                "the category export covers exactly the census cases");
        }

        var path = requested ?? Path.Combine(Path.GetTempPath(), $"tlc-census-{Guid.NewGuid():N}.json");

        try
        {
            if (requested is null)
            {
                TlcCaseCensus.Export(path);
            }

            var written = TlcCaseCensus.Read(path);
            Assert.Multiple(() =>
            {
                Assert.That(written.Keys, Is.EqualTo(expected.Keys), "keys are written in ordinal order");
                foreach (var (module, names) in expected)
                    Assert.That(written[module], Is.EqualTo(names), module);
            });
        }
        finally
        {
            if (requested is null)
            {
                File.Delete(path);
            }
        }
    }

    [Test]
    public void Each_census_case_reports_its_module_category_beside_its_fixture_category()
    {
        var census = TlcCaseCensus.Build();
        var categories = TlcCaseCensus.BuildCategories();

        Assert.Multiple(() =>
        {
            Assert.That(categories.Keys, Is.EquivalentTo(census.Values.SelectMany(names => names)));
            foreach (var (module, names) in census)
            {
                foreach (var name in names)
                {
                    Assert.That(categories[name], Does.Contain(TlcCiShard.ModuleCategoryPrefix + module), name);
                    Assert.That(categories[name], Does.Contain("Tlc"), name + " carries its fixture's category");
                    Assert.That(categories[name], Is.Ordered.Using((IComparer<string>)StringComparer.Ordinal), name);
                }
            }
        });
    }

    [Test]
    public void The_census_lists_every_TLC_case_once_under_its_module()
    {
        var census = TlcCaseCensus.Build();
        var modules = SpecModuleCatalogue.Repository();
        var prefix = typeof(TlcModelCheckTests).FullName + ".";

        Assert.Multiple(() =>
        {
            Assert.That(census.Keys, Is.EquivalentTo(modules.Select(module => module.Name)));
            Assert.That(census.Values.Sum(names => names.Count), Is.EqualTo(TlcCaseCensus.Cases().Count));
            Assert.That(census.Values.SelectMany(names => names), Is.Unique);
            foreach (var module in modules)
            {
                Assert.That(
                    census[module.Name],
                    Has.Count.EqualTo(1 + module.Manifest.Variants.Count + module.LoadMutations().Count),
                    $"{module.Name}: one base, one case per variant and one per mutation");
                Assert.That(census[module.Name], Is.All.StartsWith(prefix), module.Name);
            }
        });
    }

    [Test]
    public void Every_built_TLC_case_is_censused_under_the_module_it_checks()
    {
        var cases = TlcCaseCensus.Cases();

        Assert.Multiple(() =>
        {
            Assert.That(cases, Is.Not.Empty);
            foreach (var test in cases)
            {
                Assert.That(test, Is.InstanceOf<TestMethod>());
                var module = (SpecModule)((TestMethod)test).Arguments[0]!;
                Assert.That(TlcCaseCensus.ModuleOf(test), Is.EqualTo(module.Name), test.FullName);
            }
        });
    }

    [TestCase("ShardOwnershipFold")]
    [TestCase("ReplayPermits")]
    public void A_module_census_holds_its_base_variant_and_mutation_cases_by_their_reported_names(string name)
    {
        var module = SpecModuleCatalogue.Repository().Single(module => module.Name == name);
        var names = TlcCaseCensus.Build()[name];
        var fixture = typeof(TlcModelCheckTests).FullName;
        var expected = new[] { $"{fixture}.{nameof(TlcModelCheckTests.The_base_specification_holds)}({name})" }
            .Concat(module.Manifest.Variants.Keys.Select(variant =>
                $"{fixture}.{nameof(TlcModelCheckTests.Each_variant_configuration_holds)}({name}, {variant})"))
            .Concat(module.LoadMutations().Select(mutation =>
                $"{fixture}.{nameof(TlcModelCheckTests.Each_property_fires_under_its_mutation_and_not_on_the_base)}({name}, {mutation})"));

        Assert.That(names, Is.EquivalentTo(expected), "the names VSTest reports: NUnit joins argument display names with \", \"");
    }

    [Test]
    public void A_relative_census_path_is_refused()
    {
        Assert.Throws<ArgumentException>(() => TlcCaseCensus.Export("tlc-census.json"));
    }

    [Test]
    public void A_case_without_exactly_one_module_category_is_refused()
    {
        var method = typeof(TlcCaseCensusTests).GetMethod(nameof(A_relative_census_path_is_refused))!;
        var untagged = new TestMethod(new MethodWrapper(typeof(TlcCaseCensusTests), method));
        var doubled = new TestMethod(new MethodWrapper(typeof(TlcCaseCensusTests), method));
        doubled.Properties.Add(PropertyNames.Category, "TlcModule_A");
        doubled.Properties.Add(PropertyNames.Category, "TlcModule_B");

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidOperationException>(() => TlcCaseCensus.ModuleOf(untagged));
            Assert.Throws<InvalidOperationException>(() => TlcCaseCensus.ModuleOf(doubled));
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => TlcCaseCensus.ModuleOf(null!));
            Assert.Throws<ArgumentNullException>(() => TlcCaseCensus.Serialize(null!));
            Assert.Throws<ArgumentNullException>(() => TlcCaseCensus.Read(null!));
            Assert.Throws<ArgumentNullException>(() => TlcCaseCensus.Export(null!));
        });
    }
}
