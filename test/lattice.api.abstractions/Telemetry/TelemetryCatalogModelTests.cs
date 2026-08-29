using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises the named-query catalogue contract: the catalogue's lookup helpers and
/// empty singleton, the descriptor's parameter and instrument predicates, and the
/// server-side definition wrapper that carries the query template the client-facing
/// descriptor deliberately does not.
/// </summary>
[TestFixture]
public sealed class TelemetryCatalogModelTests
{
    private static TelemetryInstrumentReference ShardWrites() => new(
        "orleans.lattice.shard.writes",
        "orleans.lattice",
        "{op}",
        TelemetryMeasurementSemantic.PerOperation);

    private static TelemetryInstrumentReference ShardReads() => new(
        "orleans.lattice.shard.reads",
        "orleans.lattice",
        "{op}",
        TelemetryMeasurementSemantic.PerOperation);

    private static TelemetryQueryDescriptor Descriptor(string queryId = "tree.write.ops") => new()
    {
        QueryId = queryId,
        Title = "Per-tree write operations/s",
        Description = "Shard-root write operations per second. Bulk load contributes one operation per shard.",
        Unit = "{op}/s",
        Kind = TelemetryQueryKind.Range,
        Semantic = TelemetryMeasurementSemantic.PerOperation,
        Parameters = TelemetryQueryParameters.TimeRange
            | TelemetryQueryParameters.Step
            | TelemetryQueryParameters.TreeFilter,
        Bounds = new TelemetryQueryBounds { MinStep = TimeSpan.FromSeconds(15) },
        Instruments = [ShardWrites()],
    };

    [Test]
    public void Instrument_reference_preserves_every_declared_field()
    {
        var instrument = ShardWrites();

        Assert.Multiple(() =>
        {
            Assert.That(instrument.Name, Is.EqualTo("orleans.lattice.shard.writes"));
            Assert.That(instrument.Meter, Is.EqualTo("orleans.lattice"));
            Assert.That(instrument.Unit, Is.EqualTo("{op}"));
            Assert.That(instrument.Semantic, Is.EqualTo(TelemetryMeasurementSemantic.PerOperation));
        });
    }

    [Test]
    public void Instrument_reference_rejects_a_null_name()
    {
        Assert.That(
            () => new TelemetryInstrumentReference(null!, "m", "{op}", TelemetryMeasurementSemantic.PerOperation),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Instrument_reference_rejects_a_null_meter()
    {
        Assert.That(
            () => new TelemetryInstrumentReference("n", null!, "{op}", TelemetryMeasurementSemantic.PerOperation),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Instrument_reference_rejects_a_null_unit()
    {
        Assert.That(
            () => new TelemetryInstrumentReference("n", "m", null!, TelemetryMeasurementSemantic.PerOperation),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Descriptor_accepts_only_the_parameters_it_declares()
    {
        var descriptor = Descriptor();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Accepts(TelemetryQueryParameters.TimeRange), Is.True);
            Assert.That(descriptor.Accepts(TelemetryQueryParameters.Step), Is.True);
            Assert.That(descriptor.Accepts(TelemetryQueryParameters.TreeFilter), Is.True);
            Assert.That(
                descriptor.Accepts(TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step),
                Is.True);
        });
    }

    [Test]
    public void Descriptor_rejects_a_parameter_it_does_not_declare()
    {
        var descriptor = Descriptor() with { Parameters = TelemetryQueryParameters.TimeRange };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Accepts(TelemetryQueryParameters.TreeFilter), Is.False);
            Assert.That(
                descriptor.Accepts(TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.TreeFilter),
                Is.False,
                "A combined test must require every flag, not merely one of them.");
        });
    }

    [Test]
    public void Descriptor_never_accepts_the_empty_parameter_set()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Descriptor().Accepts(TelemetryQueryParameters.None), Is.False);
            Assert.That(
                (Descriptor() with { Parameters = TelemetryQueryParameters.None })
                    .Accepts(TelemetryQueryParameters.None),
                Is.False);
        });
    }

    [Test]
    public void Descriptor_reports_the_instruments_it_reads()
    {
        var descriptor = Descriptor() with { Instruments = [ShardWrites(), ShardReads()] };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.ReadsInstrument("orleans.lattice.shard.writes"), Is.True);
            Assert.That(descriptor.ReadsInstrument("orleans.lattice.shard.reads"), Is.True);
            Assert.That(descriptor.ReadsInstrument("orleans.lattice.shard.splits"), Is.False);
        });
    }

    [Test]
    public void Descriptor_matches_an_instrument_name_ordinally()
    {
        Assert.That(Descriptor().ReadsInstrument("ORLEANS.LATTICE.SHARD.WRITES"), Is.False);
    }

    [Test]
    public void Descriptor_reads_nothing_when_it_declares_no_instrument()
    {
        var descriptor = Descriptor() with { Instruments = [] };

        Assert.That(descriptor.ReadsInstrument("orleans.lattice.shard.writes"), Is.False);
    }

    [Test]
    public void Descriptor_rejects_a_null_instrument_name()
    {
        Assert.That(() => Descriptor().ReadsInstrument(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Descriptor_pairs_its_declared_semantic_with_the_instrument_it_reads()
    {
        var descriptor = Descriptor();

        Assert.That(descriptor.Semantic, Is.EqualTo(descriptor.Instruments[0].Semantic),
            "A per-operation panel over a per-operation instrument is the honest pairing this "
            + "contract exists to make checkable.");
    }

    [Test]
    public void Definition_carries_the_query_template_and_surfaces_the_descriptor_id()
    {
        var definition = new TelemetryQueryDefinition
        {
            Descriptor = Descriptor(),
            QueryTemplate = "sum by (tree) (rate(orleans_lattice_shard_writes_total[$__rate_interval]))",
        };

        Assert.Multiple(() =>
        {
            Assert.That(definition.QueryId, Is.EqualTo("tree.write.ops"));
            Assert.That(definition.QueryTemplate, Does.Contain("orleans_lattice_shard_writes_total"));
            Assert.That(definition.Descriptor.Title, Is.EqualTo("Per-tree write operations/s"));
            Assert.That(definition.Descriptor.Unit, Is.EqualTo("{op}/s"));
        });
    }

    [Test]
    public void Empty_catalogue_is_a_cached_singleton_with_no_entries()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryQueryCatalog.Empty.Count, Is.EqualTo(0));
            Assert.That(TelemetryQueryCatalog.Empty.Version, Is.EqualTo(0));
            Assert.That(TelemetryQueryCatalog.Empty.Queries, Is.Empty);
            Assert.That(TelemetryQueryCatalog.Empty, Is.SameAs(TelemetryQueryCatalog.Empty),
                "The fail-closed path must not allocate a new catalogue per call.");
        });
    }

    [Test]
    public void Empty_catalogue_resolves_no_query()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryQueryCatalog.Empty.TryGetQuery("tree.write.ops", out var descriptor), Is.False);
            Assert.That(descriptor, Is.Null);
            Assert.That(TelemetryQueryCatalog.Empty.Contains("tree.write.ops"), Is.False);
        });
    }

    [Test]
    public void Catalogue_resolves_a_declared_query_by_id()
    {
        var catalog = new TelemetryQueryCatalog
        {
            Version = 3,
            Queries = [Descriptor("a.first"), Descriptor("b.second")],
        };

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Version, Is.EqualTo(3));
            Assert.That(catalog.Count, Is.EqualTo(2));
            Assert.That(catalog.TryGetQuery("b.second", out var descriptor), Is.True);
            Assert.That(descriptor!.QueryId, Is.EqualTo("b.second"));
            Assert.That(catalog.Contains("a.first"), Is.True);
        });
    }

    [Test]
    public void Catalogue_does_not_resolve_an_undeclared_query()
    {
        var catalog = new TelemetryQueryCatalog { Version = 1, Queries = [Descriptor("a.first")] };

        Assert.Multiple(() =>
        {
            Assert.That(catalog.TryGetQuery("nope", out var descriptor), Is.False);
            Assert.That(descriptor, Is.Null);
            Assert.That(catalog.Contains("nope"), Is.False);
        });
    }

    [Test]
    public void Catalogue_matches_a_query_id_ordinally()
    {
        var catalog = new TelemetryQueryCatalog { Version = 1, Queries = [Descriptor("tree.write.ops")] };

        Assert.That(catalog.Contains("Tree.Write.Ops"), Is.False);
    }

    [Test]
    public void Catalogue_rejects_a_null_query_id()
    {
        var catalog = new TelemetryQueryCatalog { Version = 1, Queries = [Descriptor()] };

        Assert.Multiple(() =>
        {
            Assert.That(() => catalog.TryGetQuery(null!, out _), Throws.ArgumentNullException);
            Assert.That(() => catalog.Contains(null!), Throws.ArgumentNullException);
        });
    }
}
