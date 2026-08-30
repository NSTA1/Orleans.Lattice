using Microsoft.AspNetCore.Components;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.TagIndex;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The compact reflow, asserted at render level on the per-selection surfaces
/// that ship a <c>LatticeAdaptiveTable</c>: Data's key list, the dead-letter
/// queue, the per-shard hotness table, and a tag index's member list
/// (issue #1782).
/// </summary>
/// <remarks>
/// Each surface is driven by a stubbed domain supplied up front and rendered
/// twice, once per breakpoint, so no test here depends on a clock, an ordering,
/// or a background task. The four assertions come from
/// <see cref="AdaptiveReflowAssert"/> so no surface can hold itself to a weaker
/// version of them.
/// </remarks>
[TestFixture]
public sealed class CompactReflowRenderTests
{
    private static readonly DateTimeOffset When = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    // ---- Data: DataRowList ------------------------------------------------

    private const string DataSurface = "DataRowList";

    [Test]
    public async Task The_data_key_list_renders_a_table_at_expanded()
    {
        var html = await RenderDataRowsAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, DataSurface);
    }

    [Test]
    public async Task The_data_key_list_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderDataRowsAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, DataSurface);
    }

    [Test]
    public async Task The_data_key_list_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderDataRowsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDataRowsAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Key", DataSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Size", DataSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Flags", DataSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "orders/1", DataSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Size", "2,048", DataSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Flags", "tomb", DataSurface);
        });
    }

    [Test]
    public async Task The_data_key_selection_control_survives_the_reflow()
    {
        var expanded = await RenderDataRowsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDataRowsAsync(LatticeBreakpoint.Compact);

        // Selection is a button in the primary cell rather than a row click
        // handler, which is what lets it survive into the card title at all.
        AdaptiveReflowAssert.ControlSurvivesTheReflow(
            expanded, compact, "lx-data-key", DataSurface);
    }

    private static Task<string> RenderDataRowsAsync(LatticeBreakpoint breakpoint) =>
        SelectionViewRenderHarness.RenderComponentAsync<DataRowList>(
            new Dictionary<string, object?>
            {
                ["Entries"] = new[]
                {
                    new DataEntry
                    {
                        Key = "orders/1",
                        ValueLength = 2048,
                        IsTombstone = true,
                        CrdtShape = "gcounter",
                    },
                },
                ["SelectedKey"] = "orders/1",
                ["OnSelect"] = EventCallback.Factory.Create<string>(new object(), _ => { }),
            },
            breakpoint);

    // ---- DeadLetter: DeadLetterTab ----------------------------------------

    private const string DeadLetterSurface = "DeadLetterTab";

    [Test]
    public async Task The_dead_letter_queue_renders_a_table_at_expanded()
    {
        var html = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, DeadLetterSurface);
    }

    [Test]
    public async Task The_dead_letter_queue_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, DeadLetterSurface);
    }

    [Test]
    public async Task The_dead_letter_queue_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Key", DeadLetterSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Timestamp (UTC)", DeadLetterSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Source", DeadLetterSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Reason", DeadLetterSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Size", DeadLetterSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "orders/9", DeadLetterSurface);
            AdaptiveReflowAssert.CardShowsField(
                compact, "Timestamp (UTC)", "2026-03-04 05:06:07Z", DeadLetterSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Source", "Replication", DeadLetterSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Reason", "schema rule 2", DeadLetterSurface);

            // Size opts out of the card by declaration, so the omission has to
            // stay distinguishable from a column that started vanishing.
            AdaptiveReflowAssert.CardOmitsField(compact, "Size", DeadLetterSurface);
        });
    }

    [Test]
    public async Task The_dead_letter_selection_control_survives_the_reflow()
    {
        var expanded = await RenderDeadLettersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderDeadLettersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, "lx-deadletter-key", DeadLetterSurface);

            // The queue's own refresh control is the other thing an operator on
            // a phone would be stranded without.
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Refresh<", DeadLetterSurface);
        });
    }

    private static Task<string> RenderDeadLettersAsync(LatticeBreakpoint breakpoint)
    {
        var surface = Substitute.For<IDeadLetterSurface>();
        surface.CountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(1));
        surface
            .ListAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DeadLetterPage
            {
                Entries = new[]
                {
                    new DeadLetterEntry
                    {
                        Key = "orders/9",
                        Reason = "schema rule 2",
                        Source = DeadLetterSource.Replication,
                        TimestampUtc = When,
                        ValueByteLength = 512,
                    },
                },
            }));

        return SelectionViewRenderHarness.RenderAsync<DeadLetterTab, IDeadLetterSurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            breakpoint);
    }

    // ---- Metrics: MetricsTab ----------------------------------------------

    private const string MetricsSurface = "MetricsTab";

    [Test]
    public async Task The_shard_hotness_table_renders_a_table_at_expanded()
    {
        var html = await RenderMetricsAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, MetricsSurface);
    }

    [Test]
    public async Task The_shard_hotness_table_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderMetricsAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, MetricsSurface);
    }

    [Test]
    public async Task The_shard_hotness_table_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderMetricsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderMetricsAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Shard", MetricsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Ops/sec", MetricsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Live keys", MetricsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Splitting", MetricsSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "7", MetricsSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Ops/sec", "12.5", MetricsSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Live keys", "4,096", MetricsSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Splitting", "yes", MetricsSurface);
        });
    }

    [Test]
    public async Task The_metrics_refresh_control_survives_the_reflow()
    {
        var expanded = await RenderMetricsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderMetricsAsync(LatticeBreakpoint.Compact);

        // The hotness table is read-only, so the control an operator would be
        // stranded without is the surface's own refresh rather than a row
        // action.
        AdaptiveReflowAssert.ControlSurvivesTheReflow(
            expanded, compact, "lx-metrics-refresh", MetricsSurface);
    }

    private static Task<string> RenderMetricsAsync(LatticeBreakpoint breakpoint)
    {
        var surface = Substitute.For<IMetricsSurface>();
        surface.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<TreeMetrics?>(new TreeMetrics
            {
                TreeId = "orders",
                ShardCount = 1,
                LiveKeys = 4096,
                ShardHotness = new[]
                {
                    new ShardHotness
                    {
                        ShardIndex = 7,
                        OpsPerSecond = 12.5,
                        LiveKeys = 4096,
                        SplitInProgress = true,
                    },
                },
            }));

        return SelectionViewRenderHarness.RenderAsync<MetricsTab, IMetricsSurface>(
            surface,
            SelectionViewRenderHarness.Tree(),
            breakpoint);
    }

    // ---- TagIndex: TagIndexMembers ----------------------------------------

    private const string TagIndexSurface = "TagIndexMembers";

    [Test]
    public async Task The_tag_member_list_renders_a_table_at_expanded()
    {
        var html = await RenderTagMembersAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, TagIndexSurface);
    }

    [Test]
    public async Task The_tag_member_list_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderTagMembersAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, TagIndexSurface);
    }

    [Test]
    public async Task The_tag_member_list_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderTagMembersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderTagMembersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Key", TagIndexSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Tree", TagIndexSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "orders/42", TagIndexSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Tree", "orders", TagIndexSurface);
        });
    }

    [Test]
    public async Task The_tag_member_open_control_survives_the_reflow()
    {
        var expanded = await RenderTagMembersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderTagMembersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, "lx-tagindex-memberlink", TagIndexSurface);

            // Paging is how an operator reaches the rest of a large tag, so it
            // has to survive the reflow beside the row action.
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Next<", TagIndexSurface);
        });
    }

    private static Task<string> RenderTagMembersAsync(LatticeBreakpoint breakpoint) =>
        SelectionViewRenderHarness.RenderComponentAsync<TagIndexMembers>(
            new Dictionary<string, object?>
            {
                ["Tag"] = "eu-west",
                ["Members"] = new[]
                {
                    new TagMemberRow { TreeId = "orders", Key = "orders/42" },
                },
                ["HasLoadedPage"] = true,
                ["CanGoNext"] = true,
            },
            breakpoint);
}
