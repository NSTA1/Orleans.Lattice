using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Api.Apps;

internal sealed partial class LatticeAppCatalog
{
    /// <summary>How many sources' pages are fetched concurrently when a listing starts.</summary>
    internal const int MaxConcurrentFetches = 4;

    /// <summary>The most source pages one listing call fetches before it returns what it has.</summary>
    internal const int MaxFetchesPerCall = 64;

    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// The selected sources are read with bounded concurrency and merged by slug and then source key, so every
    /// source is assumed to list in ordinal slug order (the in-image source does). The continuation carries
    /// one cursor per source; a malformed or foreign continuation yields an empty final page.
    /// </para>
    /// <para>
    /// A row is joined with the tenant's install only when the install came from that row's source.
    /// <see cref="AvailableAppFilter.Installed"/> keeps joined rows, <see cref="AvailableAppFilter.Available"/>
    /// keeps rows whose slug has no live install in the tenant, and <see cref="AvailableAppFilter.Updates"/> keeps
    /// joined rows whose newest version has higher semantic-version precedence than the installed one. An entry
    /// a source cannot describe is skipped. A filter can make a page short; a short page may still carry a
    /// continuation.
    /// </para>
    /// </remarks>
    public async Task<AvailableAppPage> ListAvailableAsync(AvailableAppQuery query, CancellationToken cancellationToken = default)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(query);
            if (!Enum.IsDefined(query.Filter))
            {
                throw new ArgumentException("The available-app filter is not a defined value.", nameof(query));
            }

            if (query.SourceKey is not null && !AppSourceDescriptor.IsValidKey(query.SourceKey))
            {
                throw new ArgumentException("The source key is not a valid app source key.", nameof(query));
            }

            var pageSize = Math.Clamp(query.PageSize, 1, AvailableAppQuery.MaxPageSize);
            var tenant = await AppsFacadeAccess.ResolveTenantAsync(_tenants, cancellationToken).ConfigureAwait(false);
            await AppsFacadeAccess.AuthorizeInstallAsync(_gate, _membership, cancellationToken).ConfigureAwait(false);

            var selected = SelectSources(query.SourceKey);
            if (selected.Count == 0)
            {
                return new AvailableAppPage();
            }

            AppCatalogContinuation.SourceCursor[] start;
            if (query.Continuation is null)
            {
                start = new AppCatalogContinuation.SourceCursor[selected.Count];
                for (var i = 0; i < start.Length; i++)
                {
                    start[i] = new(selected[i].Descriptor.Key, null, 0, Done: false);
                }
            }
            else if (!AppCatalogContinuation.TryDecode(query.Continuation, selected, out start))
            {
                return new AvailableAppPage();
            }

            var installs = await ReadInstallsAsync(tenant, cancellationToken).ConfigureAwait(false);
            return await MergeAsync(selected, start, query, pageSize, tenant, installs, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (AppsControlExceptionSanitizer.TryRewrite(ex, ownApp: null, out var sanitized))
        {
            throw sanitized;
        }
    }

    private List<IAppCatalogSource> SelectSources(string? sourceKey)
    {
        var selected = new List<IAppCatalogSource>();
        foreach (var source in _sources.Sources)
        {
            var key = source.Descriptor.Key;
            if ((sourceKey is null || string.Equals(key, sourceKey, StringComparison.Ordinal))
                && _sources.TryGet(key, out var unique)
                && ReferenceEquals(unique, source))
            {
                selected.Add(source);
            }
        }

        return selected;
    }

    private async Task<Dictionary<AppSlug, AppRegistryRecord>> ReadInstallsAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        var installs = new Dictionary<AppSlug, AppRegistryRecord>();
        await foreach (var record in _registry.ListForTenantAsync(tenant, cancellationToken).ConfigureAwait(false))
        {
            if (record.Tenant == tenant && record.State != AppRegistryLifecycleState.Uninstalled)
            {
                installs[record.Slug] = record;
            }
        }

        return installs;
    }

    private async Task<AvailableAppPage> MergeAsync(
        List<IAppCatalogSource> sources,
        AppCatalogContinuation.SourceCursor[] cursors,
        AvailableAppQuery query,
        int pageSize,
        TenantId tenant,
        Dictionary<AppSlug, AppRegistryRecord> installs,
        CancellationToken cancellationToken)
    {
        var pages = new AppSourcePage?[sources.Count];
        var fetches = await FetchInitialAsync(sources, cursors, pages, query, pageSize, cancellationToken).ConfigureAwait(false);
        var apps = ImmutableArray.CreateBuilder<AvailableAppSummary>(pageSize);
        var budgetExhausted = false;
        while (apps.Count < pageSize)
        {
            var next = -1;
            for (var i = 0; i < sources.Count; i++)
            {
                if (!await EnsureHeadAsync(sources, cursors, pages, i, query, pageSize, cancellationToken, fetches).ConfigureAwait(false))
                {
                    if (fetches.Count >= MaxFetchesPerCall && !cursors[i].Done)
                    {
                        budgetExhausted = true;
                        break;
                    }

                    continue;
                }

                if (next < 0 || CompareHeads(pages, cursors, i, next) < 0)
                {
                    next = i;
                }
            }

            if (budgetExhausted || next < 0)
            {
                break;
            }

            var entry = pages[next]!.Entries[cursors[next].Offset];
            cursors[next] = cursors[next] with { Offset = cursors[next].Offset + 1 };
            if (await ToSummaryAsync(entry, cursors[next].Key, query.Filter, tenant, installs, cancellationToken).ConfigureAwait(false) is { } summary)
            {
                apps.Add(summary);
            }
        }

        // Normalise cursors whose page was consumed to the end: a final page's source is exhausted, and any
        // other advances to its next page, so neither is re-read on the next call.
        for (var i = 0; i < cursors.Length; i++)
        {
            if (!cursors[i].Done && pages[i] is { } page && cursors[i].Offset >= page.Entries.Count)
            {
                cursors[i] = page.HasMore
                    ? cursors[i] with { PageToken = page.Continuation, Offset = 0 }
                    : cursors[i] with { Done = true };
            }
        }

        return new AvailableAppPage
        {
            Apps = apps.Count == apps.Capacity ? apps.MoveToImmutable() : apps.ToImmutable(),
            Continuation = AppCatalogContinuation.Encode(cursors),
        };
    }

    private async Task<FetchCounter> FetchInitialAsync(
        List<IAppCatalogSource> sources,
        AppCatalogContinuation.SourceCursor[] cursors,
        AppSourcePage?[] pages,
        AvailableAppQuery query,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var counter = new FetchCounter();
        for (var batch = 0; batch < sources.Count; batch += MaxConcurrentFetches)
        {
            var end = Math.Min(batch + MaxConcurrentFetches, sources.Count);
            var pending = new Task<AppSourcePage?>[end - batch];
            for (var i = batch; i < end; i++)
            {
                pending[i - batch] = cursors[i].Done
                    ? Task.FromResult<AppSourcePage?>(null)
                    : FetchAsync(sources[i], cursors[i].PageToken, query.Text, pageSize, cancellationToken);
            }

            var fetched = await Task.WhenAll(pending).ConfigureAwait(false);
            for (var i = batch; i < end; i++)
            {
                if (cursors[i].Done)
                {
                    continue;
                }

                counter.Count++;
                if (fetched[i - batch] is { } page)
                {
                    pages[i] = page;
                }
                else
                {
                    cursors[i] = cursors[i] with { Done = true };
                }
            }
        }

        return counter;
    }

    /// <summary>
    /// Makes sure source <paramref name="index"/> has an unconsumed head entry, fetching its next page when the
    /// current one is consumed. Returns false when the source is exhausted or the fetch budget is spent.
    /// </summary>
    private async ValueTask<bool> EnsureHeadAsync(
        List<IAppCatalogSource> sources,
        AppCatalogContinuation.SourceCursor[] cursors,
        AppSourcePage?[] pages,
        int index,
        AvailableAppQuery query,
        int pageSize,
        CancellationToken cancellationToken,
        FetchCounter fetches)
    {
        while (!cursors[index].Done)
        {
            var page = pages[index];
            if (page is not null && cursors[index].Offset < page.Entries.Count)
            {
                return true;
            }

            if (page is not null && !page.HasMore)
            {
                cursors[index] = cursors[index] with { Done = true };
                return false;
            }

            if (fetches.Count >= MaxFetchesPerCall)
            {
                return false;
            }

            // Advance to the next page: its continuation becomes the cursor, with nothing consumed yet.
            var token = page?.Continuation ?? cursors[index].PageToken;
            cursors[index] = cursors[index] with { PageToken = token, Offset = 0 };
            fetches.Count++;
            var next = await FetchAsync(sources[index], token, query.Text, pageSize, cancellationToken).ConfigureAwait(false);
            if (next is null)
            {
                cursors[index] = cursors[index] with { Done = true };
                return false;
            }

            pages[index] = next;
        }

        return false;
    }

    private async Task<AppSourcePage?> FetchAsync(
        IAppCatalogSource source,
        string? continuation,
        string? text,
        int pageSize,
        CancellationToken cancellationToken)
    {
        try
        {
            return await source.ListAsync(
                new AppSourceQuery { Text = text, PageSize = pageSize, Continuation = continuation },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A source must never throw from a listing; one that does is treated as exhausted rather than
            // failing the whole catalogue.
            LogListingFailed(_logger, ex, source.Descriptor.Key);
            return null;
        }
    }

    private static int CompareHeads(AppSourcePage?[] pages, AppCatalogContinuation.SourceCursor[] cursors, int x, int y)
    {
        var left = pages[x]!.Entries[cursors[x].Offset].Slug.Value;
        var right = pages[y]!.Entries[cursors[y].Offset].Slug.Value;
        var order = string.CompareOrdinal(left, right);
        return order != 0 ? order : string.CompareOrdinal(cursors[x].Key, cursors[y].Key);
    }

    private async ValueTask<AvailableAppSummary?> ToSummaryAsync(
        AppSourceEntry entry,
        string sourceKey,
        AvailableAppFilter filter,
        TenantId tenant,
        Dictionary<AppSlug, AppRegistryRecord> installs,
        CancellationToken cancellationToken)
    {
        if (!entry.IsAvailable || entry.Manifest is not { } manifest || entry.Versions.Count == 0)
        {
            return null;
        }

        installs.TryGetValue(entry.Slug, out var installed);
        var joined = installed is not null && string.Equals(installed.Provenance.Source, sourceKey, StringComparison.Ordinal)
            ? installed
            : null;
        var newest = entry.Versions[0].Value;
        var keep = filter switch
        {
            AvailableAppFilter.Installed => joined is not null,
            AvailableAppFilter.Available => installed is null,
            AvailableAppFilter.Updates => joined is not null && AppVersionPrecedence.Compare(newest, joined.Version.Value) > 0,
            _ => true,
        };
        if (!keep)
        {
            return null;
        }

        AppLifecycleState? state = null;
        if (joined is not null)
        {
            var status = await _pipeline.GetStatusAsync(tenant, joined.Slug, cancellationToken).ConfigureAwait(false);
            state = AppsControlMapping.ToWireState(joined.State, status);
        }

        var versions = ImmutableArray.CreateBuilder<string>(entry.Versions.Count);
        foreach (var version in entry.Versions)
        {
            versions.Add(version.Value);
        }

        return new AvailableAppSummary
        {
            SourceKey = sourceKey,
            Slug = entry.Slug.Value,
            NewestVersion = newest,
            AvailableVersions = versions.MoveToImmutable(),
            Presentation = AppsPresentationMapping.ToWirePresentation(manifest.Presentation),
            HasUi = manifest.Ui is not null,
            InstalledVersion = joined?.Version.Value,
            InstalledState = state,
        };
    }

    [LoggerMessage(EventId = 10, Level = LogLevel.Warning,
        Message = "App source '{SourceKey}' failed to list its apps; it is treated as exhausted for this listing.")]
    private static partial void LogListingFailed(ILogger logger, Exception exception, string sourceKey);

    private sealed class FetchCounter
    {
        public int Count;
    }
}
