using System.Security.Cryptography;
using Microsoft.AspNetCore.Components.Server.ProtectedBrowserStorage;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Logging;
using Microsoft.JSInterop;
using Microsoft.JSInterop.Infrastructure;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Web;

namespace Orleans.Lattice.Explorer.Tests.Web;

[TestFixture]
public class ProtectedLocalStoragePreferenceBackingStoreTests
{
    private const string Key = UiPreferenceStore.BackingKey;

    [Test]
    public async Task GetAsync_undecryptable_document_returns_null_and_discards_it()
    {
        var browser = new FakeBrowser();

        // Written under one key ring, read under another: exactly what a rotated or
        // lost Data Protection key ring (or a re-deployed host) presents to a returning user.
        await new ProtectedLocalStorage(browser, new EphemeralDataProtectionProvider()).SetAsync(Key, "{}");
        Assert.That(browser.Items, Does.ContainKey(Key));

        var store = CreateStore(browser);

        var value = await store.GetAsync(Key);

        Assert.That(value, Is.Null);
        Assert.That(browser.Items, Does.Not.ContainKey(Key));
    }

    [Test]
    public async Task GetAsync_undecryptable_document_and_failing_delete_still_returns_null()
    {
        var browser = new FakeBrowser();
        await new ProtectedLocalStorage(browser, new EphemeralDataProtectionProvider()).SetAsync(Key, "{}");
        browser.FailRemove = true;

        var value = await CreateStore(browser).GetAsync(Key);

        Assert.That(value, Is.Null);
        Assert.That(browser.Items, Does.ContainKey(Key));
    }

    [Test]
    public async Task GetAsync_absent_key_returns_null()
    {
        var value = await CreateStore(new FakeBrowser()).GetAsync(Key);

        Assert.That(value, Is.Null);
    }

    [Test]
    public async Task SetAsync_then_GetAsync_round_trips_value()
    {
        var store = CreateStore(new FakeBrowser());

        await store.SetAsync(Key, "{\"a\":1}");

        Assert.That(await store.GetAsync(Key), Is.EqualTo("{\"a\":1}"));
    }

    [Test]
    public async Task RemoveAsync_deletes_value()
    {
        var browser = new FakeBrowser();
        var store = CreateStore(browser);
        await store.SetAsync(Key, "x");

        await store.RemoveAsync(Key);

        Assert.That(browser.Items, Does.Not.ContainKey(Key));
        Assert.That(await store.GetAsync(Key), Is.Null);
    }

    [Test]
    public void GetAsync_unreachable_browser_still_throws_so_load_is_retried()
    {
        var browser = new FakeBrowser { FailAll = true };

        Assert.ThrowsAsync<InvalidOperationException>(() => CreateStore(browser).GetAsync(Key));
    }

    [Test]
    public void Methods_null_or_empty_key_throw()
    {
        var store = CreateStore(new FakeBrowser());

        Assert.ThrowsAsync<ArgumentNullException>(() => store.GetAsync(null!));
        Assert.ThrowsAsync<ArgumentException>(() => store.GetAsync(string.Empty));
        Assert.ThrowsAsync<ArgumentException>(() => store.SetAsync(string.Empty, "v"));
        Assert.ThrowsAsync<ArgumentException>(() => store.RemoveAsync(string.Empty));
    }

    [Test]
    public void Constructor_null_storage_throws()
    {
        Assert.Throws<ArgumentNullException>(() => new ProtectedLocalStoragePreferenceBackingStore(null!));
    }

    [Test]
    public async Task UiPreferenceStore_over_undecryptable_document_loads_empty_and_persists_new_value()
    {
        var browser = new FakeBrowser();
        await new ProtectedLocalStorage(browser, new EphemeralDataProtectionProvider()).SetAsync(Key, "{\"k\":{}}");
        var backing = CreateStore(browser);
        using var preferences = new UiPreferenceStore(backing);

        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync("k", 7);

        Assert.That(preferences.IsLoaded, Is.True);
        Assert.That(preferences.GetOrDefault("k", 0), Is.EqualTo(7));
        Assert.That(await backing.GetAsync(Key), Does.Contain("\"k\""));
    }

    [Test]
    public async Task GetAsync_undecryptable_document_logs_one_discard_warning()
    {
        var browser = new FakeBrowser();
        await new ProtectedLocalStorage(browser, new EphemeralDataProtectionProvider()).SetAsync(Key, "{}");
        var logger = new CapturingLogger();

        await CreateStore(browser, logger).GetAsync(Key);

        Assert.That(logger.Entries, Has.Count.EqualTo(1));
        Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Warning));
        Assert.That(logger.Entries[0].EventId.Id, Is.EqualTo(1));
        Assert.That(logger.Entries[0].Exception, Is.InstanceOf<CryptographicException>());
        Assert.That(logger.Entries[0].Message, Does.Contain(Key));
    }

    [Test]
    public async Task GetAsync_readable_document_logs_nothing()
    {
        var logger = new CapturingLogger();
        var store = CreateStore(new FakeBrowser(), logger);
        await store.SetAsync(Key, "{}");

        await store.GetAsync(Key);

        Assert.That(logger.Entries, Is.Empty);
    }

    private static ProtectedLocalStoragePreferenceBackingStore CreateStore(FakeBrowser browser, ILogger<ProtectedLocalStoragePreferenceBackingStore>? logger = null)
        => new(new ProtectedLocalStorage(browser, new EphemeralDataProtectionProvider()), logger);

    private sealed class CapturingLogger : ILogger<ProtectedLocalStoragePreferenceBackingStore>
    {
        public List<(LogLevel Level, EventId EventId, string Message, Exception? Exception)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, eventId, formatter(state, exception), exception));
    }

    /// <summary>An in-memory stand-in for the browser's <c>localStorage</c> reached over JS interop.</summary>
    private sealed class FakeBrowser : IJSRuntime
    {
        public Dictionary<string, string> Items { get; } = new(StringComparer.Ordinal);

        public bool FailRemove { get; set; }

        public bool FailAll { get; set; }

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args)
            => Invoke<TValue>(identifier, args);

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
            => Invoke<TValue>(identifier, args);

        private ValueTask<TValue> Invoke<TValue>(string identifier, object?[]? args)
        {
            if (FailAll)
            {
                throw new InvalidOperationException("JavaScript interop calls cannot be issued at this time.");
            }

            var key = (string)args![0]!;
            switch (identifier)
            {
                case "localStorage.getItem":
                    return ValueTask.FromResult(Items.TryGetValue(key, out var stored) ? (TValue)(object)stored : default!);
                case "localStorage.setItem":
                    Items[key] = (string)args[1]!;
                    return ValueTask.FromResult<TValue>(default!);
                case "localStorage.removeItem":
                    if (FailRemove)
                    {
                        throw new JSException("removeItem failed");
                    }

                    Items.Remove(key);
                    return ValueTask.FromResult<TValue>(default!);
                default:
                    throw new NotSupportedException(identifier);
            }
        }
    }
}
