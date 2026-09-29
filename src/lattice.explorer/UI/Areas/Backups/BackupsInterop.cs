using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The Backups area's one piece of script: saving an exported artifact as a
/// browser download. The module is imported lazily, once per circuit.
/// </summary>
internal sealed class BackupsInterop : IAsyncDisposable
{
    private readonly IJSRuntime _js;
    private Task<IJSObjectReference>? _module;

    /// <summary>Creates the interop over the circuit's JavaScript runtime.</summary>
    /// <param name="js">The JavaScript runtime.</param>
    public BackupsInterop(IJSRuntime js)
    {
        ArgumentNullException.ThrowIfNull(js);
        _js = js;
    }

    /// <summary>
    /// Streams <paramref name="content"/> to the browser and saves it as
    /// <paramref name="fileName"/>. The stream is disposed when the transfer ends.
    /// </summary>
    /// <param name="fileName">The download's file name.</param>
    /// <param name="content">The bytes to save.</param>
    /// <param name="cancellationToken">Cancels the transfer.</param>
    public async Task SaveAsync(string fileName, Stream content, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(fileName);
        ArgumentNullException.ThrowIfNull(content);

        using var reference = new DotNetStreamReference(content, leaveOpen: false);
        var module = await ImportAsync().ConfigureAwait(false);
        await module.InvokeVoidAsync("saveArtifact", cancellationToken, fileName, reference).ConfigureAwait(false);
    }

    private async Task<IJSObjectReference> ImportAsync()
    {
        _module ??= _js.InvokeAsync<IJSObjectReference>("import", BackupsAssets.ModuleSpecifier).AsTask();
        try
        {
            return await _module.ConfigureAwait(false);
        }
        catch
        {
            // A failed import is not remembered, so the next export tries again.
            _module = null;
            throw;
        }
    }

    /// <summary>A file name for an artifact that is safe on every file system.</summary>
    /// <param name="backupId">The owning backup id.</param>
    /// <param name="artifactId">The artifact id.</param>
    public static string FileNameFor(string backupId, string artifactId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(artifactId);

        var raw = backupId.Length > 12 ? backupId[..12] : backupId;
        var name = string.Create(raw.Length + 1 + artifactId.Length, (raw, artifactId), static (span, state) =>
        {
            var index = 0;
            foreach (var character in state.raw)
            {
                span[index++] = Safe(character);
            }

            span[index++] = '-';
            foreach (var character in state.artifactId)
            {
                span[index++] = Safe(character);
            }
        });

        return name + ".bin";
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_module is { IsCompletedSuccessfully: true } module)
        {
            try
            {
                await module.Result.DisposeAsync().ConfigureAwait(false);
            }
            catch (JSDisconnectedException)
            {
                // The circuit has gone; there is nothing left to release.
            }
        }
    }

    private static char Safe(char character) =>
        char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.' ? character : '_';
}
