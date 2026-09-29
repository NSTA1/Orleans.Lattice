// The Backups area's one module: saves an exported backup artifact as a
// download. The bytes arrive as a .NET stream reference, so the server never
// holds the whole artifact in memory.
export async function saveArtifact(fileName, streamReference) {
    const buffer = await streamReference.arrayBuffer();
    const url = URL.createObjectURL(new Blob([buffer], { type: "application/octet-stream" }));
    try {
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = fileName;
        anchor.rel = "noopener";
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
    } finally {
        URL.revokeObjectURL(url);
    }
}
