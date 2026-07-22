// Smoke test — verify the Syncode desktop app launches and renders its shell.
//
// The @wdio/tauri-service launches the test binary (built with
// `--features webdriver`, which embeds the WebDriver server). This exercises
// the REAL Tauri webview + the in-process WS server + the boot path — catching
// regressions a browser-mode test cannot (e.g. the cache cascade, cmd-window
// hiding, IPC bridge).

describe("Syncode desktop boot", () => {
  it("launches the app window", async () => {
    await browser.waitUntil(
      async () => {
        const title = await browser.getTitle();
        return title.length > 0;
      },
      { timeout: 30_000, timeoutMsg: "app window never set a title" },
    );
    const title = await browser.getTitle();
    // tauri.conf.json productName is "Syncode".
    expect(title).toContain("Syncode");
  });

  it("renders the shell (non-empty document)", async () => {
    await browser.waitUntil(
      async () => {
        const html = await $("body").getHTML();
        return html.length > 0;
      },
      { timeout: 30_000, timeoutMsg: "shell never rendered any content" },
    );
  });

  it("does not show a fatal error overlay on boot", async () => {
    // The app renders an error boundary overlay on uncaught render errors.
    // Its presence means the shell crashed during boot.
    const errorOverlay = await $("[data-testid='fatal-error'], .fatal-error, #crash-boundary");
    const present = await errorOverlay.isExisting();
    expect(present).toBe(false);
  });
});
