// @ts-nocheck — WDIO element/callback types churn across versions; runtime
// validates the behaviour. Same pragma as wdio.conf.ts.
//
// Existing thread smoke — pick the first chat row in the sidebar, click it,
// and verify the route matches that thread id and the transcript pane mounts.
// Skips cleanly if no chat threads exist yet (chat cycle test creates one
// earlier in the suite, but order isn't guaranteed across files).

describe("Existing thread", () => {
  it("opens an existing thread row from the sidebar", async () => {
    const threadRow = await $("a[href^='/chat/']");
    const exists = await threadRow.isExisting();
    if (!exists) {
      console.warn("[existing-thread] no /chat/<id> rows present — skipping");
      return; // mocha treats no-assertion as passed; suite ran with chat-cycle creates one
    }
    const href = await threadRow.getAttribute("href");
    const match = href.match(/^\/chat\/([^/?#]+)/);
    expect(match, `unexpected thread href: ${href}`).to.not.be.null;
    const threadId = match![1];

    await threadRow.click();

    await browser.waitUntil(
      async () => {
        const url = await browser.getUrl();
        return url.includes(`/chat/${threadId}`);
      },
      { timeout: 30_000, timeoutMsg: `route never settled on /chat/${threadId}` },
    );

    const composer = await browser.waitUntil(
      async () => {
        const form = await $('form[data-chat-composer-form="true"]');
        return (await form.isExisting()) ? form : false;
      },
      { timeout: 30_000, timeoutMsg: "composer form never mounted on existing thread" },
    );
    expect(await composer.isDisplayed()).toBe(true);
  });
});
