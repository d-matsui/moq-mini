import { test, expect } from "@playwright/test";

// Wait for the relay + publisher to be ready before the browser connects.
// The orchestration script (scripts/e2e-web-msf.sh) handles startup;
// this test only drives the browser side.

test("msf-subscriber receives and renders video frames", async ({ page }) => {
  await page.goto("/msf-subscriber.html");

  // Verify page loaded
  await expect(page.locator("h2")).toHaveText("MSF Subscriber (H.264 + Opus)");

  const canvas = page.locator("#canvas");
  await expect(canvas).toBeVisible();

  // Take a screenshot of the canvas BEFORE starting (should be black)
  const beforePixels = await getCanvasPixelSum(page);

  // Click Start to connect to relay
  await page.click("#start-btn");

  // Wait for log to show subscription is active
  await expect(page.locator("#log")).toContainText("Video subscribed", {
    timeout: 15_000,
  });

  // Wait for video frames to arrive and be drawn on the canvas.
  // Poll until canvas pixels differ from the initial black state.
  await expect(async () => {
    const currentPixels = await getCanvasPixelSum(page);
    expect(currentPixels).toBeGreaterThan(beforePixels);
  }).toPass({ timeout: 15_000, intervals: [500] });

  // Verify canvas is still being updated (not frozen)
  const pixelsA = await getCanvasPixelSum(page);
  await page.waitForTimeout(500);
  const pixelsB = await getCanvasPixelSum(page);
  expect(pixelsB).not.toBe(pixelsA);

  // Wait for the full stream (10s source + margin)
  await page.waitForTimeout(12_000);

  // Take a final screenshot as artifact
  await page.screenshot({ path: "e2e-results/msf-subscriber-final.png" });
});

/** Sum all pixel values of the canvas to detect non-black content. */
async function getCanvasPixelSum(page: import("@playwright/test").Page): Promise<number> {
  return page.evaluate(() => {
    const canvas = document.getElementById("canvas") as HTMLCanvasElement;
    const ctx = canvas.getContext("2d");
    if (!ctx) return 0;
    const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
    let sum = 0;
    // Sample every 40th pixel for performance (RGBA = 4 bytes each)
    for (let i = 0; i < imageData.data.length; i += 160) {
      sum += imageData.data[i] + imageData.data[i + 1] + imageData.data[i + 2];
    }
    return sum;
  });
}
