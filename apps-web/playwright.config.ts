import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  use: {
    baseURL: "http://localhost:5173",
    // Record video for every test
    video: "on",
    // Chromium launch args for WebTransport with self-signed certs
    launchOptions: {
      args: [
        "--ignore-certificate-errors",
        "--origin-to-force-quic-on=localhost:4433",
      ],
    },
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
  ],
  // Artifacts output directory (cleaned up each run by Playwright)
  outputDir: "./e2e-results",
});
