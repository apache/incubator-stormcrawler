# Playwright
Protocol implementation for Apache StormCrawler (Incubating) based on Playwright

## Standalone Chrome

Playwright installs the browsers it needs locally, which can take more than 1 GB disk space and takes some time. Instead, you can install Chrome separately and get Playwright to connect to its CDP.

[Chrome for Testing](https://github.com/GoogleChromeLabs/chrome-for-testing) is the recommended way of installing a standalone version of Chrome. 

You can start it like so

```
 ./chrome --remote-debugging-port=9222 --user-data-dir=remote-profile --headless &
```
NOTE this is how ChromeDriver launches Chrome

```
chrome --allow-pre-commit-input --disable-background-networking --disable-client-side-phishing-detection --disable-default-apps --disable-dev-shm-usage --disable-gpu --disable-hang-monitor --disable-popup-blocking --disable-prompt-on-repost --disable-software-rasterizer --disable-sync --dns-prefetch-disable --enable-automation --enable-logging --headless --log-level=0 --mute-audio --no-first-run --no-sandbox --no-service-autorun --password-store=basic --remote-debugging-port=9222 --use-mock-keychain --user-data-dir=/tmp/.org.chromium.Chromium.CPTbw3 data:,
```

Then URL for the CDP connection is configured with _playwright.cdp.url_.

Alternatively, you can still rely on what Playwright installs but do it manually and just for the browser type you want

```
mvn exec:java -e -Dexec.mainClass=com.microsoft.playwright.CLI -Dexec.args="install chromium"
```

The setting `playwright.skip.download` to `true` in the configuration will assume that the browser has been installed and will not trigger the installation of all the different browsers.

