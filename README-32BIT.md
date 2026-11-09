# AntiGem 32-bit (armeabi-v7a) — what was changed

AntiGem v1.0.19 er 32-bit optimised copy. Original source: `antigem-master`
(upstream santoshkurmi/antigem). Focus: **Antigravity + Terminal only**.
Claude Code 32-bit ekhane kora hoy nai — niche blocker dekhun.

## Ki change kora hoise (file-by-file)

1. `app/build.gradle.kts`
   - `termux` ar `termuxShared` flavour-e `ndk { abiFilters += listOf("armeabi-v7a", "arm64-v8a") }` jog.
     Termux flavour age, karon Termux er 32-bit bootstrap ase.
   - Notun task `buildAgyIdeBridge32bit`: `GOOS=android GOARCH=arm GOARM=7
     CGO_ENABLED=0`, output `src/main/assets/bin/agy_ide_bridge_arm32`.
     Asol `buildAgyIdeBridge` (arm64) thik ache, preBuild-e 32-bit task
     lagano hoy nai — 32-bit APK bananor somoy ei task ta alada kore chalate hobe,
     tarpor `agy_ide_bridge_arm32` ke `agy_ide_bridge` nam-e copy korte hobe.
   - Chalano: `./gradlew buildAgyIdeBridge32bit`

2. `scripts/agy_ide_bridge_builder.sh`
   - `TARGET_ARCH=arm ./scripts/agy_ide_bridge_builder.sh` dile 32-bit build
     hobe (`GOARCH=arm GOARM=7`, output `agy_ide_bridge_android_arm32`).
     Default (TARGET_ARCH na dile) ager moto arm64.

3. `scripts/build_all_apks.sh`
   - `agy_ide_bridge_arm32` thakle output-e
     `agy_ide_bridge-v<version>-android-arm` nam-e copy hobe (arm64 tar
     pasapasi, `android-arm64`).

4. `app/src/main/java/com/example/gemini/data/local/LocalEnvironmentManager.kt`
   - Notun helper: `glibcAssetNameForArch()` (`glibc-min-arm.tar` / `glibc-min-arm64.tar`),
     `glibcLoaderNameForArch()` (`ld-linux-armhf.so.3` / `ld-linux-aarch64.so.1`),
     `prootAssetNameForArch()` (`bin/proot-arm` / `bin/proot`),
     `kernelReleaseArch()` (`armv7l` / `aarch64`).
   - `extractGlibcMinAsset()` ar `ensureGlibcEnvironment()` ekhon arch onujayi
     asset/loader khuje; Claude wrapper-er `LOADER` o arch onujayi.
   - `ensureProotExtracted()` 32-bit-e age `bin/proot-arm` khuje, na pele
     `bin/proot`-e fallback dey ar warning log kore.
   - `discoverBootstrapPackage()`-er hardcode `arch = "arm64"` sorano —
     ekhon `getBootstrapArch()` er result use kore.
   - proot command-er `--kernel-release` string arch onujayi (`armv7l` / `aarch64`).
   - User-Agent fallback `?: "arm64"` bodle `?: getBootstrapArch()`.

5. `agy_ide_bridge/pkg/hub/manager.go`
   - `FindAllAgyBinaries()`: `GOARCH=arm` hole age `agy-arm` binary khuje
     (`~/.local/bin`, `~/.gemini/bin`, `~/.antigravity/bin`, `~/usr/bin`, `$PREFIX/bin`),
     tarpor `agy`.
   - `agy` na pele error message ekhon 32-bit-e sposhto bole: 32-bit agy nai,
     Antigravity cholbe na, app crash korbe na — hub error status-e jabe,
     baki app cholbe.

6. `app/src/main/java/com/example/gemini/data/remote/AntigravityApiService.kt`
   - `USER_AGENT` fallback `"arm64"` bodle `"unknown"` — ABI list khali thakle
     ar vul kore arm64 bola hobe na.

7. `app/src/main/java/com/example/gemini/ui/settings/SettingsDialog.kt`
   - "Native Architecture" fallback `"arm64-v8a"` bodle `"unknown"`.

8. `app/src/main/java/com/example/gemini/ui/settings/FullScreenLocalToolsInstaller.kt`
   `app/src/main/java/com/example/gemini/ui/settings/LocalToolsInstallDialog.kt`
   - Hardcode "ARM64" string gulo arch-aware: 32-bit device-e "ARM" dekhabe,
     64-bit-e "ARM64".

## Ekhono baki — bahirer 32-bit binary lagbe (download kora hoy nai)

Egulo code diye banano jay na, alada kore supply/build korte hobe:

| Lagbe | Kotha theke / ki check korte hobe |
|---|---|
| **32-bit `agy` binary** | **Sobcheye boro blocker. Google `agy` er 32-bit build dey na** (shudhu arm64/x86_64). Eta na thakle Antigravity agent 32-bit phone-e cholbe na — hub sposhto error dibe. Alternative: 32-bit `agy` build khuje paoa gele `agy-arm` nam-e `$PREFIX/bin` ba `~/.local/bin`-e rakhte hobe. |
| **Claude Code 32-bit binary** | Anthropic `linux-arm` release dey kina verify kora hoy nai — `agy_ide_bridge/pkg/claude/native_install.go` sudhu `linux-arm64` / `linux-x64` chene. Eta scope-er baire rakha hoise. |
| **proot / loader / libtalloc.so.2 (armv7)** | `app/src/standard/assets/bin/proot`, `loader`, `app/src/standard/assets/lib/libtalloc.so.2` — ekhono arm64 ELF. 32-bit build lagbe, asset nam: `bin/proot-arm` (code ekhon ei nam khuje). Source/build kotha theke asbe, seta ekhono ber kora hoy nai. |
| **glibc-min-arm.tar** | 32-bit glibc (`ld-linux-armhf.so.3`, `libc.so.6` ityadi) diye banate hobe, asset nam `glibc-min-arm.tar`. Code ekhon ei nam khuje, kintu file ta repo-te nai. |
| **Ubuntu armhf rootfs** | **TODO — URL invent kora hoy nai.** `LocalEnvironmentManager.kt`-e `UBUNTU_ROOTFS_URL_ARMHF = ""` khali rakha ache. Age https://github.com/termux/proot-distro/releases/tag/v4.30.1 khule exact armhf `ubuntu-questing-...-pd-v4.30.1.tar.xz` asset-er nam copy kore oi constant-e bosate hobe. Na bosale 32-bit-e Ubuntu install-er URL khali thakbe ar installer warning dibe. |
| **Ubuntu rootfs (Termux flavour)** | `HARDCODED_ROOTFS_URL` (`bootrapz_2026-10-04.zip`, 350 MB) er arch code-e lekha nai — 32-bit-e use korar age check korte hobe eta kon arch-er. |

## Kivabe test korte hobe (release build NA)

- Release build banaben na. Termux flavour debug build:
  `./gradlew :app:assembleTermuxDebug`
- Age `./gradlew buildAgyIdeBridge32bit` chaliye 32-bit bridge banan, tarpor
  `agy_ide_bridge_arm32` ke `agy_ide_bridge` nam-e copy korun.
- 32-bit phone / emulator-e install kore Terminal ar Antigravity (agy thakle)
  check korun. agy na thakle hub error dekhabe — etai expected.

## Direct Cloud Mode fallback (2026-11-09 — Antigravity, 32-bit)

Gemini conversation theke asha prompt-er kotha: 32-bit phone-e local `agy` binary
nai bole Antigravity chat atke thakto. Ekhon oi phone-e chat automatic Direct
Cloud Mode-e jabe — Google Cloud Code API (`https://cloudcode-pa.googleapis.com`)
diye, kono local `agy --hub` charai. 64-bit-er hub path-e ek line-o bodol hoy nai.

Prompt-er nam gulo asol code-e miliye neoa hoise — vul nam gulo use kora hoy nai:
- `sendMessageStream` nam-e kono method asol code-e **nai**; asol nam
  `AntigravityApiService.streamGenerateContent(...) : Flow<StreamEvent>`.
- `HubStatus.BYPASS_CLOUD_DIRECT` nam-e kono enum **nai**; hub status ekta String
  (`AgyHubStatus.status`), tai bypass string hisebe implement kora hoise:
  `"bypass_cloud_direct"`.

### Ki change hoilo (file-by-file)

1. `app/src/main/java/com/example/gemini/data/preferences/AuthPreferences.kt`
   - Notun preference `antigravity_mode`: `"local"` (Local Daemon) ba `"cloud"`
     (Direct Cloud). `antigravityMode` Flow, `saveAntigravityMode()`, ar sync getter
     `getAntigravityModeSync()` — na set kora thakle 32-bit ARM-e default `"cloud"`,
     baki sob-e `"local"`.
2. `app/src/main/java/com/example/gemini/data/agent/agy/AgyChatBackend.kt`
   - `shouldUseDirectCloud()` (suspend): explicit choice thakle setai, na thakle
     `getBootstrapArch() == "arm"` hole cloud.
   - `executeStream()`-er hub path-er agei cloud branch: `startCascade` /
     `sendUserPrompt` na chaliye `executeDirectCloudStream()` chole.
   - `executeDirectCloudStream()`: token ney `getValidDirectCloudToken()` diye
     (stored access token, expired hole `GoogleOAuthManager.refreshToken()` diye
     refresh, `AuthPreferences.saveTokens()` diye save — ChatViewModel-er motoi),
     project ney `authPrefs.projectId` ba `loadCodeAssist()` diye, tarpor
     `streamGenerateContent()` collect kore. `StreamEvent.TextChunk` ar
     `ThoughtChunk` ek-i `ChatMessage`-e jay `updateAssistantMessage()` diye —
     live streaming, thinking accordion (`thoughtText`) ar markdown render hub
     mode-er motoi. `Completed` hole tokenUsage soho stream shesh.
   - `sendMessage()`-er gate: cloud mode-e bridge offline thakleo send block hoy na.
   - `startPersistentStream()` cloud mode-e skip kore — mrito hub-er stream-e
     loop-retry kore na.
3. `app/src/main/java/com/example/gemini/data/remote/AgyBridgeService.kt`
   - `HUB_STATUS_BYPASS_CLOUD_DIRECT = "bypass_cloud_direct"` constant,
     `markHubBypassed()` ar `isHubBypassed()`.
   - `updateHubStatus()` bypass thakle bridge-er "stopped" report diye overwrite
     kore na — na hole UI-te abar disconnected banner asto.
4. `app/src/main/java/com/example/gemini/data/remote/SystemConnectionState.kt`
   - `Connected.status`: `hubStatus == "bypass_cloud_direct"` hole auth thakle
     `READY` (CSRF token lage na), na thakle `UNAUTHENTICATED` — `canSend = true`,
     tai chat send kora jay ar UI disconnected dekhay na.
5. `app/src/main/java/com/example/gemini/data/local/LocalServerManager.kt`
   - `resolveServerCommand()`: Direct Cloud hole bridge command-e `" --no-hub"`
     jog hoy — Go bridge-er asol flag (main.go), hub launch-i hoy na, bridge
     sudhu IDE server hisebe chole. `markHubBypassedIfDirectCloud()` server
     start-er agei hub status bypass-e set kore.
6. `app/src/main/java/com/example/gemini/ui/agents/AgentsSetupUi.kt`
   - Settings → Agents-e notun `AntigravityModeSelector()`: "Local Daemon (64-bit)"
     vs "Direct Cloud Code API" — radio card, sathe note je cloud mode text-only.
     Server restart-e karjokor hoy (Agents-er "Apply and restart app" diye).

### Sotti kotha — ei fallback-er limit

- `streamGenerateContent` **text + thought chara ar kichu dey na.** Cascade /
  tool-call parity **nai**: file edit, terminal command, tool approval, subagent
  turn — kichui cloud mode-e asbe na. Reply ta plain chat answer hobe, agentic
  run na. Hub mode-er moto kaj korbe — eta dabi kora hobe na.
- Cloud mode-e chat history hub-e save hoy na; conversation sudhu app-er
  local state-e thake (`ChatSessionStore` / `_messages`).
- Google sign-in (OAuth token) chara cloud mode cholbe na — token na thakle
  "Authentication Required" dekhabe.

### Verify kora hoy nai

- **Compile kora hoy nai.** VM-e Gradle/Go toolchain nai, tai
  `./gradlew assembleTermuxDebug` chalano jay nai. Static check shudhu:
  brace/paren balance, ar prottek referenced method/field asol code-e ache
  kina grep diye check kora hoise.
- 32-bit phone-e live test kora hoy nai — agy chara cloud chat, Settings toggle,
  ar hub banner na dekha — ei 3 ta phone-e check korte hobe.

## Gemini fix (2026-11-09) — AntiGem-32bit-Pro.zip theke newa

Gemini amader copy te 4 ta file bodlaise, sob thik ase bole check kora hoise:

1. `app/build.gradle.kts` — `preBuild` ekhon `buildAgyIdeBridge32bit`-o chalabe,
   tai normal build-e 32-bit bridge bad porbe na.
2. `data/local/LocalServerManager.kt` — 32-bit (`getBootstrapArch() == "arm"`)
   hole assets theke `agy_ide_bridge_arm32` extract kore `agy_ide_bridge` hishebe
   chalabe, na thakle ager binary-te fallback.
3. `data/local/LocalEnvironmentManager.kt` — Termux flavour + 32-bit hole
   `BootstrapSource.Auto` ekhon 350 MB rootfs-er bodle asol Termux arm
   bootstrap (`bootstrap-arm.zip`, ~28 MB, release
   bootstrap-2026.08.30-r1+apt.android-7) download korbe. Standard flavour-e
   Ubuntu armhf URL ekhono khali (TODO), kono URL banano hoy nai.
4. `ui/settings/FullScreenLocalToolsInstaller.kt` — 32-bit device-e installer
   default source ekhon Base Minimum (~33 MB), Auto (~350 MB) na.

Choto jinish ja ekhono thik kora hoy nai: installer default ta Termux flavour-e
limit kora hoy nai, bootstrap size 33 MB lekha (asol ~28 MB), ekta unused
constant (`TERMUX_ARM32_BOOTSTRAP_TAG`) ase. Pro zip-er notun CI workflow
`.github/workflows/build-apk.yml` repo-te dewa hoy nai — workflow file-er
jonno GitHub App-er permission nai, website theke manually dite hobe.

## Baki kaj shesh (2026-11-10) — Ubuntu URL + Gemini nit fix

1. **Ubuntu 32-bit rootfs URL bosano hoise** (`LocalEnvironmentManager.kt`):
   `UBUNTU_ROOTFS_URL_ARMHF` ekhon khali na — verified proot-distro v4.30.1
   asset `ubuntu-questing-arm-pd-v4.30.1.tar.xz` (56,161,004 bytes, ~54 MB).
   Asset-er nam `arm`, `armhf` na — armhf guess korle 404 asto. Standard
   flavour + 32-bit ekhon ei URL use korbe, size ~54 MB dekhabe.
2. **Gemini nit fix:** Termux bootstrap size 33 MB theke asol ~28 MB kora
   hoise (exact 29,367,764 bytes); unused constant
   `TERMUX_ARM32_BOOTSTRAP_TAG` sorano hoise; installer-er 32-bit default
   (Base Minimum) ekhon sudhu Termux flavour-e apply hobe — standard
   flavour-e Auto (Ubuntu flow) thakbe.
3. **CI workflow toiri:** `.github/workflows/build-apk.yml` (Go `^1.26` kore
   dewa, go.mod-er 1.26.5-er sathe milay) local copy-te rakha ase — kintu
   workflow file GitHub App diye push kora jay na, GitHub website theke
   manually upload korte hobe, `release.yml`-er moto.

### Ekhono tomar kaj (manual, Muse korte parbe na)

- GitHub website theke upload: `.github/workflows/release.yml`,
  `.github/workflows/build-apk.yml`, ar 14 ta binary file (glibc tar,
  splash png, proot/loader/libtalloc, 7 ta docs jpg, gradle-wrapper.jar).
- Asol build + test: Java/Gradle/Android SDK thaka machine-e
  `./gradlew :app:assembleTermuxDebug`, tarpor 32-bit device/emulator-e test.
- Hard blocker ja code diye thik hobe na: 32-bit `agy` binary nai (Google
  dey na — Direct Cloud Mode-i workaround), 32-bit Claude Code binary
  ache kina unverified, ar 32-bit `proot`/`loader`/`libtalloc`/`glibc-min-arm.tar`
  ekhono banate hobe.
