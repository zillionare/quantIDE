# Init Wizard

## Purpose

This document defines the release-facing behavior of the PyQMT initialization wizard.

The goal is to make the initialization flow stable enough that subsequent development follows this document first, and code changes are evaluated against it.

Unless explicitly marked as a planned adjustment, statements in this document describe the current implementation.

## Scope

The initialization wizard is responsible for:

- bootstrapping runtime configuration
- optionally enabling gateway-backed live trading capabilities
- configuring notification channels
- configuring and downloading initial market data
- marking the application as initialized

This document does not describe the post-login navigation flow in detail.

## Entry Rules

### First-Time Access

When the application has not been initialized, any normal GET request is redirected to `/init-wizard` before the main application pages are served.

This behavior is enforced by the initialization middleware, so from a user perspective the first usable page is always the initialization wizard.

### Revisit After Initialization

Once initialization has completed, revisiting `/init-wizard` redirects back to `/`.

At the application level, `/` is effectively the home entry point. Any later authentication redirect behavior is outside the scope of this document.

### Forced Re-Initialization

`/init-wizard?force=true` forces the wizard to reopen even after initialization has completed.

This is the supported entry for re-running initialization intentionally.

## Current Implemented Flow

The current wizard has seven steps.

1. Welcome
2. Runtime
3. Gateway
4. Notification
5. Data Initialization
6. Data Download
7. Complete

The wizard persists step state into `app_state` as the user advances.

### Step 1: Welcome

The welcome step explains what the wizard will configure:

- runtime environment
- optional gateway
- optional notification channels
- initial data configuration and download

It also states that skipping gateway configuration leaves the system in strategy-research-only mode.

### Step 2: Runtime

The runtime step configures:

- `home`
- `host`
- `port`
- `prefix`

This information is saved when the user advances to the next step.

### Step 3: Gateway

The gateway step configures:

- whether gateway is enabled
- gateway server
- gateway port
- gateway prefix
- optional API key

The step also provides a connectivity test button.

The test checks the configured target and returns inline success or failure feedback without leaving the wizard.

### Step 4: Notification

The notification step configures DingTalk and mail delivery fields.

This information is saved when the user advances to the next step.

### Step 5: Data Initialization

The data initialization step configures:

- epoch
- Tushare token
- history download years

The effective history start date is derived from the epoch and the requested number of years.

### Step 6: Data Download

The data download step summarizes the initial download plan and the expected datasets:

- trading calendar
- stock universe
- daily bars
- adjustment factors and price limits
- ST data

The current implementation exposes a dedicated `开始下载` button instead of a normal `下一步` button on this step.

When the user clicks the button:

1. the wizard validates and persists the current data-initialization fields if they were posted again
2. a background synchronization task is started
3. a modal progress dialog is shown immediately
4. progress is streamed through SSE from `/init-wizard/sync-progress`

The progress dialog currently contains:

- a stage title
- a progress bar
- status text
- a disabled completion button that becomes enabled only after the download finishes successfully

If synchronization fails, the dialog remains in an error state and the completion button stays disabled.

### Step 7: Complete

After data synchronization completes, the wizard marks initialization complete and shows a final completion step.

The current completion page presents an entry button that routes to:

- `/trade` when gateway is enabled
- `/strategy` when gateway is not enabled

## Current Authentication Behavior

The current wizard does not include a dedicated administrator-account step.

Instead, the authentication subsystem auto-creates a default administrator account if none exists.

Current default credentials are:

- username: `admin`
- password: `admin123`

This is a temporary implementation convenience, not the desired long-term wizard behavior.

## Agreed Adjustments

The following changes are agreed and should drive the next iterations of the wizard.

These items are part of the expected behavior contract even if some are not fully implemented yet.

### Add Administrator Password Step

After the welcome step, add a dedicated administrator credential step.

The interaction should be similar in spirit to the `qmt-gateway` initialization wizard:

- explicit administrator credential entry during initialization
- password confirmation
- inline validation feedback
- credentials saved as part of the wizard flow rather than relying on a hard-coded default account

At minimum, the document requires password setup. The final UI may also include username and related explanatory text, but the wizard must no longer depend on `admin / admin123` as the primary path.

### Merge Data Initialization And Data Download

The current Step 5 and Step 6 should be merged into one step.

That merged step should contain:

- the data initialization form fields
- the download scope summary
- the explicit button to start downloading data

### Download Trigger Rules

On the merged data step, the user should be able to start the download by clicking the explicit action button.

If the user does not click the button and instead clicks `下一步`, the system must still trigger the same data download flow automatically.

In other words, the explicit button is a visible affordance, but not the only path that leads to data synchronization.

### Progress Dialog

Data download should continue to use a modal progress dialog.

The dialog should include:

- current stage text
- progress bar
- detailed status text
- final completion action once the job succeeds

The dialog is part of the expected user-facing behavior and should not be treated as an implementation detail.

## Implementation Notes For Future Changes

When code is updated to match the agreed adjustments, the following should remain true:

- `/init-wizard` remains the canonical initialization entry
- `/init-wizard?force=true` remains the canonical forced re-entry path
- initialization state continues to be stored in `app_state`
- the wizard remains resumable from persisted state
- download progress remains observable without a full page refresh

## Change Policy

Future wizard changes should update this document first or in the same change.

If implementation and this document diverge, the divergence should be treated as a bug or an undocumented design decision that must be resolved quickly.