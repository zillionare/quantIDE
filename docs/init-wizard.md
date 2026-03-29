# Init Wizard



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
3. Administrator Password
4. Gateway
5. Notification
6. Data Initialization And Download
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

The runtime step comes before administrator-password setup because the application data location must be known first.

Current implementation detail:

- application state is stored in `app_home/solo.db`
- auth storage is rebound to the same sqlite file after runtime configuration is saved

### Step 3: Administrator Password

After the welcome step, the wizard requires the operator to set the administrator password.

Current behavior:

- administrator username is fixed to `admin`
- the wizard asks for password and password confirmation
- mismatched passwords are rejected inline
- passwords shorter than 6 characters are rejected inline
- the password is written into the auth user store during the wizard flow

This step is now the primary path for administrator credential setup.

### Step 4: Gateway

The gateway step configures:

- whether gateway is enabled
- gateway server
- gateway port
- gateway prefix
- optional API key

The step also provides a connectivity test button.

The test checks the configured target and returns inline success or failure feedback without leaving the wizard.

### Step 5: Notification

The notification step configures DingTalk and mail delivery fields.

This information is saved when the user advances to the next step.

### Step 6: Data Initialization And Download

The merged data step configures:

- epoch
- Tushare token
- history download years

The effective history start date is derived from the epoch and the requested number of years.

The same step also summarizes the initial download plan and the expected datasets:

- trading calendar
- stock universe
- daily bars
- adjustment factors and price limits
- ST data

The merged step exposes both:

- an explicit `开始下载` button in the content area
- a normal `下一步` button in the footer

Both entry paths trigger the same download flow.

When the user clicks either action:

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

The authentication subsystem still auto-creates a default administrator account if no admin user exists.

However, the initialization wizard now overrides that bootstrap convenience by requiring the operator to set the administrator password during initialization.

Operationally, the expected login identity after initialization is:

- username: `admin`
- password: the value entered in Step 2

## Implementation Notes For Future Changes

The following should remain true for future changes:

- `/init-wizard` remains the canonical initialization entry
- `/init-wizard?force=true` remains the canonical forced re-entry path
- initialization state continues to be stored in `app_state`
- the wizard remains resumable from persisted state
- download progress remains observable without a full page refresh

## Change Policy

Future wizard changes should update this document first or in the same change.

If implementation and this document diverge, the divergence should be treated as a bug or an undocumented design decision that must be resolved quickly.
