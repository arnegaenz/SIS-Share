# SIS-Share: FI-Funnel Analytics Dashboard

A shareable analytics dashboard for CardSavr partners to track and visualize Financial Institution (FI) conversion funnels, combining Google Analytics data with CardSavr session and placement metrics.

## Overview

SIS-Share provides a comprehensive view of your CardSavr integration performance through:

- **FI-Funnel Analytics**: Track user journey from GA events through sessions to successful card placements
- **Auto-Fetch Capability**: Automatically fetches missing data from your CardSavr instances and Google Analytics
- **Browser-Based Credential Management**: Upload and manage credentials directly through the web interface
- **Daily Data Aggregation**: Efficient rollup system for fast dashboard performance
- **Multi-Instance Support**: Connect to multiple CardSavr instances (prod, test, staging)

## Features

### FI-Funnel Page
- Complete conversion funnel visualization
- Filter by FI, partner, integration type, and date range
- Breakdown by success vs system/UX failures
- CSV export functionality
- Automatic data refresh when missing data is detected

### Maintenance Page
- **Data Refresh Controls**: Manual refresh of GA, sessions, and placements data
- **FI Registry Editor**: Manage FI metadata, lookup keys, and cardholder counts
- **Instance Credentials**: Upload and manage CardSavr instance credentials via browser
- **GA Credentials**: Upload and manage Google Analytics service account JSON files

## Prerequisites

- **Node.js**: Version 16 or higher
- **CardSavr SDK Access**: You need access to `@strivve/strivve-sdk` npm package
- **Google Analytics 4**: A GA4 property with service account access
- **CardSavr Instance**: At least one CardSavr instance with API credentials

## Quick Start

### 1. Clone and Install

```bash
git clone <your-repo-url> sis-share
cd sis-share
npm install
```

### 2. Set Up Credentials

You have two options for setting up credentials:

#### Option A: Browser Upload (Recommended)

1. Start the server first:
   ```bash
   npm start
   ```

2. Navigate to http://localhost:8787/maintenance.html

3. Use the credential management sections to upload:
   - Instance credentials JSON (via "Instance Credentials" section)
   - GA service account JSON (via "Google Analytics Credentials" section)

#### Option B: Manual File Setup

1. Copy the example file:
   ```bash
   cp secrets/instances.json.example secrets/instances.json
   ```

2. Edit `secrets/instances.json` with your CardSavr credentials:
   ```json
   [
     {
       "name": "prod",
       "CARDSAVR_INSTANCE": "your-instance.cardsavr.io",
       "USERNAME": "your-username",
       "PASSWORD": "your-password",
       "API_KEY": "your-api-key",
       "APP_NAME": "your-app-name"
     }
   ]
   ```

3. Add your Google Analytics service account JSON file:
   - Production: `secrets/ga-service-account.json`
   - Test (optional): `secrets/ga-test.json`

   See [secrets/README.md](secrets/README.md) for detailed instructions on creating GA service accounts.

### 3. Configure Environment Variables (Required)

**IMPORTANT**: You must create a `.env` file with your Google Analytics property ID.

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` and set your actual GA property IDs:

```
GA_PROPERTY_ID=your-production-ga-property-id
GA_TEST_PROPERTY_ID=your-test-property-id
```

**Replace `your-production-ga-property-id` with your actual GA4 property ID** (e.g., `328054560`). This property ID is used when fetching data from Google Analytics during the data refresh process.

### 4. Start the Server

```bash
npm start
```

The server will start on http://localhost:8787

### 5. Access the Dashboard

- **FI-Funnel**: http://localhost:8787/funnel.html
- **Maintenance**: http://localhost:8787/maintenance.html

## How It Works

### Auto-Fetch Workflow

When you open the FI-Funnel page:

1. **Check for Missing Data**: The dashboard checks if data exists for the selected date range
2. **Auto-Trigger Fetch**: If data is missing, it automatically fetches from:
   - Your CardSavr instances (sessions and placements)
   - Google Analytics (funnel events)
3. **Build Daily Rollups**: Raw data is aggregated into daily rollups for performance
4. **Display Results**: The funnel visualization updates with the fetched data

All of this happens automatically in the background with progress updates shown in the UI.

### FI Registry

The FI registry (`fi_registry.json`) contains metadata about Financial Institutions:

- FI names and lookup keys
- Integration types (SSO, NON_SSO, etc.)
- Partner associations
- Cardholder counts (for reach percentage calculations)
- Instance associations

**The registry starts empty and auto-populates** based on data from your CardSavr instances as you fetch session and placement data. You can then edit entries through the Maintenance page to add cardholder counts, set integration types, and assign partners.

### Data Storage

All data is stored locally in your file system:

- `raw/sessions/` - Raw session data by date
- `raw/placements/` - Raw placement data by date
- `raw/ga/` - Raw Google Analytics data by date
- `data/daily/` - Aggregated daily rollups for fast loading

## Usage

### Viewing the FI-Funnel

1. Navigate to http://localhost:8787/funnel.html
2. Select your date range (defaults to last 30 days)
3. Apply filters for FI, partner, or integration type as needed
4. The funnel will show:
   - GA funnel stages (Select Merchants → User Data → Credential Entry)
   - Session metrics (total, with jobs, with success)
   - Placement attempts and success rates
   - Conversion percentages at each stage

### Managing Credentials

#### Instance Credentials

1. Go to Maintenance page → Instance Credentials section
2. Click "Choose File" and select your `instances.json` file
3. Click "Upload" - the file will be saved to `secrets/instances.json`
4. Test the connection by clicking "Test Connection"

#### Google Analytics Credentials

1. Go to Maintenance page → Google Analytics Credentials section
2. Select the environment (Production or Test)
3. Click "Choose File" and select your GA service account JSON
4. Click "Upload"
5. Test the credential by clicking "Test"

### Refreshing Data

To manually refresh data:

1. Go to http://localhost:8787/maintenance.html
2. Under "Data Refresh", select:
   - Start and end dates
   - Whether to force re-fetch (overwrite existing data)
3. Click "Refresh Data"
4. Monitor the progress in the status log

### Reloading FI Registry

To populate the registry with FIs from your instances:

1. Go to Maintenance page → FI Registry Editor section
2. Click "Reload Registry"
3. Choose reload method:
   - **From Instances** (Recommended for new setups): Fetches all FIs directly from your configured CardSavr instances
   - **From Daily Data**: Scans existing daily rollup files for FI references
4. The system will add new FI entries without overwriting existing ones
5. Check the status message to see how many FIs were added

**Note**: "From Instances" is the best option when you've just added instance credentials and haven't fetched any data yet.

### Editing FI Registry

1. Go to Maintenance page → FI Registry Editor section
2. Search for an FI by name or instance
3. Click "Edit" on the entry you want to modify
4. Update fields:
   - FI Name
   - Lookup Key (used for data aggregation)
   - Integration Type
   - Partner
   - Cardholder Count
5. Click "Save Changes"

## Scripts

- `npm start` - Start the web server (port 8787)
- `npm run fetch` - Manually fetch raw data from instances and GA
- `npm run build` - Build daily rollups from raw data

## Configuration

### Port

Default port is 8787. Currently not configurable via environment variable, but you can modify it in `scripts/serve-funnel.mjs`:

```javascript
const PORT = 8787; // Change this value
```

### Multiple Instances

Add multiple CardSavr instances to `secrets/instances.json`:

```json
[
  {
    "name": "prod",
    "CARDSAVR_INSTANCE": "prod.cardsavr.io",
    ...
  },
  {
    "name": "staging",
    "CARDSAVR_INSTANCE": "staging.cardsavr.io",
    ...
  },
  {
    "name": "test",
    "CARDSAVR_INSTANCE": "test.cardsavr.io",
    ...
  }
]
```

The dashboard will aggregate data from all configured instances.

## Troubleshooting

### "No data available" message

**Cause**: Missing data for the selected date range.

**Solution**:
- The auto-fetch should trigger automatically
- If not, go to Maintenance page and manually refresh data
- Check that your credentials are valid by testing them in the Maintenance page

### GA Authentication Errors or Wrong Property ID

**Error**: `Error: 7 PERMISSION_DENIED` or data fetching from wrong GA property

**Solution**:
1. **Create a `.env` file** if you haven't already (see step 3 in Quick Start)
2. Ensure the service account email is added to your GA4 property with Viewer access
3. **Verify GA_PROPERTY_ID in `.env` matches your actual property ID** - this is the most common issue
4. Check that the service account JSON file is valid
5. Note: The property ID field on the maintenance page is only used for testing credentials, not for data refresh. The data refresh uses the property ID from your `.env` file.

### Instance Connection Fails

**Solutions**:
- Verify instance hostname (no `https://` prefix needed)
- Check API credentials are active and not expired
- Ensure IP whitelisting if required by your instance
- Confirm APP_NAME matches your registered application

### Funnel Shows Zero for All Metrics

**Causes**:
1. No data exists for the date range
2. FI registry lookup keys don't match your data
3. Date range is outside your data collection period

**Solutions**:
- Refresh data for the specific date range
- Check FI registry entries in Maintenance page
- Verify GA is tracking the required events

### Server Won't Start

**Error**: `EADDRINUSE: address already in use`

**Solution**: Port 8787 is already in use. Either:
- Stop the other process using that port
- Change the PORT constant in `scripts/serve-funnel.mjs`

## Architecture

### Tech Stack

**Frontend:**
- Vanilla JavaScript (no framework dependencies)
- CSS with CSS variables for theming
- Modular component architecture

**Backend:**
- Node.js HTTP server
- File-based JSON storage (no database)
- Server-Sent Events (SSE) for real-time updates

**Dependencies:**
- `@strivve/strivve-sdk` - CardSavr API integration
- `@google-analytics/data` - GA4 Data API
- `googleapis` - Google services
- `dotenv` - Environment configuration

### Data Flow

```
CardSavr Instances + Google Analytics
           ↓
   [Auto-Fetch / Manual Refresh]
           ↓
      raw/*.json files
           ↓
    [Daily Rollup Aggregation]
           ↓
    data/daily/*.json files
           ↓
   [Web Server API Endpoints]
           ↓
    Browser Dashboard (Funnel)
```

### Key Files

- `scripts/serve-funnel.mjs` - HTTP server and API endpoints
- `scripts/fetch-raw.mjs` - Data fetching from instances and GA
- `scripts/build-daily-from-raw.mjs` - Daily aggregation logic
- `src/lib/daily-rollups.mjs` - Rollup calculation functions
- `src/lib/rawStorage.mjs` - File storage utilities
- `public/assets/js/funnel.js` - Funnel page main controller
- `public/assets/js/raw-data-checker.js` - Auto-fetch logic

## Security Best Practices

1. **Never commit secrets** - The `.gitignore` file excludes `secrets/`, `data/`, and `raw/`
2. **Use read-only credentials** where possible
3. **Rotate credentials** regularly
4. **Limit service account permissions** to Viewer role only
5. **Run on localhost** or behind authentication in production

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs in the Maintenance page
3. Inspect browser console for frontend errors
4. Check server console output for backend errors

## License

ISC
