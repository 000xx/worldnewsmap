# World News Map — Quick Start Guide

## What You Need Before Starting

- A GitHub account (free) — you already have this
- A web browser (Chrome, Firefox, Safari, Edge — any works)
- About 20 minutes

You do NOT need to install anything on your computer. Everything happens in your browser through GitHub's website.

---

## Step 1: Download Your Project Files

You should have a folder called `worldnewsmap` from Claude with these files inside:

```
worldnewsmap/
├── index.html
├── README.md
├── data/
│   └── live.json
├── pipeline/
│   ├── gdelt_collector.py
│   ├── article_enricher.py
│   ├── orchestrator.py
│   └── requirements.txt
└── .github/
    └── workflows/
        └── update-data.yml
```

**Important:** The `.github` folder might be hidden on your computer because folders starting with a dot are hidden by default. On Mac, press `Cmd + Shift + .` in Finder to reveal hidden files. On Windows, go to View in File Explorer and check "Hidden items."

If you only see individual files (not in a folder), create a folder called `worldnewsmap` on your desktop and put them all inside it, matching the structure above.

---

## Step 2: Create a New Repository on GitHub

1. Go to **github.com** and sign in
2. In the top-right corner, click the **+** button (next to your profile picture)
3. Click **"New repository"**
4. Fill in the settings:
   - **Repository name:** type `worldnewsmap` (or whatever you want to call it)
   - **Description:** type something like `Interactive world news map showing global events`
   - **Public** should be selected (this is required for free GitHub Pages hosting)
   - **Do NOT** check "Add a README file" — we already have one
   - **Do NOT** add a .gitignore or license — we'll handle those
5. Click the green **"Create repository"** button

You'll land on a page that says "Quick setup" with some instructions. Leave this tab open — we're coming back to it.

---

## Step 3: Upload Your Files

GitHub gives you a way to upload files right from the browser. On that "Quick setup" page you're looking at:

1. Look for the link that says **"uploading an existing file"** — click it
2. This opens the file upload page
3. Open your `worldnewsmap` folder on your computer
4. **Drag and drop ALL the files and folders** from inside your worldnewsmap folder into the browser's upload area. This means: `index.html`, `README.md`, the `data` folder, the `pipeline` folder. **Do NOT drag the worldnewsmap folder itself** — drag what's INSIDE it
5. Scroll down. In the "Commit changes" section at the bottom, the default message "Add files via upload" is fine
6. Click the green **"Commit changes"** button
7. Wait for the upload to finish

**Now check your work:** You should see your files listed on the repository page. Click around and verify that `index.html` is there, the `pipeline` folder exists with the Python files inside, and `data/live.json` exists.

**About the `.github` folder:** GitHub's drag-and-drop upload sometimes doesn't handle the `.github` folder well. If you don't see it after uploading, don't worry — we'll create it manually in Step 3b.

---

## Step 3b: Creating the GitHub Action File (if `.github` wasn't uploaded)

If you see a `.github` folder in your repository already, skip this step.

If you don't see it:

1. On your repository page, click the **"Add file"** button (near the top, next to the green "Code" button)
2. Click **"Create new file"**
3. In the "Name your file" box at the top, type exactly: `.github/workflows/update-data.yml`
   - When you type the first `/`, GitHub will automatically create the folder for you. Keep typing.
4. In the big text area below, paste the entire contents of the `update-data.yml` file. Here it is for reference:

```yaml
name: Update World News Map

on:
  schedule:
    - cron: '*/30 * * * *'
  workflow_dispatch:

permissions:
  contents: write

jobs:
  update-data:
    runs-on: ubuntu-latest
    timeout-minutes: 10

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Run pipeline
        env:
          NEWSMAP_MIN_SOURCES: '5'
          NEWSMAP_MAX_ENRICH: '40'
          NEWSMAP_MAX_HOTSPOTS: '200'
          NEWSMAP_OUTPUT: 'data/live.json'
          NEWSMAP_RSS_DELAY: '1.5'
        run: |
          cd pipeline
          python orchestrator.py

      - name: Commit and push updated data
        run: |
          git config user.name "World News Map Bot"
          git config user.email "bot@worldnewsmap.dev"
          git add data/live.json
          git diff --cached --quiet || git commit -m "Update live data $(date -u +'%Y-%m-%d %H:%M UTC')"
          git push
```

5. Scroll down and click the green **"Commit changes"** button
6. In the popup, click **"Commit changes"** again

---

## Step 4: Enable GitHub Pages (This Makes Your Map Visible on the Internet)

1. On your repository page, click **"Settings"** (the gear icon tab, far right in the tab bar)
2. In the left sidebar, scroll down and click **"Pages"**
3. Under "Build and deployment":
   - **Source:** select **"Deploy from a branch"**
   - **Branch:** select **"main"** from the dropdown (it might already be selected)
   - **Folder:** leave it as **"/ (root)"**
4. Click **"Save"**

GitHub will now build your site. This takes 1-3 minutes the first time.

5. After a minute or two, refresh the Settings > Pages page. You'll see a green box at the top that says:
   **"Your site is live at https://yourusername.github.io/worldnewsmap/"**
6. Click that link. You should see your World News Map with the demo data (the 15 hardcoded hotspots).

**Congratulations — your map is live on the internet.** You can share this URL with anyone.

---

## Step 5: Connect the Map to Live GDELT Data

Right now your map shows demo data. To make it show real, live global news data, you need to change one line in `index.html`.

1. Go back to your repository page on GitHub
2. Click on the **`index.html`** file
3. Click the **pencil icon** (top right of the file contents) to edit
4. Use `Ctrl+F` (or `Cmd+F` on Mac) to search for `dataURL: null`
5. Change this line:

**From:**
```javascript
dataURL: null, // e.g. './data/live.json'
```

**To:**
```javascript
dataURL: './data/live.json',
```

6. Scroll down and click **"Commit changes"**
7. In the popup, click **"Commit changes"** again

---

## Step 6: Run the Pipeline for the First Time

The GitHub Action is set to run automatically every 30 minutes, but let's trigger it manually right now so you don't have to wait.

1. On your repository page, click the **"Actions"** tab (near the top, between "Pull requests" and "Projects")
2. You should see **"Update World News Map"** in the left sidebar. Click it.
3. On the right side, you'll see a blue banner or a **"Run workflow"** button. Click it.
4. A dropdown appears — click the green **"Run workflow"** button inside the dropdown
5. The page will show a new workflow run appearing with a yellow dot (meaning it's in progress)
6. Click on it to watch it run. It takes about 1-2 minutes.
7. When all steps show green checkmarks, it's done!

If you go back to your repository and click into the `data` folder, `live.json` should now be much larger — filled with real GDELT event data from the last 15 minutes of global news.

**Wait about 2 minutes** for GitHub Pages to rebuild, then refresh your map URL. You should now see real, live global news hotspots.

---

## Step 7: Verify Everything Is Working

Here's how to confirm the full system is running:

1. Visit your map URL: `https://yourusername.github.io/worldnewsmap/`
2. You should see hotspots based on real news, not the demo cities
3. Click a hotspot — the popup should show article links you can click through to
4. Try the category tabs — Conflict, Politics, Economy, etc. should filter the dots
5. Go to the Actions tab on GitHub — you should see the workflow running (or scheduled to run) every 30 minutes

**If the map still shows demo data:** Check that you correctly changed `dataURL: null` to `dataURL: './data/live.json'` in Step 5, and that the Actions workflow ran successfully in Step 6.

---

## Troubleshooting

**"My Actions workflow failed"**

Click on the failed run to see the error logs. Common issues:
- GDELT's server might be temporarily slow. The workflow will retry next time.
- If you see "permission denied" errors, go to Settings > Actions > General, scroll to "Workflow permissions," and select "Read and write permissions." Click Save.

**"I can't see the .github folder"**

This folder is hidden by default on most operating systems. On GitHub's website you should be able to see it. If you created it manually in Step 3b, it should be there.

**"My map shows 'Loading...' forever"**

Open your browser's developer console (press F12, then click "Console") and look for red error messages. The most common cause is that `live.json` doesn't exist yet or the path is wrong. Make sure the Actions workflow ran at least once successfully.

**"The map loads but has no hotspots"**

This means `live.json` exists but is empty or has no events passing the 5-source filter. This can happen if you catch a very quiet 15-minute window. Wait for the next pipeline run (30 minutes) and refresh.

**"I want to use a custom domain name"**

1. Buy a domain from a registrar like Namecheap, Cloudflare, or Google Domains (around $10-12/year)
2. In your repository Settings > Pages, enter your custom domain
3. At your domain registrar, add a CNAME record pointing to `yourusername.github.io`
4. GitHub provides detailed instructions for this when you enter the custom domain

---

## What Happens Automatically From Now On

You don't need to do anything else. Here's what the system does on its own:

- **Every 30 minutes**, GitHub Actions runs your Python pipeline
- The pipeline downloads the latest GDELT event data (the last 15 minutes of global news)
- It filters to only events with 5+ independent sources
- It classifies each event into categories (Conflict, Economy, Politics, etc.)
- It calculates intensity scores based on source count, impact severity, and emotional tone
- It enriches the top hotspots with readable article headlines from Google News
- It writes the result to `data/live.json` and pushes it to your repository
- GitHub Pages serves the updated file
- Anyone visiting your map sees the latest data

**Your monthly cost: $0.**

---

## Optional Upgrades

**Add The Guardian as a news source (free, better international articles):**

1. Go to https://open-platform.theguardian.com/ and register for a free API key
2. On your repository, go to Settings > Secrets and variables > Actions
3. Click "New repository secret"
4. Name: `GUARDIAN_API_KEY`, Value: paste your API key
5. Edit `.github/workflows/update-data.yml` and uncomment the Guardian line:
   Change `# GUARDIAN_API_KEY: ${{ secrets.GUARDIAN_API_KEY }}` to `GUARDIAN_API_KEY: ${{ secrets.GUARDIAN_API_KEY }}`

**Share your map:**

Your URL `https://yourusername.github.io/worldnewsmap/` works for anyone, on any device, with no login required. Send it to friends, post it on social media, put it on your resume. It's your live project.
