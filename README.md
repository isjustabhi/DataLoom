# DataLoom Studio

DataLoom Studio is a single-page Vite + React application that turns uploaded CSV or Excel datasets into a four-stage analytics pipeline: ingest, refine, enrich, and deploy. The app profiles uploaded data in the browser, uses the OpenAI Chat Completions API for refinement and analytics recommendations, renders interactive charts with Recharts, and exports cleaned data plus deployment artifacts with no backend required.

## Tech Stack

- React 18
- Vite 5
- Tailwind CSS 3
- Recharts
- PapaParse
- SheetJS / XLSX
- OpenAI Chat Completions API via `fetch`

## Setup

1. Install dependencies:

```bash
npm install
```

2. Create an environment file:

```bash
cp .env.example .env.local
```

3. Add your OpenAI API key to `.env.local`:

```env
VITE_OPENAI_API_KEY=sk-your-key-here
```

4. Start the development server:

```bash
npm run dev
```

5. Open the local Vite URL in your browser.

## Features

- Upload CSV, XLSX, and XLS files up to 10 MB
- Client-side profiling for nulls, types, unique values, and numeric/string summaries
- AI-guided refine stage that proposes transformations and applies them locally in JavaScript
- AI-guided enrich stage that generates KPIs, charts, insights, anomalies, and PySpark pipeline code
- Deploy stage with downloadable CSV, Markdown report, and Python pipeline script
- Sticky four-stage pipeline navigator with progress visualization

## Environment

The OpenAI API key is read from:

```env
import.meta.env.VITE_OPENAI_API_KEY
```

## Deployment

This project is ready for zero-config deployment on Vercel. Import the repository in Vercel and deploy as a Vite app.
