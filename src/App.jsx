import { useEffect, useMemo, useRef, useState } from 'react';
import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  Radar,
  RadarChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const STAGES = ['ingest', 'refine', 'enrich', 'deploy'];
const STAGE_LABELS = {
  ingest: 'Ingest',
  refine: 'Refine',
  enrich: 'Enrich',
  deploy: 'Deploy',
};
const CHART_COLORS = ['#818CF8', '#A78BFA', '#67E8F9', '#34D399', '#F472B6', '#FB923C'];
const REFINE_STEPS = [
  { percent: 8, message: 'Scanning column distributions...', stage: 'refine' },
  { percent: 18, message: 'Identifying null patterns...', stage: 'refine' },
  { percent: 28, message: 'Detecting outliers...', stage: 'refine' },
  { percent: 38, message: 'Planning transformations...', stage: 'refine' },
  { percent: 48, message: 'Standardizing column names...', stage: 'refine' },
  { percent: 58, message: 'Imputing missing values...', stage: 'refine' },
  { percent: 68, message: 'Correcting data types...', stage: 'refine' },
  { percent: 78, message: 'Removing duplicates...', stage: 'refine' },
  { percent: 88, message: 'Validating data integrity...', stage: 'refine' },
  { percent: 96, message: 'Finalizing refined dataset...', stage: 'refine' },
];
const ENRICH_STEPS = [
  { percent: 10, message: 'Detecting data patterns...', stage: 'enrich' },
  { percent: 22, message: 'Computing statistical correlations...', stage: 'enrich' },
  { percent: 35, message: 'Identifying key performance indicators...', stage: 'enrich' },
  { percent: 48, message: 'Selecting optimal chart types...', stage: 'enrich' },
  { percent: 60, message: 'Generating visualizations...', stage: 'enrich' },
  { percent: 72, message: 'Mining business insights...', stage: 'enrich' },
  { percent: 84, message: 'Detecting anomalies...', stage: 'enrich' },
  { percent: 94, message: 'Compiling analytics report...', stage: 'enrich' },
];

const DEFAULT_PROFILE = {
  nullCounts: {},
  typesDetected: {},
  uniqueCounts: {},
  minMax: {},
  stringSummary: {},
  rowCount: 0,
  columnCount: 0,
  totalNulls: 0,
  totalCells: 0,
  qualityScore: 0,
};

const EMPTY_ENRICH = {
  kpis: [],
  charts: [],
  insights: [],
  correlations: [],
  anomalies: [],
};

const iconMap = {
  fill_null: {
    symbol: '◔',
    color: 'text-amber-300',
    bg: 'bg-amber-500/10',
    border: 'border-amber-400/30',
    label: 'Fill Null',
  },
  rename: {
    symbol: '⌗',
    color: 'text-sky-300',
    bg: 'bg-sky-500/10',
    border: 'border-sky-400/30',
    label: 'Rename',
  },
  fix_type: {
    symbol: '⌘',
    color: 'text-violet-300',
    bg: 'bg-violet-500/10',
    border: 'border-violet-400/30',
    label: 'Fix Type',
  },
  remove_outlier: {
    symbol: '✂',
    color: 'text-rose-300',
    bg: 'bg-rose-500/10',
    border: 'border-rose-400/30',
    label: 'Outlier',
  },
  deduplicate: {
    symbol: '⧉',
    color: 'text-cyan-300',
    bg: 'bg-cyan-500/10',
    border: 'border-cyan-400/30',
    label: 'Deduplicate',
  },
};

function App() {
  const [stage, setStage] = useState('ingest');
  const [rawData, setRawData] = useState([]);
  const [rawColumns, setRawColumns] = useState([]);
  const [fileName, setFileName] = useState('');
  const [fileSize, setFileSize] = useState('');
  const [profileStats, setProfileStats] = useState(DEFAULT_PROFILE);
  const [refinedData, setRefinedData] = useState([]);
  const [refineLog, setRefineLog] = useState([]);
  const [refineSummary, setRefineSummary] = useState(null);
  const [enrichOutput, setEnrichOutput] = useState(EMPTY_ENRICH);
  const [pipelineCode, setPipelineCode] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState({ percent: 0, message: 'Ready', stage: 'ingest' });
  const [error, setError] = useState(null);
  const [expandedSections, setExpandedSections] = useState({
    refinedPreview: false,
    anomalies: true,
    code: true,
  });
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [dragActive, setDragActive] = useState(false);
  const [copiedCode, setCopiedCode] = useState(false);
  const fileInputRef = useRef(null);

  const apiKey = import.meta.env.VITE_OPENAI_API_KEY;

  useEffect(() => {
    if (!copiedCode) return undefined;
    const timeout = window.setTimeout(() => setCopiedCode(false), 1800);
    return () => window.clearTimeout(timeout);
  }, [copiedCode]);

  const sortedPreviewData = useMemo(() => {
    const target = stage === 'ingest' || !refinedData.length ? rawData : refinedData;
    const preview = target.slice(0, 15);
    if (!sortConfig.key) return preview;
    const sorted = [...preview].sort((a, b) => {
      const aValue = normalizeSortValue(a?.[sortConfig.key]);
      const bValue = normalizeSortValue(b?.[sortConfig.key]);
      if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
    return sorted;
  }, [rawData, refinedData, sortConfig, stage]);

  const completionPercent = useMemo(() => {
    const base = STAGES.indexOf(stage);
    return Math.max(25, (base + 1) * 25);
  }, [stage]);

  const canJumpToStage = (targetStage) => STAGES.indexOf(targetStage) <= STAGES.indexOf(stage);

  const beforeMetrics = useMemo(() => buildDataSummary(rawData), [rawData]);
  const afterMetrics = useMemo(() => buildDataSummary(refinedData), [refinedData]);
  const codeLines = useMemo(() => (pipelineCode ? pipelineCode.split('\n') : []), [pipelineCode]);
  const reportContent = useMemo(
    () => buildAnalyticsReport({ fileName, refineLog, enrichOutput, pipelineCode, refinedData }),
    [enrichOutput, fileName, pipelineCode, refineLog, refinedData],
  );
  const deployStats = useMemo(
    () => [
      { label: 'Transformations Applied', value: refineLog.length },
      { label: 'KPIs Generated', value: enrichOutput.kpis.length || 4 },
      { label: 'Charts Created', value: enrichOutput.charts.length || 3 },
      { label: 'Insights Extracted', value: enrichOutput.insights.length || 4 },
    ],
    [enrichOutput, refineLog],
  );

  const handleBrowseClick = () => fileInputRef.current?.click();

  const handleFileSelect = async (event) => {
    const file = event.target.files?.[0];
    if (file) {
      await ingestFile(file);
    }
  };

  const ingestFile = async (file) => {
    setError(null);
    if (file.size > 10 * 1024 * 1024) {
      setError('The selected file exceeds the 10 MB limit. Please upload a smaller dataset.');
      return;
    }
    const extension = file.name.split('.').pop()?.toLowerCase();
    if (!['csv', 'xlsx', 'xls'].includes(extension)) {
      setError('Unsupported file type. Please upload a CSV or Excel file.');
      return;
    }

    try {
      let rows = [];
      if (extension === 'csv') {
        rows = await parseCsvFile(file);
      } else {
        rows = await parseExcelFile(file);
      }

      const sanitized = sanitizeRows(rows);
      if (!sanitized.length) {
        setError('No valid rows were found in the uploaded file.');
        return;
      }

      const columns = collectColumns(sanitized);
      const profile = computeProfileStats(sanitized, columns);
      setStage('ingest');
      setRawData(sanitized);
      setRawColumns(columns);
      setFileName(file.name);
      setFileSize(formatBytes(file.size));
      setProfileStats(profile);
      setRefinedData([]);
      setRefineLog([]);
      setRefineSummary(null);
      setEnrichOutput(EMPTY_ENRICH);
      setPipelineCode('');
      setProgress({ percent: 0, message: 'Dataset parsed successfully.', stage: 'ingest' });
      setExpandedSections({ refinedPreview: false, anomalies: true, code: true });
      setSortConfig({ key: null, direction: 'asc' });
    } catch (parseError) {
      setError(parseError.message || 'We could not parse that file. Please try a different dataset.');
    } finally {
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const startRefineStage = async () => {
    if (!rawData.length) return;
    if (!apiKey) {
      setError('Missing VITE_OPENAI_API_KEY. Add your OpenAI key to .env.local before running refinement.');
      return;
    }
    setError(null);
    setStage('refine');
    setIsProcessing(true);

    try {
      const progressCleanup = runProgressSequence(REFINE_STEPS, setProgress);
      const aiResponse = await requestRefinePlan({
        apiKey,
        rawColumns,
        rawData,
        profileStats,
      });
      progressCleanup();

      const { cleanedData, log } = applyTransformations(rawData, aiResponse.transformations || [], profileStats);
      const recomputedProfile = computeProfileStats(cleanedData, collectColumns(cleanedData));
      setRefinedData(cleanedData);
      setRefineLog(log);
      setRefineSummary({
        ...(aiResponse.summary || {}),
        rowsBefore: rawData.length,
        rowsAfter: cleanedData.length,
        nullsBefore: beforeMetrics.nulls,
        nullsAfter: recomputedProfile.totalNulls,
        qualityBefore: Math.round(profileStats.qualityScore),
        qualityAfter: Math.round(recomputedProfile.qualityScore),
        totalTransformations: log.length,
      });
      setProgress({ percent: 100, message: 'Refinement complete.', stage: 'refine' });
      setIsProcessing(false);
    } catch (refineError) {
      setIsProcessing(false);
      setProgress({ percent: 0, message: 'Refinement failed.', stage: 'refine' });
      setError(refineError.message || 'We could not complete the refinement stage.');
    }
  };

  const startEnrichStage = async () => {
    if (!refinedData.length) return;
    if (!apiKey) {
      setError('Missing VITE_OPENAI_API_KEY. Add your OpenAI key to .env.local before generating analytics.');
      return;
    }
    setError(null);
    setStage('enrich');
    setIsProcessing(true);

    try {
      const progressCleanup = runProgressSequence(ENRICH_STEPS, setProgress);
      const aiResponse = await requestEnrichInsights({
        apiKey,
        refinedData,
        rawColumns: collectColumns(refinedData),
      });
      progressCleanup();

      const normalized = normalizeEnrichOutput(aiResponse, refinedData);
      setEnrichOutput(normalized);
      setPipelineCode(normalized.pipelineCode);
      setProgress({ percent: 100, message: 'Analytics generated.', stage: 'enrich' });
      setIsProcessing(false);
    } catch (enrichError) {
      setIsProcessing(false);
      setProgress({ percent: 0, message: 'Analytics generation failed.', stage: 'enrich' });
      setError(enrichError.message || 'We could not complete the enrich stage.');
    }
  };

  const moveToDeploy = () => {
    setError(null);
    setStage('deploy');
  };

  const resetPipeline = () => {
    setStage('ingest');
    setRawData([]);
    setRawColumns([]);
    setFileName('');
    setFileSize('');
    setProfileStats(DEFAULT_PROFILE);
    setRefinedData([]);
    setRefineLog([]);
    setRefineSummary(null);
    setEnrichOutput(EMPTY_ENRICH);
    setPipelineCode('');
    setIsProcessing(false);
    setProgress({ percent: 0, message: 'Ready', stage: 'ingest' });
    setError(null);
    setExpandedSections({ refinedPreview: false, anomalies: true, code: true });
    setSortConfig({ key: null, direction: 'asc' });
    setDragActive(false);
  };

  const onDrop = async (event) => {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (file) {
      await ingestFile(file);
    }
  };

  const toggleSection = (key) => {
    setExpandedSections((current) => ({ ...current, [key]: !current[key] }));
  };

  const handleSort = (key) => {
    setSortConfig((current) => ({
      key,
      direction: current.key === key && current.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  const downloadCsv = () => {
    if (!refinedData.length) return;
    const csv = Papa.unparse(refinedData);
    downloadTextFile(csv, `${stripExtension(fileName)}_refined.csv`, 'text/csv;charset=utf-8;');
  };

  const downloadPipeline = () => {
    if (!pipelineCode) return;
    downloadTextFile(pipelineCode, `${stripExtension(fileName)}_pipeline.py`, 'text/x-python;charset=utf-8;');
  };

  const downloadReport = () => {
    downloadTextFile(reportContent, `${stripExtension(fileName)}_analytics_report.md`, 'text/markdown;charset=utf-8;');
  };

  const copyCode = async () => {
    if (!pipelineCode) return;
    try {
      await navigator.clipboard.writeText(pipelineCode);
      setCopiedCode(true);
    } catch {
      setError('Clipboard access was blocked by the browser. You can still download the pipeline script.');
    }
  };

  return (
    <div className="min-h-screen bg-[#07080D] text-slate-200">
      <style>{`
        @keyframes pulseGlow {
          0%, 100% { box-shadow: 0 0 0 0 rgba(129, 140, 248, 0.4), 0 0 28px rgba(129, 140, 248, 0.18); }
          50% { box-shadow: 0 0 0 10px rgba(129, 140, 248, 0.05), 0 0 42px rgba(129, 140, 248, 0.28); }
        }
        @keyframes shimmer {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
        @keyframes flowDot {
          0% { transform: translateX(-6px); opacity: 0; }
          20% { opacity: 1; }
          100% { transform: translateX(calc(100% + 6px)); opacity: 0; }
        }
        @keyframes fadeUp {
          0% { transform: translateY(10px); opacity: 0; }
          100% { transform: translateY(0); opacity: 1; }
        }
      `}</style>

      <PipelineNavigator
        stage={stage}
        completionPercent={completionPercent}
        canJumpToStage={canJumpToStage}
        onJump={setStage}
      />

      <main className="mx-auto flex w-full max-w-7xl flex-col gap-8 px-4 pb-16 pt-8 sm:px-6 lg:px-8">
        {error ? (
          <div className="rounded-[20px] border border-rose-400/30 bg-rose-500/10 px-5 py-4 text-sm text-rose-100">
            {error}
          </div>
        ) : null}

        {stage === 'ingest' && (
          <section className="animate-[fadeUp_0.3s_ease]">
            <div className="mx-auto max-w-5xl rounded-[20px] border border-[#1F2237] bg-[#0F1019]/95 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.28)] sm:p-8">
              <div className="mb-8 text-center">
                <div className="mx-auto mb-5 flex h-16 w-16 items-center justify-center rounded-[20px] bg-gradient-to-br from-indigo-400 via-violet-400 to-cyan-300 text-2xl font-semibold text-[#07080D] shadow-[0_0_40px_rgba(129,140,248,0.32)]">
                  DF
                </div>
                <h1 className="text-3xl font-semibold tracking-tight text-slate-100 sm:text-4xl">DataLoom</h1>
                <p className="mt-3 text-base text-slate-400 sm:text-lg">Upload. Transform. Analyze. Deploy.</p>
              </div>

              <div
                onDragEnter={(event) => {
                  event.preventDefault();
                  setDragActive(true);
                }}
                onDragOver={(event) => event.preventDefault()}
                onDragLeave={(event) => {
                  event.preventDefault();
                  setDragActive(false);
                }}
                onDrop={onDrop}
                className={`rounded-[20px] border-2 border-dashed px-6 py-14 text-center transition-all duration-200 ${
                  dragActive
                    ? 'border-indigo-400 bg-indigo-500/8 shadow-[0_0_40px_rgba(129,140,248,0.16)]'
                    : 'border-[#2E3354] bg-[#161825]/60 hover:-translate-y-0.5 hover:border-indigo-400/60'
                }`}
              >
                <div className="mx-auto mb-5 flex h-14 w-14 items-center justify-center rounded-full border border-indigo-400/30 bg-indigo-400/10 text-2xl text-indigo-200">
                  ☁
                </div>
                <h2 className="text-xl font-semibold text-slate-100">Drop your dataset here</h2>
                <p className="mt-3 text-sm text-slate-400">
                  or{' '}
                  <button type="button" onClick={handleBrowseClick} className="text-indigo-300 underline underline-offset-4">
                    browse files
                  </button>
                </p>
                <p className="mt-5 text-xs uppercase tracking-[0.24em] text-slate-500">CSV, XLSX, XLS • Up to 10MB</p>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  className="hidden"
                  onChange={handleFileSelect}
                />
              </div>

              {rawData.length ? (
                <div className="mt-8 space-y-6">
                  <FileMetadataBar
                    fileName={fileName}
                    fileSize={fileSize}
                    rowCount={rawData.length}
                    columnCount={rawColumns.length}
                  />

                  <div className="grid gap-6 lg:grid-cols-[260px_minmax(0,1fr)]">
                    <HealthGauge score={profileStats.qualityScore} />
                    <ColumnProfiles columns={rawColumns} profileStats={profileStats} data={rawData} />
                  </div>

                  <PreviewTable
                    columns={rawColumns}
                    data={sortedPreviewData}
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    highlightNulls
                  />
                </div>
              ) : null}

              <div className="mt-8 flex justify-end">
                <ActionButton onClick={startRefineStage} disabled={!rawData.length || isProcessing}>
                  Begin Refinement →
                </ActionButton>
              </div>
            </div>
          </section>
        )}

        {stage === 'refine' && (
          <section className="animate-[fadeUp_0.3s_ease] space-y-6">
            {isProcessing ? (
              <ProgressPanel progress={progress} title="AI Refinement In Progress" />
            ) : (
              <>
                <div className="grid gap-6 md:grid-cols-2">
                  <MetricBoard title="Before" metrics={beforeMetrics} tone="before" />
                  <MetricBoard
                    title="After"
                    metrics={
                      refineSummary
                        ? {
                            rows: refineSummary.rowsAfter,
                            nulls: refineSummary.nullsAfter,
                            quality: refineSummary.qualityAfter,
                          }
                        : afterMetrics
                    }
                    tone="after"
                  />
                </div>

                <TimelineSection refineLog={refineLog} />

                <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
                  <button
                    type="button"
                    onClick={() => toggleSection('refinedPreview')}
                    className="flex w-full items-center justify-between text-left"
                  >
                    <div>
                      <h3 className="text-lg font-semibold text-slate-100">Preview Refined Data</h3>
                      <p className="mt-1 text-sm text-slate-400">Inspect the first 15 rows after transformations were applied.</p>
                    </div>
                    <span className="rounded-full border border-[#2E3354] px-3 py-1 text-xs text-slate-300">
                      {expandedSections.refinedPreview ? 'Hide' : 'Show'}
                    </span>
                  </button>

                  {expandedSections.refinedPreview ? (
                    <div className="mt-5">
                      <PreviewTable
                        columns={collectColumns(refinedData)}
                        data={refinedData.slice(0, 15)}
                        sortConfig={sortConfig}
                        onSort={handleSort}
                        highlightNulls={false}
                      />
                    </div>
                  ) : null}
                </div>

                <div className="flex justify-end">
                  <ActionButton onClick={startEnrichStage} disabled={!refinedData.length}>
                    Generate Analytics →
                  </ActionButton>
                </div>
              </>
            )}
          </section>
        )}

        {stage === 'enrich' && (
          <section className="animate-[fadeUp_0.3s_ease] space-y-6">
            {isProcessing ? (
              <ProgressPanel progress={progress} title="AI Enrichment In Progress" />
            ) : (
              <>
                <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-4">
                  {enrichOutput.kpis.map((kpi, index) => (
                    <KpiCard key={`${kpi.label}-${index}`} kpi={kpi} />
                  ))}
                </div>

                <div className="grid gap-6 xl:grid-cols-3">
                  {enrichOutput.charts.map((chart, index) => (
                    <ChartCard key={`${chart.title}-${index}`} chart={chart} fallbackData={refinedData} />
                  ))}
                </div>

                <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
                  <button
                    type="button"
                    onClick={() => toggleSection('anomalies')}
                    className="flex w-full items-center justify-between text-left"
                  >
                    <div>
                      <h3 className="text-lg font-semibold text-slate-100">Anomalies Detected</h3>
                      <p className="mt-1 text-sm text-slate-400">Flagged irregularities identified from the refined dataset.</p>
                    </div>
                    <span className="rounded-full border border-[#2E3354] px-3 py-1 text-xs text-slate-300">
                      {expandedSections.anomalies ? 'Collapse' : 'Expand'}
                    </span>
                  </button>

                  {expandedSections.anomalies ? (
                    <div className="mt-5 space-y-3">
                      {enrichOutput.anomalies.length ? (
                        enrichOutput.anomalies.map((anomaly, index) => (
                          <div
                            key={`${anomaly.column}-${index}`}
                            className="rounded-2xl border border-[#1F2237] bg-[#161825] px-4 py-3"
                          >
                            <div className="flex flex-wrap items-center gap-3">
                              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${severityClasses(anomaly.severity)}`}>
                                {anomaly.severity}
                              </span>
                              <span className="font-medium text-slate-100">{anomaly.column}</span>
                            </div>
                            <p className="mt-2 text-sm text-slate-400">{anomaly.description}</p>
                          </div>
                        ))
                      ) : (
                        <div className="rounded-2xl border border-[#1F2237] bg-[#161825] px-4 py-3 text-sm text-slate-400">
                          No significant anomalies were detected in the available sample.
                        </div>
                      )}
                    </div>
                  ) : null}
                </div>

                <div className="grid gap-5 xl:grid-cols-2">
                  {enrichOutput.insights.map((insight, index) => (
                    <InsightCard key={`${insight.title}-${index}`} insight={insight} />
                  ))}
                </div>

                <div className="flex justify-end">
                  <ActionButton onClick={moveToDeploy} disabled={!enrichOutput.kpis.length}>
                    Prepare Deployment →
                  </ActionButton>
                </div>
              </>
            )}
          </section>
        )}

        {stage === 'deploy' && (
          <section className="animate-[fadeUp_0.3s_ease] space-y-6">
            <div className="rounded-[20px] border border-emerald-400/20 bg-[#0F1019] p-6">
              <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-emerald-400/30 bg-emerald-500/10 px-4 py-2 text-sm font-medium text-emerald-200">
                <span className="text-base">✓</span>
                Pipeline Complete
              </div>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                {deployStats.map((stat) => (
                  <div key={stat.label} className="rounded-2xl border border-[#1F2237] bg-[#161825] p-4">
                    <p className="text-sm text-slate-400">{stat.label}</p>
                    <p className="mt-3 font-['JetBrains_Mono'] text-3xl font-semibold text-slate-100">{stat.value}</p>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
              <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h3 className="text-lg font-semibold text-slate-100">Pipeline Code Viewer</h3>
                  <p className="mt-1 text-sm text-slate-400">Full PySpark Bronze → Silver → Gold pipeline generated from the workflow.</p>
                </div>
                <div className="flex gap-3">
                  <button
                    type="button"
                    onClick={() => toggleSection('code')}
                    className="rounded-[14px] border border-[#2E3354] px-4 py-2 text-sm text-slate-200 transition hover:-translate-y-0.5 hover:border-indigo-400/60"
                  >
                    {expandedSections.code ? 'Hide Code' : 'Show Code'}
                  </button>
                  <button
                    type="button"
                    onClick={copyCode}
                    className="rounded-[14px] border border-indigo-400/40 bg-indigo-500/10 px-4 py-2 text-sm text-indigo-100 transition hover:-translate-y-0.5 hover:bg-indigo-500/20"
                  >
                    {copiedCode ? 'Copied' : 'Copy to Clipboard'}
                  </button>
                </div>
              </div>

              {expandedSections.code ? (
                <div className="overflow-hidden rounded-[18px] border border-[#1F2237] bg-[#161825]">
                  <div className="max-h-[480px] overflow-auto">
                    <table className="w-full border-collapse font-['JetBrains_Mono'] text-sm">
                      <tbody>
                        {codeLines.map((line, index) => (
                          <tr key={`${index}-${line}`}>
                            <td className="w-12 border-r border-[#1F2237] bg-[#10131F] px-3 py-1.5 text-right text-[#4A5275]">
                              {index + 1}
                            </td>
                            <td className="px-4 py-1.5 text-slate-200">
                              <span dangerouslySetInnerHTML={{ __html: syntaxHighlight(line) }} />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="grid gap-6 xl:grid-cols-3">
              <ExportCard
                title="Refined Dataset"
                description="Download your cleaned, transformed dataset."
                meta={`${refinedData.length} rows • ${collectColumns(refinedData).length} columns`}
                cta="Download CSV →"
                onClick={downloadCsv}
              />
              <ExportCard
                title="Pipeline Script"
                description="Full PySpark Bronze→Silver→Gold pipeline."
                meta={`${codeLines.length} lines`}
                cta="Download .py →"
                onClick={downloadPipeline}
              />
              <ExportCard
                title="Analytics Report"
                description="Complete analysis with KPIs, insights, anomalies, and transformation history."
                meta={`${reportContent.split('\n').length} lines`}
                cta="Download Report →"
                onClick={downloadReport}
              />
            </div>

            <div className="flex justify-end">
              <button
                type="button"
                onClick={resetPipeline}
                className="rounded-[14px] border border-[#2E3354] bg-[#161825] px-5 py-3 font-medium text-slate-100 transition hover:-translate-y-0.5 hover:border-indigo-400/60 hover:shadow-[0_18px_48px_rgba(0,0,0,0.25)]"
              >
                Analyze Another Dataset
              </button>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

function PipelineNavigator({ stage, completionPercent, canJumpToStage, onJump }) {
  return (
    <div className="sticky top-0 z-30 border-b border-[#1F2237] bg-[#07080D]/85 backdrop-blur-xl">
      <div className="mx-auto max-w-7xl px-4 py-5 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between gap-3 overflow-x-auto pb-3">
          {STAGES.map((item, index) => {
            const currentIndex = STAGES.indexOf(stage);
            const itemIndex = STAGES.indexOf(item);
            const completed = itemIndex < currentIndex;
            const active = item === stage;
            const interactive = canJumpToStage(item);

            return (
              <div key={item} className="flex min-w-[120px] flex-1 items-center gap-3">
                <button
                  type="button"
                  onClick={() => interactive && onJump(item)}
                  className={`group flex flex-col items-center gap-2 text-center transition ${
                    interactive ? 'cursor-pointer' : 'cursor-default'
                  }`}
                >
                  <span
                    className={`flex h-11 w-11 items-center justify-center rounded-full border text-sm font-semibold transition ${
                      active
                        ? 'border-indigo-300 bg-indigo-500/20 text-indigo-100'
                        : completed
                          ? 'border-emerald-300/50 bg-emerald-500/20 text-emerald-100'
                          : 'border-[#2E3354] bg-[#0F1019] text-slate-500'
                    }`}
                    style={active ? { animation: 'pulseGlow 2.4s infinite' } : undefined}
                  >
                    {completed ? '✓' : index + 1}
                  </span>
                  <span className={`text-sm font-medium ${active || completed ? 'text-slate-100' : 'text-slate-500'}`}>
                    {STAGE_LABELS[item]}
                  </span>
                </button>

                {index < STAGES.length - 1 ? (
                  <div className="relative hidden h-px flex-1 overflow-hidden rounded-full border-t border-dashed border-[#2E3354] md:block">
                    {completed ? (
                      <>
                        <div className="absolute inset-y-0 left-0 w-full bg-gradient-to-r from-indigo-400/20 via-violet-400/20 to-cyan-300/20" />
                        <div
                          className="absolute top-1/2 h-2 w-2 -translate-y-1/2 rounded-full bg-cyan-300 shadow-[0_0_18px_rgba(103,232,249,0.65)]"
                          style={{ animation: 'flowDot 1.6s linear infinite' }}
                        />
                      </>
                    ) : null}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
        <div className="h-1.5 overflow-hidden rounded-full bg-[#161825]">
          <div
            className="h-full rounded-full bg-gradient-to-r from-indigo-400 via-violet-400 to-cyan-300 transition-all duration-500"
            style={{ width: `${completionPercent}%` }}
          />
        </div>
      </div>
    </div>
  );
}

function FileMetadataBar({ fileName, fileSize, rowCount, columnCount }) {
  const items = [
    { label: 'File', value: fileName },
    { label: 'Rows', value: rowCount },
    { label: 'Columns', value: columnCount },
    { label: 'Size', value: fileSize },
  ];

  return (
    <div className="grid gap-4 rounded-[20px] border border-[#1F2237] bg-[#161825] p-5 md:grid-cols-4">
      {items.map((item) => (
        <div key={item.label}>
          <p className="text-xs uppercase tracking-[0.18em] text-slate-500">{item.label}</p>
          <p className="mt-2 truncate text-sm font-medium text-slate-100">{item.value}</p>
        </div>
      ))}
    </div>
  );
}

function HealthGauge({ score }) {
  const tone = score > 90 ? '#34D399' : score >= 70 ? '#FBBF24' : '#F87171';
  const angle = Math.max(0, Math.min(100, score)) * 3.6;
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#161825] p-6">
      <p className="text-sm uppercase tracking-[0.2em] text-slate-500">Data Health</p>
      <div className="mt-6 flex items-center justify-center">
        <div
          className="relative flex h-48 w-48 items-center justify-center rounded-full"
          style={{
            background: `conic-gradient(${tone} ${angle}deg, rgba(74,82,117,0.3) ${angle}deg 360deg)`,
          }}
        >
          <div className="absolute inset-[14px] rounded-full bg-[#0F1019]" />
          <div className="relative text-center">
            <p className="text-xs uppercase tracking-[0.24em] text-slate-500">Data Health</p>
            <p className="mt-2 font-['JetBrains_Mono'] text-4xl font-semibold text-slate-100">{Math.round(score)}%</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function ColumnProfiles({ columns, profileStats, data }) {
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#161825] p-6">
      <div className="mb-5 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">Column Profiles</h3>
        <span className="rounded-full border border-[#2E3354] px-3 py-1 text-xs text-slate-300">{columns.length} columns</span>
      </div>
      <div className="space-y-4">
        {columns.map((column) => {
          const type = profileStats.typesDetected[column] || 'mixed';
          const nullCount = profileStats.nullCounts[column] || 0;
          const nullPct = data.length ? (nullCount / data.length) * 100 : 0;
          const numeric = profileStats.minMax[column];
          const stringSummary = profileStats.stringSummary[column];

          return (
            <div key={column} className="rounded-2xl border border-[#1F2237] bg-[#0F1019] p-4">
              <div className="flex flex-wrap items-center gap-3">
                <h4 className="font-medium text-slate-100">{column}</h4>
                <span className={`rounded-full px-3 py-1 text-xs font-semibold ${typeBadgeClasses(type)}`}>{type}</span>
              </div>

              <div className="mt-4 grid gap-4 md:grid-cols-[minmax(0,1fr)_140px_220px]">
                <div>
                  <div className="flex items-center justify-between text-sm text-slate-400">
                    <span>Nulls</span>
                    <span>{nullCount} · {nullPct.toFixed(1)}%</span>
                  </div>
                  <div className="mt-2 h-2 overflow-hidden rounded-full bg-[#0B0D15]">
                    <div className="h-full rounded-full bg-gradient-to-r from-rose-400 to-amber-300" style={{ width: `${Math.min(nullPct, 100)}%` }} />
                  </div>
                </div>

                <div>
                  <p className="text-sm text-slate-400">Unique Values</p>
                  <p className="mt-2 font-['JetBrains_Mono'] text-xl text-slate-100">{profileStats.uniqueCounts[column] ?? 0}</p>
                </div>

                <div className="text-sm text-slate-400">
                  {type === 'number' && numeric ? (
                    <div className="grid grid-cols-3 gap-2">
                      <MiniMetric label="Min" value={formatNumber(numeric.min)} />
                      <MiniMetric label="Max" value={formatNumber(numeric.max)} />
                      <MiniMetric label="Mean" value={formatNumber(numeric.mean)} />
                    </div>
                  ) : (
                    <div className="rounded-xl border border-[#1F2237] bg-[#161825] px-3 py-2">
                      <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Most Frequent</p>
                      <p className="mt-2 truncate text-slate-100">{stringSummary?.value ?? '—'}</p>
                      <p className="mt-1 text-xs text-slate-500">{stringSummary?.count ? `${stringSummary.count} occurrences` : 'No dominant value'}</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function MiniMetric({ label, value }) {
  return (
    <div className="rounded-xl border border-[#1F2237] bg-[#161825] px-3 py-2">
      <p className="text-xs uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-2 text-slate-100">{value}</p>
    </div>
  );
}

function PreviewTable({ columns, data, sortConfig, onSort, highlightNulls }) {
  return (
    <div className="overflow-hidden rounded-[20px] border border-[#1F2237] bg-[#161825]">
      <div className="overflow-auto">
        <table className="min-w-full border-collapse text-sm">
          <thead className="sticky top-0 bg-[#0F1019] text-left">
            <tr>
              {columns.map((column) => (
                <th key={column} className="border-b border-[#1F2237] px-4 py-3 font-medium text-slate-200">
                  <button type="button" onClick={() => onSort(column)} className="flex items-center gap-2">
                    <span>{column}</span>
                    {sortConfig.key === column ? <span className="text-xs text-indigo-300">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span> : null}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIndex) => (
              <tr key={rowIndex} className="border-b border-[#1F2237]/70 last:border-b-0">
                {columns.map((column) => {
                  const value = row?.[column];
                  const isNullish = isNullishValue(value);
                  const numeric = isNumericString(value);
                  return (
                    <td
                      key={`${rowIndex}-${column}`}
                      className={`px-4 py-3 align-top ${
                        highlightNulls && isNullish ? 'bg-amber-300/10 text-amber-100' : 'text-slate-300'
                      } ${numeric ? 'text-right font-[' + "'JetBrains_Mono'" + ']' : 'text-left'}`}
                    >
                      {formatCellValue(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ActionButton({ children, disabled, onClick }) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className="rounded-[14px] bg-gradient-to-r from-indigo-400 via-violet-400 to-cyan-300 px-5 py-3 font-medium text-[#07080D] transition duration-200 hover:-translate-y-0.5 hover:shadow-[0_18px_48px_rgba(129,140,248,0.28)] disabled:cursor-not-allowed disabled:opacity-45 disabled:hover:translate-y-0"
    >
      {children}
    </button>
  );
}

function ProgressPanel({ progress, title }) {
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
      <div className="mb-6 flex items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-slate-100">{title}</h2>
          <p className="mt-2 text-sm text-slate-400">{progress.message} {progress.percent}%</p>
        </div>
        <div className="rounded-full border border-[#2E3354] px-4 py-2 font-['JetBrains_Mono'] text-xl text-indigo-200">
          {progress.percent}%
        </div>
      </div>
      <div className="h-3 overflow-hidden rounded-full bg-[#161825]">
        <div
          className="h-full rounded-full bg-[linear-gradient(90deg,#818CF8,#A78BFA,#67E8F9,#818CF8)] bg-[length:200%_100%]"
          style={{ width: `${Math.max(3, progress.percent)}%`, animation: 'shimmer 2.4s linear infinite' }}
        />
      </div>
    </div>
  );
}

function MetricBoard({ title, metrics, tone }) {
  const toneClasses =
    tone === 'before'
      ? 'border-amber-400/20 bg-gradient-to-br from-amber-500/6 to-rose-500/6'
      : 'border-emerald-400/20 bg-gradient-to-br from-emerald-500/8 to-cyan-500/6';
  return (
    <div className={`rounded-[20px] border ${toneClasses} bg-[#0F1019] p-6`}>
      <div className="mb-6 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">{title}</h3>
        <span className="rounded-full border border-[#2E3354] px-3 py-1 text-xs text-slate-300">{title === 'Before' ? 'Raw' : 'Refined'}</span>
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        <AnimatedMetric label="Rows" value={metrics.rows ?? 0} />
        <AnimatedMetric label="Nulls" value={metrics.nulls ?? 0} />
        <AnimatedMetric label="Quality" value={`${Math.round(metrics.quality ?? 0)}%`} />
      </div>
    </div>
  );
}

function AnimatedMetric({ label, value }) {
  return (
    <div className="rounded-2xl border border-[#1F2237] bg-[#161825] p-4">
      <p className="text-sm text-slate-400">{label}</p>
      <p className="mt-3 font-['JetBrains_Mono'] text-3xl font-semibold text-slate-100">{value}</p>
    </div>
  );
}

function TimelineSection({ refineLog }) {
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
      <div className="mb-6 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-slate-100">Transformation Timeline</h3>
        <span className="rounded-full border border-indigo-400/30 bg-indigo-500/10 px-3 py-1 text-xs text-indigo-200">
          {refineLog.length} transformations
        </span>
      </div>

      <div className="relative space-y-5 pl-8 before:absolute before:left-[15px] before:top-2 before:h-[calc(100%-1rem)] before:w-px before:bg-[#2E3354]">
        {refineLog.map((entry, index) => {
          const icon = iconMap[entry.action] || iconMap.fix_type;
          return (
            <div
              key={`${entry.action}-${entry.column}-${index}`}
              className="relative animate-[fadeUp_0.35s_ease] rounded-[20px] border border-[#1F2237] bg-[#161825] p-5"
              style={{ animationDelay: `${index * 150}ms` }}
            >
              <div className={`absolute -left-8 top-6 flex h-8 w-8 items-center justify-center rounded-full border ${icon.border} ${icon.bg} ${icon.color}`}>
                {entry.icon || icon.symbol}
              </div>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="flex flex-wrap items-center gap-3">
                    <span className={`rounded-full px-3 py-1 text-xs font-semibold ${icon.bg} ${icon.color}`}>
                      {icon.label}
                    </span>
                    <h4 className="font-medium text-slate-100">{entry.column}</h4>
                  </div>
                  <p className="mt-3 text-sm text-slate-400">{entry.before} → {entry.after}</p>
                  {entry.detail ? <p className="mt-2 text-sm text-slate-500">{entry.detail}</p> : null}
                </div>
                <span className="rounded-full border border-[#2E3354] px-3 py-1 text-xs text-slate-300">
                  {entry.rowsAffected ?? 0} rows
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function KpiCard({ kpi }) {
  const isPositive = kpi.changeType === 'positive';
  const isNegative = kpi.changeType === 'negative';
  const borderTone = isPositive ? 'border-emerald-400/40' : isNegative ? 'border-rose-400/40' : 'border-sky-400/40';
  const badgeTone = isPositive
    ? 'bg-emerald-500/10 text-emerald-200'
    : isNegative
      ? 'bg-rose-500/10 text-rose-200'
      : 'bg-sky-500/10 text-sky-200';
  const prefix = isPositive ? '↗' : isNegative ? '↘' : '•';

  return (
    <div title={kpi.description} className={`rounded-[20px] border ${borderTone} bg-[#0F1019] p-5 transition hover:-translate-y-0.5 hover:shadow-[0_18px_48px_rgba(0,0,0,0.24)]`}>
      <div className="mb-5 flex items-start justify-between gap-3">
        <p className="text-sm text-slate-400">{kpi.label}</p>
        <span className={`rounded-full px-3 py-1 text-xs font-semibold ${badgeTone}`}>{prefix} {kpi.change}</span>
      </div>
      <p className="font-['JetBrains_Mono'] text-3xl font-semibold text-slate-100">{kpi.value}</p>
      <div className="mt-5 h-1.5 rounded-full bg-[#161825]">
        <div className={`h-full rounded-full ${isPositive ? 'bg-emerald-400' : isNegative ? 'bg-rose-400' : 'bg-sky-400'}`} style={{ width: `${Math.min(100, 55 + Math.abs(parseFloat(kpi.change)) || 0)}%` }} />
      </div>
    </div>
  );
}

function ChartCard({ chart, fallbackData }) {
  const data = Array.isArray(chart.data) && chart.data.length ? chart.data : buildFallbackChartData(fallbackData, chart.type);
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-5 transition hover:-translate-y-0.5 hover:shadow-[0_18px_48px_rgba(0,0,0,0.24)]">
      <h3 className="text-lg font-semibold text-slate-100">{chart.title}</h3>
      <p className="mt-2 text-sm text-slate-400">{chart.description}</p>
      <div className="mt-6 h-72">
        {chart.type === 'bar' && (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <defs>
                <linearGradient id={`barGradient-${chart.title}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={chart.color || '#818CF8'} stopOpacity={0.95} />
                  <stop offset="100%" stopColor={chart.color || '#818CF8'} stopOpacity={0.35} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#1F2237" vertical={false} />
              <XAxis dataKey={chart.xKey || 'name'} tick={{ fill: '#8892B0', fontSize: 12 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#8892B0', fontSize: 12 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: '#161825', border: '1px solid #1F2237', borderRadius: 16 }} />
              <Bar dataKey={chart.yKey || 'value'} radius={[10, 10, 0, 0]} fill={`url(#barGradient-${chart.title})`} animationDuration={800} />
            </BarChart>
          </ResponsiveContainer>
        )}

        {chart.type === 'pie' && (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Tooltip contentStyle={{ background: '#161825', border: '1px solid #1F2237', borderRadius: 16 }} />
              <Pie
                data={data}
                dataKey="value"
                nameKey="name"
                innerRadius={60}
                outerRadius={94}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                animationDuration={800}
              >
                {data.map((entry, index) => (
                  <Cell key={`${entry.name}-${index}`} fill={chart.colors?.[index % (chart.colors?.length || CHART_COLORS.length)] || CHART_COLORS[index % CHART_COLORS.length]} />
                ))}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
        )}

        {chart.type === 'area' && (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data}>
              <defs>
                <linearGradient id={`areaGradient-${chart.title}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={chart.color || '#67E8F9'} stopOpacity={0.65} />
                  <stop offset="100%" stopColor={chart.color || '#67E8F9'} stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#1F2237" vertical={false} />
              <XAxis dataKey={chart.xKey || 'name'} tick={{ fill: '#8892B0', fontSize: 12 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#8892B0', fontSize: 12 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: '#161825', border: '1px solid #1F2237', borderRadius: 16 }} />
              <Area
                type="monotone"
                dataKey={chart.yKey || 'value'}
                stroke={chart.color || '#67E8F9'}
                fill={`url(#areaGradient-${chart.title})`}
                strokeWidth={3}
                animationDuration={800}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}

        {chart.type === 'radar' && (
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={data}>
              <PolarGrid stroke="#1F2237" />
              <PolarAngleAxis dataKey={chart.xKey || 'name'} tick={{ fill: '#8892B0', fontSize: 12 }} />
              <PolarRadiusAxis tick={{ fill: '#4A5275', fontSize: 10 }} />
              <Radar
                name={chart.title}
                dataKey={chart.yKey || 'value'}
                stroke={chart.color || '#A78BFA'}
                fill={chart.color || '#A78BFA'}
                fillOpacity={0.35}
                animationDuration={800}
              />
              <Tooltip contentStyle={{ background: '#161825', border: '1px solid #1F2237', borderRadius: 16 }} />
            </RadarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

function InsightCard({ insight }) {
  const accent = insightTypeClasses(insight.type);
  return (
    <div className={`rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-5 ${accent.wrapper}`}>
      <div className="mb-4 flex items-start justify-between gap-3">
        <h3 className="text-lg font-semibold text-slate-100">{insight.title}</h3>
        <span className={`rounded-full px-3 py-1 text-xs font-semibold ${accent.badge}`}>{insight.type}</span>
      </div>
      <p className="text-sm leading-6 text-slate-400">{insight.detail}</p>
    </div>
  );
}

function ExportCard({ title, description, meta, cta, onClick }) {
  return (
    <div className="rounded-[20px] border border-[#1F2237] bg-[#0F1019] p-6">
      <h3 className="text-lg font-semibold text-slate-100">{title}</h3>
      <p className="mt-2 text-sm text-slate-400">{description}</p>
      <p className="mt-4 font-['JetBrains_Mono'] text-sm text-slate-300">{meta}</p>
      <button
        type="button"
        onClick={onClick}
        className="mt-6 rounded-[14px] border border-indigo-400/35 bg-indigo-500/10 px-4 py-2 text-sm font-medium text-indigo-100 transition hover:-translate-y-0.5 hover:bg-indigo-500/20"
      >
        {cta}
      </button>
    </div>
  );
}

async function parseCsvFile(file) {
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      complete: (result) => resolve(result.data || []),
      error: () => reject(new Error('Unable to parse the CSV file.')),
    });
  });
}

async function parseExcelFile(file) {
  const buffer = await file.arrayBuffer();
  const workbook = XLSX.read(buffer, { type: 'array' });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

function sanitizeRows(rows) {
  return rows
    .map((row) => {
      if (typeof row !== 'object' || row === null || Array.isArray(row)) return null;
      const cleaned = {};
      Object.entries(row).forEach(([key, value]) => {
        const safeKey = String(key || '').trim();
        if (!safeKey) return;
        cleaned[safeKey] = normalizeRawValue(value);
      });
      return Object.keys(cleaned).length ? cleaned : null;
    })
    .filter(Boolean);
}

function normalizeRawValue(value) {
  if (value === undefined) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
  }
  return value;
}

function collectColumns(rows) {
  return Array.from(
    rows.reduce((set, row) => {
      Object.keys(row || {}).forEach((key) => set.add(key));
      return set;
    }, new Set()),
  );
}

function computeProfileStats(rows, columns) {
  const nullCounts = {};
  const typesDetected = {};
  const uniqueCounts = {};
  const minMax = {};
  const stringSummary = {};
  let totalNulls = 0;

  columns.forEach((column) => {
    const values = rows.map((row) => row?.[column]);
    const nonNull = values.filter((value) => !isNullishValue(value));
    const valueTypes = new Set(nonNull.map(detectValueType));
    const type = valueTypes.size === 0 ? 'mixed' : valueTypes.size === 1 ? valueTypes.values().next().value : 'mixed';
    const unique = new Set(nonNull.map((value) => String(value))).size;
    const nullCount = values.length - nonNull.length;
    nullCounts[column] = nullCount;
    uniqueCounts[column] = unique;
    typesDetected[column] = type;
    totalNulls += nullCount;

    if (type === 'number') {
      const numerics = nonNull.map((value) => toNumber(value)).filter((value) => Number.isFinite(value));
      if (numerics.length) {
        const total = numerics.reduce((sum, value) => sum + value, 0);
        minMax[column] = {
          min: Math.min(...numerics),
          max: Math.max(...numerics),
          mean: total / numerics.length,
        };
      }
    } else {
      const counts = new Map();
      nonNull.forEach((value) => {
        const key = String(value);
        counts.set(key, (counts.get(key) || 0) + 1);
      });
      const [topValue, topCount] = [...counts.entries()].sort((a, b) => b[1] - a[1])[0] || [];
      stringSummary[column] = { value: topValue || '—', count: topCount || 0 };
    }
  });

  const totalCells = rows.length * columns.length;
  const qualityScore = totalCells ? (1 - totalNulls / totalCells) * 100 : 0;

  return {
    nullCounts,
    typesDetected,
    uniqueCounts,
    minMax,
    stringSummary,
    rowCount: rows.length,
    columnCount: columns.length,
    totalNulls,
    totalCells,
    qualityScore,
  };
}

function detectValueType(value) {
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'number' && Number.isFinite(value)) return 'number';
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') return 'mixed';
    if (isNumericString(trimmed)) return 'number';
    if (isBooleanString(trimmed)) return 'boolean';
    if (isDateString(trimmed)) return 'date';
    return 'string';
  }
  return 'mixed';
}

function buildDataSummary(rows) {
  const columns = collectColumns(rows);
  const stats = computeProfileStats(rows, columns);
  return {
    rows: rows.length,
    nulls: stats.totalNulls,
    quality: stats.qualityScore,
  };
}

function runProgressSequence(steps, setProgress) {
  let index = 0;
  setProgress(steps[0]);
  const interval = window.setInterval(() => {
    index = Math.min(index + 1, steps.length - 1);
    setProgress(steps[index]);
    if (index === steps.length - 1) {
      window.clearInterval(interval);
    }
  }, 1200);

  return () => window.clearInterval(interval);
}

async function requestRefinePlan({ apiKey, rawColumns, rawData, profileStats }) {
  const sampleRows = rawData.slice(0, 8);
  const payload = {
    columns: rawColumns.map((column) => ({
      name: column,
      type: profileStats.typesDetected[column] || 'mixed',
      nullCount: profileStats.nullCounts[column] || 0,
      uniqueCount: profileStats.uniqueCounts[column] || 0,
      numericSummary: profileStats.minMax[column] || null,
      stringSummary: profileStats.stringSummary[column] || null,
    })),
    sampleRows,
  };

  const messages = [
    {
      role: 'system',
      content:
        'You are a senior data engineer performing data quality transformations. Given a dataset profile and sample rows, determine exactly what cleaning is needed and return the specific transformations applied.\n\nCRITICAL: Base your transformations ONLY on the actual data provided. Do not invent issues that do not exist. If a column has 0 nulls, do NOT generate a fill_null action for it. Be precise.\n\nRespond ONLY with valid JSON (no markdown, no backticks, use \\n for newlines in strings):\n{\n  "transformations": [\n    {\n      "action": "fill_null",\n      "column": "actual_column_name",\n      "before": "12 null values (8.3%)",\n      "after": "0 null values — filled with median 45.2",\n      "rowsAffected": 12\n    },\n    {\n      "action": "rename",\n      "column": "messy Name",\n      "before": "messy Name",\n      "after": "messy_name",\n      "rowsAffected": 0\n    },\n    {\n      "action": "fix_type",\n      "column": "revenue",\n      "before": "string with $ symbols",\n      "after": "float64 numeric",\n      "rowsAffected": 145\n    },\n    {\n      "action": "remove_outlier",\n      "column": "age",\n      "before": "Contains value 999",\n      "after": "Capped at 3 standard deviations",\n      "rowsAffected": 2\n    },\n    {\n      "action": "deduplicate",\n      "column": "all",\n      "before": "150 total rows",\n      "after": "147 unique rows — removed 3 duplicates",\n      "rowsAffected": 3\n    }\n  ],\n  "summary": {\n    "totalTransformations": 5,\n    "rowsBefore": 150,\n    "rowsAfter": 147,\n    "nullsBefore": 24,\n    "nullsAfter": 0,\n    "qualityBefore": 84,\n    "qualityAfter": 100\n  }\n}\n\nOnly include transformations that are actually needed based on the data profile provided. Be accurate with numbers.',
    },
    {
      role: 'user',
      content: JSON.stringify(payload),
    },
  ];

  const parsed = await callOpenAiJson(apiKey, messages);
  return {
    transformations: Array.isArray(parsed.transformations) ? parsed.transformations : [],
    summary: parsed.summary || {},
  };
}

async function requestEnrichInsights({ apiKey, refinedData, rawColumns }) {
  const summaryStats = computeProfileStats(refinedData, rawColumns);
  const payload = {
    columns: rawColumns,
    summaryStats,
    sampleRows: refinedData.slice(0, 10),
  };

  const messages = [
    {
      role: 'system',
      content:
        'You are a senior data analyst and visualization expert. Given a cleaned dataset, generate a complete analytics layer with KPIs, charts, insights, and anomaly detection.\n\nCRITICAL: All numbers, percentages, and values MUST be computed from the actual data provided. Do not hallucinate statistics. Use the sample data to derive accurate metrics. If you cannot compute an exact value, provide your best estimate and note it.\n\nRespond ONLY with valid JSON (no markdown, no backticks, use \\n for newlines in strings):\n{\n  "kpis": [\n    { "label": "descriptive label", "value": "formatted value", "change": "+12.5%", "changeType": "positive", "description": "one line explaining this KPI" },\n    { "label": "...", "value": "...", "change": "-3.2%", "changeType": "negative", "description": "..." },\n    { "label": "...", "value": "...", "change": "0%", "changeType": "neutral", "description": "..." },\n    { "label": "...", "value": "...", "change": "+8.1%", "changeType": "positive", "description": "..." }\n  ],\n  "charts": [\n    {\n      "type": "bar",\n      "title": "Chart title based on actual data",\n      "description": "What this chart shows",\n      "data": [ { "name": "Category A", "value": 45 }, { "name": "Category B", "value": 72 } ],\n      "xKey": "name",\n      "yKey": "value",\n      "color": "#818CF8"\n    },\n    {\n      "type": "pie",\n      "title": "Distribution chart title",\n      "description": "What this shows",\n      "data": [ { "name": "Segment A", "value": 40 }, { "name": "Segment B", "value": 60 } ],\n      "colors": ["#818CF8", "#A78BFA", "#67E8F9", "#34D399", "#F472B6"]\n    },\n    {\n      "type": "area",\n      "title": "Trend chart title",\n      "description": "What this shows",\n      "data": [ { "name": "Point 1", "value": 30 }, { "name": "Point 2", "value": 55 } ],\n      "xKey": "name",\n      "yKey": "value",\n      "color": "#67E8F9"\n    }\n  ],\n  "insights": [\n    { "title": "Short insight title", "detail": "2-3 sentence actionable insight with specific numbers from the data", "type": "opportunity" },\n    { "title": "...", "detail": "...", "type": "risk" },\n    { "title": "...", "detail": "...", "type": "trend" },\n    { "title": "...", "detail": "...", "type": "recommendation" }\n  ],\n  "anomalies": [\n    { "column": "column_name", "description": "What anomaly was detected", "severity": "high" },\n    { "column": "column_name", "description": "...", "severity": "medium" }\n  ],\n  "pipelineCode": "# PySpark Bronze→Silver→Gold Pipeline\\nfrom pyspark.sql import SparkSession\\n# ... full working pipeline code"\n}\n\nGenerate exactly 4 KPIs, 3 charts, 4 insights, and 1-3 anomalies. All must reference actual data.',
    },
    { role: 'user', content: JSON.stringify(payload) },
  ];

  return callOpenAiJson(apiKey, messages);
}

async function callOpenAiJson(apiKey, messages) {
  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: 'gpt-4o',
      temperature: 0.2,
      messages,
      response_format: { type: 'json_object' },
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(`OpenAI request failed: ${message || response.statusText}`);
  }

  try {
    const json = await response.json();
    const content = json.choices?.[0]?.message?.content;
    if (!content) {
      throw new Error('No content was returned from the AI model.');
    }
    return JSON.parse(content);
  } catch {
    throw new Error('The AI response could not be parsed as valid JSON. Please try again.');
  }
}

function applyTransformations(rows, transformations, profileStats) {
  let workingData = rows.map((row) => ({ ...row }));
  let workingColumns = collectColumns(workingData);
  const log = [];

  transformations.forEach((transformation) => {
    const action = transformation?.action;
    const originalColumn = transformation?.column;
    const entry = {
      icon: (iconMap[action] || iconMap.fix_type).symbol,
      action,
      column: originalColumn,
      before: transformation?.before || 'N/A',
      after: transformation?.after || 'N/A',
      detail: transformation?.detail || '',
      rowsAffected: transformation?.rowsAffected || 0,
    };

    if (action === 'rename' && originalColumn && typeof transformation.after === 'string') {
      const nextColumn = transformation.after.includes(' ') ? slugifyColumnName(originalColumn) : transformation.after.trim();
      if (nextColumn && nextColumn !== originalColumn) {
        workingData = workingData.map((row) => {
          if (!(originalColumn in row)) return row;
          const clone = { ...row };
          clone[nextColumn] = row[originalColumn];
          delete clone[originalColumn];
          return clone;
        });
        entry.column = nextColumn;
        workingColumns = collectColumns(workingData);
      }
      log.push(entry);
      return;
    }

    if (action === 'fill_null' && originalColumn && workingColumns.includes(originalColumn)) {
      const type = profileStats.typesDetected[originalColumn];
      const values = workingData.map((row) => row?.[originalColumn]).filter((value) => !isNullishValue(value));
      const fillValue = type === 'number' ? median(values.map(toNumber).filter(Number.isFinite)) : mode(values);
      let affected = 0;
      workingData = workingData.map((row) => {
        if (isNullishValue(row?.[originalColumn])) {
          affected += 1;
          return { ...row, [originalColumn]: fillValue ?? 'Unknown' };
        }
        return row;
      });
      entry.rowsAffected = affected;
      entry.after = `0 null values — filled with ${fillValue ?? 'Unknown'}`;
      log.push(entry);
      return;
    }

    if (action === 'fix_type' && originalColumn && workingColumns.includes(originalColumn)) {
      let affected = 0;
      workingData = workingData.map((row) => {
        const current = row?.[originalColumn];
        const cleaned = coerceValue(current);
        if (cleaned !== current) {
          affected += 1;
          return { ...row, [originalColumn]: cleaned };
        }
        return row;
      });
      entry.rowsAffected = affected;
      log.push(entry);
      return;
    }

    if (action === 'remove_outlier' && originalColumn && workingColumns.includes(originalColumn)) {
      const numericValues = workingData.map((row) => toNumber(row?.[originalColumn])).filter(Number.isFinite);
      const stats = numericStats(numericValues);
      if (stats) {
        const capHigh = stats.mean + 3 * stats.stdDev;
        const capLow = stats.mean - 3 * stats.stdDev;
        let affected = 0;
        workingData = workingData.map((row) => {
          const value = toNumber(row?.[originalColumn]);
          if (!Number.isFinite(value)) return row;
          if (value > capHigh || value < capLow) {
            affected += 1;
            return { ...row, [originalColumn]: Number(value > capHigh ? capHigh : capLow).toFixed(2) };
          }
          return row;
        });
        entry.rowsAffected = affected;
      }
      log.push(entry);
      return;
    }

    if (action === 'deduplicate') {
      const seen = new Set();
      const beforeCount = workingData.length;
      workingData = workingData.filter((row) => {
        const key = JSON.stringify(row);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      entry.rowsAffected = beforeCount - workingData.length;
      entry.after = `${workingData.length} unique rows — removed ${entry.rowsAffected} duplicates`;
      log.push(entry);
      return;
    }

    if (action && originalColumn) {
      log.push(entry);
    }
  });

  return {
    cleanedData: workingData,
    log,
    summary: buildDataSummary(workingData),
  };
}

function normalizeEnrichOutput(aiResponse, refinedData) {
  const fallback = buildFallbackInsights(refinedData);
  const kpis = Array.isArray(aiResponse.kpis) && aiResponse.kpis.length ? aiResponse.kpis.slice(0, 4) : fallback.kpis;
  const charts = Array.isArray(aiResponse.charts) && aiResponse.charts.length ? aiResponse.charts.slice(0, 3) : fallback.charts;
  const insights = Array.isArray(aiResponse.insights) && aiResponse.insights.length ? aiResponse.insights.slice(0, 4) : fallback.insights;
  const anomalies = Array.isArray(aiResponse.anomalies) && aiResponse.anomalies.length ? aiResponse.anomalies.slice(0, 3) : fallback.anomalies;

  return {
    kpis,
    charts,
    insights,
    anomalies,
    correlations: aiResponse.correlations || [],
    pipelineCode: typeof aiResponse.pipelineCode === 'string' && aiResponse.pipelineCode.trim()
      ? aiResponse.pipelineCode
      : buildDefaultPipelineCode(refinedData),
  };
}

function buildFallbackInsights(data) {
  const columns = collectColumns(data);
  const stats = computeProfileStats(data, columns);
  const numericColumns = columns.filter((column) => stats.typesDetected[column] === 'number');
  const firstNumeric = numericColumns[0];
  const firstString = columns.find((column) => stats.typesDetected[column] === 'string');
  const values = firstNumeric ? data.map((row) => toNumber(row[firstNumeric])).filter(Number.isFinite) : [];
  const total = values.reduce((sum, value) => sum + value, 0);
  const average = values.length ? total / values.length : 0;

  return {
    kpis: [
      { label: 'Rows Processed', value: `${data.length}`, change: '+0%', changeType: 'neutral', description: 'Total records available after refinement.' },
      { label: 'Columns Modeled', value: `${columns.length}`, change: '+0%', changeType: 'neutral', description: 'Unique fields included in the analytics model.' },
      { label: firstNumeric ? `${firstNumeric} Total` : 'Null Cells', value: firstNumeric ? formatNumber(total) : `${stats.totalNulls}`, change: '+0%', changeType: 'positive', description: 'Aggregate value derived from the primary measure.' },
      { label: firstNumeric ? `${firstNumeric} Average` : 'Quality Score', value: firstNumeric ? formatNumber(average) : `${Math.round(stats.qualityScore)}%`, change: '+0%', changeType: 'neutral', description: 'Average value across the leading numeric series.' },
    ],
    charts: [
      { type: 'bar', title: 'Top Category Distribution', description: 'Largest category counts from the leading categorical column.', data: buildCategoryCounts(data, firstString).slice(0, 6), xKey: 'name', yKey: 'value', color: '#818CF8' },
      { type: 'pie', title: 'Category Share', description: 'Distribution of the primary categorical breakdown.', data: buildCategoryCounts(data, firstString).slice(0, 5), colors: CHART_COLORS },
      { type: 'area', title: 'Primary Metric Trend Sample', description: 'Sequential trend across the leading numeric metric.', data: buildSequentialNumericData(data, firstNumeric), xKey: 'name', yKey: 'value', color: '#67E8F9' },
    ],
    insights: [
      { title: 'Dataset coverage is stable', detail: `${data.length} refined rows remain available for downstream analytics with ${Math.round(stats.qualityScore)}% health after cleanup.`, type: 'trend' },
      { title: 'Strongest measure identified', detail: firstNumeric ? `${firstNumeric} averages ${formatNumber(average)} across the available sample, which is useful for scorecarding and threshold alerts.` : 'The dataset is primarily categorical, so segmentation is a better first analytics step than numeric forecasting.', type: 'opportunity' },
      { title: 'Categorical concentration to monitor', detail: firstString ? `${buildCategoryCounts(data, firstString)[0]?.name || 'Unknown'} appears most often in ${firstString}, which could signal concentration risk or a dominant segment.` : 'A limited number of categorical dimensions were available, so dashboard breadth should stay focused.', type: 'risk' },
      { title: 'Recommended deployment path', detail: 'Use the exported pipeline script to formalize Bronze, Silver, and Gold layers, then schedule analytics regeneration after each batch refresh.', type: 'recommendation' },
    ],
    anomalies: columns.length
      ? [{ column: columns[0], description: 'No severe anomaly surfaced from fallback analysis, but this field should be monitored as a primary quality checkpoint.', severity: 'low' }]
      : [],
  };
}

function buildFallbackChartData(data, type) {
  const columns = collectColumns(data);
  const stats = computeProfileStats(data, columns);
  const categoryColumn = columns.find((column) => stats.typesDetected[column] === 'string');
  const numericColumn = columns.find((column) => stats.typesDetected[column] === 'number');
  if (type === 'pie' || type === 'bar') return buildCategoryCounts(data, categoryColumn).slice(0, 6);
  if (type === 'area' || type === 'radar') return buildSequentialNumericData(data, numericColumn);
  return [];
}

function buildCategoryCounts(data, column) {
  if (!column) return [];
  const counts = new Map();
  data.forEach((row) => {
    const key = String(row?.[column] ?? 'Unknown');
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([name, value]) => ({ name, value }));
}

function buildSequentialNumericData(data, column) {
  if (!column) return data.slice(0, 8).map((_, index) => ({ name: `Row ${index + 1}`, value: index + 1 }));
  return data.slice(0, 10).map((row, index) => ({ name: `Row ${index + 1}`, value: Number(toNumber(row?.[column]) || 0) }));
}

function buildDefaultPipelineCode(data) {
  const columns = collectColumns(data);
  const selectColumns = columns.map((column) => `    col("${column}")`).join(',\n');
  return `# PySpark Bronze→Silver→Gold Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

spark = SparkSession.builder.appName("DataFlowStudioPipeline").getOrCreate()

bronze_df = spark.read.option("header", True).csv("input/data.csv")

silver_df = (
    bronze_df
${columns.length ? `.select(\n${selectColumns}\n)` : ''}
)

gold_df = silver_df

gold_df.write.mode("overwrite").parquet("output/gold")
`;
}

function buildAnalyticsReport({ fileName, refineLog, enrichOutput, pipelineCode, refinedData }) {
  const lines = [
    `# DataFlow Studio Analytics Report`,
    ``,
    `## Dataset`,
    `- File: ${fileName || 'Uploaded dataset'}`,
    `- Refined rows: ${refinedData.length}`,
    `- Columns: ${collectColumns(refinedData).length}`,
    ``,
    `## KPIs`,
    ...enrichOutput.kpis.map((kpi) => `- ${kpi.label}: ${kpi.value} (${kpi.change}) — ${kpi.description}`),
    ``,
    `## Insights`,
    ...enrichOutput.insights.map((insight) => `- ${insight.title}: ${insight.detail}`),
    ``,
    `## Anomalies`,
    ...(enrichOutput.anomalies.length
      ? enrichOutput.anomalies.map((anomaly) => `- [${anomaly.severity}] ${anomaly.column}: ${anomaly.description}`)
      : ['- No significant anomalies were detected.']),
    ``,
    `## Transformation Log`,
    ...(refineLog.length
      ? refineLog.map((entry) => `- ${entry.action} on ${entry.column}: ${entry.before} → ${entry.after} (${entry.rowsAffected} rows affected)`)
      : ['- No transformations were recorded.']),
    ``,
    `## Pipeline Code`,
    '```python',
    pipelineCode,
    '```',
  ];
  return lines.join('\n');
}

function stripExtension(name) {
  return name.replace(/\.[^/.]+$/, '');
}

function downloadTextFile(content, fileName, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** exponent;
  return `${value.toFixed(value >= 10 || exponent === 0 ? 0 : 1)} ${units[exponent]}`;
}

function isNullishValue(value) {
  return value === null || value === undefined || (typeof value === 'string' && value.trim() === '');
}

function formatCellValue(value) {
  if (isNullishValue(value)) return 'Null';
  if (typeof value === 'number') return formatNumber(value);
  return String(value);
}

function toNumber(value) {
  if (typeof value === 'number') return value;
  if (typeof value !== 'string') return Number(value);
  const cleaned = value.replace(/[$,%\s,]/g, '');
  const numeric = Number(cleaned);
  return Number.isFinite(numeric) ? numeric : NaN;
}

function isNumericString(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return true;
  if (typeof value !== 'string') return false;
  const trimmed = value.trim();
  if (!trimmed) return false;
  const cleaned = trimmed.replace(/[$,%\s,]/g, '');
  return cleaned !== '' && Number.isFinite(Number(cleaned));
}

function isBooleanString(value) {
  return ['true', 'false', 'yes', 'no'].includes(String(value).toLowerCase());
}

function isDateString(value) {
  if (typeof value !== 'string') return false;
  const parsed = Date.parse(value);
  return !Number.isNaN(parsed) && /[-/]/.test(value);
}

function coerceValue(value) {
  if (isNullishValue(value)) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (isBooleanString(trimmed)) return ['true', 'yes'].includes(trimmed.toLowerCase());
    if (isNumericString(trimmed)) return Number(toNumber(trimmed).toFixed(2));
    if (isDateString(trimmed)) return new Date(trimmed).toISOString().slice(0, 10);
    return trimmed;
  }
  return value;
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function mode(values) {
  if (!values.length) return null;
  const counts = new Map();
  values.forEach((value) => counts.set(String(value), (counts.get(String(value)) || 0) + 1));
  return [...counts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || null;
}

function numericStats(values) {
  if (!values.length) return null;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return {
    mean,
    stdDev: Math.sqrt(variance),
  };
}

function normalizeSortValue(value) {
  if (isNullishValue(value)) return '';
  if (isNumericString(value)) return toNumber(value);
  return String(value).toLowerCase();
}

function slugifyColumnName(value) {
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function formatNumber(value) {
  if (!Number.isFinite(Number(value))) return String(value ?? '—');
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(Number(value));
}

function typeBadgeClasses(type) {
  if (type === 'number') return 'bg-emerald-500/15 text-emerald-200';
  if (type === 'string') return 'bg-sky-500/15 text-sky-200';
  if (type === 'date') return 'bg-violet-500/15 text-violet-200';
  return 'bg-slate-500/15 text-slate-300';
}

function severityClasses(severity) {
  if (severity === 'high') return 'bg-rose-500/15 text-rose-200';
  if (severity === 'medium') return 'bg-amber-500/15 text-amber-200';
  return 'bg-sky-500/15 text-sky-200';
}

function insightTypeClasses(type) {
  if (type === 'opportunity') {
    return { wrapper: 'border-l-4 border-l-emerald-400', badge: 'bg-emerald-500/15 text-emerald-200' };
  }
  if (type === 'risk') {
    return { wrapper: 'border-l-4 border-l-rose-400', badge: 'bg-rose-500/15 text-rose-200' };
  }
  if (type === 'recommendation') {
    return { wrapper: 'border-l-4 border-l-violet-400', badge: 'bg-violet-500/15 text-violet-200' };
  }
  return { wrapper: 'border-l-4 border-l-sky-400', badge: 'bg-sky-500/15 text-sky-200' };
}

function syntaxHighlight(line) {
  const escaped = line
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  return escaped
    .replace(/(#.*)$/g, '<span style="color:#6B7280;">$1</span>')
    .replace(/\b(from|import|for|in|if|else|return|True|False|None|class|def)\b/g, '<span style="color:#A78BFA;">$1</span>')
    .replace(/("[^"]*"|'[^']*')/g, '<span style="color:#34D399;">$1</span>');
}

export default App;
