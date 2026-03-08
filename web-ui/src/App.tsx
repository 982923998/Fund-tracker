import React, { useState, useEffect } from 'react';
import { 
  LayoutDashboard, 
  History, 
  Clock, 
  Bell, 
  Settings, 
  TrendingUp, 
  PlusCircle,
  Play,
  Pause,
  XCircle,
  RefreshCw,
  Info,
  AlertCircle
} from 'lucide-react';
import { 
  PieChart, 
  Pie, 
  Cell, 
  ResponsiveContainer, 
  Tooltip,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid
} from 'recharts';

const API_BASE = 'http://127.0.0.1:8000/api';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d', '#ffc658'];

const truncateText = (text: string, limit = 12) => {
  if (!text) return '';
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
};

const formatDateTime = (value?: string) => {
  if (!value) return '-';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString('zh-CN', { hour12: false });
};

const reportTypeMeta = (reportType: string) => {
  switch (reportType) {
    case 'external_monthly':
      return { label: '增强月报', className: 'bg-purple-100 text-purple-700' };
    case 'manual':
      return { label: '本地手动分析', className: 'bg-emerald-100 text-emerald-700' };
    default:
      return { label: reportType || '未分类', className: 'bg-gray-100 text-gray-700' };
  }
};

const REPORT_SECTION_TITLES: Record<string, string[]> = {
  external_monthly: [
    '资金调配方案',
    '配置与操作建议',
    '国内宏观与相关行业',
    '海外宏观与相关行业',
    '商品与大类资产及相关行业',
  ],
};

const parseReportSections = (reportBody?: string, reportType?: string) => {
  if (!reportBody) return [];

  const sectionTitles = reportType
    ? (REPORT_SECTION_TITLES[reportType] || [])
    : Array.from(new Set(Object.values(REPORT_SECTION_TITLES).flat()));
  if (sectionTitles.length === 0) return [];

  const lines = reportBody.split('\n');
  const sectionMarkers = lines
    .map((line, index) => ({ title: line.trim(), index }))
    .filter((item) => sectionTitles.includes(item.title));

  if (sectionMarkers.length === 0) {
    return [];
  }

  return sectionMarkers.map((marker, index) => {
    const nextIndex = sectionMarkers[index + 1]?.index ?? lines.length;
    const content = lines
      .slice(marker.index + 1, nextIndex)
      .join('\n')
      .trim();

    return {
      title: marker.title,
      content: content || '暂无内容',
    };
  });
};

const sectionCardMeta = (title: string) => {
  switch (title) {
    case '资金调配方案':
      return {
        className: 'border-cyan-100 bg-cyan-50/80',
        badgeClassName: 'bg-cyan-100 text-cyan-700',
      };
    case '配置与操作建议':
      return {
        className: 'border-blue-100 bg-blue-50/70',
        badgeClassName: 'bg-blue-100 text-blue-700',
      };
    case '国内宏观与相关行业':
      return {
        className: 'border-emerald-100 bg-emerald-50/70',
        badgeClassName: 'bg-emerald-100 text-emerald-700',
      };
    case '海外宏观与相关行业':
      return {
        className: 'border-violet-100 bg-violet-50/70',
        badgeClassName: 'bg-violet-100 text-violet-700',
      };
    case '商品与大类资产及相关行业':
      return {
        className: 'border-amber-100 bg-amber-50/70',
        badgeClassName: 'bg-amber-100 text-amber-700',
      };
    default:
      return {
        className: 'border-gray-100 bg-white',
        badgeClassName: 'bg-gray-100 text-gray-700',
      };
  }
};

const normalizeActionLine = (line: string) =>
  line
    .replace(/^[\s\-•●▪◦·]+/, '')
    .replace(/^\d+[.、)]\s+/, '')
    .replace(/^（\d+）/, '')
    .trim();

const parseJsonSafely = (value: any) => {
  if (!value) return null;
  if (typeof value === 'object') return value;
  if (typeof value !== 'string') return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

const formatAmount = (value?: number) => {
  if (value === undefined || value === null || Number.isNaN(value)) return '-';
  return `¥${value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}`;
};

const formatDailyLimit = (value?: number | null) => {
  if (value === undefined || value === null || Number.isNaN(value)) return '未设置';
  return `¥${value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}`;
};

type PlanItem = {
  sign: string;
  action: string;
  explicitAction: boolean;
  label: string;
  amount?: number;
  unit?: string;
  detail: string;
  raw: string;
};

type EditableExecutionAction = {
  id: string;
  action_type: string;
  sign: string;
  action_label: string;
  fund_code: string;
  fund_name: string;
  amount: string;
  frequency: string;
  run_rule: string;
  note: string;
};

const EXECUTION_ACTION_OPTIONS = [
  { value: 'buy', label: '买入' },
  { value: 'sell', label: '卖出' },
  { value: 'create_dca', label: '新增定投' },
  { value: 'update_dca', label: '修改定投' },
  { value: 'pause_dca', label: '暂停定投' },
  { value: 'resume_dca', label: '恢复定投' },
  { value: 'cancel_dca', label: '取消定投' },
];

const EXECUTION_DCA_ACTIONS = new Set(['create_dca', 'update_dca', 'pause_dca', 'resume_dca', 'cancel_dca']);
const EXECUTION_ACTIONS_WITH_AMOUNT = new Set(['buy', 'sell', 'create_dca', 'update_dca']);
const WEEKDAY_OPTIONS = [
  { value: 'MON', label: '周一' },
  { value: 'TUE', label: '周二' },
  { value: 'WED', label: '周三' },
  { value: 'THU', label: '周四' },
  { value: 'FRI', label: '周五' },
  { value: 'SAT', label: '周六' },
  { value: 'SUN', label: '周日' },
];

const normalizeFundDisplayLabel = (value: string) => {
  const trimmed = value.trim();
  const codeFirstMatched = trimmed.match(/^(\d{6})\s+(.+)$/);
  if (codeFirstMatched) {
    return `${codeFirstMatched[2].trim()}（${codeFirstMatched[1]}）`;
  }

  const nameFirstMatched = trimmed.match(/^(.+?)\s*[（(](\d{6})[）)]$/);
  if (nameFirstMatched) {
    return `${nameFirstMatched[1].trim()}（${nameFirstMatched[2]}）`;
  }

  return trimmed;
};

const splitFundDisplayLabel = (value: string) => {
  const matched = value.match(/^(.+?)（(\d{6})）$/);
  if (!matched) {
    return { name: value, code: '' };
  }
  return {
    name: matched[1].trim(),
    code: matched[2].trim(),
  };
};

const normalizePlanAction = (value?: string) => {
  const compact = (value || '').replace(/\s+/g, '');
  if (/取消定投|停止定投|暂停定投/.test(compact)) return '取消定投';
  if (/恢复定投/.test(compact)) return '恢复定投';
  if (/卖出|减仓|减持/.test(compact)) return '卖出';
  if (/定投/.test(compact)) return '定投';
  if (/买入|加仓|补仓/.test(compact)) return '买入';
  return '买入';
};

const inferPlanSign = (action: string, explicitSign?: string) => {
  if (explicitSign === '+' || explicitSign === '-') return explicitSign;
  if (['卖出', '取消定投'].includes(action)) return '-';
  return '+';
};

const parsePlanItems = (content?: string) => {
  if (!content) return [];

  const items: PlanItem[] = [];

  content
    .split('\n')
    .map((line) => normalizeActionLine(line))
    .filter(Boolean)
    .forEach((line) => {
      const basicMatched = line.match(/^(?<sign>[+-])?\s*(?:(?<action>取消定投|停止定投|暂停定投|恢复定投|卖出|买入|定投|减仓|减持|加仓|补仓)\s+)?(?<rest>.+)$/);
      if (!basicMatched?.groups?.rest) return;

      const action = normalizePlanAction(basicMatched.groups.action);
      const sign = inferPlanSign(action, basicMatched.groups.sign);
      const amountMatched = basicMatched.groups.rest.match(/^(.*?)[：:]\s*([0-9]+(?:\.[0-9]+)?)\s*(元|份)$/);

      if (amountMatched) {
        items.push({
          sign,
          action,
          explicitAction: Boolean(basicMatched.groups.sign || basicMatched.groups.action),
          label: normalizeFundDisplayLabel(amountMatched[1].trim()),
          amount: Number(amountMatched[2]),
          unit: amountMatched[3],
          detail: '',
          raw: line,
        });
        return;
      }

      const detailMatched = basicMatched.groups.rest.match(/^(.*?)[：:]\s*(.+)$/);
      items.push({
        sign,
        action,
        explicitAction: Boolean(basicMatched.groups.sign || basicMatched.groups.action),
        label: normalizeFundDisplayLabel((detailMatched?.[1] || basicMatched.groups.rest).trim()),
        detail: (detailMatched?.[2] || '').trim(),
        raw: line,
      });
    });

  return items.filter((item) => {
      if (item.amount !== undefined) return true;
      if (/[（(]\d{6}[）)]/.test(item.label)) return true;
      if (item.explicitAction) return true;
      return false;
    });
};

const actionLabelFromType = (actionType: string) =>
  EXECUTION_ACTION_OPTIONS.find((item) => item.value === actionType)?.label || '买入';

const signForActionType = (actionType: string) =>
  ['sell', 'pause_dca', 'cancel_dca'].includes(actionType) ? '-' : '+';

const isDcaActionType = (actionType: string) => EXECUTION_DCA_ACTIONS.has(actionType);

const actionNeedsAmount = (actionType: string) => EXECUTION_ACTIONS_WITH_AMOUNT.has(actionType);

const weekdayFromRunRule = (runRule?: string | null) => {
  if (!runRule) return 'MON';
  if (runRule === 'daily') return 'MON';
  const matched = runRule.match(/^weekly:(MON|TUE|WED|THU|FRI|SAT|SUN)$/i);
  return matched?.[1]?.toUpperCase() || 'MON';
};

const buildRunRule = (frequency: string, weekday?: string) => {
  if (frequency === 'daily') return 'daily';
  const normalizedWeekday = (weekday || 'MON').toUpperCase();
  return `weekly:${normalizedWeekday}`;
};

const WEEKDAY_CN_TO_CODE: Record<string, string> = {
  一: 'MON',
  二: 'TUE',
  三: 'WED',
  四: 'THU',
  五: 'FRI',
  六: 'SAT',
  日: 'SUN',
  天: 'SUN',
};

const createDraftId = () => `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

const createEmptyExecutionAction = (): EditableExecutionAction => ({
  id: createDraftId(),
  action_type: 'buy',
  sign: '+',
  action_label: '买入',
  fund_code: '',
  fund_name: '',
  amount: '',
  frequency: 'daily',
  run_rule: 'daily',
  note: '',
});

const inferExecutionActionType = (item: PlanItem) => {
  if (item.action === '卖出' || item.sign === '-') return 'sell';
  if (item.action === '取消定投') return 'cancel_dca';
  if (item.action === '恢复定投') return 'resume_dca';
  if (item.action === '定投') return 'create_dca';
  return 'buy';
};

const toEditableExecutionAction = (raw: any, index: number): EditableExecutionAction | null => {
  if (!raw || typeof raw !== 'object') return null;
  const actionType = String(raw.action_type || '').trim();
  if (!actionType) return null;

  return {
    id: String(raw.id || `report-action-${index}`),
    action_type: actionType,
    sign: String(raw.sign || signForActionType(actionType)),
    action_label: String(raw.action_label || actionLabelFromType(actionType)),
    fund_code: String(raw.fund_code || '').trim(),
    fund_name: String(raw.fund_name || '').trim(),
    amount: raw.amount === undefined || raw.amount === null ? '' : String(raw.amount),
    frequency: String(raw.frequency || (isDcaActionType(actionType) ? 'daily' : '')).trim(),
    run_rule: String(raw.run_rule || (isDcaActionType(actionType) ? 'daily' : '')).trim(),
    note: String(raw.note || '').trim(),
  };
};

const deriveExecutionActionsFromReport = (
  reportId: number,
  parsedSnapshot: any,
  planItems: PlanItem[],
  currentDcaPlans: any[],
): EditableExecutionAction[] => {
  if (Array.isArray(parsedSnapshot?.execution_plan) && parsedSnapshot.execution_plan.length > 0) {
    return parsedSnapshot.execution_plan
      .map((item: any, index: number) =>
        toEditableExecutionAction({ ...item, id: `${reportId}-execution-${index}` }, index),
      )
      .filter(Boolean) as EditableExecutionAction[];
  }

  return planItems
    .map((item, index) => {
      const { name, code } = splitFundDisplayLabel(item.label);
      const rawText = `${item.raw || ''} ${item.detail || ''}`.trim();
      const matchedPlan = currentDcaPlans.find((plan) => {
        const planCode = String(plan?.fund_code || '').trim();
        const planName = String(plan?.fund_name || '').trim();
        return (code && planCode === code) || (name && planName === name);
      });

      if (/按原计划执行后.*暂停后续.*定投/.test(rawText)) {
        return null;
      }

      const explicitWeekday = rawText.match(/每周([一二三四五六日天])/);
      const explicitFrequency = /日定投|每天/.test(rawText)
        ? 'daily'
        : /周定投|每周/.test(rawText)
          ? 'weekly'
          : String(matchedPlan?.frequency || '').trim().toLowerCase() || '';
      const normalizedFrequency = explicitFrequency === 'weekly' ? 'weekly' : 'daily';
      const weekdayCode = explicitWeekday?.[1] ? WEEKDAY_CN_TO_CODE[explicitWeekday[1]] : '';
      const normalizedRunRule = buildRunRule(
        normalizedFrequency,
        weekdayCode || weekdayFromRunRule(String(matchedPlan?.run_rule || '')),
      );
      const targetAmountMatch = rawText.match(
        /(?:下调至|降到|调整到|调至|改为|上调至|提高到)\s*([0-9]+(?:\.[0-9]+)?)元/,
      );

      if (/定投/.test(rawText) && targetAmountMatch) {
        return {
          id: `${reportId}-fallback-${index}`,
          action_type: matchedPlan ? 'update_dca' : 'create_dca',
          sign: '+',
          action_label: matchedPlan ? '修改定投' : '新增定投',
          fund_code: code,
          fund_name: name,
          amount: String(targetAmountMatch[1]),
          frequency: normalizedFrequency,
          run_rule: normalizedRunRule,
          note: item.detail || '',
        } satisfies EditableExecutionAction;
      }

      if (/暂停.*定投/.test(rawText)) {
        return {
          id: `${reportId}-fallback-${index}`,
          action_type: 'pause_dca',
          sign: '-',
          action_label: '暂停定投',
          fund_code: code,
          fund_name: name,
          amount: '',
          frequency: normalizedFrequency,
          run_rule: normalizedRunRule,
          note: item.detail || '',
        } satisfies EditableExecutionAction;
      }

      if (/取消.*定投/.test(rawText)) {
        return {
          id: `${reportId}-fallback-${index}`,
          action_type: 'cancel_dca',
          sign: '-',
          action_label: '取消定投',
          fund_code: code,
          fund_name: name,
          amount: '',
          frequency: normalizedFrequency,
          run_rule: normalizedRunRule,
          note: item.detail || '',
        } satisfies EditableExecutionAction;
      }

      if (/恢复.*定投/.test(rawText)) {
        return {
          id: `${reportId}-fallback-${index}`,
          action_type: 'resume_dca',
          sign: '+',
          action_label: '恢复定投',
          fund_code: code,
          fund_name: name,
          amount: '',
          frequency: normalizedFrequency,
          run_rule: normalizedRunRule,
          note: item.detail || '',
        } satisfies EditableExecutionAction;
      }

      const actionType = inferExecutionActionType(item);
      return {
        id: `${reportId}-fallback-${index}`,
        action_type: actionType,
        sign: signForActionType(actionType),
        action_label: actionLabelFromType(actionType),
        fund_code: code,
        fund_name: name,
        amount: item.amount === undefined || item.amount === null ? '' : String(item.amount),
        frequency: actionType === 'create_dca' ? 'daily' : '',
        run_rule: actionType === 'create_dca' ? 'daily' : '',
        note: item.detail || '',
      } satisfies EditableExecutionAction;
    })
    .filter((item): item is EditableExecutionAction => Boolean(item))
    .filter((item) => item.fund_code || item.fund_name);
};

const matchesActionDcaPlan = (action: EditableExecutionAction, plan: any) => {
  const actionCode = action.fund_code.trim();
  const actionName = action.fund_name.trim();
  return (
    (actionCode && String(plan?.fund_code || '').trim() === actionCode) ||
    (actionName && String(plan?.fund_name || '').trim() === actionName)
  );
};

const shouldDisplayExecutionAction = (
  action: EditableExecutionAction,
  currentDcaPlans: any[],
) => {
  if (!isDcaActionType(action.action_type)) return true;

  const matchedPlans = currentDcaPlans.filter((plan) => matchesActionDcaPlan(action, plan));
  const activePlan = matchedPlans.find((plan) => Boolean(plan?.enabled));

  if (action.action_type === 'pause_dca' || action.action_type === 'cancel_dca') {
    return Boolean(activePlan);
  }

  if (action.action_type === 'resume_dca') {
    return !activePlan && matchedPlans.length > 0;
  }

  if (action.action_type === 'create_dca' || action.action_type === 'update_dca') {
    if (!activePlan) return true;

    const nextAmount = Number(action.amount);
    const currentAmount = Number(activePlan?.amount);
    const sameAmount =
      Number.isFinite(nextAmount) &&
      Number.isFinite(currentAmount) &&
      Math.abs(nextAmount - currentAmount) < 1e-6;
    const nextFrequency = String(action.frequency || '').trim().toLowerCase() || 'daily';
    const currentFrequency = String(activePlan?.frequency || '').trim().toLowerCase();
    const nextRunRule = buildRunRule(nextFrequency, weekdayFromRunRule(action.run_rule));
    const currentRunRule = String(activePlan?.run_rule || '').trim();

    return !(sameAmount && nextFrequency === currentFrequency && nextRunRule === currentRunRule);
  }

  return true;
};

const CustomYAxisTick = (props: any) => {
  const { x, y, payload } = props;
  const name = payload.value || '';
  const limit = 7;
  
  if (name.length > limit) {
    return (
      <g transform={`translate(${x},${y})`}>
        <text x={-10} y={-4} textAnchor="end" fill="#6B7280" fontSize={11}>
          {name.substring(0, limit)}
        </text>
        <text x={-10} y={10} textAnchor="end" fill="#6B7280" fontSize={11}>
          {name.substring(limit, limit + 7)}
        </text>
      </g>
    );
  }

  return (
    <text x={x - 10} y={y + 4} textAnchor="end" fill="#6B7280" fontSize={11}>
      {name}
    </text>
  );
};

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [summary, setSummary] = useState<any>(null);
  const [funds, setFunds] = useState<any[]>([]);
  const [holdings, setHoldings] = useState<any[]>([]);
  const [transactions, setTransactions] = useState<any[]>([]);
  const [dcaPlans, setDcaPlans] = useState<any[]>([]);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [analysisReports, setAnalysisReports] = useState<any[]>([]);
  const [config, setConfig] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [commandText, setCommandText] = useState('');
  const [availableCash, setAvailableCash] = useState('2000');
  const [statusMsg, setStatusMsg] = useState({ type: '', text: '' });
  const [reportDrafts, setReportDrafts] = useState<Record<number, EditableExecutionAction[]>>({});
  const [submittingReportId, setSubmittingReportId] = useState<number | null>(null);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [sumRes, fundRes, holdRes, transRes, dcaRes, alertRes, analysisRes, confRes] = await Promise.all([
        fetch(`${API_BASE}/summary`),
        fetch(`${API_BASE}/funds`),
        fetch(`${API_BASE}/holdings`),
        fetch(`${API_BASE}/transactions`),
        fetch(`${API_BASE}/dca-plans`),
        fetch(`${API_BASE}/alerts`),
        fetch(`${API_BASE}/analysis-reports`),
        fetch(`${API_BASE}/config`),
      ]);
      
      setSummary(await sumRes.json());
      setFunds(await fundRes.json());
      setHoldings(await holdRes.json());
      setTransactions(await transRes.json());
      setDcaPlans(await dcaRes.json());
      setAlerts(await alertRes.json());
      setAnalysisReports(await analysisRes.json());
      setConfig(await confRes.json());
    } catch (err) {
      console.error('Fetch error:', err);
      showStatus('error', '无法连接到后端服务');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const showStatus = (type: string, text: string) => {
    setStatusMsg({ type, text });
    setTimeout(() => setStatusMsg({ type: '', text: '' }), 5000);
  };

  const handleApplyCommand = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!commandText.trim()) return;
    try {
      const res = await fetch(`${API_BASE}/commands/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: commandText }),
      });
      const data = await res.json();
      if (res.ok) {
        showStatus('success', data.message);
        setCommandText('');
        fetchData();
      } else {
        showStatus('error', data.detail || '指令执行失败');
      }
    } catch (err) {
      showStatus('error', '请求失败');
    }
  };

  const runTask = async (endpoint: string, label: string) => {
    try {
      setLoading(true);
      const res = await fetch(`${API_BASE}/tasks/${endpoint}`, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        showStatus('success', `${label}已完成: ${data.message}`);
        fetchData();
      } else {
        showStatus('error', `${label}失败: ${data.detail}`);
      }
    } catch (err) {
      showStatus('error', '请求失败');
    } finally {
      setLoading(false);
    }
  };

  const generateAnalysis = async (mode: string, label: string) => {
    try {
      setLoading(true);
      const parsedAmount = Number(availableCash);
      const query =
        mode === 'monthly' && Number.isFinite(parsedAmount) && parsedAmount > 0
          ? `${API_BASE}/analysis-reports/generate?mode=${encodeURIComponent(mode)}&available_cash=${encodeURIComponent(parsedAmount)}`
          : `${API_BASE}/analysis-reports/generate?mode=${encodeURIComponent(mode)}`;
      const res = await fetch(query, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        showStatus('success', `${label}已生成`);
        fetchData();
      } else {
        showStatus('error', data.detail || `${label}生成失败`);
      }
    } catch (err) {
      showStatus('error', '请求失败');
    } finally {
      setLoading(false);
    }
  };

  const dcaAction = async (fundCode: string, action: string) => {
    try {
      const res = await fetch(`${API_BASE}/dca-plans/${fundCode}/${action}`, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        showStatus('success', data.message);
        fetchData();
      } else {
        showStatus('error', data.detail);
      }
    } catch (err) {
      showStatus('error', '请求失败');
    }
  };

  const updateReportDraft = (
    reportId: number,
    initialActions: EditableExecutionAction[],
    updater: (actions: EditableExecutionAction[]) => EditableExecutionAction[],
  ) => {
    setReportDrafts((prev) => {
      const current = prev[reportId] || initialActions;
      return {
        ...prev,
        [reportId]: updater(current),
      };
    });
  };

  const resetReportDraft = (reportId: number) => {
    setReportDrafts((prev) => {
      const next = { ...prev };
      delete next[reportId];
      return next;
    });
  };

  const applyExecutionPlan = async (reportId: number, actions: EditableExecutionAction[]) => {
    try {
      setSubmittingReportId(reportId);
      const payload = {
        actions: actions
          .filter((item) => item.fund_code.trim() || item.fund_name.trim())
          .map((item) => ({
            action_type: item.action_type,
            sign: item.sign,
            action_label: item.action_label || actionLabelFromType(item.action_type),
            fund_code: item.fund_code.trim() || undefined,
            fund_name: item.fund_name.trim() || undefined,
            amount: actionNeedsAmount(item.action_type) && item.amount !== '' ? Number(item.amount) : null,
            frequency: isDcaActionType(item.action_type) ? item.frequency || null : null,
            run_rule: isDcaActionType(item.action_type) ? item.run_rule || null : null,
            note: item.note.trim() || null,
          })),
      };
      const res = await fetch(`${API_BASE}/analysis-reports/${reportId}/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (res.ok) {
        showStatus('success', data.message || '方案已执行');
        resetReportDraft(reportId);
        fetchData();
      } else {
        showStatus('error', data.detail || '方案执行失败');
      }
    } catch (err) {
      showStatus('error', '请求失败');
    } finally {
      setSubmittingReportId(null);
    }
  };

  const renderDashboard = () => (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-sm font-medium text-gray-500">总市值</p>
          <p className="text-2xl font-bold text-gray-900">¥ {summary?.portfolio?.total_market_value?.toLocaleString()}</p>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-sm font-medium text-gray-500">今日收益</p>
          <p className={`text-2xl font-bold ${summary?.portfolio?.total_daily_pnl >= 0 ? 'text-red-500' : 'text-green-500'}`}>
            {summary?.portfolio?.total_daily_pnl >= 0 ? '+' : ''}
            {summary?.portfolio?.total_daily_pnl?.toFixed(2)} 元
          </p>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-sm font-medium text-gray-500">当前持仓收益</p>
          <p className={`text-2xl font-bold ${summary?.portfolio?.total_unrealized_pnl >= 0 ? 'text-red-500' : 'text-green-500'}`}>
            {summary?.portfolio?.total_unrealized_pnl >= 0 ? '+' : ''}
            {summary?.portfolio?.total_unrealized_pnl?.toLocaleString()}
          </p>
          <p className={`mt-2 text-sm font-medium ${summary?.portfolio?.total_return_pct >= 0 ? 'text-red-500' : 'text-green-500'}`}>
            当前持仓收益率 {summary?.portfolio?.total_return_pct?.toFixed(2)}%
          </p>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-sm font-medium text-gray-500">总收益</p>
          <p className={`text-2xl font-bold ${summary?.portfolio?.total_return >= 0 ? 'text-red-500' : 'text-green-500'}`}>
            {summary?.portfolio?.total_return >= 0 ? '+' : ''}
            {summary?.portfolio?.total_return?.toLocaleString()}
          </p>
          <p className={`mt-2 text-sm font-medium ${summary?.portfolio?.total_realized_pnl >= 0 ? 'text-red-500' : 'text-green-500'}`}>
            已实现收益 {summary?.portfolio?.total_realized_pnl >= 0 ? '+' : ''}
            {summary?.portfolio?.total_realized_pnl?.toLocaleString()}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Charts */}
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 min-h-[450px]">
          <h3 className="text-lg font-semibold mb-4">资产分布</h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={holdings}
                  dataKey="market_value"
                  nameKey="fund_name"
                  cx="50%"
                  cy="50%"
                  outerRadius={100}
                  label={false}
                  labelLine={false}
                >
                  {holdings.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(val: number) => `¥${val.toLocaleString()}`} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-2">
            {holdings.map((holding, index) => (
              <div
                key={holding.fund_code}
                className="flex items-center justify-between rounded-lg bg-gray-50 px-3 py-2 text-sm"
                title={`${holding.fund_name} (${holding.weight_pct}%)`}
              >
                <div className="flex items-center gap-2 min-w-0">
                  <span
                    className="h-3 w-3 rounded-full shrink-0"
                    style={{ backgroundColor: COLORS[index % COLORS.length] }}
                  />
                  <span className="truncate text-gray-700">
                    {truncateText(holding.fund_name, 10)}
                  </span>
                </div>
                <span className="ml-3 shrink-0 font-medium text-gray-900">
                  {holding.weight_pct}%
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 min-h-[450px]">
          <h3 className="text-lg font-semibold mb-4">收益排行 Top 5</h3>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={holdings.slice(0, 5)} layout="vertical" margin={{ left: 20, right: 40 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                <XAxis type="number" />
                <YAxis dataKey="fund_name" type="category" width={120} tick={<CustomYAxisTick />} />
                <Tooltip formatter={(val: number) => `¥${val.toLocaleString()}`} />
                <Bar dataKey="unrealized_pnl" fill="#3b82f6" name="浮动盈亏" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">持仓明细</h3>
          <span className="text-sm text-gray-500">共 {holdings.length} 条</span>
        </div>
        {renderHoldings()}
      </div>

      <div className="bg-blue-50 p-6 rounded-xl border border-blue-100">
        <h3 className="text-blue-800 font-semibold mb-4 flex items-center gap-2">
          <TrendingUp className="w-5 h-5" /> 快速操作
        </h3>
        <div className="flex flex-wrap gap-4">
          <button onClick={() => runTask('daily-run', '每日任务')} className="bg-blue-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-blue-700">
            <RefreshCw className="w-4 h-4" /> 运行每日任务
          </button>
          <button onClick={() => runTask('test-notification', '通知测试')} className="bg-white text-blue-600 border border-blue-200 px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-blue-50">
            <Bell className="w-4 h-4" /> 测试 macOS 通知
          </button>
        </div>
      </div>
    </div>
  );

  const renderHoldings = () => (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
      <table className="w-full text-left">
        <thead className="bg-gray-50 border-b border-gray-100">
          <tr>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700">基金代码</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700">基金名称</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">单日限额</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">持有份额</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">平均成本</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">当前净值</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">市值</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">浮动盈亏</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">收益率</th>
            <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">日涨跌</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {holdings.map((h) => (
            <tr key={h.fund_code} className="hover:bg-gray-50 transition-colors">
              <td className="px-6 py-4 text-sm font-mono">{h.fund_code}</td>
              <td className="px-6 py-4 text-sm font-medium">{h.fund_name}</td>
              <td className="px-6 py-4 text-sm text-right">
                <span className={`inline-flex rounded-full px-2 py-1 text-xs font-medium ${h.daily_purchase_limit_amount ? 'bg-cyan-50 text-cyan-700' : 'bg-gray-100 text-gray-500'}`}>
                  {formatDailyLimit(h.daily_purchase_limit_amount)}
                </span>
              </td>
              <td className="px-6 py-4 text-sm text-right">{h.shares.toFixed(2)}</td>
              <td className="px-6 py-4 text-sm text-right">{h.average_cost_nav.toFixed(4)}</td>
              <td className="px-6 py-4 text-sm text-right">{h.latest_nav.toFixed(4)}</td>
              <td className="px-6 py-4 text-sm text-right font-medium">¥{h.market_value.toLocaleString()}</td>
              <td className={`px-6 py-4 text-sm text-right font-medium ${h.unrealized_pnl >= 0 ? 'text-red-500' : 'text-green-500'}`}>
                {h.unrealized_pnl >= 0 ? '+' : ''}{h.unrealized_pnl.toLocaleString()}
              </td>
              <td className={`px-6 py-4 text-sm text-right font-medium ${h.return_pct >= 0 ? 'text-red-500' : 'text-green-500'}`}>
                {h.return_pct.toFixed(2)}%
              </td>
              <td className={`px-6 py-4 text-sm text-right ${h.daily_pct_change >= 0 ? 'text-red-500' : 'text-green-500'}`}>
                {h.daily_pct_change ? `${h.daily_pct_change > 0 ? '+' : ''}${h.daily_pct_change.toFixed(2)}%` : '-'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  const renderTransactions = () => (
    <div className="space-y-6">
      {/* Quick Entry */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
        <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
          <PlusCircle className="w-5 h-5 text-blue-600" /> 指令录入 (支持自然语言)
        </h3>
        <form onSubmit={handleApplyCommand} className="flex gap-4">
          <input 
            type="text" 
            value={commandText}
            onChange={(e) => setCommandText(e.target.value)}
            placeholder='例如: 买入 005827 1000元'
            className="flex-1 border border-gray-200 rounded-lg px-4 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
          />
          <button type="submit" className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700">执行</button>
        </form>
        <p className="mt-2 text-xs text-gray-400 italic">提示: 005827 买 1000 | 005827 卖 500份 | 新增定投 005827 500 每周一</p>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <table className="w-full text-left">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">日期</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">类型</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">基金</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">金额</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">成交价</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">份额</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">备注</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {transactions.map((t) => (
              <tr key={t.id} className="hover:bg-gray-50 transition-colors">
                <td className="px-6 py-4 text-sm text-gray-500">{t.trade_date}</td>
                <td className="px-6 py-4 text-sm">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium 
                    ${t.trade_type === 'buy' || t.trade_type === 'dca' ? 'bg-red-50 text-red-600' : 
                      t.trade_type === 'sell' ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-600'}`}>
                    {t.trade_type === 'buy' ? '买入' : t.trade_type === 'sell' ? '卖出' : t.trade_type === 'dca' ? '定投' : '初始'}
                  </span>
                </td>
                <td className="px-6 py-4 text-sm font-medium">{t.fund_name} <span className="text-gray-400 font-normal">({t.fund_code})</span></td>
                <td className="px-6 py-4 text-sm text-right font-medium">¥{t.amount.toLocaleString()}</td>
                <td className="px-6 py-4 text-sm text-right">{t.nav.toFixed(4)}</td>
                <td className="px-6 py-4 text-sm text-right">{t.shares.toFixed(2)}</td>
                <td className="px-6 py-4 text-sm text-gray-400 truncate max-w-[200px]">{t.note || t.raw_text}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderDCA = () => (
    <div className="space-y-6">
      <div className="rounded-xl border border-cyan-100 bg-cyan-50/70 p-5">
        <div className="flex items-start gap-3">
          <Info className="mt-0.5 h-5 w-5 text-cyan-600 shrink-0" />
          <div>
            <h4 className="text-sm font-semibold text-cyan-900">单日限额</h4>
            <p className="mt-1 text-sm leading-6 text-cyan-900/80">
              系统会自动从公开基金页面同步申购状态和单日限额。增强月报会把这些自动维护的限额、今天会执行的定投金额，以及你的可支配资金一起考虑进去。
            </p>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-100">
          <h3 className="text-base font-semibold text-gray-900">系统自动同步的买入限制</h3>
        </div>
        <table className="w-full text-left">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">基金</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">今日定投占用</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">单日限额</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">今日剩余容量</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">状态</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {funds.map((fund) => (
              <tr key={`limit-${fund.fund_code}`} className="hover:bg-gray-50 transition-colors">
                <td className="px-6 py-4 text-sm font-medium">
                  {fund.fund_name} <span className="text-gray-400 font-normal">({fund.fund_code})</span>
                </td>
                <td className="px-6 py-4 text-sm text-right font-medium">
                  {fund.today_due_dca_amount ? formatAmount(fund.today_due_dca_amount) : '-'}
                </td>
                <td className="px-6 py-4 text-sm text-right">
                  <span className={`inline-flex rounded-full px-2 py-1 text-xs font-medium ${fund.daily_purchase_limit_amount ? 'bg-cyan-50 text-cyan-700' : 'bg-gray-100 text-gray-500'}`}>
                    {formatDailyLimit(fund.daily_purchase_limit_amount)}
                  </span>
                </td>
                <td className="px-6 py-4 text-sm text-right">
                  {fund.today_remaining_purchase_capacity === null || fund.today_remaining_purchase_capacity === undefined
                    ? <span className="text-gray-400">不限</span>
                    : <span className={`font-medium ${fund.today_remaining_purchase_capacity > 0 ? 'text-cyan-700' : 'text-amber-600'}`}>{formatAmount(fund.today_remaining_purchase_capacity)}</span>}
                </td>
                <td className="px-6 py-4 text-sm">
                  {fund.today_limit_exceeded ? (
                    <span className="inline-flex rounded-full bg-red-50 px-2 py-1 text-xs font-medium text-red-600">定投超限</span>
                  ) : fund.daily_purchase_limit_amount ? (
                    <span className="inline-flex rounded-full bg-cyan-50 px-2 py-1 text-xs font-medium text-cyan-700">已限制</span>
                  ) : (
                    <span className="inline-flex rounded-full bg-gray-100 px-2 py-1 text-xs font-medium text-gray-500">未设置</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <table className="w-full text-left">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">基金</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">金额</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">频率</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">规则</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">单日限额</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">今日状态</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700">状态</th>
              <th className="px-6 py-4 font-semibold text-sm text-gray-700 text-right">操作</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {dcaPlans.map((p) => (
              <tr key={p.id} className="hover:bg-gray-50 transition-colors">
                <td className="px-6 py-4 text-sm font-medium">{p.fund_name} <span className="text-gray-400 font-normal">({p.fund_code})</span></td>
                <td className="px-6 py-4 text-sm text-right font-medium">¥{p.amount.toLocaleString()}</td>
                <td className="px-6 py-4 text-sm">{p.frequency === 'weekly' ? '每周' : '每日'}</td>
                <td className="px-6 py-4 text-sm">{p.run_rule}</td>
                <td className="px-6 py-4 text-sm text-right">
                  <span className={`inline-flex rounded-full px-2 py-1 text-xs font-medium ${p.daily_purchase_limit_amount ? 'bg-cyan-50 text-cyan-700' : 'bg-gray-100 text-gray-500'}`}>
                    {formatDailyLimit(p.daily_purchase_limit_amount)}
                  </span>
                </td>
                <td className="px-6 py-4 text-sm">
                  {p.today_limit_exceeded ? (
                    <span className="inline-flex rounded-full bg-red-50 px-2 py-1 text-xs font-medium text-red-600">今日定投超限</span>
                  ) : p.is_due_today ? (
                    <span className="inline-flex rounded-full bg-amber-50 px-2 py-1 text-xs font-medium text-amber-700">
                      今日执行 {p.today_remaining_purchase_capacity === null ? '' : `· 剩余 ${formatAmount(p.today_remaining_purchase_capacity)}`}
                    </span>
                  ) : (
                    <span className="inline-flex rounded-full bg-gray-100 px-2 py-1 text-xs font-medium text-gray-500">今日不执行</span>
                  )}
                </td>
                <td className="px-6 py-4 text-sm">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${p.enabled ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-400'}`}>
                    {p.enabled ? '运行中' : '已暂停'}
                  </span>
                </td>
                <td className="px-6 py-4 text-sm text-right flex justify-end gap-2">
                  {p.enabled ? (
                    <button onClick={() => dcaAction(p.fund_code, 'pause')} className="p-2 hover:bg-gray-100 rounded-lg text-amber-600" title="暂停">
                      <Pause className="w-4 h-4" />
                    </button>
                  ) : (
                    <button onClick={() => dcaAction(p.fund_code, 'resume')} className="p-2 hover:bg-gray-100 rounded-lg text-green-600" title="恢复">
                      <Play className="w-4 h-4" />
                    </button>
                  )}
                  <button onClick={() => dcaAction(p.fund_code, 'cancel')} className="p-2 hover:bg-gray-100 rounded-lg text-red-600" title="取消">
                    <XCircle className="w-4 h-4" />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderAlerts = () => (
    <div className="space-y-4">
      {alerts.map((a) => (
        <div key={a.id} className="bg-white p-4 rounded-xl shadow-sm border border-gray-100 flex gap-4 items-start">
          <div className="p-2 bg-red-50 rounded-lg">
            <Bell className="w-5 h-5 text-red-600" />
          </div>
          <div className="flex-1">
            <div className="flex justify-between items-start">
              <h4 className="font-semibold">{a.fund_name} 跌破阈值</h4>
              <span className="text-xs text-gray-400">{a.alert_date}</span>
            </div>
            <p className="text-sm text-gray-600 mt-1">{a.message}</p>
            <div className="mt-2 flex items-center gap-2">
              <span className="text-xs px-2 py-0.5 bg-gray-100 rounded-full text-gray-500">
                触发值: {a.trigger_value.toFixed(2)}%
              </span>
              <span className="text-xs px-2 py-0.5 bg-gray-100 rounded-full text-gray-500">
                渠道: {a.delivery_status}
              </span>
            </div>
          </div>
        </div>
      ))}
      {alerts.length === 0 && (
        <div className="bg-white p-12 text-center rounded-xl border border-dashed border-gray-200">
          <Info className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-400">暂无提醒记录</p>
        </div>
      )}
    </div>
  );

  const renderAnalysis = () => (
    <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between mb-4">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">投资建议</h3>
          <p className="text-sm text-gray-500">按时间倒序保留历史建议；生成增强月报时，会把当前可支配资金、定投路径和单日限额一起纳入首段资金调配方案。</p>
        </div>
        <div className="flex flex-col sm:flex-row gap-3 sm:items-center">
          <div className="flex items-center gap-2 rounded-lg border border-cyan-200 bg-cyan-50/60 px-3 py-2">
            <span className="text-sm text-gray-500">可支配资金</span>
            <input
              type="number"
              min="0"
              step="10"
              value={availableCash}
              onChange={(e) => setAvailableCash(e.target.value)}
              className="w-28 border-0 bg-transparent text-right text-sm font-semibold text-gray-900 focus:outline-none"
            />
            <span className="text-sm text-gray-500">元</span>
          </div>
          <button
            onClick={() => generateAnalysis('monthly', '增强月报')}
            className="bg-blue-600 text-white px-4 py-2 rounded-lg flex items-center gap-2 hover:bg-blue-700 disabled:opacity-60 disabled:cursor-not-allowed"
            disabled={loading}
          >
            <TrendingUp className="w-4 h-4" /> 生成增强月报
          </button>
        </div>
      </div>

      <div className="space-y-4">
        {analysisReports.map((report) => {
          const sections = parseReportSections(report.report_body, report.report_type);
          const isMonthlyReport = report.report_type === 'external_monthly';
          const hasStructuredSections = isMonthlyReport && sections.length > 0;
          const parsedSnapshot = parseJsonSafely(report.input_snapshot);
          const reportAvailableCash = parsedSnapshot?.available_cash;
          const executionApplied = parsedSnapshot?.execution_applied;
          const isPlanApplied = Boolean(executionApplied?.executed_at);
          const isSubmittingThisReport = submittingReportId === report.id;

          const summarySection = sections.find((section) => section.title === '资金调配方案');
          const planItems = parsePlanItems(summarySection?.content);
          const initialExecutionActions = deriveExecutionActionsFromReport(
            report.id,
            parsedSnapshot,
            planItems,
            dcaPlans,
          );
          const executionPlanDraft =
            reportDrafts[report.id] ||
            (initialExecutionActions.length > 0 ? initialExecutionActions : [createEmptyExecutionAction()]);
          const visibleExecutionPlanDraft = executionPlanDraft.filter((action) =>
            shouldDisplayExecutionAction(action, dcaPlans),
          );
          const detailSections = sections.filter((section) => {
            if (section.title === summarySection?.title) return false;
            return true;
          });

          return (
            <div key={report.id} className="rounded-xl border border-gray-100 bg-gray-50 p-4">
              <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex items-center gap-2">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${reportTypeMeta(report.report_type).className}`}>
                    {reportTypeMeta(report.report_type).label}
                  </span>
                  <span className="text-xs text-gray-400">{report.skill_name}</span>
                  {isMonthlyReport && typeof reportAvailableCash === 'number' && (
                    <span className="px-2 py-1 rounded text-xs font-medium bg-cyan-100 text-cyan-700">
                      本次资金 {formatAmount(reportAvailableCash)}
                    </span>
                  )}
                </div>
                <span className="text-sm text-gray-500">{formatDateTime(report.created_at)}</span>
              </div>

              {hasStructuredSections ? (
                <div className="mt-4 space-y-4">
                  <div className="rounded-xl border border-cyan-100 bg-gradient-to-r from-cyan-50 to-sky-50 p-5 shadow-sm">
                    <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                      <div className="flex items-center gap-2">
                        <span className="rounded-full bg-cyan-100 px-2.5 py-1 text-xs font-semibold text-cyan-700">
                          {summarySection?.title || '资金调配方案'}
                        </span>
                        <span className="text-xs text-cyan-600">可编辑后直接执行</span>
                        {isPlanApplied && (
                          <span className="rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-semibold text-emerald-700">
                            已执行
                          </span>
                        )}
                      </div>
                      {isPlanApplied && executionApplied?.executed_at && (
                        <span className="text-xs text-gray-500">
                          执行时间：{formatDateTime(executionApplied.executed_at)}
                        </span>
                      )}
                    </div>

                    <div className="space-y-3">
                      {visibleExecutionPlanDraft.map((action) => {
                        const weeklyDay = weekdayFromRunRule(action.run_rule);
                        return (
                          <div
                            key={action.id}
                            className="rounded-lg border border-white/80 bg-white/90 p-4"
                          >
                            <div className="grid grid-cols-1 gap-3 xl:grid-cols-12">
                              <div className="xl:col-span-2">
                                <label className="mb-1 block text-xs font-medium text-gray-500">动作</label>
                                <div className="flex items-center gap-2">
                                  <span className={`inline-flex h-9 min-w-9 items-center justify-center rounded-full text-sm font-bold ${action.sign === '-' ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}`}>
                                    {action.sign}
                                  </span>
                                  <select
                                    value={action.action_type}
                                    disabled={isPlanApplied || isSubmittingThisReport}
                                    onChange={(e) => {
                                      const nextActionType = e.target.value;
                                      updateReportDraft(report.id, executionPlanDraft, (current) =>
                                        current.map((item) =>
                                          item.id === action.id
                                            ? {
                                                ...item,
                                                action_type: nextActionType,
                                                sign: signForActionType(nextActionType),
                                                action_label: actionLabelFromType(nextActionType),
                                                frequency: isDcaActionType(nextActionType) ? (item.frequency || 'daily') : '',
                                                run_rule: isDcaActionType(nextActionType)
                                                  ? (item.run_rule || 'daily')
                                                  : '',
                                                amount: actionNeedsAmount(nextActionType) ? item.amount : '',
                                              }
                                            : item,
                                        ),
                                      );
                                    }}
                                    className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                  >
                                    {EXECUTION_ACTION_OPTIONS.map((option) => (
                                      <option key={option.value} value={option.value}>
                                        {option.label}
                                      </option>
                                    ))}
                                  </select>
                                </div>
                              </div>

                              <div className="xl:col-span-2">
                                <label className="mb-1 block text-xs font-medium text-gray-500">基金代码</label>
                                <input
                                  value={action.fund_code}
                                  disabled={isPlanApplied || isSubmittingThisReport}
                                  onChange={(e) =>
                                    updateReportDraft(report.id, executionPlanDraft, (current) =>
                                      current.map((item) =>
                                        item.id === action.id ? { ...item, fund_code: e.target.value } : item,
                                      ),
                                    )
                                  }
                                  className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                  placeholder="如 164701"
                                />
                              </div>

                              <div className="xl:col-span-3">
                                <label className="mb-1 block text-xs font-medium text-gray-500">基金名称</label>
                                <input
                                  value={action.fund_name}
                                  disabled={isPlanApplied || isSubmittingThisReport}
                                  onChange={(e) =>
                                    updateReportDraft(report.id, executionPlanDraft, (current) =>
                                      current.map((item) =>
                                        item.id === action.id ? { ...item, fund_name: e.target.value } : item,
                                      ),
                                    )
                                  }
                                  className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                  placeholder="可留空，后端可仅按代码解析"
                                />
                              </div>

                              <div className="xl:col-span-2">
                                <label className="mb-1 block text-xs font-medium text-gray-500">
                                  {actionNeedsAmount(action.action_type) ? '金额（元）' : '金额（无需填写）'}
                                </label>
                                <input
                                  type="number"
                                  min="0"
                                  step="10"
                                  value={action.amount}
                                  disabled={isPlanApplied || isSubmittingThisReport || !actionNeedsAmount(action.action_type)}
                                  onChange={(e) =>
                                    updateReportDraft(report.id, executionPlanDraft, (current) =>
                                      current.map((item) =>
                                        item.id === action.id ? { ...item, amount: e.target.value } : item,
                                      ),
                                    )
                                  }
                                  className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none disabled:bg-gray-50"
                                  placeholder="10 的整数倍"
                                />
                              </div>

                              <div className="xl:col-span-3">
                                <label className="mb-1 block text-xs font-medium text-gray-500">备注</label>
                                <div className="flex items-center gap-2">
                                  <input
                                    value={action.note}
                                    disabled={isPlanApplied || isSubmittingThisReport}
                                    onChange={(e) =>
                                      updateReportDraft(report.id, executionPlanDraft, (current) =>
                                        current.map((item) =>
                                          item.id === action.id ? { ...item, note: e.target.value } : item,
                                        ),
                                      )
                                    }
                                    className="h-10 w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                    placeholder="可选备注"
                                  />
                                  {!isPlanApplied && (
                                    <button
                                      type="button"
                                      disabled={isSubmittingThisReport}
                                      onClick={() =>
                                        updateReportDraft(report.id, executionPlanDraft, (current) =>
                                          current.filter((item) => item.id !== action.id),
                                        )
                                      }
                                      className="h-10 rounded-lg border border-rose-200 px-3 py-2 text-sm text-rose-600 hover:bg-rose-50 disabled:opacity-60"
                                    >
                                      删除
                                    </button>
                                  )}
                                </div>
                              </div>
                            </div>

                            {isDcaActionType(action.action_type) && (
                              <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                                <div>
                                  <label className="mb-1 block text-xs font-medium text-gray-500">定投频率</label>
                                  <select
                                    value={action.frequency || 'daily'}
                                    disabled={isPlanApplied || isSubmittingThisReport}
                                    onChange={(e) => {
                                      const nextFrequency = e.target.value;
                                      updateReportDraft(report.id, executionPlanDraft, (current) =>
                                        current.map((item) =>
                                          item.id === action.id
                                            ? {
                                                ...item,
                                                frequency: nextFrequency,
                                                run_rule: buildRunRule(nextFrequency, weekdayFromRunRule(item.run_rule)),
                                              }
                                            : item,
                                        ),
                                      );
                                    }}
                                    className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                  >
                                    <option value="daily">每日</option>
                                    <option value="weekly">每周</option>
                                  </select>
                                </div>
                                <div>
                                  <label className="mb-1 block text-xs font-medium text-gray-500">执行规则</label>
                                  {action.frequency === 'weekly' ? (
                                    <select
                                      value={weeklyDay}
                                      disabled={isPlanApplied || isSubmittingThisReport}
                                      onChange={(e) =>
                                        updateReportDraft(report.id, executionPlanDraft, (current) =>
                                          current.map((item) =>
                                            item.id === action.id
                                              ? { ...item, run_rule: buildRunRule('weekly', e.target.value) }
                                              : item,
                                          ),
                                        )
                                      }
                                      className="w-full rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-cyan-400 focus:outline-none"
                                    >
                                      {WEEKDAY_OPTIONS.map((option) => (
                                        <option key={option.value} value={option.value}>
                                          {option.label}
                                        </option>
                                      ))}
                                    </select>
                                  ) : (
                                    <input
                                      value="每天"
                                      disabled
                                      className="w-full rounded-lg border border-gray-200 bg-gray-50 px-3 py-2 text-sm text-gray-500"
                                    />
                                  )}
                                </div>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>

                    {!isPlanApplied && (
                      <div className="mt-4 flex flex-wrap gap-3">
                        <button
                          type="button"
                          disabled={isSubmittingThisReport}
                          onClick={() =>
                            updateReportDraft(report.id, executionPlanDraft, (current) => [
                              ...current,
                              createEmptyExecutionAction(),
                            ])
                          }
                          className="rounded-lg border border-cyan-200 px-4 py-2 text-sm font-medium text-cyan-700 hover:bg-cyan-100 disabled:opacity-60"
                        >
                          新增一行
                        </button>
                        <button
                          type="button"
                          disabled={isSubmittingThisReport}
                          onClick={() => resetReportDraft(report.id)}
                          className="rounded-lg border border-gray-200 px-4 py-2 text-sm font-medium text-gray-600 hover:bg-gray-100 disabled:opacity-60"
                        >
                          恢复 AI 方案
                        </button>
                        <button
                          type="button"
                          disabled={isSubmittingThisReport}
                          onClick={() => applyExecutionPlan(report.id, visibleExecutionPlanDraft)}
                          className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          {isSubmittingThisReport ? '提交中...' : '提交方案'}
                        </button>
                      </div>
                    )}
                  </div>

                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    {detailSections.map((section) => {
                      const meta = sectionCardMeta(section.title);
                      const isWideCard = section.title === '配置与操作建议';
                      return (
                        <div
                          key={`${report.id}-${section.title}`}
                          className={`rounded-xl border p-4 shadow-sm ${meta.className} ${isWideCard ? 'xl:col-span-2' : ''}`}
                        >
                          <div className="mb-3 flex items-center gap-2">
                            <span className={`px-2.5 py-1 rounded-full text-xs font-semibold ${meta.badgeClassName}`}>
                              {section.title}
                            </span>
                            {section.title === '配置与操作建议' && (
                              <span className="text-xs text-gray-500">完整分析保留</span>
                            )}
                          </div>
                          <div className="whitespace-pre-wrap text-sm leading-7 text-gray-700">
                            {section.content}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : (
                <div className="mt-3 whitespace-pre-wrap text-sm leading-6 text-gray-700">
                  {report.report_body}
                </div>
              )}
            </div>
          );
        })}

        {analysisReports.length === 0 && (
          <div className="rounded-xl border border-dashed border-gray-200 p-8 text-center">
            <Info className="w-10 h-10 text-gray-300 mx-auto mb-3" />
            <p className="text-gray-400">暂时还没有投资建议，输入可支配资金后点击上方按钮生成增强月报。</p>
          </div>
        )}
      </div>
    </div>
  );

  const renderSettings = () => (
    <div className="bg-white p-8 rounded-xl shadow-sm border border-gray-100 max-w-2xl">
      <h3 className="text-xl font-bold mb-6">系统配置</h3>
      <div className="space-y-6">
        <div className="grid grid-cols-3 py-4 border-b border-gray-50">
          <div className="text-sm font-medium text-gray-500">数据库路径</div>
          <div className="col-span-2 text-sm font-mono break-all">{config?.db_path}</div>
        </div>
        <div className="grid grid-cols-3 py-4 border-b border-gray-50">
          <div className="text-sm font-medium text-gray-500">快照目录</div>
          <div className="col-span-2 text-sm font-mono break-all">{config?.snapshot_dir}</div>
        </div>
        <div className="grid grid-cols-3 py-4 border-b border-gray-50">
          <div className="text-sm font-medium text-gray-500">macOS 本地通知</div>
          <div className="col-span-2 text-sm">
            <span className={`px-2 py-1 rounded text-xs ${config?.notifications?.macos_enabled ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
              {config?.notifications?.macos_enabled ? '已启用' : '已禁用'}
            </span>
          </div>
        </div>
        <div className="grid grid-cols-3 py-4 border-b border-gray-50">
          <div className="text-sm font-medium text-gray-500">邮件通知</div>
          <div className="col-span-2 text-sm">
            <span className={`px-2 py-1 rounded text-xs ${config?.email_enabled ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
              {config?.email_enabled ? '已启用' : '已禁用'}
            </span>
          </div>
        </div>
      </div>
      <div className="mt-8 p-4 bg-amber-50 border border-amber-100 rounded-lg flex gap-3 text-amber-800 text-sm">
        <AlertCircle className="w-5 h-5 shrink-0" />
        <p>目前 Web 界面仅支持只读配置。如需修改，请直接编辑项目根目录下的 <code>config/fund_tracker.yaml</code> 文件并重启服务。</p>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gray-50 flex">
      {/* Sidebar */}
      <div className="w-64 bg-white border-r border-gray-200 flex flex-col fixed h-full">
        <div className="p-6 border-b border-gray-100 flex items-center gap-3">
          <div className="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center">
            <TrendingUp className="text-white w-5 h-5" />
          </div>
          <h1 className="text-xl font-bold tracking-tight text-gray-900">Fund Tracker</h1>
        </div>
        
        <nav className="flex-1 p-4 space-y-1">
          {[
            { id: 'dashboard', icon: LayoutDashboard, label: '仪表盘' },
            { id: 'analysis', icon: TrendingUp, label: '投资建议' },
            { id: 'transactions', icon: History, label: '交易流水' },
            { id: 'dca', icon: Clock, label: '定投计划' },
            { id: 'alerts', icon: Bell, label: '提醒记录' },
            { id: 'settings', icon: Settings, label: '设置' },
          ].map((item) => (
            <button
              key={item.id}
              onClick={() => setActiveTab(item.id)}
              className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all ${
                activeTab === item.id 
                  ? 'bg-blue-50 text-blue-700 font-semibold' 
                  : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900'
              }`}
            >
              <item.icon className="w-5 h-5" />
              {item.label}
            </button>
          ))}
        </nav>

        <div className="p-6 border-t border-gray-100">
          <div className="flex items-center gap-3 text-sm text-gray-400">
            <div className={`w-2 h-2 rounded-full ${loading ? 'bg-amber-400 animate-pulse' : 'bg-green-400'}`}></div>
            {loading ? '同步中...' : '服务在线'}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 ml-64 p-8">
        {/* Header */}
        <div className="flex justify-between items-center mb-8">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 capitalize">
              {activeTab === 'dashboard' ? '概览' : 
               activeTab === 'analysis' ? '投资建议' : 
               activeTab === 'transactions' ? '历史交易' : 
               activeTab === 'dca' ? '定投计划' : 
               activeTab === 'alerts' ? '提醒通知' : '系统设置'}
            </h2>
            <p className="text-gray-500">
              {new Date().toLocaleDateString('zh-CN', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}
            </p>
          </div>
          <button 
            onClick={fetchData} 
            className="p-2 hover:bg-white hover:shadow-sm border border-transparent hover:border-gray-200 rounded-lg transition-all"
            title="手动刷新"
          >
            <RefreshCw className={`w-5 h-5 text-gray-400 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {/* Alerts Overlay */}
        {statusMsg.text && (
          <div className={`fixed top-8 right-8 z-50 px-6 py-4 rounded-xl shadow-xl border animate-in slide-in-from-top-4 ${
            statusMsg.type === 'error' ? 'bg-red-50 border-red-100 text-red-800' : 'bg-green-50 border-green-100 text-green-800'
          }`}>
            <div className="flex items-center gap-3">
              {statusMsg.type === 'error' ? <XCircle className="w-5 h-5" /> : <RefreshCw className="w-5 h-5" />}
              <span className="font-medium">{statusMsg.text}</span>
            </div>
          </div>
        )}

        {/* Content Area */}
        {activeTab === 'dashboard' && renderDashboard()}
        {activeTab === 'analysis' && renderAnalysis()}
        {activeTab === 'transactions' && renderTransactions()}
        {activeTab === 'dca' && renderDCA()}
        {activeTab === 'alerts' && renderAlerts()}
        {activeTab === 'settings' && renderSettings()}
      </div>
    </div>
  );
}

export default App;
