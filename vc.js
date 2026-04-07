require('dotenv').config();

const { Telegraf, Markup } = require('telegraf');
const { google } = require('googleapis');
const axios = require('axios');
const stream = require('stream');
const XLSX = require('xlsx');
const cron = require('node-cron');

/**
 * --- CONFIGURATION ---
 * Secrets MUST come from environment variables (.env)
 */
const CONFIG = {
    TELEGRAM_TOKEN: process.env.TELEGRAM_TOKEN,
    ADMIN_ID: process.env.ADMIN_ID ? Number(process.env.ADMIN_ID) : undefined,
    ALERT_CHAT_ID: process.env.ALERT_CHAT_ID ? Number(process.env.ALERT_CHAT_ID) : undefined,

    GEMINI_API_KEY: process.env.GEMINI_API_KEY,
    GEMINI_API_KEYS: process.env.GEMINI_API_KEYS,
    GEMINI_MODEL: process.env.GEMINI_MODEL || 'gemini-2.0-flash',
    GEMINI_RPM: process.env.GEMINI_RPM ? Number(process.env.GEMINI_RPM) : 15,
    GEMINI_RDP: process.env.GEMINI_RDP ? Number(process.env.GEMINI_RDP) : 1500,

    SHEET_ID: process.env.SHEET_ID,
    ROOT_DRIVE_FOLDER_ID: process.env.ROOT_DRIVE_FOLDER_ID,
    SERVICE_ACCOUNT_KEYFILE: process.env.SERVICE_ACCOUNT_KEYFILE || 'service-account.json',

    RETRY_ATTEMPTS: process.env.RETRY_ATTEMPTS ? Number(process.env.RETRY_ATTEMPTS) : 3,
    TIMEZONE: process.env.TIMEZONE || 'Asia/Yangon',
};

const missing = [];
if (!CONFIG.TELEGRAM_TOKEN) missing.push('TELEGRAM_TOKEN');
if (!CONFIG.ADMIN_ID) missing.push('ADMIN_ID');
if (!CONFIG.GEMINI_API_KEY && !CONFIG.GEMINI_API_KEYS) missing.push('GEMINI_API_KEY (or GEMINI_API_KEYS)');
if (!CONFIG.SHEET_ID) missing.push('SHEET_ID');
if (!CONFIG.ROOT_DRIVE_FOLDER_ID) missing.push('ROOT_DRIVE_FOLDER_ID');
if (missing.length) {
    console.error(`❌ Missing required env vars: ${missing.join(', ')}`);
    console.error('Create .env using .env.example then restart the bot.');
    process.exit(1);
}

const bot = new Telegraf(CONFIG.TELEGRAM_TOKEN);

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function dateKeyLocal(d = new Date()) {
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
}

function parseGeminiApiKeys() {
    const raw = CONFIG.GEMINI_API_KEYS || CONFIG.GEMINI_API_KEY || '';
    return raw
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
}

const geminiKeyPool = parseGeminiApiKeys().map((key) => ({
    key,
    minuteStamps: [],
    dayKey: dateKeyLocal(),
    dayCount: 0,
    cooldownUntil: 0,
    backoffMs: 0,
    disabled: false,
}));

function pruneMinuteWindow(state, nowMs) {
    const cutoff = nowMs - 60_000;
    while (state.minuteStamps.length && state.minuteStamps[0] <= cutoff) state.minuteStamps.shift();
}

function classifyGeminiError(err) {
    const status = err && err.response && err.response.status;
    const message =
        (err && err.response && err.response.data && (err.response.data.error?.message || err.response.data.message)) ||
        err?.message ||
        '';

    const retryableStatuses = new Set([408, 429, 500, 502, 503, 504]);
    const isRetryable = !status || retryableStatuses.has(status) || /rate|quota|limit|exhaust/i.test(String(message));
    const isAuthError = status === 401 || status === 403;

    return { status, message: String(message), isRetryable, isAuthError };
}

function describeHttpError(err, fallback = 'Request failed') {
    const status = err && err.response && err.response.status;
    const apiMessage =
        (err && err.response && err.response.data && (err.response.data.error?.message || err.response.data.message)) ||
        '';
    const baseMessage = String(apiMessage || err?.message || fallback);
    if (!status) return baseMessage;
    return `HTTP ${status}: ${baseMessage}`;
}

async function acquireGeminiKey() {
    if (!geminiKeyPool.length) throw new Error('No Gemini API keys configured');

    // Wait until any key is available under RPM/RDP + cooldown.
    // This is global per-process; your voucher queue already serializes most requests.
    for (;;) {
        const nowMs = Date.now();
        const today = dateKeyLocal();

        for (const s of geminiKeyPool) {
            if (s.dayKey !== today) {
                s.dayKey = today;
                s.dayCount = 0;
                s.minuteStamps = [];
                s.cooldownUntil = 0;
                s.backoffMs = 0;
            }
            pruneMinuteWindow(s, nowMs);
        }

        const candidates = geminiKeyPool
            .filter((s) => !s.disabled)
            .filter((s) => s.cooldownUntil <= nowMs)
            .filter((s) => s.dayCount < CONFIG.GEMINI_RDP)
            .filter((s) => s.minuteStamps.length < CONFIG.GEMINI_RPM)
            .sort((a, b) => a.minuteStamps.length - b.minuteStamps.length);

        if (candidates.length) {
            const s = candidates[0];
            // Reserve a slot immediately to avoid burst oversubscription.
            s.minuteStamps.push(nowMs);
            s.dayCount += 1;
            return s;
        }

        // Compute when the next key becomes available.
        let nextMs = Infinity;
        for (const s of geminiKeyPool) {
            if (s.disabled) continue;
            if (s.dayCount >= CONFIG.GEMINI_RDP) {
                const tomorrow = new Date();
                tomorrow.setDate(tomorrow.getDate() + 1);
                tomorrow.setHours(0, 0, 1, 0);
                nextMs = Math.min(nextMs, tomorrow.getTime());
                continue;
            }
            nextMs = Math.min(nextMs, s.cooldownUntil || Infinity);
            if (s.minuteStamps.length >= CONFIG.GEMINI_RPM) {
                const oldest = s.minuteStamps[0];
                nextMs = Math.min(nextMs, oldest + 60_000);
            }
        }
        if (!Number.isFinite(nextMs)) throw new Error('All Gemini API keys are disabled');
        const waitMs = Math.max(250, Math.min(10_000, nextMs - nowMs));
        await sleep(waitMs);
    }
}

async function geminiGenerateContent(parts) {
    const modelName = String(CONFIG.GEMINI_MODEL || '').replace(/^models\//, '');
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelName)}:generateContent`;
    let lastErr;
    const attempts = Math.max(1, Number(CONFIG.RETRY_ATTEMPTS) || 1);
    for (let i = 0; i < attempts; i++) {
        const keyState = await acquireGeminiKey();
        try {
            const res = await axios.post(
                url,
                { contents: [{ parts }] },
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'x-goog-api-key': keyState.key,
                    },
                    timeout: 60000,
                }
            );

            const candidates = res.data && res.data.candidates;
            const first = Array.isArray(candidates) ? candidates[0] : undefined;
            const outParts = first && first.content && Array.isArray(first.content.parts) ? first.content.parts : [];
            const text = outParts.map((p) => p.text).filter(Boolean).join('');

            if (!text) {
                const reason = first && first.finishReason ? String(first.finishReason) : '';
                throw new Error(`Gemini returned empty response${reason ? ` (finishReason=${reason})` : ''}`);
            }

            keyState.backoffMs = 0;
            return text;
        } catch (err) {
            const info = classifyGeminiError(err);
            lastErr = describeHttpError(err, info.message || 'Gemini request failed');

            if (info.isAuthError) {
                keyState.disabled = true;
            } else if (info.isRetryable) {
                const base = keyState.backoffMs || 5000;
                keyState.backoffMs = Math.min(base * 2, 60_000);
                keyState.cooldownUntil = Date.now() + base;
            }

            if (!info.isRetryable || i === attempts - 1) {
                throw new Error(lastErr || 'Gemini request failed');
            }
        }
    }
    throw new Error(lastErr || 'Gemini request failed');
}

// State & Queue Management
const userState = new Map();
const pendingApprovals = new Map();
const adminEditState = new Map();
const pendingRejectConfirms = new Map();
const voucherQueue = [];
let isProcessing = false;

// Google Auth
const auth = new google.auth.GoogleAuth({
    keyFile: CONFIG.SERVICE_ACCOUNT_KEYFILE,
    scopes: [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
    ],
});

const SHEETS = {
    MAIN: 'Sheet1',
    ITEMS: 'Items_Log',
    CONFIG: 'Bot_Config',
    MIN_STOCK: 'Min_Stock',
};

function isAdmin(userId) {
    return Number(userId) === Number(CONFIG.ADMIN_ID);
}

async function ensureSheetExists(title, headerRow) {
    const sheets = google.sheets({ version: 'v4', auth });
    const meta = await sheets.spreadsheets.get({ spreadsheetId: CONFIG.SHEET_ID });
    const exists = (meta.data.sheets || []).some((s) => s.properties && s.properties.title === title);
    if (exists) return;
    await sheets.spreadsheets.batchUpdate({
        spreadsheetId: CONFIG.SHEET_ID,
        resource: {
            requests: [{ addSheet: { properties: { title } } }],
        },
    });
    if (headerRow && headerRow.length) {
        await sheets.spreadsheets.values.update({
            spreadsheetId: CONFIG.SHEET_ID,
            range: `${title}!A1:${String.fromCharCode(64 + headerRow.length)}1`,
            valueInputOption: 'RAW',
            resource: { values: [headerRow] },
        });
    }
}

async function getConfigValue(key) {
    await ensureSheetExists(SHEETS.CONFIG, ['key', 'value']);
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.CONFIG}!A:B` });
    const rows = res.data.values || [];
    for (const r of rows.slice(1)) {
        if (r[0] === key) return r[1];
    }
    return undefined;
}

async function setConfigValue(key, value) {
    await ensureSheetExists(SHEETS.CONFIG, ['key', 'value']);
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.CONFIG}!A:B` });
    const rows = res.data.values || [];
    let rowIndex = -1;
    for (let i = 1; i < rows.length; i++) {
        if (rows[i][0] === key) {
            rowIndex = i + 1; // 1-based row
            break;
        }
    }
    if (rowIndex === -1) {
        await sheets.spreadsheets.values.append({
            spreadsheetId: CONFIG.SHEET_ID,
            range: `${SHEETS.CONFIG}!A:B`,
            valueInputOption: 'RAW',
            resource: { values: [[key, String(value)]] },
        });
        return;
    }
    await sheets.spreadsheets.values.update({
        spreadsheetId: CONFIG.SHEET_ID,
        range: `${SHEETS.CONFIG}!A${rowIndex}:B${rowIndex}`,
        valueInputOption: 'RAW',
        resource: { values: [[key, String(value)]] },
    });
}

async function getAlertChatId() {
    const cfg = await getConfigValue('alert_chat_id');
    const fromCfg = cfg ? Number(cfg) : undefined;
    return fromCfg || CONFIG.ALERT_CHAT_ID || CONFIG.ADMIN_ID;
}

async function getStaffIds() {
    const raw = await getConfigValue('staff_ids');
    if (!raw) return new Set();
    const ids = raw
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .map((s) => Number(s))
        .filter((n) => Number.isFinite(n));
    return new Set(ids);
}

async function getExpiryDisabledItems() {
    const raw = await getConfigValue('expiry_disabled_items');
    if (!raw) return new Set();
    const items = raw
        .split(',')
        .map((s) => s.trim().toLowerCase())
        .filter(Boolean);
    return new Set(items);
}

async function setExpiryDisabledItems(set) {
    const list = Array.from(set)
        .map((s) => String(s || '').trim())
        .filter(Boolean)
        .join(',');
    await setConfigValue('expiry_disabled_items', list);
}

async function isStaffOrAdmin(userId) {
    if (isAdmin(userId)) return true;
    const staff = await getStaffIds();
    return staff.has(Number(userId));
}

function nowISODate() {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
}

function toMonthKeyFromDateValue(input) {
    const raw = String(input ?? '').trim();
    if (!raw) return '';

    // ISO-like: YYYY-MM or YYYY/MM/DD
    let m = raw.match(/^(\d{4})[\-\/.](\d{1,2})(?:[\-\/.]\d{1,2})?$/);
    if (m) {
        const year = Number(m[1]);
        const month = Number(m[2]);
        if (month >= 1 && month <= 12) return `${year}-${String(month).padStart(2, '0')}`;
    }

    // D/M/YYYY or M/D/YYYY (prefer D/M/YYYY when ambiguous)
    m = raw.match(/^(\d{1,2})[\-\/.](\d{1,2})[\-\/.](\d{2,4})$/);
    if (m) {
        const a = Number(m[1]);
        const b = Number(m[2]);
        let y = Number(m[3]);
        if (y < 100) y += 2000;

        let month = b;
        if (a <= 12 && b > 12) month = a;
        else if (a > 12 && b <= 12) month = b;

        if (month >= 1 && month <= 12) return `${y}-${String(month).padStart(2, '0')}`;
    }

    // Google Sheets serial date number.
    if (/^\d+(?:\.\d+)?$/.test(raw)) {
        const serial = Number(raw);
        if (Number.isFinite(serial) && serial > 20000 && serial < 90000) {
            const epoch = new Date(Date.UTC(1899, 11, 30));
            const ms = Math.round(serial * 24 * 60 * 60 * 1000);
            const dt = new Date(epoch.getTime() + ms);
            const year = dt.getUTCFullYear();
            const month = dt.getUTCMonth() + 1;
            return `${year}-${String(month).padStart(2, '0')}`;
        }
    }

    return '';
}

function parseExpiryToDate(expiryStr) {
    if (!expiryStr) return null;
    const s = String(expiryStr).trim();
    let m;
    // MM/YY or MM/YYYY
    m = s.match(/^(\d{1,2})\s*[\/\-]\s*(\d{2}|\d{4})$/);
    if (m) {
        const month = Number(m[1]);
        let year = Number(m[2]);
        if (year < 100) year = 2000 + year;
        if (month < 1 || month > 12) return null;
        // last day of month
        return new Date(year, month, 0, 23, 59, 59);
    }
    // YYYY-MM
    m = s.match(/^(\d{4})\s*[\/\-]\s*(\d{1,2})$/);
    if (m) {
        const year = Number(m[1]);
        const month = Number(m[2]);
        if (month < 1 || month > 12) return null;
        return new Date(year, month, 0, 23, 59, 59);
    }
    return null;
}

function parseNumber(val) {
    const raw = String(val ?? '').trim();
    if (!raw) return 0;

    // Keep digits, decimal point and minus sign; tolerate currency/unit text like "MMK".
    const cleaned = raw
        .replace(/\(([^)]+)\)/g, '-$1')
        .replace(/[^0-9.\-]/g, '')
        .replace(/(?!^)-/g, '');

    const n = Number(cleaned);
    return Number.isFinite(n) ? n : 0;
}

function normalizePaymentStatus(input) {
    const raw = String(input || '').trim();
    if (!raw) return 'Unpaid';
    const lower = raw.toLowerCase();

    // Check unpaid markers first so words like "unpaid" are not treated as "paid".
    if (lower.includes('unpaid') || lower.includes('not paid') || lower.includes('မပေး')) return 'Unpaid';
    if (lower.includes('paid') || lower.includes('ပေးပြီး')) return 'Paid';
    return 'Unpaid';
}

function normalizeInvoiceKey(input) {
    return String(input || '')
        .trim()
        .replace(/^'+/, '')
        .replace(/\s+/g, '')
        .toLowerCase();
}

function round2(n) {
    return Math.round((Number(n) + Number.EPSILON) * 100) / 100;
}

function isFocItem(row) {
    const name = String(row?.name || '').toLowerCase();
    const qty = parseNumber(row?.qty);
    const price = parseNumber(row?.price);
    const total = parseNumber(row?.total);
    const aiFoc = String(row?.is_foc ?? row?.foc ?? '').toLowerCase();
    const aiSample = String(row?.is_sample ?? row?.sample ?? '').toLowerCase();
    const byAiFlag = ['true', '1', 'yes', 'y'].includes(aiFoc) || ['true', '1', 'yes', 'y'].includes(aiSample);
    const byName = /\bfoc\b|free|bonus|complimentary|sample/.test(name);
    const byAmount = qty > 0 && (price === 0 || total === 0);
    return byAiFlag || byName || byAmount;
}

function buildVoucherPricing(voucher) {
    const items = Array.isArray(voucher?.items) ? voucher.items : [];
    let grossTotal = 0;
    let itemsNetTotal = 0;
    let focQty = 0;
    let focLines = 0;

    for (const row of items) {
        const qty = parseNumber(row?.qty);
        const price = parseNumber(row?.price);
        const lineTotal = parseNumber(row?.total);
        const calculated = qty > 0 && price > 0 ? round2(qty * price) : 0;

        if (isFocItem(row)) {
            focLines += 1;
            focQty += qty;
        }

        if (calculated > 0) grossTotal += calculated;
        if (lineTotal > 0) itemsNetTotal += lineTotal;
        else if (calculated > 0) itemsNetTotal += calculated;
    }

    const aiGrossTotal = round2(parseNumber(voucher?.gross_total));
    grossTotal = round2(aiGrossTotal > 0 ? aiGrossTotal : grossTotal);
    itemsNetTotal = round2(itemsNetTotal);
    const aiNetTotal = round2(parseNumber(voucher?.net_total));
    const netTotal = aiNetTotal > 0 ? aiNetTotal : itemsNetTotal;
    const aiDiscountTotal = round2(parseNumber(voucher?.discount_total));
    const discount = aiDiscountTotal > 0 ? aiDiscountTotal : grossTotal > 0 ? round2(Math.max(0, grossTotal - netTotal)) : 0;

    return {
        grossTotal,
        netTotal,
        discount,
        focQty: round2(focQty),
        focLines,
    };
}

function buildVoucherCounterCheck(voucher) {
    const items = Array.isArray(voucher?.items) ? voucher.items : [];
    let itemsTotal = 0;
    let itemLineMismatchCount = 0;

    for (const row of items) {
        const qty = parseNumber(row?.qty);
        const price = parseNumber(row?.price);
        const rowTotal = parseNumber(row?.total);

        const calculated = qty > 0 && price > 0 ? round2(qty * price) : 0;
        if (rowTotal > 0 && calculated > 0 && Math.abs(rowTotal - calculated) > 1) {
            itemLineMismatchCount += 1;
        }

        if (rowTotal > 0) itemsTotal += rowTotal;
        else if (calculated > 0) itemsTotal += calculated;
    }

    itemsTotal = round2(itemsTotal);
    const aiTotal = round2(parseNumber(voucher?.net_total));
    const totalMismatch = aiTotal > 0 && itemsTotal > 0 && Math.abs(aiTotal - itemsTotal) > 1;
    const recommendedTotal = itemsTotal > 0 ? itemsTotal : aiTotal;

    return {
        aiTotal,
        itemsTotal,
        totalMismatch,
        itemLineMismatchCount,
        recommendedTotal,
        hasIssue: totalMismatch || itemLineMismatchCount > 0,
    };
}

async function appendSheetValues(range, values, valueInputOption = 'USER_ENTERED') {
    const sheets = google.sheets({ version: 'v4', auth });
    const attempts = Math.max(1, Number(CONFIG.RETRY_ATTEMPTS) || 1);
    let lastErr;

    for (let i = 0; i < attempts; i++) {
        try {
            return await sheets.spreadsheets.values.append({
                spreadsheetId: CONFIG.SHEET_ID,
                range,
                valueInputOption,
                resource: { values },
            });
        } catch (err) {
            lastErr = err;
            const status = err && err.response && err.response.status;
            const retryable = !status || [408, 429, 500, 502, 503, 504].includes(status);
            if (!retryable || i === attempts - 1) break;
            await sleep(800 * (i + 1));
        }
    }

    throw new Error(describeHttpError(lastErr, `Sheet append failed for ${range}`));
}

async function updateSheetValues(range, values, valueInputOption = 'USER_ENTERED') {
    const sheets = google.sheets({ version: 'v4', auth });
    const attempts = Math.max(1, Number(CONFIG.RETRY_ATTEMPTS) || 1);
    let lastErr;

    for (let i = 0; i < attempts; i++) {
        try {
            return await sheets.spreadsheets.values.update({
                spreadsheetId: CONFIG.SHEET_ID,
                range,
                valueInputOption,
                resource: { values },
            });
        } catch (err) {
            lastErr = err;
            const status = err && err.response && err.response.status;
            const retryable = !status || [408, 429, 500, 502, 503, 504].includes(status);
            if (!retryable || i === attempts - 1) break;
            await sleep(800 * (i + 1));
        }
    }

    throw new Error(describeHttpError(lastErr, `Sheet update failed for ${range}`));
}

async function findInvoiceRowIndex(invoiceNo) {
    const invKey = normalizeInvoiceKey(invoiceNo);
    if (!invKey || invKey === '-') return -1;
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!C:C` });
    const values = res.data.values || [];

    let rowIndex = -1;
    for (let i = 1; i < values.length; i++) {
        const rowKey = normalizeInvoiceKey(values[i][0]);
        if (rowKey && rowKey === invKey) rowIndex = i + 1;
    }
    return rowIndex;
}

async function getInvoiceStatusInfo(invoiceNo) {
    const invKey = normalizeInvoiceKey(invoiceNo);
    if (!invKey || invKey === '-') return { found: false, rowIndex: -1, status: '' };

    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!A:N` });
    const rows = res.data.values || [];

    let rowIndex = -1;
    let status = '';
    for (let i = 1; i < rows.length; i++) {
        const rowKey = normalizeInvoiceKey(rows[i][2]);
        if (rowKey && rowKey === invKey) {
            rowIndex = i + 1;
            status = String(rows[i][8] || '').trim();
        }
    }

    if (rowIndex === -1) return { found: false, rowIndex: -1, status: '' };
    return { found: true, rowIndex, status };
}

async function updateInvoiceStatus(invoiceNo, status) {
    const rowIndex = await findInvoiceRowIndex(invoiceNo);
    if (rowIndex === -1) return false;
    await updateSheetValues(`${SHEETS.MAIN}!I${rowIndex}:I${rowIndex}`, [[normalizePaymentStatus(status)]], 'RAW');
    return true;
}

async function updateInvoiceDriveUrl(invoiceNo, driveUrl) {
    const rowIndex = await findInvoiceRowIndex(invoiceNo);
    if (rowIndex === -1) return false;
    await updateSheetValues(`${SHEETS.MAIN}!J${rowIndex}:J${rowIndex}`, [[String(driveUrl || '-').trim() || '-']], 'RAW');
    return true;
}

async function updateInvoiceField(lookupInvoiceNo, field, value) {
    const rowIndex = await findInvoiceRowIndex(lookupInvoiceNo);
    if (rowIndex === -1) return false;

    if (field === 'vendor') {
        await updateSheetValues(`${SHEETS.MAIN}!B${rowIndex}:B${rowIndex}`, [[String(value || '-').trim() || '-']], 'USER_ENTERED');
        return true;
    }

    if (field === 'date') {
        await updateSheetValues(`${SHEETS.MAIN}!A${rowIndex}:A${rowIndex}`, [[String(value || '').trim()]], 'USER_ENTERED');
        return true;
    }

    if (field === 'category') {
        await updateSheetValues(`${SHEETS.MAIN}!D${rowIndex}:D${rowIndex}`, [[String(value || '-').trim() || '-']], 'USER_ENTERED');
        return true;
    }

    if (field === 'invoice_no') {
        await updateSheetValues(`${SHEETS.MAIN}!C${rowIndex}:C${rowIndex}`, [[String(value || '-').trim() || '-']], 'USER_ENTERED');
        return true;
    }

    if (field === 'net_total') {
        const total = String(value || '0').trim() || '0';
        await updateSheetValues(`${SHEETS.MAIN}!E${rowIndex}:E${rowIndex}`, [[total]], 'USER_ENTERED');
        await updateSheetValues(`${SHEETS.MAIN}!G${rowIndex}:G${rowIndex}`, [[total]], 'USER_ENTERED');
        return true;
    }

    return false;
}

async function upsertDraftMainRowFromApproval(data, editedBy = 'Admin Edit') {
    const invoice = String(data?.invoice_no || '').trim();
    if (!invoice || invoice === '-') return false;

    const counterCheck = buildVoucherCounterCheck(data);
    const pricing = buildVoucherPricing(data);
    const safeDate = String(data?.date || nowISODate()).trim() || nowISODate();
    const safeVendor = String(data?.vendor || '-').trim() || '-';
    const safeInvoice = invoice;
    const safeCategory = String(data?.category || '-').trim() || '-';
    const safePhone = String(data?.phone || '-').trim() || '-';
    const safeUser = String(editedBy || '-').trim() || '-';
    const total = round2(parseNumber(data?.net_total || pricing.netTotal || counterCheck.recommendedTotal || 0));
    const discount = round2(pricing.discount);
    const note = 'DRAFT_FROM_ADMIN_EDIT';
    const status = normalizePaymentStatus(data?.status);

    const row = [safeDate, safeVendor, safeInvoice, safeCategory, total, discount, total, note, status, '-', safeUser, new Date().toLocaleString(), 'Draft', safePhone];
    const rowIndex = await findInvoiceRowIndex(safeInvoice);
    if (rowIndex > 0) {
        await updateSheetValues(`${SHEETS.MAIN}!A${rowIndex}:N${rowIndex}`, [row], 'USER_ENTERED');
        return true;
    }

    await appendSheetValues(`${SHEETS.MAIN}!A:N`, [row], 'USER_ENTERED');
    return true;
}

/**
 * DRIVE: Optimized Storage
 */
async function getOrCreateFolder(drive, folderName, parentId) {
    const query = `name = '${folderName}' and '${parentId}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`;
    try {
        const res = await drive.files.list({ 
            q: query, 
            fields: 'files(id)',
            supportsAllDrives: true,
            includeItemsFromAllDrives: true
        });
        if (res.data.files && res.data.files.length > 0) return res.data.files[0].id;

        const folder = await drive.files.create({
            resource: { name: folderName, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
            fields: 'id',
            supportsAllDrives: true 
        });
        return folder.data.id;
    } catch (error) {
        console.error("Drive Folder Error:", error.message);
        throw error;
    }
}

async function uploadToDrive(buffer, filename) {
    const drive = google.drive({ version: 'v3', auth });
    const now = new Date();
    const y = now.getFullYear().toString(), m = (now.getMonth() + 1).toString().padStart(2, '0'), d = now.getDate().toString().padStart(2, '0');
    try {
        const yId = await getOrCreateFolder(drive, y, CONFIG.ROOT_DRIVE_FOLDER_ID);
        const mId = await getOrCreateFolder(drive, m, yId);
        const dId = await getOrCreateFolder(drive, d, mId);

        const res = await drive.files.create({
            resource: { name: filename, parents: [dId] },
            media: { mimeType: 'image/jpeg', body: stream.Readable.from(buffer) },
            fields: 'id, webViewLink',
            supportsAllDrives: true 
        });
        return res.data.webViewLink;
    } catch (error) {
        console.error("Upload Error:", error.message);
        throw error;
    }
}

/**
 * AI Logic
 */
async function analyzeVoucherAI(buffer, mimeType) {
        const prompt = `Extract voucher data into JSON.
Priorities:
1) Detect status from Paid/Unpaid words, stamps, checkmarks.
2) Detect pricing structure including gross total, discount total, net total.
3) Detect line items that are FOC/free/sample/bonus.

Rules:
- If line text indicates free/FOC/sample/bonus, set is_foc or is_sample true.
- Keep numbers as plain numeric strings without currency symbols.
- If a field is missing, return empty string.

Return STRICT JSON only (no markdown). JSON schema:
{
    "invoice_no":"",
    "vendor":"",
    "phone":"",
    "date":"YYYY-MM-DD",
    "category":"",
    "gross_total":"",
    "discount_total":"",
    "net_total":"",
    "status":"Paid|Unpaid",
    "items":[
        {
            "name":"",
            "qty":"",
            "price":"",
            "total":"",
            "is_foc":false,
            "is_sample":false,
            "discount":""
        }
    ]
}`;
    const text = await geminiGenerateContent([
        { text: prompt },
        { inline_data: { mime_type: mimeType, data: buffer.toString('base64') } },
    ]);

    // Gemini can return JSON wrapped with extra prose or code fences.
    // Extract the first balanced JSON object and parse that only.
    function extractFirstJsonObject(input) {
        const source = String(input || '').replace(/```json|```/gi, '').trim();
        const start = source.indexOf('{');
        if (start === -1) return null;

        let depth = 0;
        let inString = false;
        let escaped = false;

        for (let i = start; i < source.length; i++) {
            const ch = source[i];
            if (inString) {
                if (escaped) {
                    escaped = false;
                    continue;
                }
                if (ch === '\\') {
                    escaped = true;
                    continue;
                }
                if (ch === '"') inString = false;
                continue;
            }

            if (ch === '"') {
                inString = true;
                continue;
            }
            if (ch === '{') depth += 1;
            if (ch === '}') {
                depth -= 1;
                if (depth === 0) return source.slice(start, i + 1);
            }
        }

        return null;
    }

    const jsonText = extractFirstJsonObject(text);
    if (!jsonText) throw new Error('AI did not return JSON');
    const parsed = JSON.parse(jsonText);
    if (!parsed.items || !Array.isArray(parsed.items)) parsed.items = [];
    parsed.gross_total = String(parsed.gross_total || '').trim();
    parsed.discount_total = String(parsed.discount_total || '').trim();
    parsed.net_total = String(parsed.net_total || '').trim();
    parsed.items = parsed.items.map((row) => ({
        name: String(row?.name || '').trim(),
        qty: String(row?.qty || '').trim(),
        price: String(row?.price || '').trim(),
        total: String(row?.total || '').trim(),
        is_foc: ['true', '1', 'yes', 'y'].includes(String(row?.is_foc ?? row?.foc ?? '').toLowerCase()) || /\bfoc\b|free|bonus|complimentary/.test(String(row?.name || '').toLowerCase()),
        is_sample: ['true', '1', 'yes', 'y'].includes(String(row?.is_sample ?? row?.sample ?? '').toLowerCase()) || /sample/.test(String(row?.name || '').toLowerCase()),
        discount: String(row?.discount || '').trim(),
    }));
    parsed.status = normalizePaymentStatus(parsed.status);
    return parsed;
}

async function saveToSheets(data, user, driveUrl, expiries) {
    await ensureSheetExists(SHEETS.MAIN, ['date', 'vendor', 'invoice_no', 'category', 'net_total', 'discount', 'grand_total', 'note', 'status', 'drive_url', 'created_by', 'created_at', 'source', 'phone']);
    await ensureSheetExists(SHEETS.ITEMS, ['date', 'invoice_no', 'vendor', 'item', 'qty', 'price', 'total', 'by', 'phone', 'expiry', 'status']);
    const counterCheck = buildVoucherCounterCheck(data);
    const pricing = buildVoucherPricing(data);
    const safeDate = String(data.date || nowISODate()).trim() || nowISODate();
    const safeVendor = String(data.vendor || '-').trim() || '-';
    const safeInvoice = String(data.invoice_no || '-').trim() || '-';
    const safeCategory = String(data.category || '-').trim() || '-';
    const safeTotalRaw = String(data.net_total || '').trim();
    const grandTotal = round2(parseNumber(safeTotalRaw || pricing.netTotal || counterCheck.recommendedTotal || 0));
    const discountTotal = round2(pricing.discount);
    const safePhone = String(data.phone || '-').trim() || '-';
    const safeDriveUrl = String(driveUrl || '-').trim() || '-';
    const safeUser = String(user || '-').trim() || '-';
    const noteParts = [];
    if (counterCheck.hasIssue) {
        noteParts.push(`CHECK AI:${counterCheck.aiTotal} ITEMS:${counterCheck.itemsTotal} LINE_MISMATCH:${counterCheck.itemLineMismatchCount}`);
    }
    if (pricing.focLines > 0 || pricing.discount > 0) {
        noteParts.push(`FOC_LINES:${pricing.focLines} FOC_QTY:${pricing.focQty} DISCOUNT:${pricing.discount}`);
    }
    const note = noteParts.length ? noteParts.join(' | ') : '-';

    const status = normalizePaymentStatus(data.status);
    const mainRow = [safeDate, safeVendor, safeInvoice, safeCategory, grandTotal, discountTotal, grandTotal, note, status, safeDriveUrl, safeUser, new Date().toLocaleString(), "Voucher", safePhone];
    const existingRowIndex = await findInvoiceRowIndex(safeInvoice);
    if (existingRowIndex > 0) {
        await updateSheetValues(`${SHEETS.MAIN}!A${existingRowIndex}:N${existingRowIndex}`, [mainRow], 'USER_ENTERED');
    } else {
        await appendSheetValues(`${SHEETS.MAIN}!A:N`, [mainRow], 'USER_ENTERED');
    }

    const safeItems = Array.isArray(data.items) ? data.items : [];
    if (!safeItems.length) {
        return {
            mainSaved: true,
            itemRowsSaved: 0,
            status,
            mainMode: existingRowIndex > 0 ? 'updated' : 'appended',
            counterCheck,
            pricing,
        };
    }

    const itemRows = safeItems.map((i, idx) => [safeDate, safeInvoice, safeVendor, i.name, i.qty, i.price, i.total, safeUser, safePhone, expiries[idx] || "-", "Active"]);
    try {
        await appendSheetValues(`${SHEETS.ITEMS}!A:K`, itemRows, 'USER_ENTERED');
        return { mainSaved: true, itemRowsSaved: itemRows.length, status, mainMode: existingRowIndex > 0 ? 'updated' : 'appended', counterCheck, pricing };
    } catch (itemErr) {
        return {
            mainSaved: true,
            itemRowsSaved: 0,
            status,
            mainMode: existingRowIndex > 0 ? 'updated' : 'appended',
            counterCheck,
            pricing,
            itemError: describeHttpError(itemErr, 'Items_Log write failed'),
        };
    }
}

async function invoiceExists(invoiceNo) {
    const inv = String(invoiceNo || '').trim();
    if (!inv) return false;
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!C:C` });
    const values = res.data.values || [];
    return values.some((row) => String(row[0] || '').trim() === inv);
}

async function appendItemLogRow(row) {
    await ensureSheetExists(SHEETS.ITEMS, ['date', 'invoice_no', 'vendor', 'item', 'qty', 'price', 'total', 'by', 'phone', 'expiry', 'status']);
    const sheets = google.sheets({ version: 'v4', auth });
    await sheets.spreadsheets.values.append({
        spreadsheetId: CONFIG.SHEET_ID,
        range: `${SHEETS.ITEMS}!A:K`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [row] },
    });
}

async function getStockSummary() {
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.ITEMS}!A:K` });
    const rows = res.data.values || [];
    const summary = {};
    rows.slice(1).forEach((r) => {
        const name = r[3];
        if (!name) return;
        const qty = parseNumber(r[4]);
        summary[name] = (summary[name] || 0) + qty;
    });
    return summary;
}

async function getMinStockMap() {
    await ensureSheetExists(SHEETS.MIN_STOCK, ['item', 'min_qty']);
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MIN_STOCK}!A:B` });
    const rows = res.data.values || [];
    const map = {};
    rows.slice(1).forEach((r) => {
        const item = (r[0] || '').trim();
        if (!item) return;
        map[item] = parseNumber(r[1]);
    });
    return map;
}

async function upsertMinStock(itemName, minQty) {
    await ensureSheetExists(SHEETS.MIN_STOCK, ['item', 'min_qty']);
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MIN_STOCK}!A:B` });
    const rows = res.data.values || [];
    let rowIndex = -1;
    for (let i = 1; i < rows.length; i++) {
        if (String(rows[i][0] || '').trim().toLowerCase() === String(itemName).trim().toLowerCase()) {
            rowIndex = i + 1;
            break;
        }
    }
    if (rowIndex === -1) {
        await sheets.spreadsheets.values.append({
            spreadsheetId: CONFIG.SHEET_ID,
            range: `${SHEETS.MIN_STOCK}!A:B`,
            valueInputOption: 'RAW',
            resource: { values: [[String(itemName).trim(), String(minQty)]] },
        });
        return;
    }
    await sheets.spreadsheets.values.update({
        spreadsheetId: CONFIG.SHEET_ID,
        range: `${SHEETS.MIN_STOCK}!A${rowIndex}:B${rowIndex}`,
        valueInputOption: 'RAW',
        resource: { values: [[String(itemName).trim(), String(minQty)]] },
    });
}

async function runExpiryAlert(days = 30) {
    const alertChatId = await getAlertChatId();
    const disabledItems = await getExpiryDisabledItems();
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.ITEMS}!A:K` });
    const rows = res.data.values || [];
    if (rows.length <= 1) return;

    const now = new Date();
    const soon = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);
    const expiring = [];
    const expired = [];

    rows.slice(1).forEach((r) => {
        const item = r[3];
        const expiry = r[9];
        const status = r[10];
        if (!item || !expiry) return;
        if (disabledItems.has(String(item).trim().toLowerCase())) return;
        if (String(status || '').toLowerCase() !== 'active') return;
        const dt = parseExpiryToDate(expiry);
        if (!dt) return;
        if (dt < now) expired.push({ item, expiry });
        else if (dt <= soon) expiring.push({ item, expiry });
    });

    const formatList = (arr) => {
        const byItem = {};
        arr.forEach(({ item, expiry }) => {
            const key = String(item).trim();
            byItem[key] = byItem[key] || new Set();
            byItem[key].add(String(expiry).trim());
        });
        return Object.entries(byItem)
            .slice(0, 50)
            .map(([k, set]) => `• ${k} (${Array.from(set).join(', ')})`)
            .join('\n');
    };

    if (!expired.length && !expiring.length) return;
    let msg = `⏰ Expiry Alert (${nowISODate()})\n`;
    if (expired.length) msg += `\n❗ Expired\n${formatList(expired)}`;
    if (expiring.length) msg += `\n\n⚠️ Expiring within ${days} days\n${formatList(expiring)}`;
    await bot.telegram.sendMessage(alertChatId, msg);
}

async function runLowStockAlert() {
    const alertChatId = await getAlertChatId();
    const stock = await getStockSummary();
    const minMap = await getMinStockMap();
    const low = [];
    for (const [item, minQty] of Object.entries(minMap)) {
        const qty = stock[item] || 0;
        if (qty <= minQty) low.push({ item, qty, minQty });
    }
    if (!low.length) return;
    const msg =
        `📉 Low Stock Alert (${nowISODate()})\n\n` +
        low
            .slice(0, 50)
            .map((x) => `• ${x.item}: ${x.qty} (min ${x.minQty})`)
            .join('\n');
    await bot.telegram.sendMessage(alertChatId, msg);
}

/**
 * BOT HANDLERS
 */

// Debugging: Bot က message လက်ခံရရှိကြောင်း သိရအောင် console မှာ ပြပါမယ်
bot.use(async (ctx, next) => {
    if (ctx.message) {
        console.log(`Incoming message from ${ctx.from.id} (${ctx.from.first_name}): ${ctx.message.text || "[Media]"}`);
    }
    return next();
});

bot.command('start', (ctx) => ctx.reply('Bot is ready. Send a voucher photo.'));

bot.command('help', async (ctx) => {
    const staff = await isStaffOrAdmin(ctx.from.id);
    const msg =
        `Commands:\n` +
        `- /stock\n` +
        `- /use <item> <qty>\n` +
        `- /expiry [days]\n` +
        `- /expiry disable <item>\n` +
        `- /expiry enable <item>\n` +
        `- /expiry disabled\n` +
        `- /summary [YYYY-MM]\n` +
        `- /unpaid\n` +
        `- /markpaid <invoice_no>\n` +
        (isAdmin(ctx.from.id)
            ? `\nAdmin:\n- /addstaff <id>\n- /rmstaff <id>\n- /staff\n- /setgroup (run in target chat)\n- /unsetgroup\n- /setmin <item> <min_qty>\n- /mins\n`
            : '') +
        `\nAccess: ${staff ? 'allowed' : 'not allowed (ask admin)'}`;
    ctx.reply(msg);
});

bot.command('expiry', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const parts = ctx.message.text.split(' ').slice(1).filter(Boolean);
    const sub = (parts[0] || '').toLowerCase();
    const itemName = parts.slice(1).join(' ').trim();

    if (sub === 'disable') {
        if (!itemName) return ctx.reply('Usage: /expiry disable <item>');
        const disabled = await getExpiryDisabledItems();
        disabled.add(itemName.toLowerCase());
        await setExpiryDisabledItems(disabled);
        return ctx.reply(`✅ Expiry alert disabled for: ${itemName}`);
    }

    if (sub === 'enable') {
        if (!itemName) return ctx.reply('Usage: /expiry enable <item>');
        const disabled = await getExpiryDisabledItems();
        disabled.delete(itemName.toLowerCase());
        await setExpiryDisabledItems(disabled);
        return ctx.reply(`✅ Expiry alert enabled for: ${itemName}`);
    }

    if (sub === 'disabled') {
        const disabled = await getExpiryDisabledItems();
        const list = Array.from(disabled).sort();
        return ctx.reply(list.length ? `Disabled expiry items:\n${list.join('\n')}` : 'No disabled expiry items.');
    }

    const days = Math.max(1, parseInt(parts[0] || '30', 10) || 30);
    await ctx.reply(`⏳ Checking expiry within ${days} days...`);
    try {
        await runExpiryAlert(days);
        await ctx.reply('✅ Expiry alert sent.');
    } catch (e) {
        console.error('Expiry command error:', e);
        await ctx.reply('❌ Error: ' + (e?.message || 'Expiry alert failed'));
    }
});

bot.on('photo', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const fileId = ctx.message.photo[ctx.message.photo.length - 1].file_id;
    voucherQueue.push({ fileId, user: ctx.from.first_name, userId: ctx.from.id });
    if (!isProcessing) processVoucher(ctx);
});

async function processVoucher(ctx) {
    if (isProcessing || voucherQueue.length === 0) return;
    isProcessing = true;
    const task = voucherQueue.shift();
    let imageBuffer;
    const statusMsg = await ctx.telegram.sendMessage(task.userId, "⏳ AI က စစ်ဆေးနေပါတယ်...");
    try {
        const link = await ctx.telegram.getFileLink(task.fileId);
        const res = await axios.get(link.href, { responseType: 'arraybuffer' });
        imageBuffer = Buffer.from(res.data);
        task.buffer = imageBuffer;
        const aiData = await analyzeVoucherAI(imageBuffer, 'image/jpeg');

        // Auto reconcile payment: if voucher says Paid and invoice exists as Unpaid,
        // update sheet status immediately without waiting for manual approval.
        const statusInfo = await getInvoiceStatusInfo(aiData.invoice_no);
        if (normalizePaymentStatus(aiData.status) === 'Paid' && statusInfo.found) {
            let driveUrl = '-';
            let driveWarning = '';
            try {
                driveUrl = await uploadToDrive(imageBuffer, `PAID_${aiData.invoice_no || 'V'}_${Date.now()}.jpg`);
                await updateInvoiceDriveUrl(aiData.invoice_no, driveUrl);
            } catch (driveErr) {
                driveWarning = `\n⚠️ Drive upload failed: ${describeHttpError(driveErr, 'Drive upload failed')}`;
            }
            const driveLine = driveUrl && driveUrl !== '-' ? `\nDrive: ${driveUrl}` : '';
            const current = normalizePaymentStatus(statusInfo.status);
            if (current !== 'Paid') {
                await updateSheetValues(`${SHEETS.MAIN}!I${statusInfo.rowIndex}:I${statusInfo.rowIndex}`, [['Paid']], 'RAW');
                await ctx.telegram
                    .sendMessage(
                        task.userId,
                        `✅ Existing invoice ${aiData.invoice_no || '-'} was Unpaid and is now updated to Paid in Google Sheet.${driveWarning}`
                    )
                    .catch(() => {});
                await bot.telegram
                    .sendMessage(
                        CONFIG.ADMIN_ID,
                        `✅ Auto payment update\nInvoice: ${aiData.invoice_no || '-'}\nStatus: Unpaid -> Paid\nSource: paid voucher from ${task.user || 'user'}${driveLine}${driveWarning}`
                    )
                    .catch(() => {});
                return;
            }

            await ctx.telegram
                .sendMessage(
                    task.userId,
                    `ℹ️ Invoice ${aiData.invoice_no || '-'} is already Paid in Google Sheet.${driveWarning}`
                )
                .catch(() => {});
            return;
        }

        userState.set(task.userId, { step: 'EXPIRY', aiData, buffer: imageBuffer, index: 0, expiries: [], user: task.user, userId: task.userId, fileId: task.fileId });
        if (!aiData.items.length) {
            ctx.telegram.sendMessage(task.userId, '⚠️ AI က items မတွေ့ပါ။ Admin ကို approve တင်ပေးမယ်။');
            const appId = `APP_${Date.now()}`;
            pendingApprovals.set(appId, { ...userState.get(task.userId) });
            userState.delete(task.userId);
            const dup = await invoiceExists(aiData.invoice_no);
            const check = buildVoucherCounterCheck(aiData);
            const pricing = buildVoucherPricing(aiData);
            const checkLine = check.hasIssue
                ? `Counter Check: ⚠️ AI=${check.aiTotal}, Items=${check.itemsTotal}, LineMismatch=${check.itemLineMismatchCount}`
                : 'Counter Check: ✅ OK';
            const pricingLine = `Pricing: Gross=${pricing.grossTotal}, Discount=${pricing.discount}, FOC Lines=${pricing.focLines}`;
            const cap =
                `Approval Required\n` +
                `Vendor: ${aiData.vendor || '-'}\n` +
                `Invoice: ${aiData.invoice_no || '-'}${dup ? ' (DUPLICATE?)' : ''}\n` +
                `Total: ${aiData.net_total || '-'}\n` +
                `Status: ${aiData.status || '-'}\n` +
                `${checkLine}\n` +
                `${pricingLine}\n`;
            const msg = await bot.telegram.sendPhoto(CONFIG.ADMIN_ID, task.fileId, {
                caption: cap,
                ...Markup.inlineKeyboard([
                    [
                        Markup.button.callback('✅ Approve', `ok_${appId}`),
                        Markup.button.callback('❌ Reject', `no_${appId}`),
                    ],
                    [Markup.button.callback('✏️ Edit', `edit_${appId}`)],
                ]),
            });
            pendingApprovals.set(appId, { ...pendingApprovals.get(appId), adminChatId: msg.chat.id, adminMsgId: msg.message_id });
        } else {
            ctx.telegram.sendMessage(task.userId, `📦 Items (${aiData.items.length}) တွေ့ပါတယ်။\n1) ${aiData.items[0].name} ရဲ့ Expiry (MM/YY):`);
        }
    } catch (e) {
        console.error('Voucher Error:', e);
        const detailedError = describeHttpError(e, 'Voucher processing failed');

        // Fallback to admin review so voucher flow continues even if AI/API is temporarily failing.
        try {
            const appId = `APP_${Date.now()}`;
            const fallbackData = {
                date: nowISODate(),
                vendor: '-',
                invoice_no: '-',
                category: '-',
                net_total: '-',
                status: 'Unpaid',
                phone: '-',
                items: [],
            };

            const rawBuffer = task.buffer || imageBuffer || Buffer.alloc(0);
            pendingApprovals.set(appId, {
                aiData: fallbackData,
                buffer: rawBuffer,
                expiries: [],
                user: task.user,
                userId: task.userId,
                fileId: task.fileId,
            });

            const cap =
                `Approval Required (AI/API error)\n` +
                `Reason: ${detailedError}\n` +
                `Vendor: -\n` +
                `Invoice: -\n` +
                `Total: -\n` +
                `Status: Unpaid\n`;

            const msg = await bot.telegram.sendPhoto(CONFIG.ADMIN_ID, task.fileId, {
                caption: cap,
                ...Markup.inlineKeyboard([
                    [
                        Markup.button.callback('✅ Approve', `ok_${appId}`),
                        Markup.button.callback('❌ Reject', `no_${appId}`),
                    ],
                    [Markup.button.callback('✏️ Edit', `edit_${appId}`)],
                ]),
            });

            pendingApprovals.set(appId, { ...pendingApprovals.get(appId), adminChatId: msg.chat.id, adminMsgId: msg.message_id });
            ctx.telegram.sendMessage(task.userId, `⚠️ AI service error (${detailedError}). Sent to admin for manual approval.`).catch(() => {});
        } catch (fallbackErr) {
            console.error('Voucher Fallback Error:', fallbackErr);
            ctx.telegram.sendMessage(task.userId, `❌ Error: ${detailedError}`).catch(() => {});
        }
    }
    finally { isProcessing = false; ctx.telegram.deleteMessage(task.userId, statusMsg.message_id).catch(()=>{}); setTimeout(()=>processVoucher(ctx), 1000); }
}

bot.on('text', async (ctx, next) => {
    // အကယ်၍ command ဖြစ်နေလျှင် နောက်ထပ် command handlers တွေဆီ လွှတ်ပေးရန်
    if (ctx.message.text.startsWith('/')) return next();

    const text = ctx.message.text;
    const state = userState.get(ctx.from.id);

    // Admin edit flow
    const adminState = adminEditState.get(ctx.from.id);
    if (adminState && isAdmin(ctx.from.id)) {
        const data = pendingApprovals.get(adminState.appId);
        if (!data) {
            adminEditState.delete(ctx.from.id);
            return ctx.reply('⚠️ Approval task not found.');
        }
        const value = text.trim();
        const oldInvoiceNo = String(data.aiData.invoice_no || '').trim();
        let sheetWriteNote = '';
        if (adminState.field === 'status') {
            data.aiData.status = normalizePaymentStatus(value);
            try {
                const updated = await updateInvoiceStatus(data.aiData.invoice_no, data.aiData.status);
                if (updated) {
                    sheetWriteNote = '\n✅ Status updated in Google Sheet for existing invoice.';
                } else {
                    const draft = await upsertDraftMainRowFromApproval(data.aiData, ctx.from.first_name || 'Admin Edit');
                    sheetWriteNote = draft
                        ? '\n✅ Invoice draft row created/updated in Google Sheet.'
                        : '\nℹ️ Invoice key missing. Changes will be saved on Approve.';
                }
            } catch (statusErr) {
                sheetWriteNote = `\n⚠️ Status write failed now: ${describeHttpError(statusErr, 'Status update failed')}. Will retry on Approve.`;
            }
        } else if (adminState.field === 'net_total') {
            data.aiData.net_total = value;
        } else if (adminState.field === 'vendor') {
            data.aiData.vendor = value;
        } else if (adminState.field === 'invoice_no') {
            data.aiData.invoice_no = value;
        } else if (adminState.field === 'date') {
            data.aiData.date = value;
        } else if (adminState.field === 'category') {
            data.aiData.category = value;
        }

        if (adminState.field !== 'status') {
            try {
                const lookupInvoiceNo = adminState.field === 'invoice_no' ? oldInvoiceNo : data.aiData.invoice_no;
                const updated = await updateInvoiceField(lookupInvoiceNo, adminState.field, value);
                if (updated) {
                    sheetWriteNote = '\n✅ Field updated in Google Sheet for existing invoice.';
                } else {
                    const draft = await upsertDraftMainRowFromApproval(data.aiData, ctx.from.first_name || 'Admin Edit');
                    sheetWriteNote = draft
                        ? '\n✅ Invoice draft row created/updated in Google Sheet.'
                        : '\nℹ️ Invoice key missing. Changes will be saved on Approve.';
                }
            } catch (fieldErr) {
                sheetWriteNote = `\n⚠️ Field write failed now: ${describeHttpError(fieldErr, 'Field update failed')}. Will retry on Approve.`;
            }
        }

        pendingApprovals.set(adminState.appId, data);
        adminEditState.delete(ctx.from.id);
        const dup = await invoiceExists(data.aiData.invoice_no);
        const check = buildVoucherCounterCheck(data.aiData);
        const pricing = buildVoucherPricing(data.aiData);
        const checkLine = check.hasIssue
            ? `Counter Check: ⚠️ AI=${check.aiTotal}, Items=${check.itemsTotal}, LineMismatch=${check.itemLineMismatchCount}`
            : 'Counter Check: ✅ OK';
        const pricingLine = `Pricing: Gross=${pricing.grossTotal}, Discount=${pricing.discount}, FOC Lines=${pricing.focLines}`;
        const cap =
            `Approval Required\n` +
            `Vendor: ${data.aiData.vendor || '-'}\n` +
            `Invoice: ${data.aiData.invoice_no || '-'}${dup ? ' (DUPLICATE?)' : ''}\n` +
            `Category: ${data.aiData.category || '-'}\n` +
            `Total: ${data.aiData.net_total || '-'}\n` +
            `Status: ${data.aiData.status || '-'}\n` +
            `Items: ${(data.aiData.items || []).length}\n` +
            `${checkLine}\n` +
            `${pricingLine}`;
        if (data.adminChatId && data.adminMsgId) {
            await bot.telegram
                .editMessageCaption(data.adminChatId, data.adminMsgId, undefined, cap, {
                    reply_markup: Markup.inlineKeyboard([
                        [
                            Markup.button.callback('✅ Approve', `ok_${adminState.appId}`),
                            Markup.button.callback('❌ Reject', `no_${adminState.appId}`),
                        ],
                        [Markup.button.callback('✏️ Edit', `edit_${adminState.appId}`)],
                    ]).reply_markup,
                })
                .catch(() => {});
        }
        await ctx.reply(`✅ Updated.${sheetWriteNote}`);
        return ctx.reply(
            'Edit another field before approve:',
            Markup.inlineKeyboard([
                [Markup.button.callback('Vendor', `ef_vendor_${adminState.appId}`), Markup.button.callback('Date', `ef_date_${adminState.appId}`)],
                [Markup.button.callback('Invoice', `ef_invoice_no_${adminState.appId}`), Markup.button.callback('Category', `ef_category_${adminState.appId}`)],
                [Markup.button.callback('Total', `ef_net_total_${adminState.appId}`), Markup.button.callback('Status', `ef_status_${adminState.appId}`)],
            ])
        );
    }

    if (state && state.step === 'EXPIRY') {
        state.expiries.push(text); state.index++;
        if (state.index < state.aiData.items.length) {
            return ctx.reply(`${state.index + 1}) ${state.aiData.items[state.index].name} ရဲ့ Expiry (MM/YY):`);
        }
        const appId = `APP_${Date.now()}`;
        pendingApprovals.set(appId, { ...state });
        userState.delete(ctx.from.id);
        const dup = await invoiceExists(state.aiData.invoice_no);
        const check = buildVoucherCounterCheck(state.aiData);
        const pricing = buildVoucherPricing(state.aiData);
        const checkLine = check.hasIssue
            ? `Counter Check: ⚠️ AI=${check.aiTotal}, Items=${check.itemsTotal}, LineMismatch=${check.itemLineMismatchCount}`
            : 'Counter Check: ✅ OK';
        const pricingLine = `Pricing: Gross=${pricing.grossTotal}, Discount=${pricing.discount}, FOC Lines=${pricing.focLines}`;
        const cap =
            `Approval Required\n` +
            `Vendor: ${state.aiData.vendor || '-'}\n` +
            `Invoice: ${state.aiData.invoice_no || '-'}${dup ? ' (DUPLICATE?)' : ''}\n` +
            `Category: ${state.aiData.category || '-'}\n` +
            `Total: ${state.aiData.net_total || '-'}\n` +
            `Status: ${state.aiData.status || '-'}\n` +
            `Items: ${(state.aiData.items || []).length}\n` +
            `${checkLine}\n` +
            `${pricingLine}`;
        const msg = await bot.telegram.sendPhoto(CONFIG.ADMIN_ID, state.fileId, {
            caption: cap,
            ...Markup.inlineKeyboard([
                [
                    Markup.button.callback('✅ Approve', `ok_${appId}`),
                    Markup.button.callback('❌ Reject', `no_${appId}`),
                ],
                [Markup.button.callback('✏️ Edit', `edit_${appId}`)],
            ]),
        });
        pendingApprovals.set(appId, { ...pendingApprovals.get(appId), adminChatId: msg.chat.id, adminMsgId: msg.message_id });
        return ctx.reply('📨 Admin ဆီ approval တင်ပြီးပါပြီ။');
    }
});

bot.command('stock', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const parts = ctx.message.text.split(' ').slice(1).filter(Boolean);
    const page = Math.max(1, parseInt(parts[0] || '1', 10) || 1);
    const pageSize = 20;
    console.log("Stock command processing for", ctx.from.id);
    const loading = await ctx.reply("📊 စာရင်းတွက်ချက်နေပါသည်...");
    try {
        const summary = await getStockSummary();
        if (!Object.keys(summary).length) {
            return ctx.telegram.editMessageText(ctx.chat.id, loading.message_id, null, "❌ စာရင်းထဲတွင် data မရှိသေးပါ။");
        }
        const stockRows = Object.entries(summary)
            .filter(([n, q]) => q !== 0)
            .sort((a, b) => a[0].localeCompare(b[0]));

        if (!stockRows.length) {
            return ctx.telegram.editMessageText(ctx.chat.id, loading.message_id, null, "🏥 လက်ရှိဆေးလက်ကျန်\n\nလက်ကျန်မရှိပါ။");
        }

        const totalPages = Math.max(1, Math.ceil(stockRows.length / pageSize));
        const safePage = Math.min(page, totalPages);
        const start = (safePage - 1) * pageSize;
        const pageRows = stockRows.slice(start, start + pageSize);
        const stockList = pageRows.map(([n, q]) => `• ${n}: ${q}`).join('\n');

        ctx.telegram.editMessageText(
            ctx.chat.id,
            loading.message_id,
            null,
            `🏥 လက်ရှိဆေးလက်ကျန် (page ${safePage}/${totalPages}, total ${stockRows.length})\n\n${stockList}\n\nUse /stock <page>`
        );
    } catch (e) { 
        console.error("Stock Command Error:", e);
        ctx.telegram.editMessageText(ctx.chat.id, loading.message_id, null, "❌ Error: " + e.message); 
    }
});

bot.command('use', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const parts = ctx.message.text.split(' ').slice(1).filter(Boolean);
    if (parts.length < 2) return ctx.reply('Usage: /use <item> <qty>');
    const qty = parseNumber(parts[parts.length - 1]);
    const item = parts.slice(0, -1).join(' ').trim();
    if (!item) return ctx.reply('❌ Item is required.');
    if (!qty) return ctx.reply('❌ Qty is required.');
    const row = [
        nowISODate(),
        `USE_${Date.now()}`,
        'Clinic Use',
        item,
        -Math.abs(qty),
        0,
        0,
        ctx.from.first_name || '-',
        '-',
        '-',
        'Used',
    ];
    await appendItemLogRow(row);
    ctx.reply(`✅ Logged usage: ${item} (-${Math.abs(qty)})`);
});

bot.command('setmin', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    const parts = ctx.message.text.split(' ').slice(1).filter(Boolean);
    if (parts.length < 2) return ctx.reply('Usage: /setmin <item> <min_qty>');
    const minQty = parseNumber(parts[parts.length - 1]);
    const item = parts.slice(0, -1).join(' ').trim();
    await upsertMinStock(item, minQty);
    ctx.reply(`✅ Min stock set: ${item} = ${minQty}`);
});

bot.command('mins', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    const map = await getMinStockMap();
    const list = Object.entries(map)
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([k, v]) => `• ${k}: ${v}`)
        .join('\n');
    ctx.reply(list ? `Min Stock:\n${list}` : 'No min stock configured.');
});

bot.command('setgroup', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    await setConfigValue('alert_chat_id', String(ctx.chat.id));
    ctx.reply(`✅ Alerts will be sent to chat_id: ${ctx.chat.id}`);
});

bot.command('unsetgroup', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    await setConfigValue('alert_chat_id', '');
    ctx.reply('✅ Alert chat unset. Alerts will go to admin.');
});

bot.command('addstaff', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    const id = Number(ctx.message.text.split(' ')[1]);
    if (!id) return ctx.reply('Usage: /addstaff <telegram_user_id>');
    const staff = await getStaffIds();
    staff.add(id);
    await setConfigValue('staff_ids', Array.from(staff).join(','));
    ctx.reply(`✅ Staff added: ${id}`);
});

bot.command('rmstaff', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    const id = Number(ctx.message.text.split(' ')[1]);
    if (!id) return ctx.reply('Usage: /rmstaff <telegram_user_id>');
    const staff = await getStaffIds();
    staff.delete(id);
    await setConfigValue('staff_ids', Array.from(staff).join(','));
    ctx.reply(`✅ Staff removed: ${id}`);
});

bot.command('staff', async (ctx) => {
    if (!isAdmin(ctx.from.id)) return ctx.reply('❌ Admin only.');
    const staff = await getStaffIds();
    ctx.reply(`Staff IDs:\n${Array.from(staff).join('\n') || '(none)'}`);
});

bot.command('unpaid', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const parts = ctx.message.text.split(' ').slice(1).filter(Boolean);
    const page = Math.max(1, parseInt(parts[0] || '1', 10) || 1);
    const pageSize = 20;
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!A:N` });
    const rows = res.data.values || [];
    const unpaid = rows
        .slice(1)
        .filter((r) => String(r[8] || '').trim().toLowerCase() !== 'paid')
        .reverse();
    if (!unpaid.length) return ctx.reply('✅ No unpaid vouchers.');

    const totalPages = Math.max(1, Math.ceil(unpaid.length / pageSize));
    const safePage = Math.min(page, totalPages);
    const start = (safePage - 1) * pageSize;
    const pageRows = unpaid.slice(start, start + pageSize);
    const unpaidTotal = round2(unpaid.reduce((sum, r) => sum + (parseNumber(r[6]) || parseNumber(r[4])), 0));
    const msg = pageRows
        .map((r) => `• ${r[0] || '-'} | ${r[1] || '-'} | ${r[2] || '-'} | ${r[4] || '-'}MMK`)
        .join('\n');
    ctx.reply(`Unpaid (page ${safePage}/${totalPages}, total ${unpaid.length}): Unpaid Total= ${unpaidTotal}mmk\n${msg}\n\nUse /unpaid <page>`);
});

bot.command('markpaid', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const invoice = ctx.message.text.split(' ').slice(1).join(' ').trim();
    if (!invoice) return ctx.reply('Usage: /markpaid <invoice_no>');
    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!A:N` });
    const rows = res.data.values || [];
    let rowIndex = -1;
    for (let i = 1; i < rows.length; i++) {
        if (String(rows[i][2] || '').trim() === invoice) {
            rowIndex = i + 1;
            break;
        }
    }
    if (rowIndex === -1) return ctx.reply('❌ Invoice not found.');
    await sheets.spreadsheets.values.update({
        spreadsheetId: CONFIG.SHEET_ID,
        range: `${SHEETS.MAIN}!I${rowIndex}`,
        valueInputOption: 'RAW',
        resource: { values: [['Paid']] },
    });
    ctx.reply(`✅ Marked Paid: ${invoice}`);
});

bot.command('summary', async (ctx) => {
    const allowed = await isStaffOrAdmin(ctx.from.id);
    if (!allowed) return ctx.reply('❌ Not authorized.');
    const arg = ctx.message.text.split(' ').slice(1).join(' ').trim();
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const defaultTarget = `${yyyy}-${mm}`;
    const isYearOnly = /^\d{4}$/.test(arg);
    const target = toMonthKeyFromDateValue(arg) || (/^\d{4}-\d{2}$/.test(arg) ? arg : defaultTarget);

    const sheets = google.sheets({ version: 'v4', auth });
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: `${SHEETS.MAIN}!A:N` });
    const rows = res.data.values || [];
    const dataRows = rows.filter((r) => {
        const c0 = String(r[0] || '').trim().toLowerCase();
        const c1 = String(r[1] || '').trim().toLowerCase();
        const c2 = String(r[2] || '').trim().toLowerCase();

        // Skip header-like rows even if they appear beyond row 1.
        if (c0 === 'date' || c1 === 'vendor' || c2 === 'invoice_no' || c2 === 'invoice') return false;
        return Boolean(toMonthKeyFromDateValue(r[0]));
    });

    const filtered = dataRows.filter((r) => {
        const monthKey = toMonthKeyFromDateValue(r[0]);
        if (!monthKey) return false;
        if (isYearOnly) return monthKey.startsWith(`${arg}-`);
        return monthKey === target;
    });
    const sums = filtered.reduce(
        (acc, r) => {
            const amount = parseNumber(r[6]) || parseNumber(r[4]);
            const status = normalizePaymentStatus(r[8]);
            if (status === 'Paid') {
                acc.paidCount += 1;
                acc.paidTotal += amount;
            } else {
                acc.unpaidCount += 1;
                acc.unpaidTotal += amount;
            }
            return acc;
        },
        { paidCount: 0, unpaidCount: 0, paidTotal: 0, unpaidTotal: 0 }
    );

    ctx.reply(
        `Summary ${isYearOnly ? arg : target}\n` +
        `Paid count: ${sums.paidCount}\n` +
        `Paid total: ${round2(sums.paidTotal)} MMK\n` +
        `Unpaid count: ${sums.unpaidCount}\n` +
        `Unpaid total: ${round2(sums.unpaidTotal)} MMK`
    );
});

bot.action(/^ok_(.+)$/, async (ctx) => {
    const data = pendingApprovals.get(ctx.match[1]);
    if (data) {
        try {
            let buffer = data.buffer;
            if (!buffer || !buffer.length) {
                if (!data.fileId) throw new Error('Missing voucher image file id');
                const link = await bot.telegram.getFileLink(data.fileId);
                const res = await axios.get(link.href, { responseType: 'arraybuffer' });
                buffer = Buffer.from(res.data);
            }
            let url = '-';
            let driveWarning = '';
            try {
                url = await uploadToDrive(buffer, `V_${Date.now()}.jpg`);
            } catch (driveErr) {
                driveWarning = `\n⚠️ Drive upload failed: ${describeHttpError(driveErr, 'Drive upload failed')}`;
            }
            const result = await saveToSheets(data.aiData, data.user, url, data.expiries);
                const mainInfo = result.mainMode === 'updated' ? 'Main row: updated existing invoice' : 'Main row: appended new invoice';
            const itemInfo = result.itemRowsSaved > 0 ? `Items rows: ${result.itemRowsSaved}` : 'Items rows: 0 (qty not updated)';
            const warning = result.itemError ? `\n⚠️ ${result.itemError}` : '';
            const check = result.counterCheck || buildVoucherCounterCheck(data.aiData);
            const pricing = result.pricing || buildVoucherPricing(data.aiData);
            const checkWarning = check.hasIssue
                ? `\n⚠️ Counter Check: AI Total=${check.aiTotal}, Items Total=${check.itemsTotal}, Line mismatch=${check.itemLineMismatchCount}`
                : '\n✅ Counter Check: totals look consistent';
            const pricingInfo = `\n💸 Pricing: Gross=${pricing.grossTotal}, Discount=${pricing.discount}, FOC Lines=${pricing.focLines}`;
            await ctx.editMessageCaption(`✅ Approved + saved.\nStatus: ${result.status}\n${mainInfo}\n${itemInfo}${checkWarning}${pricingInfo}${warning}${driveWarning}`).catch(() => {});
            if (data.userId) {
                await bot.telegram
                    .sendMessage(data.userId, `✅ Voucher approved and saved.\nStatus: ${result.status}\n${mainInfo}\n${itemInfo}${checkWarning}${pricingInfo}${warning}${driveWarning}`)
                    .catch(() => {});
            }
            pendingApprovals.delete(ctx.match[1]);
        } catch (e) {
            const message = describeHttpError(e, 'Save failed');
            await ctx.reply('❌ Error: ' + message);
            if (data.userId) await bot.telegram.sendMessage(data.userId, `❌ Voucher save failed: ${message}`).catch(() => {});
        }
    }
});

bot.action(/^no_(.+)$/, async (ctx) => {
    const appId = ctx.match[1];
    const data = pendingApprovals.get(appId);
    if (!data) return;

    pendingRejectConfirms.set(appId, { at: Date.now(), by: ctx.from.id });
    await ctx
        .reply(
            'Confirm reject?',
            Markup.inlineKeyboard([
                [Markup.button.callback('✅ Confirm Reject', `noconfirm_${appId}`)],
                [Markup.button.callback('↩️ Cancel', `nocancel_${appId}`)],
            ])
        )
        .catch(() => {});
});

bot.action(/^noconfirm_(.+)$/, async (ctx) => {
    const appId = ctx.match[1];
    const data = pendingApprovals.get(appId);
    const pending = pendingRejectConfirms.get(appId);
    if (!data || !pending) return;

    pendingRejectConfirms.delete(appId);
    await ctx.answerCbQuery('Rejected').catch(() => {});
    await ctx.editMessageText('✅ Rejection confirmed.').catch(() => {});
    await bot.telegram.editMessageCaption(data.adminChatId, data.adminMsgId, undefined, '❌ Rejected.').catch(() => {});
    if (data.userId) await bot.telegram.sendMessage(data.userId, '❌ Voucher rejected by admin.').catch(() => {});
    pendingApprovals.delete(appId);
});

bot.action(/^nocancel_(.+)$/, async (ctx) => {
    const appId = ctx.match[1];
    pendingRejectConfirms.delete(appId);
    await ctx.answerCbQuery('Cancelled').catch(() => {});
    await ctx.editMessageText('↩️ Rejection cancelled.').catch(() => {});
});

bot.action(/^edit_(.+)$/, async (ctx) => {
    if (!isAdmin(ctx.from.id)) return;
    const appId = ctx.match[1];
    const data = pendingApprovals.get(appId);
    if (!data) return ctx.reply('⚠️ Task not found.');
    await ctx.reply(
        'Select field to edit:',
        Markup.inlineKeyboard([
            [Markup.button.callback('Vendor', `ef_vendor_${appId}`), Markup.button.callback('Date', `ef_date_${appId}`)],
            [Markup.button.callback('Invoice', `ef_invoice_no_${appId}`), Markup.button.callback('Category', `ef_category_${appId}`)],
            [Markup.button.callback('Total', `ef_net_total_${appId}`), Markup.button.callback('Status', `ef_status_${appId}`)],
        ])
    );
});

bot.action(/^ef_(vendor|date|invoice_no|category|net_total|status)_(.+)$/, async (ctx) => {
    if (!isAdmin(ctx.from.id)) return;
    const field = ctx.match[1];
    const appId = ctx.match[2];
    const data = pendingApprovals.get(appId);
    if (!data) return ctx.reply('⚠️ Task not found.');
    adminEditState.set(ctx.from.id, { appId, field });
    ctx.reply(`Send new value for ${field}:`);
});

// Scheduled alerts
cron.schedule('0 9 * * *', () => runExpiryAlert(30).catch((e) => console.error('Expiry alert error', e)), { timezone: CONFIG.TIMEZONE });
cron.schedule('5 9 * * *', () => runLowStockAlert().catch((e) => console.error('Low stock alert error', e)), { timezone: CONFIG.TIMEZONE });

bot.launch().then(() => console.log('🚀 Bot is Active! Waiting for messages...'));
