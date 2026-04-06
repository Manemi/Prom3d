// ============================================
// Prom3D + Mercato AI Ads Agent — Backend Server
// Node.js + Express + Google Ads API + GA4 MCP
// ============================================

const express = require('express');
const cors = require('cors');
const { GoogleAdsApi } = require('google-ads-api');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const app = express();
app.use(cors());
app.use(express.json());

app.get('/debug', (req, res) => {
  res.json({
    has_client_email: !!process.env.GA4_CLIENT_EMAIL,
    has_private_key: !!process.env.GA4_PRIVATE_KEY,
    client_email: process.env.GA4_CLIENT_EMAIL || 'NOT SET',
    project_id: process.env.GA4_PROJECT_ID || 'NOT SET',
  });
});

// ============ КОНФІГ ============
const CONFIG = {
  ADS_DEVELOPER_TOKEN: 'uMvPr7R4QQvCjYfGV25gcQ',
  ADS_CLIENT_ID: '546482431037-pnc30oc3o04npp3k4oo1ifpf3is788c4.apps.googleusercontent.com',
  ADS_CLIENT_SECRET: process.env.ADS_CLIENT_SECRET || '',
  ADS_REFRESH_TOKEN: process.env.ADS_REFRESH_TOKEN || '',
  // Prom3D
  ADS_CUSTOMER_ID: '5522488607',
  GA4_PROPERTY_ID: '503012124',
  // Mercato
  ADS_CUSTOMER_ID_MERCATO: process.env.ADS_CUSTOMER_ID_MERCATO || '4433061490',
  GA4_PROPERTY_ID_MERCATO: process.env.GA4_PROPERTY_ID_MERCATO || '286553038',
};

// ── Google Ads клієнт ──
const adsClient = new GoogleAdsApi({
  client_id: CONFIG.ADS_CLIENT_ID,
  client_secret: CONFIG.ADS_CLIENT_SECRET,
  developer_token: CONFIG.ADS_DEVELOPER_TOKEN,
});

// ── GA4 клієнт ──
const ga4Credentials = {
  type: 'service_account',
  project_id: process.env.GA4_PROJECT_ID || 'prom3d-agent',
  private_key_id: process.env.GA4_PRIVATE_KEY_ID || '',
  private_key: (process.env.GA4_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
  client_email: process.env.GA4_CLIENT_EMAIL || '',
  client_id: process.env.GA4_CLIENT_ID || '',
  auth_uri: 'https://accounts.google.com/o/oauth2/auth',
  token_uri: 'https://oauth2.googleapis.com/token',
};

const ga4Client = new BetaAnalyticsDataClient({ credentials: ga4Credentials });

// helper GA4
function parsePeriod(query) {
  const p = query.period || 'last7';
  const today = new Date();
  const fmt = d => d.toISOString().split('T')[0];
  if (p === 'today')      return { startDate: 'today',     endDate: 'today' };
  if (p === 'yesterday')  return { startDate: 'yesterday', endDate: 'yesterday' };
  if (p === 'week')       return { startDate: '7daysAgo',  endDate: 'today' };
  if (p === 'month')      return { startDate: '30daysAgo', endDate: 'today' };
  if (p === 'last_month') {
    const first = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    const last  = new Date(today.getFullYear(), today.getMonth(), 0);
    return { startDate: fmt(first), endDate: fmt(last) };
  }
  if (p === 'custom' && query.startDate && query.endDate)
    return { startDate: query.startDate, endDate: query.endDate };
  return { startDate: '7daysAgo', endDate: 'today' };
}

// ═══════════════════════════════
// ЗАГАЛЬНІ РОУТИ
// ═══════════════════════════════
// ============================================================
// ВСТАВТЕ ЦЕЙ КОД У ВАШ server.js — ЗАМІСТЬ поточних роутів
// Зміни:
//  1. ga4Report приймає startDate/endDate з query params
//  2. Нові зведені ендпоінти /api/ga4/mercato та /api/ga4/prom3d
//  3. Підтримка ?period=today|yesterday|week|month|last_month|custom
// ============================================================

// ── Замінити поточний ga4Report helper ──────────────────────
function parsePeriod(query) {
  const p = query.period || 'last7';
  const today = new Date();
  const fmt = d => d.toISOString().split('T')[0];

  if (p === 'today')      return { startDate: 'today',    endDate: 'today' };
  if (p === 'yesterday')  return { startDate: 'yesterday', endDate: 'yesterday' };
  if (p === 'week')       return { startDate: '7daysAgo',  endDate: 'today' };
  if (p === 'month')      return { startDate: '30daysAgo', endDate: 'today' };
  if (p === 'last_month') {
    const first = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    const last  = new Date(today.getFullYear(), today.getMonth(), 0);
    return { startDate: fmt(first), endDate: fmt(last) };
  }
  if (p === 'custom' && query.startDate && query.endDate) {
    return { startDate: query.startDate, endDate: query.endDate };
  }
  return { startDate: '7daysAgo', endDate: 'today' }; // default
}

async function ga4Report(propertyId, metrics, dimensions, orderBys, limit, dateRange) {
  const range = dateRange || { startDate: '7daysAgo', endDate: 'today' };
  const params = {
    property: `properties/${propertyId}`,
    dateRanges: [range],
    metrics, dimensions,
  };
  if (orderBys) params.orderBys = orderBys;
  if (limit) params.limit = limit;
  const [response] = await ga4Client.runReport(params);
  return response.rows || [];
}

// ── Новий зведений ендпоінт для Mercato ──────────────────────
// GET /api/ga4/mercato?period=month
// GET /api/ga4/mercato?period=custom&startDate=2025-03-01&endDate=2025-03-31
app.get('/api/ga4/mercato', async (req, res) => {
  try {
    const dateRange = parsePeriod(req.query);
    const propertyId = CONFIG.GA4_PROPERTY_ID_MERCATO;

    const [channelRows, pagesRows, totalsRows] = await Promise.all([
      // Канали трафіку
      ga4Report(propertyId,
        [{ name: 'sessions' }, { name: 'activeUsers' }, { name: 'bounceRate' },
         { name: 'averageSessionDuration' }, { name: 'conversions' },
         { name: 'screenPageViewsPerSession' }],
        [{ name: 'sessionDefaultChannelGrouping' }],
        [{ metric: { metricName: 'sessions' }, desc: true }],
        10, dateRange),

      // Топ сторінки
      ga4Report(propertyId,
        [{ name: 'screenPageViews' }, { name: 'bounceRate' },
         { name: 'averageSessionDuration' }, { name: 'conversions' }],
        [{ name: 'pagePath' }],
        [{ metric: { metricName: 'screenPageViews' }, desc: true }],
        10, dateRange),

      // Загальні метрики (без розбивки по каналах)
      ga4Report(propertyId,
        [{ name: 'sessions' }, { name: 'totalUsers' }, { name: 'newUsers' },
         { name: 'bounceRate' }, { name: 'averageSessionDuration' },
         { name: 'screenPageViewsPerSession' }, { name: 'ecommercePurchases' },
         { name: 'purchaseRevenue' }, { name: 'conversions' }],
        [], null, null, dateRange),
    ]);

    // Totals row
    const t = totalsRows[0]?.metricValues || [];
    const v = (i) => parseFloat(t[i]?.value || '0');

    res.json({
      success: true, shop: 'mercato',
      period: dateRange,
      // Зведені метрики
      sessions:         Math.round(v(0)),
      users:            Math.round(v(1)),
      newUsers:         Math.round(v(2)),
      bounceRate:       parseFloat((v(3) * 100).toFixed(1)),
      avgDuration:      Math.round(v(4)),
      pagesPerSession:  parseFloat(v(5).toFixed(1)),
      transactions:     Math.round(v(6)),
      revenue:          parseFloat(v(7).toFixed(0)),
      conversions:      Math.round(v(8)),
      // Деталі по каналах
      channelRows: channelRows.map(row => ({
        channel:        row.dimensionValues[0].value,
        sessions:       parseInt(row.metricValues[0].value),
        users:          parseInt(row.metricValues[1].value),
        bounceRate:     parseFloat((parseFloat(row.metricValues[2].value) * 100).toFixed(1)),
        avgDuration:    Math.round(parseFloat(row.metricValues[3].value)),
        conversions:    parseInt(row.metricValues[4].value),
        pagesPerSession: parseFloat(parseFloat(row.metricValues[5].value).toFixed(1)),
      })),
      // Топ сторінки
      topPages: pagesRows.map(row => ({
        page:        row.dimensionValues[0].value,
        views:       parseInt(row.metricValues[0].value),
        bounceRate:  parseFloat((parseFloat(row.metricValues[1].value) * 100).toFixed(1)),
        avgDuration: Math.round(parseFloat(row.metricValues[2].value)),
        conversions: parseInt(row.metricValues[3].value),
      })),
    });
  } catch(err) {
    console.error('GA4 Mercato error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Новий зведений ендпоінт для Prom3D ───────────────────────
// GET /api/ga4/prom3d?period=month
app.get('/api/ga4/prom3d', async (req, res) => {
  try {
    const dateRange = parsePeriod(req.query);
    const propertyId = CONFIG.GA4_PROPERTY_ID;

    const [channelRows, pagesRows, totalsRows, eventsRows] = await Promise.all([
      // Канали трафіку
      ga4Report(propertyId,
        [{ name: 'sessions' }, { name: 'activeUsers' }, { name: 'bounceRate' },
         { name: 'averageSessionDuration' }, { name: 'conversions' },
         { name: 'screenPageViewsPerSession' }],
        [{ name: 'sessionDefaultChannelGrouping' }],
        [{ metric: { metricName: 'sessions' }, desc: true }],
        10, dateRange),

      // Топ сторінки
      ga4Report(propertyId,
        [{ name: 'screenPageViews' }, { name: 'bounceRate' },
         { name: 'averageSessionDuration' }, { name: 'conversions' }],
        [{ name: 'pagePath' }],
        [{ metric: { metricName: 'screenPageViews' }, desc: true }],
        10, dateRange),

      // Загальні метрики
      ga4Report(propertyId,
        [{ name: 'sessions' }, { name: 'totalUsers' }, { name: 'newUsers' },
         { name: 'bounceRate' }, { name: 'averageSessionDuration' },
         { name: 'screenPageViewsPerSession' }, { name: 'conversions' }],
        [], null, null, dateRange),

      // ПОДІЇ — щоб знайти реальні форми/дзвінки
      // Показує всі conversion events з їх кількістю
      ga4Report(propertyId,
        [{ name: 'eventCount' }, { name: 'conversions' }],
        [{ name: 'eventName' }],
        [{ metric: { metricName: 'eventCount' }, desc: true }],
        20, dateRange),
    ]);

    const t = totalsRows[0]?.metricValues || [];
    const v = (i) => parseFloat(t[i]?.value || '0');

    // Конверсійні події — реальні дані без вигадок
    const conversionEvents = eventsRows
      .map(row => ({
        eventName:   row.dimensionValues[0].value,
        eventCount:  parseInt(row.metricValues[0].value),
        conversions: parseInt(row.metricValues[1].value),
      }))
      .filter(e => e.conversions > 0 || ['generate_lead','form_submit','click','contact'].some(k => e.eventName.includes(k)));

    res.json({
      success: true, shop: 'prom3d',
      period: dateRange,
      sessions:         Math.round(v(0)),
      users:            Math.round(v(1)),
      newUsers:         Math.round(v(2)),
      bounceRate:       parseFloat((v(3) * 100).toFixed(1)),
      avgDuration:      Math.round(v(4)),
      pagesPerSession:  parseFloat(v(5).toFixed(1)),
      conversions:      Math.round(v(6)),  // всі конверсії
      // Реальні конверсійні події — без вигадок
      conversionEvents,
      channelRows: channelRows.map(row => ({
        channel:        row.dimensionValues[0].value,
        sessions:       parseInt(row.metricValues[0].value),
        users:          parseInt(row.metricValues[1].value),
        bounceRate:     parseFloat((parseFloat(row.metricValues[2].value) * 100).toFixed(1)),
        avgDuration:    Math.round(parseFloat(row.metricValues[3].value)),
        conversions:    parseInt(row.metricValues[4].value),
        pagesPerSession: parseFloat(parseFloat(row.metricValues[5].value).toFixed(1)),
      })),
      topPages: pagesRows.map(row => ({
        page:        row.dimensionValues[0].value,
        views:       parseInt(row.metricValues[0].value),
        bounceRate:  parseFloat((parseFloat(row.metricValues[1].value) * 100).toFixed(1)),
        avgDuration: Math.round(parseFloat(row.metricValues[2].value)),
        conversions: parseInt(row.metricValues[3].value),
      })),
    });
  } catch(err) {
    console.error('GA4 Prom3D error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Оновити існуючі ендпоінти щоб теж приймали period ────────
// Замінити в /api/ga4/metrics, /api/mercato/ga4/metrics і т.д.:
// const rows = await ga4Report(ID, metrics, dims, ...);
// → const rows = await ga4Report(ID, metrics, dims, orderBys, limit, parsePeriod(req.query));
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Prom3D + Mercato Ads Agent Server running 🚀 v3', shops: ['prom3d', 'mercato'] });
});

// ═══════════════════════════════
// PROM3D — Google Ads
// ═══════════════════════════════

app.get('/api/ads/metrics', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const campaigns = await customer.query(`
      SELECT campaign.id, campaign.name, campaign.status,
        metrics.impressions, metrics.clicks, metrics.ctr,
        metrics.conversions, metrics.cost_micros, metrics.average_cpc
      FROM campaign
      WHERE segments.date DURING LAST_7_DAYS AND campaign.status = 'ENABLED'
      ORDER BY metrics.impressions DESC LIMIT 10
    `);
    const data = campaigns.map(c => ({
      id: c.campaign.id, name: c.campaign.name,
      impressions: c.metrics.impressions, clicks: c.metrics.clicks,
      ctr: (c.metrics.ctr * 100).toFixed(2), conversions: c.metrics.conversions,
      cost: (c.metrics.cost_micros / 1_000_000).toFixed(2),
      avgCpc: (c.metrics.average_cpc / 1_000_000).toFixed(2),
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/ads/keywords', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const keywords = await customer.query(`
      SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
        ad_group_criterion.status, metrics.impressions, metrics.clicks,
        metrics.ctr, metrics.conversions, metrics.cost_micros,
        ad_group_criterion.quality_info.quality_score
      FROM keyword_view WHERE segments.date DURING LAST_7_DAYS
      ORDER BY metrics.impressions DESC LIMIT 20
    `);
    const data = keywords.map(k => ({
      keyword: k.ad_group_criterion.keyword.text,
      matchType: k.ad_group_criterion.keyword.match_type,
      status: k.ad_group_criterion.status,
      impressions: k.metrics.impressions, clicks: k.metrics.clicks,
      ctr: (k.metrics.ctr * 100).toFixed(2), conversions: k.metrics.conversions,
      cost: (k.metrics.cost_micros / 1_000_000).toFixed(2),
      qualityScore: k.ad_group_criterion.quality_info?.quality_score || 0,
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/ads/search-terms', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const terms = await customer.query(`
      SELECT search_term_view.search_term, search_term_view.status,
        metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros
      FROM search_term_view
      WHERE segments.date DURING LAST_7_DAYS AND metrics.impressions > 5
      ORDER BY metrics.impressions DESC LIMIT 50
    `);
    const minusWords = ['безкоштовно','free','своїми руками','diy','скачати','thingiverse','stl','курс','навчання'];
    const data = terms.map(t => ({
      term: t.search_term_view.search_term,
      impressions: t.metrics.impressions, clicks: t.metrics.clicks,
      conversions: t.metrics.conversions,
      cost: (t.metrics.cost_micros / 1_000_000).toFixed(2),
      isSuggestedMinus: minusWords.some(mw => t.search_term_view.search_term.toLowerCase().includes(mw)),
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/ads/pause-campaign', async (req, res) => {
  try {
    const { campaignId } = req.body;
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    await customer.campaigns.update([{ resource_name: `customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${campaignId}`, status: 'PAUSED' }]);
    res.json({ success: true, message: `Кампанія ${campaignId} призупинена` });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/ads/add-negative-keywords', async (req, res) => {
  try {
    const { keywords, campaignId } = req.body;
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const negativeKeywords = keywords.map(kw => ({ campaign: `customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${campaignId}`, keyword: { text: kw, match_type: 'BROAD' } }));
    await customer.campaignCriteria.create(negativeKeywords);
    res.json({ success: true, message: `Додано ${keywords.length} мінус-слів` });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ═══════════════════════════════
// PROM3D — GA4
// ═══════════════════════════════

app.get('/api/ga4/metrics', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID,
      [{ name: 'sessions' }, { name: 'activeUsers' }, { name: 'bounceRate' }, { name: 'averageSessionDuration' }, { name: 'conversions' }, { name: 'screenPageViewsPerSession' }],
      [{ name: 'sessionDefaultChannelGrouping' }]);
    const data = rows.map(row => ({
      channel: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value, users: row.metricValues[1].value,
      bounceRate: (parseFloat(row.metricValues[2].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[3].value)) + 'с',
      conversions: row.metricValues[4].value,
      pagesPerSession: parseFloat(row.metricValues[5].value).toFixed(1),
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/ga4/pages', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID,
      [{ name: 'screenPageViews' }, { name: 'bounceRate' }, { name: 'averageSessionDuration' }, { name: 'conversions' }],
      [{ name: 'pagePath' }],
      [{ metric: { metricName: 'screenPageViews' }, desc: true }], 10);
    const data = rows.map(row => ({
      page: row.dimensionValues[0].value, views: row.metricValues[0].value,
      bounceRate: (parseFloat(row.metricValues[1].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[2].value)) + 'с',
      conversions: row.metricValues[3].value,
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/ga4/devices', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID,
      [{ name: 'sessions' }, { name: 'conversions' }, { name: 'totalRevenue' }],
      [{ name: 'deviceCategory' }]);
    const data = rows.map(row => ({
      device: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value, conversions: row.metricValues[1].value,
      revenue: parseFloat(row.metricValues[2].value).toFixed(2),
    }));
    res.json({ success: true, shop: 'prom3d', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ═══════════════════════════════
// MERCATO — Google Ads
// ═══════════════════════════════

app.get('/api/mercato/ads/metrics', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID_MERCATO, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const campaigns = await customer.query(`
      SELECT campaign.id, campaign.name, campaign.status,
        metrics.impressions, metrics.clicks, metrics.ctr,
        metrics.conversions, metrics.cost_micros, metrics.average_cpc
      FROM campaign
      WHERE segments.date DURING LAST_7_DAYS AND campaign.status = 'ENABLED'
      ORDER BY metrics.impressions DESC LIMIT 10
    `);
    const data = campaigns.map(c => ({
      id: c.campaign.id, name: c.campaign.name,
      impressions: c.metrics.impressions, clicks: c.metrics.clicks,
      ctr: (c.metrics.ctr * 100).toFixed(2), conversions: c.metrics.conversions,
      cost: (c.metrics.cost_micros / 1_000_000).toFixed(2),
      avgCpc: (c.metrics.average_cpc / 1_000_000).toFixed(2),
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/mercato/ads/keywords', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID_MERCATO, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const keywords = await customer.query(`
      SELECT ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type,
        ad_group_criterion.status, metrics.impressions, metrics.clicks,
        metrics.ctr, metrics.conversions, metrics.cost_micros,
        ad_group_criterion.quality_info.quality_score
      FROM keyword_view WHERE segments.date DURING LAST_7_DAYS
      ORDER BY metrics.impressions DESC LIMIT 20
    `);
    const data = keywords.map(k => ({
      keyword: k.ad_group_criterion.keyword.text,
      matchType: k.ad_group_criterion.keyword.match_type,
      status: k.ad_group_criterion.status,
      impressions: k.metrics.impressions, clicks: k.metrics.clicks,
      ctr: (k.metrics.ctr * 100).toFixed(2), conversions: k.metrics.conversions,
      cost: (k.metrics.cost_micros / 1_000_000).toFixed(2),
      qualityScore: k.ad_group_criterion.quality_info?.quality_score || 0,
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/mercato/ads/search-terms', async (req, res) => {
  try {
    const customer = adsClient.Customer({ customer_id: CONFIG.ADS_CUSTOMER_ID_MERCATO, refresh_token: CONFIG.ADS_REFRESH_TOKEN });
    const terms = await customer.query(`
      SELECT search_term_view.search_term, search_term_view.status,
        metrics.impressions, metrics.clicks, metrics.conversions, metrics.cost_micros
      FROM search_term_view
      WHERE segments.date DURING LAST_7_DAYS AND metrics.impressions > 5
      ORDER BY metrics.impressions DESC LIMIT 50
    `);
    const minusWords = ['безкоштовно','free','своїми руками','diy','скачати','доставка безкоштовна'];
    const data = terms.map(t => ({
      term: t.search_term_view.search_term,
      impressions: t.metrics.impressions, clicks: t.metrics.clicks,
      conversions: t.metrics.conversions,
      cost: (t.metrics.cost_micros / 1_000_000).toFixed(2),
      isSuggestedMinus: minusWords.some(mw => t.search_term_view.search_term.toLowerCase().includes(mw)),
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ═══════════════════════════════
// MERCATO — GA4
// ═══════════════════════════════

app.get('/api/mercato/ga4/metrics', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID_MERCATO,
      [{ name: 'sessions' }, { name: 'activeUsers' }, { name: 'bounceRate' }, { name: 'averageSessionDuration' }, { name: 'conversions' }, { name: 'screenPageViewsPerSession' }],
      [{ name: 'sessionDefaultChannelGrouping' }]);
    const data = rows.map(row => ({
      channel: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value, users: row.metricValues[1].value,
      bounceRate: (parseFloat(row.metricValues[2].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[3].value)) + 'с',
      conversions: row.metricValues[4].value,
      pagesPerSession: parseFloat(row.metricValues[5].value).toFixed(1),
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/mercato/ga4/pages', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID_MERCATO,
      [{ name: 'screenPageViews' }, { name: 'bounceRate' }, { name: 'averageSessionDuration' }, { name: 'conversions' }],
      [{ name: 'pagePath' }],
      [{ metric: { metricName: 'screenPageViews' }, desc: true }], 10);
    const data = rows.map(row => ({
      page: row.dimensionValues[0].value, views: row.metricValues[0].value,
      bounceRate: (parseFloat(row.metricValues[1].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[2].value)) + 'с',
      conversions: row.metricValues[3].value,
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/mercato/ga4/devices', async (req, res) => {
  try {
    const rows = await ga4Report(CONFIG.GA4_PROPERTY_ID_MERCATO,
      [{ name: 'sessions' }, { name: 'conversions' }, { name: 'totalRevenue' }],
      [{ name: 'deviceCategory' }]);
    const data = rows.map(row => ({
      device: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value, conversions: row.metricValues[1].value,
      revenue: parseFloat(row.metricValues[2].value).toFixed(2),
    }));
    res.json({ success: true, shop: 'mercato', data });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ═══════════════════════════════
// СПІЛЬНІ РОУТИ
// ═══════════════════════════════

app.get('/api/cross-analysis', async (req, res) => {
  try {
    const [adsRes, ga4Res, devicesRes] = await Promise.allSettled([
      fetch(`http://localhost:${PORT}/api/ads/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/ga4/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/ga4/devices`).then(r => r.json()),
    ]);
    res.json({ success: true, shop: 'prom3d', ads: adsRes.value?.data || [], ga4: ga4Res.value?.data || [], devices: devicesRes.value?.data || [], timestamp: new Date().toISOString() });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/mercato/cross-analysis', async (req, res) => {
  try {
    const [adsRes, ga4Res, devicesRes] = await Promise.allSettled([
      fetch(`http://localhost:${PORT}/api/mercato/ads/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/mercato/ga4/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/mercato/ga4/devices`).then(r => r.json()),
    ]);
    res.json({ success: true, shop: 'mercato', ads: adsRes.value?.data || [], ga4: ga4Res.value?.data || [], devices: devicesRes.value?.data || [], timestamp: new Date().toISOString() });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.get('/api/auto-audit', async (req, res) => {
  const alerts = [];
  try {
    // Prom3D audit
    const adsData = await fetch(`http://localhost:${PORT}/api/ads/metrics`).then(r => r.json());
    const totalCost = adsData.data?.reduce((sum, c) => sum + parseFloat(c.cost), 0) || 0;
    if (totalCost / 7 > 690 * 1.15) alerts.push({ type: 'danger', shop: 'prom3d', source: 'Ads', message: `Prom3D перевитрата! Факт ${(totalCost/7).toFixed(0)}грн/день` });
    const ga4Data = await fetch(`http://localhost:${PORT}/api/ga4/pages`).then(r => r.json());
    ga4Data.data?.filter(p => parseFloat(p.bounceRate) > 70).forEach(p => alerts.push({ type: 'warning', shop: 'prom3d', source: 'GA4', message: `Prom3D відмова ${p.bounceRate} на ${p.page}` }));
    adsData.data?.filter(c => parseFloat(c.ctr) < 1.5).forEach(c => alerts.push({ type: 'warning', shop: 'prom3d', source: 'Ads', message: `Prom3D низький CTR ${c.ctr}% у "${c.name}"`, campaignId: c.id }));
    // Mercato audit
    const mAdsData = await fetch(`http://localhost:${PORT}/api/mercato/ads/metrics`).then(r => r.json());
    const mCost = mAdsData.data?.reduce((sum, c) => sum + parseFloat(c.cost), 0) || 0;
    if (mCost / 7 > 500 * 1.15) alerts.push({ type: 'danger', shop: 'mercato', source: 'Ads', message: `Mercato перевитрата! Факт ${(mCost/7).toFixed(0)}грн/день` });
    const mGa4Data = await fetch(`http://localhost:${PORT}/api/mercato/ga4/pages`).then(r => r.json());
    mGa4Data.data?.filter(p => parseFloat(p.bounceRate) > 70).forEach(p => alerts.push({ type: 'warning', shop: 'mercato', source: 'GA4', message: `Mercato відмова ${p.bounceRate} на ${p.page}` }));
    mAdsData.data?.filter(c => parseFloat(c.ctr) < 1.5).forEach(c => alerts.push({ type: 'warning', shop: 'mercato', source: 'Ads', message: `Mercato низький CTR ${c.ctr}% у "${c.name}"`, campaignId: c.id }));
    res.json({ success: true, alerts, timestamp: new Date().toISOString() });
  } catch (err) { res.status(500).json({ success: false, error: err.message, alerts }); }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`
  ╔══════════════════════════════════════════╗
  ║   Prom3D + Mercato Ads Agent Server      ║
  ║   Running on port ${PORT}                   ║
  ║   http://localhost:${PORT}                  ║
  ╚══════════════════════════════════════════╝
  `);
});

module.exports = app;
