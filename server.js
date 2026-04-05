// ============================================
// Prom3D AI Ads Agent — Backend Server
// Node.js + Express + Google Ads API + GA4 MCP
// ============================================

const express = require('express');
const cors = require('cors');
const { GoogleAdsApi } = require('google-ads-api');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const app = express();
app.use(cors());
app.use(express.json());

// ============ КОНФІГ (заповни своїми ключами) ============
const CONFIG = {
  // Google Ads
  ADS_DEVELOPER_TOKEN: 'uMvPr7R4QQvCjYfGV25gcQ',
  ADS_CLIENT_ID: '546482431037-pnc30oc3o04npp3k4oo1ifpf3is788c4.apps.googleusercontent.com',
  ADS_CLIENT_SECRET: process.env.ADS_CLIENT_SECRET || 'YOUR_CLIENT_SECRET',
  ADS_REFRESH_TOKEN: process.env.ADS_REFRESH_TOKEN || '',
  ADS_CUSTOMER_ID: '5522488607', // без тире

  // Google Analytics 4
  GA4_PROPERTY_ID: '503012124',
  GA4_KEY_FILE: process.env.GA4_KEY_FILE || './service-account.json',
};
// =========================================================

// ── Google Ads клієнт ──
const adsClient = new GoogleAdsApi({
  client_id: CONFIG.ADS_CLIENT_ID,
  client_secret: CONFIG.ADS_CLIENT_SECRET,
  developer_token: CONFIG.ADS_DEVELOPER_TOKEN,
});

// ── GA4 клієнт ──
let ga4Credentials = {};
try { ga4Credentials = JSON.parse(Buffer.from(process.env.GA4_KEY_BASE64 || '', 'base64').toString() || '{}'); } catch(e) {}
const ga4Client = new BetaAnalyticsDataClient({
  credentials: ga4Credentials,
});

// ═══════════════════════════════════════════
// РОУТИ
// ═══════════════════════════════════════════

// Перевірка що сервер живий
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Prom3D Ads Agent Server running 🚀' });
});

// ── [1] Метрики Google Ads ──
app.get('/api/ads/metrics', async (req, res) => {
  try {
    const customer = adsClient.Customer({
      customer_id: CONFIG.ADS_CUSTOMER_ID,
      refresh_token: CONFIG.ADS_REFRESH_TOKEN,
    });

    const campaigns = await customer.query(`
      SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.conversions,
        metrics.cost_micros,
        metrics.average_cpc
      FROM campaign
      WHERE segments.date DURING LAST_7_DAYS
        AND campaign.status = 'ENABLED'
      ORDER BY metrics.impressions DESC
      LIMIT 10
    `);

    const data = campaigns.map(c => ({
      id: c.campaign.id,
      name: c.campaign.name,
      impressions: c.metrics.impressions,
      clicks: c.metrics.clicks,
      ctr: (c.metrics.ctr * 100).toFixed(2),
      conversions: c.metrics.conversions,
      cost: (c.metrics.cost_micros / 1_000_000).toFixed(2),
      avgCpc: (c.metrics.average_cpc / 1_000_000).toFixed(2),
    }));

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [2] Ключові слова Google Ads ──
app.get('/api/ads/keywords', async (req, res) => {
  try {
    const customer = adsClient.Customer({
      customer_id: CONFIG.ADS_CUSTOMER_ID,
      refresh_token: CONFIG.ADS_REFRESH_TOKEN,
    });

    const keywords = await customer.query(`
      SELECT
        ad_group_criterion.keyword.text,
        ad_group_criterion.keyword.match_type,
        ad_group_criterion.status,
        metrics.impressions,
        metrics.clicks,
        metrics.ctr,
        metrics.conversions,
        metrics.cost_micros,
        ad_group_criterion.quality_info.quality_score
      FROM keyword_view
      WHERE segments.date DURING LAST_7_DAYS
      ORDER BY metrics.impressions DESC
      LIMIT 20
    `);

    const data = keywords.map(k => ({
      keyword: k.ad_group_criterion.keyword.text,
      matchType: k.ad_group_criterion.keyword.match_type,
      status: k.ad_group_criterion.status,
      impressions: k.metrics.impressions,
      clicks: k.metrics.clicks,
      ctr: (k.metrics.ctr * 100).toFixed(2),
      conversions: k.metrics.conversions,
      cost: (k.metrics.cost_micros / 1_000_000).toFixed(2),
      qualityScore: k.ad_group_criterion.quality_info?.quality_score || 0,
    }));

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [3] Пошукові запити (для мінус-слів) ──
app.get('/api/ads/search-terms', async (req, res) => {
  try {
    const customer = adsClient.Customer({
      customer_id: CONFIG.ADS_CUSTOMER_ID,
      refresh_token: CONFIG.ADS_REFRESH_TOKEN,
    });

    const terms = await customer.query(`
      SELECT
        search_term_view.search_term,
        search_term_view.status,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions,
        metrics.cost_micros
      FROM search_term_view
      WHERE segments.date DURING LAST_7_DAYS
        AND metrics.impressions > 5
      ORDER BY metrics.impressions DESC
      LIMIT 50
    `);

    // Автоматично визначаємо потенційні мінус-слова
    const minusWords = [
      'безкоштовно', 'free', 'своїми руками', 'diy', 'скачати',
      'thingiverse', 'stl', 'курс', 'навчання', 'принтер купити',
      '3д кіно', 'окуляри', 'фільм'
    ];

    const data = terms.map(t => ({
      term: t.search_term_view.search_term,
      impressions: t.metrics.impressions,
      clicks: t.metrics.clicks,
      conversions: t.metrics.conversions,
      cost: (t.metrics.cost_micros / 1_000_000).toFixed(2),
      isSuggestedMinus: minusWords.some(mw =>
        t.search_term_view.search_term.toLowerCase().includes(mw)
      ),
    }));

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [4] Призупинити оголошення ──
app.post('/api/ads/pause-campaign', async (req, res) => {
  try {
    const { campaignId } = req.body;
    const customer = adsClient.Customer({
      customer_id: CONFIG.ADS_CUSTOMER_ID,
      refresh_token: CONFIG.ADS_REFRESH_TOKEN,
    });

    await customer.campaigns.update([{
      resource_name: `customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${campaignId}`,
      status: 'PAUSED',
    }]);

    res.json({ success: true, message: `Кампанія ${campaignId} призупинена` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [5] Додати мінус-слова ──
app.post('/api/ads/add-negative-keywords', async (req, res) => {
  try {
    const { keywords, campaignId } = req.body;
    const customer = adsClient.Customer({
      customer_id: CONFIG.ADS_CUSTOMER_ID,
      refresh_token: CONFIG.ADS_REFRESH_TOKEN,
    });

    const negativeKeywords = keywords.map(kw => ({
      campaign: `customers/${CONFIG.ADS_CUSTOMER_ID}/campaigns/${campaignId}`,
      keyword: { text: kw, match_type: 'BROAD' },
    }));

    await customer.campaignCriteria.create(negativeKeywords);
    res.json({ success: true, message: `Додано ${keywords.length} мінус-слів` });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [6] GA4 — Метрики сайту ──
app.get('/api/ga4/metrics', async (req, res) => {
  try {
    const [response] = await ga4Client.runReport({
      property: `properties/${CONFIG.GA4_PROPERTY_ID}`,
      dateRanges: [{ startDate: '7daysAgo', endDate: 'today' }],
      metrics: [
        { name: 'sessions' },
        { name: 'activeUsers' },
        { name: 'bounceRate' },
        { name: 'averageSessionDuration' },
        { name: 'conversions' },
        { name: 'screenPageViewsPerSession' },
      ],
      dimensions: [{ name: 'sessionDefaultChannelGrouping' }],
    });

    const data = response.rows?.map(row => ({
      channel: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value,
      users: row.metricValues[1].value,
      bounceRate: (parseFloat(row.metricValues[2].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[3].value)) + 'с',
      conversions: row.metricValues[4].value,
      pagesPerSession: parseFloat(row.metricValues[5].value).toFixed(1),
    })) || [];

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [7] GA4 — Топ сторінки ──
app.get('/api/ga4/pages', async (req, res) => {
  try {
    const [response] = await ga4Client.runReport({
      property: `properties/${CONFIG.GA4_PROPERTY_ID}`,
      dateRanges: [{ startDate: '7daysAgo', endDate: 'today' }],
      metrics: [
        { name: 'screenPageViews' },
        { name: 'bounceRate' },
        { name: 'averageSessionDuration' },
        { name: 'conversions' },
      ],
      dimensions: [{ name: 'pagePath' }],
      orderBys: [{ metric: { metricName: 'screenPageViews' }, desc: true }],
      limit: 10,
    });

    const data = response.rows?.map(row => ({
      page: row.dimensionValues[0].value,
      views: row.metricValues[0].value,
      bounceRate: (parseFloat(row.metricValues[1].value) * 100).toFixed(1) + '%',
      avgDuration: Math.round(parseFloat(row.metricValues[2].value)) + 'с',
      conversions: row.metricValues[3].value,
    })) || [];

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [8] GA4 — Пристрої ──
app.get('/api/ga4/devices', async (req, res) => {
  try {
    const [response] = await ga4Client.runReport({
      property: `properties/${CONFIG.GA4_PROPERTY_ID}`,
      dateRanges: [{ startDate: '7daysAgo', endDate: 'today' }],
      metrics: [
        { name: 'sessions' },
        { name: 'conversions' },
        { name: 'totalRevenue' },
      ],
      dimensions: [{ name: 'deviceCategory' }],
    });

    const data = response.rows?.map(row => ({
      device: row.dimensionValues[0].value,
      sessions: row.metricValues[0].value,
      conversions: row.metricValues[1].value,
      revenue: parseFloat(row.metricValues[2].value).toFixed(2),
    })) || [];

    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [9] Перехресний аналіз Ads + GA4 ──
app.get('/api/cross-analysis', async (req, res) => {
  try {
    // Отримуємо дані паралельно
    const [adsRes, ga4Res, devicesRes] = await Promise.allSettled([
      fetch(`http://localhost:${PORT}/api/ads/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/ga4/metrics`).then(r => r.json()),
      fetch(`http://localhost:${PORT}/api/ga4/devices`).then(r => r.json()),
    ]);

    res.json({
      success: true,
      ads: adsRes.status === 'fulfilled' ? adsRes.value.data : [],
      ga4: ga4Res.status === 'fulfilled' ? ga4Res.value.data : [],
      devices: devicesRes.status === 'fulfilled' ? devicesRes.value.data : [],
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── [10] Автоматичний аудит (запускається щодня) ──
app.get('/api/auto-audit', async (req, res) => {
  const alerts = [];

  try {
    // Перевірка витрат
    const adsData = await fetch(`http://localhost:${PORT}/api/ads/metrics`).then(r => r.json());
    const totalCost = adsData.data?.reduce((sum, c) => sum + parseFloat(c.cost), 0) || 0;
    const dailyBudget = 690;

    if (totalCost / 7 > dailyBudget * 1.15) {
      alerts.push({
        type: 'danger',
        source: 'Ads',
        message: `Перевитрата бюджету! Денний факт ${(totalCost/7).toFixed(0)}₴ vs план ${dailyBudget}₴`,
        action: 'reduce_budget',
      });
    }

    // Перевірка bounce rate
    const ga4Data = await fetch(`http://localhost:${PORT}/api/ga4/pages`).then(r => r.json());
    const highBouncePages = ga4Data.data?.filter(p => parseFloat(p.bounceRate) > 70) || [];

    highBouncePages.forEach(page => {
      alerts.push({
        type: 'warning',
        source: 'GA4',
        message: `Висока відмова ${page.bounceRate} на сторінці ${page.page}`,
        action: 'check_page',
        page: page.page,
      });
    });

    // Перевірка слабких кампаній
    const weakCampaigns = adsData.data?.filter(c => parseFloat(c.ctr) < 1.5) || [];
    weakCampaigns.forEach(c => {
      alerts.push({
        type: 'warning',
        source: 'Ads',
        message: `Низький CTR ${c.ctr}% у кампанії "${c.name}"`,
        action: 'pause_or_update',
        campaignId: c.id,
      });
    });

    res.json({ success: true, alerts, timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message, alerts });
  }
});

// ── Старт сервера ──
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`
  ╔══════════════════════════════════════╗
  ║   Prom3D AI Ads Agent Server         ║
  ║   Running on port ${PORT}               ║
  ║   http://localhost:${PORT}              ║
  ╚══════════════════════════════════════╝
  `);
});

module.exports = app;
