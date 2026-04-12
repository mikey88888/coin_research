function normalizeTime(value) {
  if (typeof value === "number") {
    return value;
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return value;
  }
  return Math.floor(parsed / 1000);
}

function buildCandlestickChart(element) {
  const candles = JSON.parse(element.dataset.candles || "[]").map((item) => ({
    ...item,
    time: normalizeTime(item.time),
  }));
  const overlay = JSON.parse(element.dataset.overlay || "{\"waveLine\":[],\"markers\":[]}");
  if (!candles.length || !window.LightweightCharts) {
    return;
  }
  const chart = LightweightCharts.createChart(element, {
    width: element.clientWidth,
    layout: {
      background: { color: "#fffdf8" },
      textColor: "#1d1a16",
    },
    grid: {
      vertLines: { color: "rgba(29, 26, 22, 0.08)" },
      horzLines: { color: "rgba(29, 26, 22, 0.08)" },
    },
    rightPriceScale: { borderColor: "rgba(29, 26, 22, 0.18)" },
    timeScale: { borderColor: "rgba(29, 26, 22, 0.18)" },
  });
  const series = chart.addCandlestickSeries({
    upColor: "#0f766e",
    downColor: "#b45309",
    wickUpColor: "#0f766e",
    wickDownColor: "#b45309",
    borderVisible: false,
  });
  series.setData(candles);
  if (overlay.waveLine && overlay.waveLine.length) {
    const waveSeries = chart.addLineSeries({
      color: "#2563eb",
      lineWidth: 2,
      crosshairMarkerVisible: false,
      lastValueVisible: false,
      priceLineVisible: false,
    });
    waveSeries.setData(
      overlay.waveLine.map((item) => ({
        time: normalizeTime(item.time),
        value: item.value,
      })),
    );
  }
  if (overlay.markers && overlay.markers.length) {
    series.setMarkers(
      overlay.markers.map((item) => ({
        ...item,
        time: normalizeTime(item.time),
      })),
    );
  }
  chart.timeScale().fitContent();
  window.addEventListener("resize", () => {
    chart.applyOptions({ width: element.clientWidth });
  });
}

function buildLineChart(element) {
  const points = JSON.parse(element.dataset.line || "[]").map((item) => ({
    ...item,
    time: normalizeTime(item.time),
  }));
  if (!points.length || !window.LightweightCharts) {
    return;
  }
  const chart = LightweightCharts.createChart(element, {
    width: element.clientWidth,
    layout: {
      background: { color: "#fffdf8" },
      textColor: "#1d1a16",
    },
    grid: {
      vertLines: { color: "rgba(29, 26, 22, 0.08)" },
      horzLines: { color: "rgba(29, 26, 22, 0.08)" },
    },
    rightPriceScale: { borderColor: "rgba(29, 26, 22, 0.18)" },
    timeScale: { borderColor: "rgba(29, 26, 22, 0.18)" },
  });
  const series = chart.addLineSeries({
    color: "#0f766e",
    lineWidth: 2,
  });
  series.setData(points);
  chart.timeScale().fitContent();
  window.addEventListener("resize", () => {
    chart.applyOptions({ width: element.clientWidth });
  });
}

window.addEventListener("DOMContentLoaded", () => {
  const priceChart = document.getElementById("price-chart");
  if (priceChart) {
    buildCandlestickChart(priceChart);
  }
  const equityChart = document.getElementById("equity-chart");
  if (equityChart) {
    buildLineChart(equityChart);
  }
});
